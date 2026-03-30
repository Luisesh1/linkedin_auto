"""
Content generation module using Grok API (xAI).
Selects a non-repetitive topic and writes a LinkedIn post.
"""

from __future__ import annotations

import json
import re

from src.llm import get_text_model, get_xai_client
from src.logging_utils import get_logger

logger = get_logger(__name__)


def _category_text(category_cfg: dict | None, key: str, fallback: str) -> str:
    if not category_cfg:
        return fallback
    value = (category_cfg.get(key) or "").strip()
    return value or fallback


def _generate_post_from_topics(topics: list, history: list, category_cfg: dict | None = None) -> dict:
    """
    Given trending topics and recent post history, select the best topic
    and generate a full LinkedIn post.

    Returns:
        dict with keys: topic, reasoning, post_text
    """
    topics_str = "\n".join(f"- {t}" for t in topics)
    recent_topics = [h.get("topic", h.get("post_text", "")[:80]) for h in history]
    history_str = (
        "\n".join(f"- {t}" for t in recent_topics) if recent_topics else "Ninguno"
    )
    category_name = category_cfg.get("name", "default") if category_cfg else "default"
    post_length = int(category_cfg.get("post_length", 200) or 200) if category_cfg else 200
    language_setting = (category_cfg.get("language") or "auto").strip() if category_cfg else "auto"
    hashtag_count = int(category_cfg.get("hashtag_count", 4) or 4) if category_cfg else 4
    use_emojis = bool(category_cfg.get("use_emojis", 0)) if category_cfg else False
    lang_instruction = {
        "es": "Escribe SOLO en español.",
        "en": "Write ONLY in English.",
    }.get(language_setting, "Elige el idioma (español o inglés) más natural para el tema.")
    emoji_instruction = "Puedes usar emojis con moderación para dar énfasis y dinamismo." if use_emojis else "No uses emojis en ninguna parte del texto."
    history_instruction = _category_text(
        category_cfg,
        "history_prompt",
        "Evita repetir temas o enfoques demasiado similares y mantén coherencia profesional.",
    )
    content_instruction = _category_text(
        category_cfg,
        "content_prompt",
        "Escribe una publicación profesional, útil y conversacional para LinkedIn.",
    )
    hashtag_line = f"- Incluir exactamente {hashtag_count} hashtags relevantes al final" if hashtag_count > 0 else "- No incluyas hashtags"
    negative_prompt = (category_cfg.get("negative_prompt") or "").strip() if category_cfg else ""

    prompt = f"""Eres un estratega de contenido para LinkedIn especializado en tecnología y mundo laboral.

## Categoría activa:
{category_name}

## Temas de tendencia disponibles:
{topics_str}

## Temas usados en publicaciones recientes (EVITAR estos o muy similares):
{history_str}

## Instrucción sobre cómo usar el historial:
{history_instruction}

## Estilo/objetivo específico para esta categoría:
{content_instruction}

## Tarea:
1. Selecciona el ÚNICO mejor tema de la lista de tendencias que NO sea repetitivo con las publicaciones recientes
2. Escribe una publicación de LinkedIn atractiva y profesional sobre ese tema

## Requisitos de la publicación de LinkedIn:
- Extensión: {post_length}-{post_length + 60} palabras
- {lang_instruction}
- {emoji_instruction}
- Tono: Profesional pero conversacional, perspicaz y auténtico
- Estructura:
  * Primera línea impactante (hook que se muestre como preview antes del "ver más")
  * 2-3 párrafos de insights con valor real
  * Cierre con pregunta o llamada a la acción
{hashtag_line}
- Sin frases motivacionales genéricas o clichés

## Evitar estrictamente (antiprompt):
{"- " + negative_prompt if negative_prompt else "- (Sin restricciones adicionales)"}

## Formato de salida (SOLO JSON, sin markdown, sin texto adicional):
{{
  "topic": "tema seleccionado",
  "reasoning": "una frase explicando por qué elegiste este tema",
  "post_text": "texto completo de la publicación de LinkedIn incluyendo hashtags"
}}"""

    response = get_xai_client().chat.completions.create(
        model=get_text_model(),
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1200,
    )

    raw = response.choices[0].message.content.strip()

    # Strip markdown code fences if present
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    raw = raw.strip()

    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning(
            "Respuesta inválida al generar contenido",
            extra={"event": "content.invalid_json"},
            exc_info=exc,
        )
        raise RuntimeError("El modelo devolvió una respuesta inválida al generar el contenido.") from exc


def _brief_history(history: list[dict]) -> str:
    if not history:
        return "Ninguno"
    lines = []
    for item in history[:8]:
        topic = item.get("topic", "")
        pillar = item.get("pillar", "")
        fmt = item.get("content_format", "")
        cta = item.get("cta_type", "")
        parts = [part for part in (topic, pillar, fmt, cta) if part]
        if parts:
            lines.append("- " + " | ".join(parts))
    return "\n".join(lines) if lines else "Ninguno"


def _generate_post_from_brief(
    content_brief: dict,
    history: list,
    category_cfg: dict | None = None,
    feedback: str = "",
) -> dict:
    topic = (content_brief.get("topic") or "").strip()
    if not topic:
        raise RuntimeError("El content brief no incluye topic.")

    category_name = category_cfg.get("name", "default") if category_cfg else "default"
    post_length = int(category_cfg.get("post_length", 200) or 200) if category_cfg else 200
    language_setting = (content_brief.get("language") or category_cfg.get("language") or "auto").strip() if category_cfg else str(content_brief.get("language", "auto")).strip()
    hashtag_count = int(category_cfg.get("hashtag_count", 4) or 4) if category_cfg else 4
    use_emojis = bool(category_cfg.get("use_emojis", 0)) if category_cfg else False
    lang_instruction = {
        "es": "Escribe SOLO en español.",
        "en": "Write ONLY in English.",
    }.get(language_setting, "Elige el idioma (español o inglés) más natural para el tema.")
    emoji_instruction = "Puedes usar emojis con moderación para dar énfasis y dinamismo." if use_emojis else "No uses emojis en ninguna parte del texto."
    content_instruction = _category_text(
        category_cfg,
        "content_prompt",
        "Escribe una publicación profesional, útil y conversacional para LinkedIn.",
    )
    negative_prompt = (category_cfg.get("negative_prompt") or "").strip() if category_cfg else ""
    hashtag_line = f"Incluye exactamente {hashtag_count} hashtags al final." if hashtag_count > 0 else "No incluyas hashtags."
    originality_level = int(content_brief.get("originality_level", category_cfg.get("originality_level", 3) if category_cfg else 3) or 3)
    evidence_mode = str(content_brief.get("evidence_mode", category_cfg.get("evidence_mode", "balanced") if category_cfg else "balanced") or "balanced")
    evidence_instruction = {
        "balanced": "Combina criterio propio con ejemplos concretos o micro-evidencia útil.",
        "examples": "Aterriza el argumento con ejemplos reales o plausibles y evita abstracciones vacías.",
        "data": "Incluye señales, datos o hechos concretos cuando aporten claridad. No inventes cifras.",
        "story": "Apóyate en una mini-historia o escena concreta para darle originalidad al post.",
    }.get(evidence_mode, "Combina criterio propio con ejemplos concretos o micro-evidencia útil.")
    originality_instruction = {
        1: "Mantén un enfoque seguro y muy claro.",
        2: "Busca un ángulo ligeramente menos obvio que el promedio.",
        3: "Busca un ángulo distintivo y evita lugares comunes.",
        4: "Busca un ángulo poco explotado y formula una observación memorable.",
        5: "Prioriza máxima originalidad: enfoque contraintuitivo, específico y nada genérico.",
    }.get(originality_level, "Busca un ángulo distintivo y evita lugares comunes.")

    prompt = f"""Eres un estratega editorial para LinkedIn.

## Categoría
{category_name}

## Brief estructurado
- Topic: {topic}
- Pillar: {content_brief.get("pillar", "productivity")}
- Angle: {content_brief.get("angle", "")}
- Content format: {content_brief.get("content_format", "insight")}
- Audience: {content_brief.get("audience", "tech leaders")}
- Hook goal: {content_brief.get("hook_goal", "clarity")}
- CTA type: {content_brief.get("cta_type", "question")}
- Originality level: {originality_level}/5
- Evidence mode: {evidence_mode}

## Historial reciente
{_brief_history(history)}

## Instrucciones de categoría
{content_instruction}

## Requisitos
- Extensión: {post_length}-{post_length + 60} palabras
- {lang_instruction}
- {emoji_instruction}
- {hashtag_line}
- Debe sentirse fresco frente al historial
- Evita clichés, frases vacías y generalidades
- Mantén coherencia total con el brief
- {evidence_instruction}
- {originality_instruction}

## Antiprompt
{"- " + negative_prompt if negative_prompt else "- Sin restricciones adicionales"}

## Feedback automático del sistema
{feedback or "Sin feedback previo."}

## Salida
Devuelve SOLO JSON con esta forma:
{{
  "topic": "tema final",
  "reasoning": "por qué esta versión del post es adecuada",
  "post_text": "texto completo del post",
  "hook_type": "tipo de hook usado",
  "cta_type": "tipo de CTA usado",
  "angle_signature": "firma breve del ángulo"
}}"""

    response = get_xai_client().chat.completions.create(
        model=get_text_model(),
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1400,
    )

    raw = response.choices[0].message.content.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    raw = raw.strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.warning(
            "Respuesta inválida al generar contenido desde brief",
            extra={"event": "content.invalid_json_brief"},
            exc_info=exc,
        )
        raise RuntimeError("El modelo devolvió una respuesta inválida al generar el contenido.") from exc

    data.setdefault("topic", topic)
    data.setdefault("hook_type", content_brief.get("hook_goal", "clarity"))
    data.setdefault("cta_type", content_brief.get("cta_type", "question"))
    data.setdefault("angle_signature", content_brief.get("angle", "")[:120])
    return data


def generate_post(
    content_input,
    history: list,
    category_cfg: dict | None = None,
    feedback: str = "",
) -> dict:
    if isinstance(content_input, dict):
        return _generate_post_from_brief(content_input, history, category_cfg=category_cfg, feedback=feedback)
    return _generate_post_from_topics(content_input, history, category_cfg=category_cfg)
