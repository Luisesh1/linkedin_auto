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


def _category_int(category_cfg: dict | None, key: str, fallback: int) -> int:
    if not category_cfg:
        return fallback
    value = category_cfg.get(key)
    if value is None or value == "":
        return fallback
    return int(value)


def _category_list(category_cfg: dict | None, key: str) -> list[str]:
    if not category_cfg:
        return []
    raw = category_cfg.get(key)
    if isinstance(raw, list):
        return [str(item).strip() for item in raw if str(item).strip()]
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
    except Exception:
        pass
    return []


_EVIDENCE_INSTRUCTIONS = {
    "balanced": "Combina criterio propio con ejemplos concretos o micro-evidencia útil.",
    "examples": "Aterriza el argumento con ejemplos reales o plausibles y evita abstracciones vacías.",
    "data": "Incluye señales, datos o hechos concretos cuando aporten claridad. No inventes cifras.",
    "story": "Apóyate en una mini-historia o escena concreta para darle originalidad al post.",
}

_ORIGINALITY_INSTRUCTIONS = {
    1: "Mantén un enfoque seguro y muy claro.",
    2: "Busca un ángulo ligeramente menos obvio que el promedio.",
    3: "Busca un ángulo distintivo y evita lugares comunes.",
    4: "Busca un ángulo poco explotado y formula una observación memorable.",
    5: "Prioriza máxima originalidad: enfoque contraintuitivo, específico y nada genérico.",
}


def _format_negative_block(negative_prompt: str) -> str:
    text = (negative_prompt or "").strip()
    if not text:
        return "- Sin restricciones adicionales"
    parts = [chunk.strip(" .") for chunk in re.split(r"[.\n;]+", text) if chunk.strip()]
    if not parts:
        return f"- {text}"
    return "\n".join(f"- {item}." for item in parts)


def _format_forbidden_block(phrases: list[str]) -> str:
    if not phrases:
        return "Ninguna restricción específica."
    return "\n".join(f"- \"{phrase}\"" for phrase in phrases)


def _format_voice_block(examples: list[str]) -> str:
    if not examples:
        return "Sin ejemplos de voz cargados — sigue las instrucciones de categoría como guía."
    blocks = []
    for idx, example in enumerate(examples[:3], start=1):
        blocks.append(f"Ejemplo {idx}:\n\"\"\"\n{example}\n\"\"\"")
    return "\n\n".join(blocks)


def _brief_history(history: list[dict]) -> str:
    if not history:
        return "Ninguno"
    lines = []
    for item in history[:3]:
        topic = item.get("topic", "")
        if topic:
            lines.append(f"- {topic}")
    return "\n".join(lines) if lines else "Ninguno"


def _brief_from_topics(topics: list, history: list, category_cfg: dict | None) -> dict:
    """Build a minimal content brief from a raw topic list (legacy path adapter)."""
    history_topics = [item.get("topic", "") for item in history if item.get("topic")]
    selected_topic = next(
        (topic for topic in topics if topic and topic not in history_topics),
        topics[0] if topics else "",
    )
    category_name = (category_cfg or {}).get("name", "default") if category_cfg else "default"
    return {
        "topic": str(selected_topic),
        "pillar": "general",
        "angle": "",
        "content_format": "insight",
        "audience": (category_cfg or {}).get("audience_focus", "tech leaders"),
        "hook_goal": (category_cfg or {}).get("hook_style", "auto"),
        "cta_type": (category_cfg or {}).get("cta_style", "auto"),
        "category": category_name,
        "originality_level": (category_cfg or {}).get("originality_level", 3),
        "evidence_mode": (category_cfg or {}).get("evidence_mode", "balanced"),
        "language": (category_cfg or {}).get("language", "auto"),
    }


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
    hashtag_count = _category_int(category_cfg, "hashtag_count", 4)
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
    history_instruction = _category_text(
        category_cfg,
        "history_prompt",
        "Evita repetir temas o enfoques demasiado similares y mantén coherencia profesional.",
    )
    negative_prompt = (category_cfg.get("negative_prompt") or "").strip() if category_cfg else ""
    forbidden_phrases = _category_list(category_cfg, "forbidden_phrases")
    voice_examples = _category_list(category_cfg, "voice_examples")
    hashtag_line = f"Incluye exactamente {hashtag_count} hashtags al final." if hashtag_count > 0 else "No incluyas hashtags."
    originality_level = int(content_brief.get("originality_level", category_cfg.get("originality_level", 3) if category_cfg else 3) or 3)
    evidence_mode = str(content_brief.get("evidence_mode", category_cfg.get("evidence_mode", "balanced") if category_cfg else "balanced") or "balanced")
    evidence_instruction = _EVIDENCE_INSTRUCTIONS.get(evidence_mode, _EVIDENCE_INSTRUCTIONS["balanced"])
    originality_instruction = _ORIGINALITY_INSTRUCTIONS.get(originality_level, _ORIGINALITY_INSTRUCTIONS[3])

    from src.pipeline import _extract_keywords

    keywords = _extract_keywords(topic, limit=4)
    keywords_line = ", ".join(keywords) if keywords else topic

    sections = [
        "Eres un estratega editorial para LinkedIn.",
        f"Categoría: {category_name}",
        (
            "Brief:\n"
            f"- Topic: {topic}\n"
            f"- Angle: {content_brief.get('angle', '')}\n"
            f"- Format: {content_brief.get('content_format', 'insight')} | Hook: {content_brief.get('hook_goal', 'clarity')} | CTA: {content_brief.get('cta_type', 'question')}\n"
            f"- Audience: {content_brief.get('audience', 'tech leaders')} | Originalidad: {originality_level}/5 | Evidencia: {evidence_mode}"
        ),
        f"Keywords obligatorias (deben aparecer literalmente al menos la mitad): {keywords_line}",
        f"Historial reciente (no repetir):\n{_brief_history(history)}",
        (
            f"Instrucciones: {content_instruction} {history_instruction} "
            f"{lang_instruction} {emoji_instruction} {hashtag_line} "
            f"Extensión {post_length}-{post_length + 60} palabras. "
            f"{evidence_instruction} {originality_instruction} "
            "Evita clichés y generalidades."
        ),
    ]
    if voice_examples:
        sections.append(f"Voz de referencia:\n{_format_voice_block(voice_examples)}")
    if negative_prompt:
        sections.append(f"Evita:\n{_format_negative_block(negative_prompt)}")
    if forbidden_phrases:
        sections.append(f"Frases prohibidas:\n{_format_forbidden_block(forbidden_phrases)}")
    if feedback:
        sections.append(f"Feedback a corregir: {feedback}")
    sections.append(
        'Devuelve SOLO JSON: {"topic":"...","reasoning":"...","post_text":"...","hook_type":"...","cta_type":"...","angle_signature":"..."}'
    )
    prompt = "\n\n".join(sections)

    response = get_xai_client().chat.completions.create(
        model=get_text_model(),
        messages=[{"role": "user", "content": prompt}],
        max_tokens=700,
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
    brief = _brief_from_topics(list(content_input or []), history, category_cfg)
    return _generate_post_from_brief(brief, history, category_cfg=category_cfg, feedback=feedback)
