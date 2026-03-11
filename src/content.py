"""
Content generation module using Grok API (xAI).
Selects a non-repetitive topic and writes a LinkedIn post.
"""

import json
import re

from openai import OpenAI


def _load_api_key() -> str:
    import yaml
    with open("config.yaml") as f:
        cfg = yaml.safe_load(f)
    return cfg["grok"]["api_key"]


def _client() -> OpenAI:
    return OpenAI(api_key=_load_api_key(), base_url="https://api.x.ai/v1")


def _category_text(category_cfg: dict | None, key: str, fallback: str) -> str:
    if not category_cfg:
        return fallback
    value = (category_cfg.get(key) or "").strip()
    return value or fallback


def generate_post(topics: list, history: list, category_cfg: dict | None = None) -> dict:
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
- Extensión: 150-250 palabras
- Tono: Profesional pero conversacional, perspicaz y auténtico
- Estructura:
  * Primera línea impactante (hook que se muestre como preview antes del "ver más")
  * 2-3 párrafos de insights con valor real
  * Cierre con pregunta o llamada a la acción
- Incluir 3-5 hashtags relevantes al final
- Sin frases motivacionales genéricas o clichés
- Puede estar en español o inglés (elige el más apropiado para el tema)

## Formato de salida (SOLO JSON, sin markdown, sin texto adicional):
{{
  "topic": "tema seleccionado",
  "reasoning": "una frase explicando por qué elegiste este tema",
  "post_text": "texto completo de la publicación de LinkedIn incluyendo hashtags"
}}"""

    response = _client().chat.completions.create(
        model="grok-3",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1200,
    )

    raw = response.choices[0].message.content.strip()

    # Strip markdown code fences if present
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    raw = raw.strip()

    return json.loads(raw)
