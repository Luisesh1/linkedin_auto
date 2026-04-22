"""
Shared xAI/OpenAI client helpers.
"""

from __future__ import annotations

from openai import OpenAI

from src.config import get_setting


def get_xai_client() -> OpenAI:
    api_key = get_setting("grok", "api_key", "")
    if not api_key:
        raise RuntimeError(
            "Falta configurar la API key de xAI. Usa XAI_API_KEY/GROK_API_KEY o config.yaml."
        )
    return OpenAI(api_key=api_key, base_url="https://api.x.ai/v1", timeout=60.0, max_retries=1)


def get_text_model() -> str:
    return str(get_setting("grok", "model", "grok-3"))


def get_image_model() -> str:
    return str(get_setting("grok", "image_model", "grok-imagine-image"))


def get_vision_model() -> str:
    return str(get_setting("grok", "vision_model", "") or "").strip()
