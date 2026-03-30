"""
Image generation module using xAI Grok API.
Generates professional images for LinkedIn posts.
"""

from datetime import UTC, datetime
import os
import uuid

import requests

from src.llm import get_image_model, get_text_model, get_xai_client
from src.logging_utils import get_logger

logger = get_logger(__name__)

STYLE_DIRECTIONS = {
    "editorial": {
        "prompt": "Create a polished editorial illustration with magazine-quality composition.",
        "composition_type": "editorial portrait",
        "color_direction": "deep blues and soft neutrals",
    },
    "minimal": {
        "prompt": "Create a minimal conceptual scene with clean geometry and negative space.",
        "composition_type": "minimal concept",
        "color_direction": "monochrome neutrals with one accent color",
    },
    "diagram": {
        "prompt": "Create a concept art scene inspired by systems diagrams and abstract data flows.",
        "composition_type": "abstract systems diagram",
        "color_direction": "cyan, graphite and electric accents",
    },
    "cinematic": {
        "prompt": "Create a cinematic business scene with dramatic lighting and layered depth.",
        "composition_type": "cinematic tableau",
        "color_direction": "teal, steel and warm highlights",
    },
    "illustrated": {
        "prompt": "Create an illustrated narrative scene with expressive shapes and modern editorial energy.",
        "composition_type": "narrative illustration",
        "color_direction": "warm earth tones and vivid accents",
    },
    "anime": {
        "prompt": "Create a refined anime-inspired professional illustration with strong visual storytelling.",
        "composition_type": "anime-inspired illustration",
        "color_direction": "vibrant cool tones with controlled contrast",
    },
}


def _category_text(category_cfg: dict | None, key: str, fallback: str) -> str:
    if not category_cfg:
        return fallback
    value = (category_cfg.get(key) or "").strip()
    return value or fallback


def generate_image(content_input, category_cfg: dict | None = None) -> dict:
    """
    Generate a professional image for the given topic or content brief.

    Returns:
        dict with keys: image_url (local preview URL), image_path (filesystem path)
    """
    client = get_xai_client()
    if isinstance(content_input, dict):
        topic = str(content_input.get("topic", "")).strip()
        visual_style = str(content_input.get("visual_style", "editorial")).strip() or "editorial"
        angle = str(content_input.get("angle", "")).strip()
    else:
        topic = str(content_input).strip()
        visual_style = "editorial"
        angle = ""
    style_cfg = STYLE_DIRECTIONS.get(visual_style, STYLE_DIRECTIONS["editorial"])

    image_instruction = _category_text(
        category_cfg,
        "image_prompt",
        "Genera una imagen editorial profesional, sobria y conceptual apta para LinkedIn.",
    )
    prompt = (
        f"Topic: {topic}. "
        f"Editorial angle: {angle or topic}. "
        f"Visual style: {visual_style}. "
        f"Category style objective: {image_instruction} "
        f"{style_cfg['prompt']} "
        "Clean composition, polished lighting, no text or typography, suitable for a LinkedIn post. "
        "High quality, detailed illustration."
    )

    response = client.images.generate(
        model=get_image_model(),
        prompt=prompt,
        n=1,
    )

    image_url_remote = response.data[0].url

    # Download immediately (remote URLs may expire)
    save_dir = os.path.join("static", "generated")
    os.makedirs(save_dir, exist_ok=True)
    img_filename = f"post_{uuid.uuid4().hex[:10]}.jpg"
    img_path = os.path.join(save_dir, img_filename)

    img_resp = requests.get(image_url_remote, timeout=30)
    img_resp.raise_for_status()
    with open(img_path, "wb") as f:
        f.write(img_resp.content)

    # Generate a short description of the image for the DB record
    description = _describe_image(client, prompt, topic, visual_style)

    return {
        "image_url": f"/static/generated/{img_filename}",
        "image_path": img_path,
        "prompt_used": prompt,
        "image_desc": description,
        "visual_style": visual_style,
        "composition_type": style_cfg["composition_type"],
        "color_direction": style_cfg["color_direction"],
    }


def _describe_image(client, prompt: str, topic: str, visual_style: str) -> str:
    """Ask Grok to write a concise description of the generated image."""
    try:
        resp = client.chat.completions.create(
            model=get_text_model(),
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"Write a concise 1-2 sentence description (in Spanish) of a {visual_style}-style "
                        f"illustration generated with this prompt:\n\n\"{prompt}\"\n\n"
                        f"The image represents the concept: {topic}. "
                        "Describe the visual composition, dominant colors, and mood."
                    ),
                }
            ],
            max_tokens=120,
        )
        return resp.choices[0].message.content.strip()
    except Exception as exc:
        logger.info(
            "No se pudo describir la imagen generada",
            extra={"event": "image.describe_fallback"},
            exc_info=exc,
        )
        return f"Ilustración generada el {datetime.now(UTC).date().isoformat()} representando: {topic}."
