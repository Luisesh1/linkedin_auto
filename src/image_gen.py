"""
Image generation module using xAI Grok API.
Generates anime-style professional images for LinkedIn posts.
"""

import os
import uuid

import requests
from openai import OpenAI


def _load_api_key() -> str:
    import yaml
    with open("config.yaml") as f:
        cfg = yaml.safe_load(f)
    return cfg["grok"]["api_key"]


def _category_text(category_cfg: dict | None, key: str, fallback: str) -> str:
    if not category_cfg:
        return fallback
    value = (category_cfg.get(key) or "").strip()
    return value or fallback


def generate_image(topic: str, category_cfg: dict | None = None) -> dict:
    """
    Generate a sober/elegant anime-style image for the given topic.

    Returns:
        dict with keys: image_url (local preview URL), image_path (filesystem path)
    """
    api_key = _load_api_key()

    client = OpenAI(
        api_key=api_key,
        base_url="https://api.x.ai/v1",
    )

    image_instruction = _category_text(
        category_cfg,
        "image_prompt",
        "Genera una imagen editorial profesional, sobria y conceptual apta para LinkedIn.",
    )
    prompt = (
        f"Topic: {topic}. "
        f"Category style objective: {image_instruction} "
        "Create a professional anime-style illustration with strong visual storytelling. "
        "Clean composition, polished lighting, no text or typography, suitable for a LinkedIn post. "
        "High quality, detailed illustration."
    )

    response = client.images.generate(
        model="grok-imagine-image",
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
    description = _describe_image(client, prompt, topic)

    return {
        "image_url": f"/static/generated/{img_filename}",
        "image_path": img_path,
        "prompt_used": prompt,
        "image_desc": description,
    }


def _describe_image(client: OpenAI, prompt: str, topic: str) -> str:
    """Ask Grok to write a concise description of the generated image."""
    try:
        resp = client.chat.completions.create(
            model="grok-3",
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"Write a concise 1-2 sentence description (in Spanish) of an anime-style "
                        f"illustration generated with this prompt:\n\n\"{prompt}\"\n\n"
                        f"The image represents the concept: {topic}. "
                        "Describe the visual composition, dominant colors, and mood."
                    ),
                }
            ],
            max_tokens=120,
        )
        return resp.choices[0].message.content.strip()
    except Exception:
        return f"Ilustración anime estilo Ghost in the Shell representando: {topic}."
