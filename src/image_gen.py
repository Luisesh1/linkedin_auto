"""
Image generation module using xAI Grok API.
Builds a visual brief, creates prompt variants, and auto-selects the strongest
image for LinkedIn feed usage.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
import json
import os
import re
import uuid

import requests

from src.llm import get_image_model, get_text_model, get_vision_model, get_xai_client
from src.logging_utils import get_logger

logger = get_logger(__name__)

LINKEDIN_ASPECT_RATIO = "1:1"
LINKEDIN_RESOLUTION = "2k"
MAX_GENERATION_VARIANTS = 3

STYLE_DIRECTIONS = {
    "editorial": {
        "prompt": "Create a polished editorial illustration with magazine-quality composition and realistic business context.",
        "composition_type": "editorial portrait",
        "color_direction": "deep blues and soft neutrals",
        "realism_bias": "high",
    },
    "minimal": {
        "prompt": "Create a minimal conceptual scene with clean geometry, controlled realism and strong negative space.",
        "composition_type": "minimal concept",
        "color_direction": "monochrome neutrals with one accent color",
        "realism_bias": "medium",
    },
    "diagram": {
        "prompt": "Create a systems-oriented visual that feels like an editorial diagram grounded in real operations, not sci-fi UI.",
        "composition_type": "abstract systems diagram",
        "color_direction": "cyan, graphite and electric accents",
        "realism_bias": "medium",
    },
    "cinematic": {
        "prompt": "Create a cinematic business scene with layered depth, believable lighting and professional realism.",
        "composition_type": "cinematic tableau",
        "color_direction": "teal, steel and warm highlights",
        "realism_bias": "high",
    },
    "illustrated": {
        "prompt": "Create an illustrated editorial scene with expressive but grounded storytelling and readable subject focus.",
        "composition_type": "narrative illustration",
        "color_direction": "warm earth tones and vivid accents",
        "realism_bias": "medium",
    },
    "anime": {
        "prompt": "Create a refined anime-inspired professional illustration that still feels coherent, clean and socially credible.",
        "composition_type": "anime-inspired illustration",
        "color_direction": "vibrant cool tones with controlled contrast",
        "realism_bias": "low",
    },
}

PROMPT_FAMILIES = (
    {
        "name": "literal_editorial",
        "label": "literal editorial",
        "instruction": (
            "Translate the thesis into one believable professional scene with a clear subject, clear action, and a realistic work context."
        ),
    },
    {
        "name": "hybrid_editorial_conceptual",
        "label": "hybrid editorial-conceptual",
        "instruction": (
            "Blend a plausible professional setting with one controlled symbolic device that reinforces the message without overwhelming the scene."
        ),
    },
    {
        "name": "symbolic_grounded",
        "label": "symbolic but grounded",
        "instruction": (
            "Use a symbolic composition, but keep materials, lighting and business context believable enough for LinkedIn."
        ),
    },
)

LINKEDIN_VISUAL_RULES = (
    "Designed for LinkedIn feed consumption. Square composition. Strong focal point. Readable at thumbnail size. "
    "One central idea only. Clean background separation. No text, typography, captions, logos or watermarks. "
    "No fake dashboards or unreadable UI. No distorted hands. No stock-photo stiffness. "
    "Avoid glowing robots, holograms, generic blue circuits and empty AI sci-fi clichés unless the copy explicitly requires them."
)


def _category_text(category_cfg: dict | None, key: str, fallback: str) -> str:
    if not category_cfg:
        return fallback
    value = (category_cfg.get(key) or "").strip()
    return value or fallback


def _trim_sentence(value: str, limit: int = 180) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if len(text) <= limit:
        return text
    shortened = text[:limit].rsplit(" ", 1)[0].strip()
    return shortened or text[:limit].strip()


def _build_image_brief_fallback(content_input: dict, category_cfg: dict | None = None) -> dict:
    topic = str(content_input.get("topic", "")).strip()
    angle = str(content_input.get("angle", "")).strip() or topic
    post_text = str(content_input.get("post_text", "")).strip()
    content_format = str(content_input.get("content_format", "insight")).strip() or "insight"
    audience = str(content_input.get("audience", "tech leaders")).strip() or "tech leaders"
    visual_style = str(content_input.get("visual_style", "editorial")).strip() or "editorial"
    category_instruction = _category_text(
        category_cfg,
        "image_prompt",
        "Imagen editorial limpia, profesional y alineada al argumento principal del post.",
    )
    sentences = [item.strip() for item in re.split(r"(?<=[.!?])\s+", post_text) if item.strip()]
    core_idea = _trim_sentence(angle or (sentences[0] if sentences else topic), 160)
    supporting = [_trim_sentence(item, 140) for item in sentences[1:3]]
    negative_prompt = _category_text(
        category_cfg,
        "negative_prompt",
        "Evita humo visual, abstracciones vacias y clichés de IA.",
    )
    return {
        "topic": topic,
        "angle": angle,
        "core_idea": core_idea or topic,
        "supporting_points": supporting,
        "scene": f"Professional scene about {angle or topic}",
        "subject": "One clear professional subject or object that anchors the idea",
        "setting": "A credible work environment with restrained detail",
        "focal_point": "Single clear focal point visible at thumbnail size",
        "mood": "Confident, modern and useful",
        "social_goal": "Image should stop the scroll without looking like clickbait",
        "audience": audience,
        "content_format": content_format,
        "category_instruction": category_instruction,
        "negative_prompt": negative_prompt,
        "visual_style": visual_style,
        "visual_metaphor": "",
        "realism_bias": STYLE_DIRECTIONS.get(visual_style, STYLE_DIRECTIONS["editorial"]).get("realism_bias", "high"),
    }


def _build_image_brief(content_input: dict, category_cfg: dict | None = None) -> dict:
    brief = _build_image_brief_fallback(content_input, category_cfg=category_cfg)
    brief["supporting_points"] = [
        _trim_sentence(item, 140) for item in brief.get("supporting_points", []) if str(item).strip()
    ][:3]
    brief["core_idea"] = _trim_sentence(str(brief.get("core_idea") or ""), 180)
    brief["scene"] = _trim_sentence(str(brief.get("scene") or ""), 180)
    brief["subject"] = _trim_sentence(str(brief.get("subject") or ""), 160)
    brief["setting"] = _trim_sentence(str(brief.get("setting") or ""), 160)
    brief["focal_point"] = _trim_sentence(str(brief.get("focal_point") or ""), 160)
    brief["mood"] = _trim_sentence(str(brief.get("mood") or ""), 120)
    brief["social_goal"] = _trim_sentence(str(brief.get("social_goal") or ""), 150)
    brief["visual_metaphor"] = _trim_sentence(str(brief.get("visual_metaphor") or ""), 160)
    return brief


def _format_brief_for_storage(brief: dict) -> dict:
    return {
        "topic": brief.get("topic", ""),
        "angle": brief.get("angle", ""),
        "core_idea": brief.get("core_idea", ""),
        "scene": brief.get("scene", ""),
        "subject": brief.get("subject", ""),
        "setting": brief.get("setting", ""),
        "focal_point": brief.get("focal_point", ""),
        "mood": brief.get("mood", ""),
        "visual_metaphor": brief.get("visual_metaphor", ""),
        "visual_style": brief.get("visual_style", ""),
        "supporting_points": list(brief.get("supporting_points", [])),
    }


def _build_prompt_variant(brief: dict, family: dict) -> dict:
    visual_style = brief.get("visual_style", "editorial")
    style_cfg = STYLE_DIRECTIONS.get(visual_style, STYLE_DIRECTIONS["editorial"])
    supporting_points = "; ".join(brief.get("supporting_points", [])[:2]) or "No extra supporting points."
    metaphor_instruction = ""
    if brief.get("visual_metaphor") and family["name"] != "literal_editorial":
        metaphor_instruction = f"Use this symbolic idea carefully: {brief['visual_metaphor']}. "
    audience = str(brief.get("audience", "") or "tech leaders")

    prompt = (
        f"Create a social-media-ready image for LinkedIn. "
        f"Topic: {brief.get('topic', '')}. "
        f"Core thesis: {brief.get('core_idea', '')}. "
        f"Editorial angle: {brief.get('angle', '')}. "
        f"Target audience: {audience}. "
        f"Visual style: {visual_style}. "
        f"Style direction: {style_cfg['prompt']} "
        f"Prompt family: {family['label']}. "
        f"{family['instruction']} "
        f"Primary scene: {brief.get('scene', '')}. "
        f"Subject: {brief.get('subject', '')}. "
        f"Setting: {brief.get('setting', '')}. "
        f"Focal point: {brief.get('focal_point', '')}. "
        f"Mood: {brief.get('mood', '')}. "
        f"Supporting cues: {supporting_points}. "
        f"{metaphor_instruction}"
        f"Category guidance: {brief.get('category_instruction', '')}. "
        f"Negative constraints: {brief.get('negative_prompt', '')}. "
        f"{LINKEDIN_VISUAL_RULES} "
        f"Color direction: {style_cfg['color_direction']}. "
        f"Composition target: {style_cfg['composition_type']}. "
        "Use a clean, premium, editorial finish. Avoid clutter. Keep it representative of the copy."
    )
    return {
        "family": family["name"],
        "family_label": family["label"],
        "prompt": prompt,
        "visual_style": visual_style,
        "composition_type": style_cfg["composition_type"],
        "color_direction": style_cfg["color_direction"],
    }


def _generate_single_candidate(variant: dict) -> dict:
    client = get_xai_client()
    response = client.images.generate(
        model=get_image_model(),
        prompt=variant["prompt"],
        n=1,
        extra_body={"aspect_ratio": LINKEDIN_ASPECT_RATIO, "resolution": LINKEDIN_RESOLUTION},
    )
    image_url_remote = response.data[0].url
    return {**variant, "remote_url": image_url_remote}


def _generate_image_candidates(variants: list[dict]) -> list[dict]:
    with ThreadPoolExecutor(max_workers=min(MAX_GENERATION_VARIANTS, len(variants))) as executor:
        return list(executor.map(_generate_single_candidate, variants))


def _fallback_selection(candidates: list[dict], reason: str) -> dict:
    family_priority = {"literal_editorial": 3, "hybrid_editorial_conceptual": 2, "symbolic_grounded": 1}
    ordered = sorted(candidates, key=lambda item: family_priority.get(item["family"], 0), reverse=True)
    chosen = ordered[0]
    return {
        "selected_candidate": chosen,
        "image_alignment_score": 6.4,
        "image_selection_reason": reason,
        "image_prompt_family": chosen["family"],
        "candidate_scores": [],
    }


def _score_with_vision(candidates: list[dict], brief: dict, vision_model: str) -> dict | None:
    prompt = (
        "Evaluate these LinkedIn image candidates and choose the best one. "
        f"Topic: {brief.get('topic', '')}. Angle: {brief.get('angle', '')}. "
        f"Core idea: {brief.get('core_idea', '')}. "
        "Score each on alignment_with_copy, feed_clarity, professionalism, composition_strength, distraction_control (0-10) "
        "and compute total_score as the average. "
        "Return ONLY JSON with keys: selected_index (1-based), selection_reason, candidates "
        "(each with candidate_index, total_score, rationale)."
    )
    content = [{"type": "text", "text": prompt}]
    for candidate in candidates:
        content.append({"type": "image_url", "image_url": {"url": candidate["remote_url"]}})
    try:
        response = get_xai_client().chat.completions.create(
            model=vision_model,
            messages=[{"role": "user", "content": content}],
            max_tokens=500,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content
        return json.loads(raw) if raw else None
    except Exception as exc:
        logger.info("Fallo en evaluación visual", extra={"event": "image.vision_fail"}, exc_info=exc)
        return None


def _select_best_candidate(candidates: list[dict], brief: dict) -> dict:
    if len(candidates) == 1:
        only = candidates[0]
        return {
            "selected_candidate": only,
            "image_alignment_score": 7.0,
            "image_selection_reason": "Variante única generada según familia preferida de la categoría.",
            "image_prompt_family": only["family"],
            "candidate_scores": [],
        }

    vision_model = get_vision_model()
    if vision_model:
        parsed = _score_with_vision(candidates, brief, vision_model)
        if parsed:
            idx = min(max(int(parsed.get("selected_index", 1) or 1), 1), len(candidates))
            chosen = candidates[idx - 1]
            scores = parsed.get("candidates", []) or []
            for item in scores:
                if int(item.get("candidate_index", 0) or 0) == idx:
                    chosen = {**chosen, "total_score": float(item.get("total_score", 0) or 0)}
                    break
            return {
                "selected_candidate": chosen,
                "image_alignment_score": round(float(chosen.get("total_score", 7.0) or 7.0), 2),
                "image_selection_reason": str(parsed.get("selection_reason", "") or "").strip()
                or "Seleccionada por scoring visual.",
                "image_prompt_family": chosen["family"],
                "candidate_scores": scores,
            }

    return _fallback_selection(candidates, "Selección por prioridad de familia (sin visión).")


def _download_selected_image(image_url_remote: str) -> tuple[str, str]:
    save_dir = os.path.join("static", "generated")
    os.makedirs(save_dir, exist_ok=True)
    img_filename = f"post_{uuid.uuid4().hex[:10]}.jpg"
    img_path = os.path.join(save_dir, img_filename)
    img_resp = requests.get(image_url_remote, timeout=30)
    img_resp.raise_for_status()
    with open(img_path, "wb") as f:
        f.write(img_resp.content)
    return f"/static/generated/{img_filename}", img_path


def generate_image(content_input, category_cfg: dict | None = None, progress_callback=None) -> dict:
    """
    Generate a professional image for the given topic or content brief.

    Returns:
        dict with keys: image_url (local preview URL), image_path (filesystem path)
    """
    if isinstance(content_input, dict):
        brief_input = dict(content_input)
    else:
        brief_input = {"topic": str(content_input).strip(), "visual_style": "editorial"}

    topic = str(brief_input.get("topic", "")).strip()
    visual_style = str(brief_input.get("visual_style", "editorial")).strip() or "editorial"
    image_brief = _build_image_brief(brief_input, category_cfg=category_cfg)

    preferred_family = ""
    if category_cfg:
        preferred_family = str(category_cfg.get("preferred_image_family") or "").strip()
    vision_model = get_vision_model()
    families_by_name = {family["name"]: family for family in PROMPT_FAMILIES}

    if vision_model:
        families = list(PROMPT_FAMILIES)
    elif preferred_family and preferred_family in families_by_name:
        families = [families_by_name[preferred_family]]
    else:
        families = [PROMPT_FAMILIES[0]]

    variants = [_build_prompt_variant(image_brief, family) for family in families]
    if progress_callback:
        progress_callback(f"Generando {len(variants)} variante(s) visual(es) para LinkedIn...")
    candidates = (
        _generate_image_candidates(variants) if len(variants) > 1 else [_generate_single_candidate(variants[0])]
    )
    selection = _select_best_candidate(candidates, image_brief)

    selected_candidate = selection["selected_candidate"]

    if progress_callback:
        progress_callback(
            f"Imagen seleccionada: {selected_candidate['family_label']} · score {selection['image_alignment_score']:.1f}/10."
        )

    image_url_local, image_path = _download_selected_image(selected_candidate["remote_url"])
    description = f"Ilustración {visual_style} generada el {datetime.now(UTC).date().isoformat()} representando: {topic}."

    return {
        "image_url": image_url_local,
        "image_path": image_path,
        "prompt_used": selected_candidate["prompt"],
        "image_desc": description,
        "visual_style": visual_style,
        "composition_type": selected_candidate["composition_type"],
        "color_direction": selected_candidate["color_direction"],
        "image_alignment_score": selection["image_alignment_score"],
        "image_selection_reason": selection["image_selection_reason"],
        "image_prompt_family": selected_candidate["family"],
        "image_brief": _format_brief_for_storage(image_brief),
    }
