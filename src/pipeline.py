"""
Pipeline orchestration and automatic feedback helpers.
"""

from __future__ import annotations

import re
from collections import Counter
from copy import deepcopy

from src import content, image_gen, trends
from src.logging_utils import get_logger

logger = get_logger(__name__)

CONTENT_FORMATS = ["insight", "storytelling", "opinion", "tutorial", "listicle", "case-study"]
CTA_TYPES = ["question", "reflection", "invite", "follow", "comment"]
HOOK_GOALS = ["surprise", "clarity", "contrarian", "story", "urgency"]
AUDIENCES = ["tech leaders", "software teams", "job seekers", "founders", "knowledge workers"]
VISUAL_STYLES = ["editorial", "minimal", "diagram", "cinematic", "illustrated", "anime"]
CLICHE_PATTERNS = [
    "en un mundo",
    "cambia las reglas del juego",
    "ya no es opcional",
    "el futuro ya está aquí",
    "sin duda",
    "debemos abrazar",
]
PILLAR_KEYWORDS = {
    "ai": "artificial intelligence automation llm generative agent prompt model machine learning ai",
    "engineering": "software engineering developer code architecture platform devops api backend frontend",
    "leadership": "leadership management manager team culture strategy executive mentor",
    "careers": "career hiring recruiting talent interview salary resume job market layoff",
    "productivity": "productivity workflow collaboration async meeting focus system process",
    "cybersecurity": "cybersecurity security breach zero trust ransomware compliance privacy incident",
    "startups": "startup founder venture capital growth saas market innovation go-to-market",
}


class PipelineStageError(RuntimeError):
    def __init__(self, step: int, message: str):
        super().__init__(message)
        self.step = step


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9áéíóúñü ]+", " ", (value or "").lower())).strip()


def _tokenize(value: str) -> set[str]:
    return {token for token in _normalize_text(value).split() if len(token) > 2}


def text_similarity(left: str, right: str) -> float:
    left_tokens = _tokenize(left)
    right_tokens = _tokenize(right)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def build_signature(value: str, *, limit: int = 8) -> str:
    tokens = list(_tokenize(value))
    return " ".join(tokens[:limit])


def infer_pillar(text: str) -> str:
    normalized = _normalize_text(text)
    best = ("productivity", 0)
    for pillar, words in PILLAR_KEYWORDS.items():
        score = sum(1 for token in words.split() if token in normalized)
        if score > best[1]:
            best = (pillar, score)
    return best[0]


def _history_value(item: dict, key: str, default: str = "") -> str:
    value = item.get(key, default)
    return value if isinstance(value, str) else str(value or default)


def _choose_rotating_option(options: list[str], recent_values: list[str], preferred: str | None = None) -> str:
    counts = Counter([value for value in recent_values if value])
    ordered = sorted(options, key=lambda value: (counts.get(value, 0), options.index(value)))
    if preferred and preferred in ordered:
        return preferred if counts.get(preferred, 0) == 0 else ordered[0]
    return ordered[0]


def _category_list(category_cfg: dict | None, key: str, fallback: list[str]) -> list[str]:
    values = (category_cfg or {}).get(key, [])
    if not isinstance(values, list):
        return fallback
    cleaned = [str(item).strip() for item in values if str(item).strip()]
    return cleaned or fallback


def repetition_threshold_for_category(category_cfg: dict | None) -> float:
    level = int((category_cfg or {}).get("originality_level", 3) or 3)
    return {1: 0.8, 2: 0.74, 3: 0.68, 4: 0.6, 5: 0.52}.get(level, 0.68)


def _build_feedback_text(issues: list[str]) -> str:
    if not issues:
        return ""
    return "Corrige estrictamente lo siguiente: " + "; ".join(issues)


def score_topic_candidates(topic_candidates: list[dict], history: list[dict], category_cfg: dict | None = None) -> list[dict]:
    keywords = [str(item).lower() for item in (category_cfg or {}).get("topic_keywords", [])]
    recent = history[:8]
    scored: list[dict] = []

    for candidate in topic_candidates:
        topic = _history_value(candidate, "topic")
        pillar = _history_value(candidate, "pillar") or infer_pillar(topic)
        freshness = float(candidate.get("freshness_score", 0.55) or 0.55)
        topic_similarity = max(
            (
                max(
                    text_similarity(topic, _history_value(post, "topic")),
                    text_similarity(topic, _history_value(post, "post_text")),
                )
                for post in recent
            ),
            default=0.0,
        )
        pillar_repeat = 1.0 if recent and _history_value(recent[0], "pillar") == pillar else 0.0
        angle_repeat = max(
            (
                text_similarity(_history_value(candidate, "why_now"), _history_value(post, "angle_signature"))
                for post in recent
            ),
            default=0.0,
        )
        repetition_score = round(min(1.0, topic_similarity * 0.6 + pillar_repeat * 0.25 + angle_repeat * 0.15), 3)
        keyword_fit = 0.0
        if keywords:
            keyword_fit = min(
                1.0,
                sum(1 for keyword in keywords if keyword in _normalize_text(topic)) / max(1, len(keywords)),
            )
        pillar_recent_hits = sum(1 for post in recent[:3] if _history_value(post, "pillar") == pillar)
        rotation_score = round(max(0.0, 1.0 - pillar_recent_hits / 3), 3)
        final_score = round(
            freshness * 0.35 + (1.0 - repetition_score) * 0.35 + keyword_fit * 0.15 + rotation_score * 0.15,
            3,
        )
        scored.append(
            {
                **candidate,
                "pillar": pillar,
                "topic_signature": build_signature(topic),
                "category_fit_score": round(keyword_fit, 3),
                "repetition_score": repetition_score,
                "rotation_score": rotation_score,
                "score": final_score,
            }
        )

    return sorted(scored, key=lambda item: (-float(item.get("score", 0)), float(item.get("repetition_score", 1))))


def select_topic_candidate(scored_candidates: list[dict], *, repetition_threshold: float = 0.68) -> dict:
    filtered = [candidate for candidate in scored_candidates if float(candidate.get("repetition_score", 1)) < repetition_threshold]
    if filtered:
        return filtered[0]
    if scored_candidates:
        return scored_candidates[0]
    raise RuntimeError("No hay candidatos de tema disponibles.")


def build_content_brief(selected_candidate: dict, history: list[dict], category_cfg: dict | None = None) -> dict:
    recent = history[:6]
    recent_formats = [_history_value(post, "content_format") for post in recent]
    recent_ctas = [_history_value(post, "cta_type") for post in recent]
    recent_styles = [_history_value(post, "visual_style") for post in recent]
    recent_hooks = [_history_value(post, "hook_type") for post in recent]
    language = _history_value(category_cfg or {}, "language", "auto") or "auto"
    allowed_formats = _category_list(category_cfg, "preferred_formats", CONTENT_FORMATS)
    allowed_styles = _category_list(category_cfg, "preferred_visual_styles", VISUAL_STYLES)

    content_format = _choose_rotating_option(allowed_formats, recent_formats)
    configured_cta = _history_value(category_cfg or {}, "cta_style", "auto")
    cta_type = configured_cta if configured_cta != "auto" else _choose_rotating_option(CTA_TYPES, recent_ctas)
    visual_style = _choose_rotating_option(allowed_styles, recent_styles)
    configured_hook = _history_value(category_cfg or {}, "hook_style", "auto")
    hook_goal = configured_hook if configured_hook != "auto" else _choose_rotating_option(HOOK_GOALS, recent_hooks)
    audience = _history_value(category_cfg or {}, "audience_focus") or _choose_rotating_option(AUDIENCES, [])

    return {
        "topic": _history_value(selected_candidate, "topic"),
        "pillar": _history_value(selected_candidate, "pillar") or infer_pillar(_history_value(selected_candidate, "topic")),
        "angle": _history_value(selected_candidate, "why_now")
        or f"Explica por qué {_history_value(selected_candidate, 'topic')} importa ahora para profesionales.",
        "content_format": content_format,
        "audience": audience,
        "hook_goal": hook_goal,
        "cta_type": cta_type,
        "language": language,
        "visual_style": visual_style,
        "originality_level": int((category_cfg or {}).get("originality_level", 3) or 3),
        "evidence_mode": _history_value(category_cfg or {}, "evidence_mode", "balanced") or "balanced",
    }


def validate_post_copy(post_data: dict, history: list[dict], category_cfg: dict | None = None) -> dict:
    post_text = _history_value(post_data, "post_text")
    words = [word for word in re.split(r"\s+", post_text.strip()) if word]
    expected = int((category_cfg or {}).get("post_length", 200) or 200)
    min_words = max(35, expected - 80)
    max_words = expected + 90
    issues: list[str] = []
    score = 1.0
    originality_threshold = repetition_threshold_for_category(category_cfg)

    if len(words) < min_words or len(words) > max_words:
        issues.append(f"la longitud debe estar entre {min_words} y {max_words} palabras")
        score -= 0.2

    lower_text = post_text.lower()
    cliche_hits = [pattern for pattern in CLICHE_PATTERNS if pattern in lower_text]
    if cliche_hits:
        issues.append("evita clichés o frases demasiado genéricas")
        score -= min(0.3, 0.08 * len(cliche_hits))

    max_similarity = max(
        (text_similarity(post_text, _history_value(item, "post_text")) for item in history[:8]),
        default=0.0,
    )
    if max_similarity > max(0.38, originality_threshold - 0.12):
        issues.append("el copy se parece demasiado a publicaciones recientes")
        score -= 0.3

    hook = post_text.splitlines()[0].strip() if post_text.strip() else ""
    hook_similarity = max(
        (
            text_similarity(hook, _history_value(item, "post_text").splitlines()[0].strip())
            for item in history[:5]
            if _history_value(item, "post_text").strip()
        ),
        default=0.0,
    )
    if hook and hook_similarity > 0.75:
        issues.append("el hook inicial se siente repetido frente al historial")
        score -= 0.15

    lines = [line.strip() for line in post_text.splitlines() if line.strip()]
    closing_line = lines[-1] if lines else ""
    cta_similarity = max(
        (
            text_similarity(closing_line, _history_value(item, "post_text").splitlines()[-1].strip())
            for item in history[:5]
            if _history_value(item, "post_text").strip()
        ),
        default=0.0,
    )
    if closing_line and cta_similarity > 0.8:
        issues.append("el cierre o CTA repite demasiado el patrón reciente")
        score -= 0.15

    return {
        "passed": not issues,
        "score": round(max(0.0, score), 3),
        "issues": issues,
        "word_count": len(words),
        "max_similarity": round(max_similarity, 3),
        "hook_similarity": round(hook_similarity, 3),
        "cta_similarity": round(cta_similarity, 3),
        "cliche_hits": cliche_hits,
    }


def validate_visual_plan(content_brief: dict, history: list[dict]) -> dict:
    style = _history_value(content_brief, "visual_style") or "editorial"
    recent_styles = [_history_value(item, "visual_style") for item in history[:4]]
    repeated_recently = recent_styles.count(style)
    issues: list[str] = []
    score = 1.0
    if repeated_recently >= 2:
        issues.append(f"el estilo visual '{style}' está saturado en publicaciones recientes")
        score -= 0.45
    if style == "anime" and recent_styles and recent_styles[0] == "anime":
        issues.append("conviene alternar el estilo anime con otro lenguaje visual")
        score -= 0.2
    return {
        "passed": not issues,
        "score": round(max(0.0, score), 3),
        "issues": issues,
        "repeated_recently": repeated_recently,
    }


def evaluate_publish_readiness(
    selected_candidate: dict,
    content_brief: dict,
    post_data: dict,
    image_data: dict,
    copy_check: dict,
    visual_check: dict,
) -> dict:
    issues: list[str] = []
    score = 1.0
    repetition = float(selected_candidate.get("repetition_score", 0))
    if repetition > 0.68:
        issues.append("el tema seleccionado sigue siendo demasiado parecido al historial")
        score -= 0.35
    if not copy_check.get("passed"):
        issues.extend(copy_check.get("issues", []))
        score -= 0.3
    if not visual_check.get("passed"):
        issues.extend(visual_check.get("issues", []))
        score -= 0.2
    if _history_value(content_brief, "topic") != _history_value(post_data, "topic"):
        issues.append("el copy perdió coherencia con el brief del tema")
        score -= 0.15
    if not _history_value(image_data, "image_path"):
        issues.append("la imagen no quedó lista para publicación")
        score -= 0.25
    return {
        "passed": score >= 0.55 and not issues,
        "score": round(max(0.0, score), 3),
        "issues": issues,
    }


def run_feedback_pipeline(
    *,
    category_cfg: dict,
    history_fetcher,
    existing_payload: dict | None = None,
    from_step: int = 1,
    emit=None,
) -> dict:
    payload = dict(existing_payload or {})

    def send(step: int, status: str, *, message: str = "", result=None, stage: str = "", skipped: bool = False):
        if emit:
            emit(
                {
                    "step": step,
                    "status": status,
                    "message": message,
                    "result": result,
                    "stage": stage,
                    "skipped": skipped,
                }
            )

    history = history_fetcher(8)
    payload["history"] = history

    if from_step <= 1 or not payload.get("topic_candidates"):
        send(1, "running", message="Investigando señales y construyendo candidatos...", stage="candidate_research")
        topic_bundle = trends.get_topic_candidates(category_cfg=category_cfg)
        payload["signal_evidence"] = topic_bundle.get("evidence", [])
        payload["topic_candidates"] = topic_bundle.get("topic_candidates", [])
        send(
            1,
            "done",
            result=[item.get("topic", "") for item in payload["topic_candidates"][:5]],
            stage="candidate_research",
        )
    else:
        send(
            1,
            "done",
            result=[item.get("topic", "") for item in payload.get("topic_candidates", [])[:5]],
            stage="candidate_research",
            skipped=True,
        )

    if from_step <= 2 or not payload.get("selected_candidate"):
        send(2, "running", message="Midiendo novedad y seleccionando el mejor tema...", stage="candidate_scoring")
        scored = score_topic_candidates(payload.get("topic_candidates", []), history, category_cfg=category_cfg)
        try:
            selected = select_topic_candidate(scored, repetition_threshold=repetition_threshold_for_category(category_cfg))
        except RuntimeError as exc:
            raise PipelineStageError(2, str(exc)) from exc
        if float(selected.get("repetition_score", 0)) >= repetition_threshold_for_category(category_cfg):
            diversified = trends.get_topic_candidates(
                category_cfg=category_cfg,
                diversify_hint="Busca ángulos distintos y evita repetir temas recientes.",
            )
            payload["signal_evidence"] = diversified.get("evidence", payload.get("signal_evidence", []))
            payload["topic_candidates"] = diversified.get("topic_candidates", [])
            scored = score_topic_candidates(payload.get("topic_candidates", []), history, category_cfg=category_cfg)
            try:
                selected = select_topic_candidate(scored, repetition_threshold=repetition_threshold_for_category(category_cfg))
            except RuntimeError as exc:
                raise PipelineStageError(2, str(exc)) from exc
        payload["scored_candidates"] = scored
        payload["selected_candidate"] = selected
        payload["repetition_score"] = selected.get("repetition_score", 0)
        payload["rotation_score"] = selected.get("rotation_score", 0)
        send(
            2,
            "done",
            result={
                "topic": selected.get("topic", ""),
                "score": selected.get("score", 0),
                "repetition": selected.get("repetition_score", 0),
            },
            stage="candidate_scoring",
        )
    else:
        selected = payload.get("selected_candidate", {})
        send(
            2,
            "done",
            result={
                "topic": selected.get("topic", ""),
                "score": selected.get("score", 0),
                "repetition": selected.get("repetition_score", 0),
            },
            stage="candidate_scoring",
            skipped=True,
        )

    if from_step <= 3 or not payload.get("content_brief"):
        send(3, "running", message="Creando brief editorial interno...", stage="brief_generation")
        content_brief = build_content_brief(payload["selected_candidate"], history, category_cfg=category_cfg)
        payload["content_brief"] = content_brief
        send(
            3,
            "done",
            result={
                "topic": content_brief.get("topic", ""),
                "content_format": content_brief.get("content_format", ""),
                "visual_style": content_brief.get("visual_style", ""),
            },
            stage="brief_generation",
        )
    else:
        content_brief = payload.get("content_brief", {})
        send(
            3,
            "done",
            result={
                "topic": content_brief.get("topic", ""),
                "content_format": content_brief.get("content_format", ""),
                "visual_style": content_brief.get("visual_style", ""),
            },
            stage="brief_generation",
            skipped=True,
        )

    if from_step <= 4 or not payload.get("post_text"):
        send(4, "running", message="Generando y validando el copy...", stage="copy_validation")
        feedback = ""
        post_data = {}
        copy_check = {}
        for _attempt in range(3):
            post_data = content.generate_post(content_brief, history, category_cfg=category_cfg, feedback=feedback)
            copy_check = validate_post_copy(post_data, history, category_cfg=category_cfg)
            if copy_check.get("passed"):
                break
            feedback = _build_feedback_text(copy_check.get("issues", []))
        if not copy_check.get("passed"):
            raise PipelineStageError(4, "El copy generado no superó las validaciones automáticas.")
        payload.update(
            {
                "topic": post_data.get("topic", content_brief.get("topic", "")),
                "post_text": post_data.get("post_text", ""),
                "reasoning": post_data.get("reasoning", ""),
                "hook_type": post_data.get("hook_type", content_brief.get("hook_goal", "")),
                "cta_type": post_data.get("cta_type", content_brief.get("cta_type", "")),
                "angle_signature": post_data.get("angle_signature", build_signature(content_brief.get("angle", ""))),
            }
        )
        quality_checks = dict(payload.get("quality_checks", {}))
        quality_checks["copy_validation"] = copy_check
        payload["quality_checks"] = quality_checks
        send(
            4,
            "done",
            result={
                "topic": payload.get("topic", ""),
                "hook_type": payload.get("hook_type", ""),
                "cta_type": payload.get("cta_type", ""),
            },
            stage="copy_validation",
        )
    else:
        post_data = {
            "topic": payload.get("topic", ""),
            "post_text": payload.get("post_text", ""),
            "reasoning": payload.get("reasoning", ""),
            "hook_type": payload.get("hook_type", ""),
            "cta_type": payload.get("cta_type", ""),
            "angle_signature": payload.get("angle_signature", ""),
        }
        send(
            4,
            "done",
            result={
                "topic": payload.get("topic", ""),
                "hook_type": payload.get("hook_type", ""),
                "cta_type": payload.get("cta_type", ""),
            },
            stage="copy_validation",
            skipped=True,
        )

    if from_step <= 5 or not payload.get("image_path"):
        send(5, "running", message="Generando imagen y validando diversidad visual...", stage="visual_validation")
        visual_check = {}
        image_data = {}
        brief_for_image = deepcopy(content_brief)
        tried_styles = {brief_for_image.get("visual_style", "")}
        allowed_styles = _category_list(category_cfg, "preferred_visual_styles", VISUAL_STYLES)
        for _attempt in range(3):
            visual_check = validate_visual_plan(brief_for_image, history)
            if not visual_check.get("passed"):
                brief_for_image["visual_style"] = _choose_rotating_option(
                    allowed_styles,
                    [_history_value(item, "visual_style") for item in history[:6]],
                )
                if brief_for_image["visual_style"] in tried_styles:
                    remaining = [style for style in allowed_styles if style not in tried_styles]
                    if remaining:
                        brief_for_image["visual_style"] = remaining[0]
                tried_styles.add(brief_for_image["visual_style"])
                visual_check = validate_visual_plan(brief_for_image, history)
            image_data = image_gen.generate_image(brief_for_image, category_cfg=category_cfg)
            if visual_check.get("passed"):
                break
        if not visual_check.get("passed"):
            raise PipelineStageError(5, "La imagen generada no superó las validaciones de diversidad visual.")
        content_brief.update({"visual_style": brief_for_image.get("visual_style", content_brief.get("visual_style", ""))})
        payload["content_brief"] = content_brief
        payload.update(
            {
                "image_path": image_data.get("image_path", ""),
                "image_url": image_data.get("image_url", ""),
                "image_desc": image_data.get("image_desc", ""),
                "prompt_used": image_data.get("prompt_used", ""),
                "visual_style": image_data.get("visual_style", content_brief.get("visual_style", "")),
                "composition_type": image_data.get("composition_type", ""),
                "color_direction": image_data.get("color_direction", ""),
            }
        )
        quality_checks = dict(payload.get("quality_checks", {}))
        quality_checks["visual_validation"] = visual_check
        payload["quality_checks"] = quality_checks
        send(
            5,
            "done",
            result={
                "image_url": payload.get("image_url", ""),
                "visual_style": payload.get("visual_style", ""),
            },
            stage="visual_validation",
        )
    else:
        image_data = {
            "image_path": payload.get("image_path", ""),
            "image_url": payload.get("image_url", ""),
            "image_desc": payload.get("image_desc", ""),
            "prompt_used": payload.get("prompt_used", ""),
            "visual_style": payload.get("visual_style", ""),
            "composition_type": payload.get("composition_type", ""),
            "color_direction": payload.get("color_direction", ""),
        }
        send(
            5,
            "done",
            result={
                "image_url": payload.get("image_url", ""),
                "visual_style": payload.get("visual_style", ""),
            },
            stage="visual_validation",
            skipped=True,
        )

    if from_step <= 6 or not payload.get("publish_readiness"):
        send(6, "running", message="Ejecutando gate final de calidad...", stage="publish_gate")
        quality_checks = dict(payload.get("quality_checks", {}))
        copy_check = quality_checks.get("copy_validation") or validate_post_copy(post_data, history, category_cfg=category_cfg)
        visual_check = quality_checks.get("visual_validation") or validate_visual_plan(content_brief, history)
        readiness = evaluate_publish_readiness(
            payload.get("selected_candidate", {}),
            content_brief,
            post_data,
            image_data,
            copy_check,
            visual_check,
        )
        payload["publish_readiness"] = readiness
        payload["quality_score"] = readiness.get("score", 0)
        if not readiness.get("passed"):
            raise PipelineStageError(6, "El gate final rechazó la publicación: " + "; ".join(readiness.get("issues", [])))
        send(
            6,
            "done",
            result={"score": readiness.get("score", 0), "status": "ready"},
            stage="publish_gate",
        )
    else:
        readiness = payload.get("publish_readiness", {})
        send(
            6,
            "done",
            result={"score": readiness.get("score", 0), "status": "ready"},
            stage="publish_gate",
            skipped=True,
        )

    return payload
