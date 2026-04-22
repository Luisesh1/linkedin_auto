"""
Pipeline orchestration and automatic feedback helpers.
"""

from __future__ import annotations

import re
from collections import Counter
from copy import deepcopy

from src import content, image_gen, metrics, trends
from src.logging_utils import get_logger

logger = get_logger(__name__)

CONTENT_FORMATS = ["insight", "storytelling", "opinion", "tutorial", "listicle", "case-study"]
CTA_TYPES = ["question", "reflection", "invite", "follow", "comment", "debate", "action"]
HOOK_GOALS = ["question", "surprise", "clarity", "contrarian", "story", "bold", "urgency"]
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


_KEYWORD_STOPWORDS = {
    "para", "con", "sin", "por", "una", "unos", "unas", "del", "las", "los",
    "que", "como", "más", "mas", "muy", "este", "esta", "estos", "estas",
    "cuando", "donde", "sobre", "entre", "desde", "hasta", "pero", "porque",
    "the", "and", "for", "with", "from", "into", "that", "this", "these",
    "those", "your", "their", "about", "when", "where", "what", "while",
}


def _extract_keywords(text: str, *, limit: int = 4) -> list[str]:
    tokens = [tok for tok in _normalize_text(text).split() if len(tok) > 3 and tok not in _KEYWORD_STOPWORDS]
    seen: list[str] = []
    for tok in tokens:
        if tok not in seen:
            seen.append(tok)
        if len(seen) >= limit:
            break
    return seen


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


def _load_metrics_feedback() -> str:
    """Pull recent posts with metrics and turn them into LLM-readable feedback.

    Imports `db` lazily so unit tests that mock `pipeline` can avoid the
    sqlite dependency entirely. Any failure here is non-fatal: the pipeline
    falls back to validation-only feedback.
    """
    try:
        from src import db

        recent = db.get_posts_with_metrics(minimum_impressions=1, limit=20, days=60)
    except Exception as exc:
        logger.warning(
            "No se pudo cargar el historial de métricas para feedback",
            extra={"event": "pipeline.metrics_feedback_failed"},
            exc_info=exc,
        )
        return ""
    if not recent:
        return ""
    try:
        return metrics.build_pipeline_feedback(recent)
    except Exception as exc:
        logger.warning(
            "Error construyendo feedback de métricas",
            extra={"event": "pipeline.metrics_feedback_build_failed"},
            exc_info=exc,
        )
        return ""


def _combine_feedback(metrics_feedback: str, validation_feedback: str) -> str:
    parts = [item for item in (metrics_feedback, validation_feedback) if item]
    return "\n\n".join(parts)


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


def _enrich_brief_with_llm(topic: str, pillar: str, why_now: str, feedback: str) -> dict:
    import json as _json
    from src.llm import get_text_model, get_xai_client
    prompt = (
        "Eres estratega editorial para LinkedIn. Afila este brief para que el post tenga un ángulo distintivo.\n\n"
        f"Topic: {topic}\nPillar: {pillar}\nWhy now: {why_now}\n"
        f"Feedback de posts recientes (si hay): {feedback or '(sin datos)'}\n\n"
        "Devuelve SOLO JSON con: "
        '{"angle":"ángulo específico y no genérico (≤30 palabras)",'
        '"hook_idea":"idea concreta para abrir el post (≤20 palabras)",'
        '"evidence_seed":"dato, ejemplo o escena concreta que sustente el ángulo (≤25 palabras)"}'
    )
    try:
        response = get_xai_client().chat.completions.create(
            model=get_text_model(),
            messages=[{"role": "user", "content": prompt}],
            max_tokens=250,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content or ""
        data = _json.loads(raw)
        return {
            "angle": str(data.get("angle") or "").strip(),
            "hook_idea": str(data.get("hook_idea") or "").strip(),
            "evidence_seed": str(data.get("evidence_seed") or "").strip(),
        }
    except Exception as exc:
        logger.info("No se pudo enriquecer el brief con LLM", extra={"event": "brief.enrich_failed"}, exc_info=exc)
        return {}


def build_content_brief(
    selected_candidate: dict,
    history: list[dict],
    category_cfg: dict | None = None,
    *,
    enrich: bool = True,
    metrics_feedback: str = "",
) -> dict:
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

    topic = _history_value(selected_candidate, "topic")
    pillar = _history_value(selected_candidate, "pillar") or infer_pillar(topic)
    why_now = _history_value(selected_candidate, "why_now")
    default_angle = why_now or f"Explica por qué {topic} importa ahora para profesionales."

    enrichment: dict = {}
    if enrich and topic and str((category_cfg or {}).get("enrich_brief", "1")) not in ("0", "false", "False"):
        enrichment = _enrich_brief_with_llm(topic, pillar, why_now, metrics_feedback)

    return {
        "topic": topic,
        "pillar": pillar,
        "angle": enrichment.get("angle") or default_angle,
        "hook_idea": enrichment.get("hook_idea", ""),
        "evidence_seed": enrichment.get("evidence_seed", ""),
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


def validate_topic_coherence(content_brief: dict, post_data: dict) -> dict:
    brief_topic = _history_value(content_brief, "topic")
    post_topic = _history_value(post_data, "topic")
    post_text = _history_value(post_data, "post_text")
    keywords = _extract_keywords(brief_topic, limit=4)
    haystack = _normalize_text(f"{post_topic} {post_text}")
    hits = [kw for kw in keywords if kw in haystack]
    coverage = len(hits) / len(keywords) if keywords else 0.0
    passed = bool(keywords) and (coverage >= 0.5 or brief_topic == post_topic)
    issues: list[str] = []
    if not passed:
        missing = [kw for kw in keywords if kw not in hits] or [brief_topic]
        issues.append("incluye literalmente en el post: " + ", ".join(missing))
    return {
        "passed": passed,
        "score": round(coverage, 3),
        "keywords": keywords,
        "hits": hits,
        "issues": issues,
    }


def collect_quality_report(
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
    coherence_check = validate_topic_coherence(content_brief, post_data)
    if repetition > 0.68:
        issues.append("el tema seleccionado sigue siendo demasiado parecido al historial")
        score -= 0.35
    if not copy_check.get("passed"):
        issues.extend(copy_check.get("issues", []))
        score -= 0.3
    if not visual_check.get("passed"):
        issues.extend(visual_check.get("issues", []))
        score -= 0.2
    if not coherence_check.get("passed"):
        issues.extend(coherence_check.get("issues", []))
        score -= 0.15
    if not _history_value(image_data, "image_path"):
        issues.append("la imagen no quedó lista para publicación")
        score -= 0.25
    return {
        "passed": score >= 0.55 and not issues,
        "score": round(max(0.0, score), 3),
        "issues": issues,
        "topic_coherence": coherence_check,
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
        metrics_feedback_for_brief = payload.get("metrics_feedback") or _load_metrics_feedback()
        payload["metrics_feedback"] = metrics_feedback_for_brief
        content_brief = build_content_brief(
            payload["selected_candidate"],
            history,
            category_cfg=category_cfg,
            metrics_feedback=metrics_feedback_for_brief,
        )
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
        metrics_feedback = _load_metrics_feedback()
        payload["metrics_feedback"] = metrics_feedback
        validation_feedback = ""
        post_data = {}
        copy_check = {}
        coherence_check: dict = {}
        for _attempt in range(2):
            combined_feedback = _combine_feedback(metrics_feedback, validation_feedback)
            post_data = content.generate_post(
                content_brief,
                history,
                category_cfg=category_cfg,
                feedback=combined_feedback,
            )
            copy_check = validate_post_copy(post_data, history, category_cfg=category_cfg)
            coherence_check = validate_topic_coherence(content_brief, post_data)
            combined_issues = list(copy_check.get("issues", [])) + list(coherence_check.get("issues", []))
            if copy_check.get("passed") and coherence_check.get("passed"):
                break
            validation_feedback = _build_feedback_text(combined_issues)
        if not copy_check.get("passed"):
            raise PipelineStageError(4, "El copy generado no superó las validaciones automáticas.")
        if not coherence_check.get("passed"):
            raise PipelineStageError(4, "El copy no respeta las keywords del brief: " + "; ".join(coherence_check.get("issues", [])))
        try:
            from src import db as _db
            recent_for_leaders = _db.get_posts_with_metrics(minimum_impressions=1, limit=20, days=60)
            leaders = metrics.compute_feedback_leaders(recent_for_leaders)
        except Exception:
            leaders = {}
        followed: list[str] = []
        missed: list[str] = []
        if leaders:
            proposed = {
                "hook_type": post_data.get("hook_type") or content_brief.get("hook_goal"),
                "cta_type": post_data.get("cta_type") or content_brief.get("cta_type"),
                "content_format": content_brief.get("content_format"),
                "visual_style": content_brief.get("visual_style"),
            }
            for dim, leader_value in leaders.items():
                actual = proposed.get(dim)
                if actual is None:
                    continue
                (followed if actual == leader_value else missed).append(dim)
        payload["feedback_tracking"] = {
            "leaders": leaders,
            "followed": followed,
            "missed": missed,
        }
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
        quality_checks["topic_coherence"] = coherence_check
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
        send(5, "running", message="Generando variantes visuales y eligiendo la mejor...", stage="visual_validation")
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
            try:
                image_data = image_gen.generate_image(
                    brief_for_image,
                    category_cfg=category_cfg,
                    progress_callback=lambda message: send(5, "running", message=message, stage="visual_validation"),
                )
            except Exception as exc:
                raise PipelineStageError(5, "No se pudo generar una imagen válida con Grok.") from exc
            if visual_check.get("passed"):
                break
        if not visual_check.get("passed"):
            logger.warning(
                "Validación de diversidad visual no superada tras %d intentos; continuando con estilo '%s' (todos los estilos permitidos están saturados)",
                len(tried_styles),
                brief_for_image.get("visual_style", ""),
                extra={
                    "event": "pipeline.visual_diversity_warning",
                    "tried_styles": sorted(tried_styles),
                    "score": visual_check.get("score"),
                    "issues": visual_check.get("issues", []),
                },
            )
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
                "image_alignment_score": image_data.get("image_alignment_score", 0),
                "image_selection_reason": image_data.get("image_selection_reason", ""),
                "image_prompt_family": image_data.get("image_prompt_family", ""),
                "image_brief": image_data.get("image_brief", {}),
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
                "selected_family": payload.get("image_prompt_family", ""),
                "score": payload.get("image_alignment_score", 0),
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
            "image_alignment_score": payload.get("image_alignment_score", 0),
            "image_selection_reason": payload.get("image_selection_reason", ""),
            "image_prompt_family": payload.get("image_prompt_family", ""),
            "image_brief": payload.get("image_brief", {}),
        }
        send(
            5,
            "done",
            result={
                "image_url": payload.get("image_url", ""),
                "visual_style": payload.get("visual_style", ""),
                "selected_family": payload.get("image_prompt_family", ""),
                "score": payload.get("image_alignment_score", 0),
            },
            stage="visual_validation",
            skipped=True,
        )

    quality_checks = dict(payload.get("quality_checks", {}))
    copy_check = quality_checks.get("copy_validation") or validate_post_copy(post_data, history, category_cfg=category_cfg)
    visual_check = quality_checks.get("visual_validation") or validate_visual_plan(content_brief, history)
    readiness = collect_quality_report(
        payload.get("selected_candidate", {}),
        content_brief,
        post_data,
        image_data,
        copy_check,
        visual_check,
    )
    payload["publish_readiness"] = readiness
    payload["quality_score"] = readiness.get("score", 0)

    return payload
