"""
Post performance analytics helpers.
Turns raw post metrics into actionable content insights.
"""

from __future__ import annotations

from collections import defaultdict
from statistics import mean

VERDICT_LABELS = {
    "top": "Top performer",
    "above": "Sobre la media",
    "average": "En la media",
    "below": "Bajo la media",
    "no_data": "Sin datos",
}


def _safe_int(value) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _safe_float(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _word_count(text: str) -> int:
    return len([part for part in str(text or "").split() if part.strip()])


def _length_bucket(text: str) -> str:
    words = _word_count(text)
    if words < 90:
        return "short"
    if words < 180:
        return "medium"
    return "long"


def _hour_bucket(created_at: str) -> str:
    raw = str(created_at or "")
    if "T" not in raw:
        return "unknown"
    time_part = raw.split("T", 1)[1][:2]
    try:
        hour = int(time_part)
    except ValueError:
        return "unknown"
    if 5 <= hour < 12:
        return "morning"
    if 12 <= hour < 18:
        return "afternoon"
    if 18 <= hour < 23:
        return "evening"
    return "night"


def _metric_row(post: dict) -> dict:
    impressions = _safe_int(post.get("impressions"))
    reactions = _safe_int(post.get("reactions"))
    comments = _safe_int(post.get("comments"))
    reposts = _safe_int(post.get("reposts"))
    profile_visits = _safe_int(post.get("profile_visits"))
    link_clicks = _safe_int(post.get("link_clicks"))
    saves = _safe_int(post.get("saves"))
    engagement_total = reactions + comments + reposts + profile_visits + link_clicks + saves
    engagement_rate = _safe_float(post.get("engagement_rate"))
    if impressions > 0 and engagement_rate <= 0:
        engagement_rate = engagement_total / impressions
    comment_rate = comments / impressions if impressions > 0 else 0.0
    save_rate = saves / impressions if impressions > 0 else 0.0
    return {
        **post,
        "impressions": impressions,
        "reactions": reactions,
        "comments": comments,
        "reposts": reposts,
        "profile_visits": profile_visits,
        "link_clicks": link_clicks,
        "saves": saves,
        "engagement_total": engagement_total,
        "engagement_rate": round(engagement_rate, 4),
        "comment_rate": round(comment_rate, 4),
        "save_rate": round(save_rate, 4),
        "word_count": _word_count(post.get("post_text", "")),
        "length_bucket": _length_bucket(post.get("post_text", "")),
        "hour_bucket": _hour_bucket(str(post.get("created_at") or post.get("date") or "")),
    }


def _group_key(post: dict, field: str) -> str:
    value = str(post.get(field, "") or "").strip()
    return value or "unknown"


def summarize_group(posts: list[dict], field: str, *, min_posts: int = 1) -> list[dict]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for post in posts:
        grouped[_group_key(post, field)].append(post)

    summary: list[dict] = []
    for key, items in grouped.items():
        if len(items) < min_posts:
            continue
        summary.append(
            {
                "key": key,
                "posts": len(items),
                "avg_impressions": round(mean(item["impressions"] for item in items), 1),
                "avg_engagement_rate": round(mean(item["engagement_rate"] for item in items), 4),
                "avg_comments": round(mean(item["comments"] for item in items), 2),
                "avg_saves": round(mean(item["saves"] for item in items), 2),
                "avg_word_count": round(mean(item["word_count"] for item in items), 1),
            }
        )
    return sorted(summary, key=lambda item: (-item["avg_engagement_rate"], -item["avg_impressions"], item["key"]))


def build_recommendations(posts: list[dict], group_summaries: dict[str, list[dict]]) -> list[str]:
    if not posts:
        return ["Todavía no hay suficientes métricas para extraer aprendizajes."]

    recommendations: list[str] = []

    def _top(field: str, label: str, metric: str = "avg_engagement_rate") -> None:
        items = group_summaries.get(field, [])
        if items:
            best = items[0]
            recommendations.append(
                f"Prioriza {label} '{best['key']}' porque lidera con engagement promedio de {best[metric] * 100:.1f}%."
            )

    _top("hook_type", "hooks")
    _top("cta_type", "CTAs")
    _top("visual_style", "estilos visuales")
    _top("length_bucket", "longitud")

    by_impressions = sorted(posts, key=lambda item: (-item["impressions"], -item["engagement_rate"]))
    top_post = by_impressions[0]
    recommendations.append(
        f"Replica el ángulo del post '{top_post.get('topic', 'sin tema')}' que logró {top_post['impressions']} impresiones y {top_post['engagement_rate'] * 100:.1f}% de engagement."
    )

    if len(posts) >= 3:
        avg_comment_rate = mean(item["comment_rate"] for item in posts)
        avg_save_rate = mean(item["save_rate"] for item in posts)
        if avg_comment_rate > avg_save_rate:
            recommendations.append("El contenido está generando más conversación que guardados: conviene cerrar con preguntas o debate.")
        else:
            recommendations.append("Los posts se guardan más de lo que se comentan: conviene reforzar valor práctico, frameworks y checklists.")

    return recommendations[:6]


def compute_trend(posts: list[dict]) -> list[dict]:
    """Groups posts by publication date and returns time-series engagement data."""
    by_date: dict[str, list[dict]] = defaultdict(list)
    for post in posts:
        raw = str(post.get("created_at") or post.get("date") or "")
        date = raw[:10] if len(raw) >= 10 else "unknown"
        if date != "unknown":
            by_date[date].append(post)

    result = []
    for date in sorted(by_date.keys()):
        items = by_date[date]
        normalized = [_metric_row(p) for p in items if _safe_int(p.get("impressions")) > 0]
        result.append(
            {
                "date": date,
                "posts_count": len(normalized),
                "avg_engagement_rate": round(mean(p["engagement_rate"] for p in normalized), 4) if normalized else 0.0,
                "avg_impressions": round(mean(p["impressions"] for p in normalized), 1) if normalized else 0.0,
            }
        )
    return result


def analyze_posts(posts: list[dict]) -> dict:
    normalized = [_metric_row(post) for post in posts if _safe_int(post.get("impressions")) > 0]
    if not normalized:
        return {
            "summary": {
                "tracked_posts": 0,
                "total_impressions": 0,
                "avg_engagement_rate": 0.0,
                "avg_comments": 0.0,
                "avg_saves": 0.0,
            },
            "top_posts": [],
            "insights": {},
            "recommendations": ["Todavía no hay posts con impresiones registradas."],
        }

    insights = {
        "category": summarize_group(normalized, "category"),
        "pillar": summarize_group(normalized, "pillar"),
        "content_format": summarize_group(normalized, "content_format"),
        "hook_type": summarize_group(normalized, "hook_type"),
        "cta_type": summarize_group(normalized, "cta_type"),
        "visual_style": summarize_group(normalized, "visual_style"),
        "length_bucket": summarize_group(normalized, "length_bucket"),
        "hour_bucket": summarize_group(normalized, "hour_bucket"),
    }

    top_posts = sorted(
        normalized,
        key=lambda item: (-item["engagement_rate"], -item["impressions"], -item["comments"]),
    )[:5]

    return {
        "summary": {
            "tracked_posts": len(normalized),
            "total_impressions": sum(item["impressions"] for item in normalized),
            "avg_engagement_rate": round(mean(item["engagement_rate"] for item in normalized), 4),
            "avg_comments": round(mean(item["comments"] for item in normalized), 2),
            "avg_saves": round(mean(item["saves"] for item in normalized), 2),
        },
        "top_posts": [
            {
                "id": item["id"],
                "topic": item.get("topic", ""),
                "category": item.get("category", ""),
                "impressions": item["impressions"],
                "engagement_rate": item["engagement_rate"],
                "comments": item["comments"],
                "saves": item["saves"],
                "hook_type": item.get("hook_type", ""),
                "cta_type": item.get("cta_type", ""),
                "visual_style": item.get("visual_style", ""),
            }
            for item in top_posts
        ],
        "insights": insights,
        "recommendations": build_recommendations(normalized, insights),
    }


# ─── Per-post diagnosis ──────────────────────────────────────────────────────


def _percentile_position(value: float, sorted_values: list[float]) -> float:
    """Return the percentile (0-1) of `value` in a sorted ascending list."""
    if not sorted_values:
        return 0.0
    below = sum(1 for item in sorted_values if item < value)
    return below / len(sorted_values)


def _verdict_from_percentile(percentile: float) -> str:
    if percentile >= 0.8:
        return "top"
    if percentile >= 0.6:
        return "above"
    if percentile >= 0.4:
        return "average"
    return "below"


def _attribute_highlights(post: dict, summaries: dict[str, list[dict]]) -> tuple[list[str], list[str]]:
    """Compare a post's attributes against the leaderboard for each dimension.

    Returns (highlights, weaknesses) — highlights when the post's attribute
    leads its dimension, weaknesses when it sits at the bottom.
    """
    fields = {
        "hook_type": "hook",
        "cta_type": "CTA",
        "visual_style": "estilo visual",
        "length_bucket": "longitud",
        "hour_bucket": "franja horaria",
        "content_format": "formato",
    }
    highlights: list[str] = []
    weaknesses: list[str] = []
    for field, label in fields.items():
        rows = summaries.get(field) or []
        if not rows:
            continue
        post_value = str(post.get(field) or "").strip() or "unknown"
        leader = rows[0]
        worst = rows[-1]
        if post_value == leader["key"] and len(rows) > 1:
            highlights.append(
                f"El {label} '{post_value}' lidera tu historial con {leader['avg_engagement_rate'] * 100:.1f}% engagement promedio."
            )
        elif post_value == worst["key"] and len(rows) > 1 and worst["avg_engagement_rate"] < leader["avg_engagement_rate"]:
            weaknesses.append(
                f"El {label} '{post_value}' rinde por debajo de la media ({worst['avg_engagement_rate'] * 100:.1f}% vs {leader['avg_engagement_rate'] * 100:.1f}% del mejor)."
            )
    return highlights, weaknesses


def diagnose_post(post: dict, peer_posts: list[dict]) -> dict:
    """Diagnose a single post against its peer pool.

    Returns:
        {
          "verdict": "top" | "above" | "average" | "below" | "no_data",
          "verdict_label": <human friendly Spanish>,
          "engagement_rate": float,
          "engagement_vs_peers": float,  # 1.0 == in line with median
          "percentile": float,           # 0..1
          "comparison_pool_size": int,
          "comparison_pool_label": "category" | "all",
          "score": float,                # 0..10
          "highlights": list[str],
          "weaknesses": list[str],
        }
    """
    if not post:
        return {
            "verdict": "no_data",
            "verdict_label": VERDICT_LABELS["no_data"],
            "engagement_rate": 0.0,
            "engagement_vs_peers": 0.0,
            "percentile": 0.0,
            "comparison_pool_size": 0,
            "comparison_pool_label": "all",
            "score": 0.0,
            "highlights": [],
            "weaknesses": [],
        }

    normalized_post = _metric_row(post)
    if normalized_post["impressions"] <= 0:
        return {
            "verdict": "no_data",
            "verdict_label": VERDICT_LABELS["no_data"],
            "engagement_rate": 0.0,
            "engagement_vs_peers": 0.0,
            "percentile": 0.0,
            "comparison_pool_size": 0,
            "comparison_pool_label": "all",
            "score": 0.0,
            "highlights": [],
            "weaknesses": ["Sin impresiones registradas todavía — no hay base para diagnosticar."],
        }

    normalized_peers = [_metric_row(peer) for peer in peer_posts if _safe_int(peer.get("impressions")) > 0]
    # Exclude the post itself from the comparison pool to avoid biasing the percentile.
    post_id = normalized_post.get("id")
    pool_all = [peer for peer in normalized_peers if peer.get("id") != post_id]

    same_category = [
        peer for peer in pool_all if str(peer.get("category", "")) == str(normalized_post.get("category", ""))
    ]
    if len(same_category) >= 5:
        comparison_pool = same_category
        comparison_label = "category"
    else:
        comparison_pool = pool_all
        comparison_label = "all"

    if not comparison_pool:
        # Single post in the system — give it a neutral verdict so the UI can
        # render something useful.
        return {
            "verdict": "average",
            "verdict_label": VERDICT_LABELS["average"],
            "engagement_rate": normalized_post["engagement_rate"],
            "engagement_vs_peers": 1.0,
            "percentile": 0.5,
            "comparison_pool_size": 0,
            "comparison_pool_label": comparison_label,
            "score": 5.0,
            "highlights": [],
            "weaknesses": ["Aún no hay otros posts con métricas para comparar."],
        }

    peer_engagements = sorted(peer["engagement_rate"] for peer in comparison_pool)
    median_engagement = peer_engagements[len(peer_engagements) // 2]
    percentile = _percentile_position(normalized_post["engagement_rate"], peer_engagements)
    verdict = _verdict_from_percentile(percentile)
    engagement_vs_peers = (
        normalized_post["engagement_rate"] / median_engagement if median_engagement > 0 else 1.0
    )
    score = round(min(10.0, max(0.0, percentile * 10)), 2)

    summaries = {
        "hook_type": summarize_group(comparison_pool, "hook_type", min_posts=2),
        "cta_type": summarize_group(comparison_pool, "cta_type", min_posts=2),
        "visual_style": summarize_group(comparison_pool, "visual_style", min_posts=2),
        "length_bucket": summarize_group(comparison_pool, "length_bucket", min_posts=2),
        "hour_bucket": summarize_group(comparison_pool, "hour_bucket", min_posts=2),
        "content_format": summarize_group(comparison_pool, "content_format", min_posts=2),
    }
    highlights, weaknesses = _attribute_highlights(normalized_post, summaries)

    if verdict == "top":
        highlights.insert(
            0,
            f"Engagement {normalized_post['engagement_rate'] * 100:.1f}% — top {round((1 - percentile) * 100)}% de tu historial.",
        )
    elif verdict == "above":
        highlights.insert(
            0,
            f"Engagement {normalized_post['engagement_rate'] * 100:.1f}% por encima de la media de tu pool.",
        )
    elif verdict == "below":
        weaknesses.insert(
            0,
            f"Engagement {normalized_post['engagement_rate'] * 100:.1f}% — bajo el {round(percentile * 100)}% percentil de tus posts.",
        )

    return {
        "verdict": verdict,
        "verdict_label": VERDICT_LABELS[verdict],
        "engagement_rate": normalized_post["engagement_rate"],
        "engagement_vs_peers": round(engagement_vs_peers, 2),
        "percentile": round(percentile, 3),
        "comparison_pool_size": len(comparison_pool),
        "comparison_pool_label": comparison_label,
        "score": score,
        "highlights": highlights[:5],
        "weaknesses": weaknesses[:5],
    }


# ─── Pipeline feedback (loop into next post generation) ──────────────────────


def compute_feedback_leaders(recent_posts: list[dict]) -> dict:
    """Return the leading value per dimension when its lead is significant.

    Used to track whether the next generated post follows the feedback signal.
    """
    normalized = [_metric_row(post) for post in recent_posts if _safe_int(post.get("impressions")) > 0]
    if len(normalized) < 2:
        return {}
    dimensions = ["hook_type", "cta_type", "content_format", "length_bucket", "hour_bucket", "visual_style"]
    leaders: dict = {}
    for dim in dimensions:
        rows = summarize_group(normalized, dim, min_posts=2)
        if len(rows) < 2:
            continue
        leader = rows[0]
        trailer = rows[-1]
        if leader["avg_engagement_rate"] <= 0:
            continue
        relative_gap = (leader["avg_engagement_rate"] - trailer["avg_engagement_rate"]) / max(
            leader["avg_engagement_rate"], 0.0001
        )
        if relative_gap >= 0.3:
            leaders[dim] = leader["key"]
    return leaders


def compute_feedback_roi(recent_posts: list[dict]) -> dict:
    """Compare how posts that match current leaders perform vs those that don't."""
    leaders = compute_feedback_leaders(recent_posts)
    if not leaders:
        return {"leaders": {}, "high_follow": None, "low_follow": None, "sample_size": 0}
    high: list[dict] = []
    low: list[dict] = []
    dims = list(leaders.keys())
    for post in recent_posts:
        row = _metric_row(post)
        if _safe_int(post.get("impressions")) <= 0:
            continue
        followed = sum(1 for dim in dims if row.get(dim) == leaders[dim])
        (high if followed >= max(2, len(dims) // 2) else low).append(row["engagement_rate"])
    def _avg(values: list[float]) -> float | None:
        return round(sum(values) / len(values), 4) if values else None
    return {
        "leaders": leaders,
        "dimensions": dims,
        "high_follow": {"count": len(high), "avg_engagement_rate": _avg(high)},
        "low_follow": {"count": len(low), "avg_engagement_rate": _avg(low)},
        "sample_size": len(high) + len(low),
    }


def build_pipeline_feedback(recent_posts: list[dict], *, max_examples: int = 5) -> str:
    """Build a feedback string the LLM should use to refine the next post.

    Resumes which dimensions (hook/CTA/format/length/hour/visual) are leading
    and which are dragging engagement, plus a list of top vs flop topics.
    Returns "" when there is not enough data — callers should fall back to the
    plain validation feedback in that case.
    """
    normalized = [_metric_row(post) for post in recent_posts if _safe_int(post.get("impressions")) > 0]
    if len(normalized) < 2:
        return ""

    # Sort dimensions by reliability — those with at least 2 posts per bucket.
    dimensions = {
        "hook_type": ("hooks", summarize_group(normalized, "hook_type", min_posts=2)),
        "cta_type": ("CTAs", summarize_group(normalized, "cta_type", min_posts=2)),
        "content_format": ("formatos", summarize_group(normalized, "content_format", min_posts=2)),
        "length_bucket": ("longitud", summarize_group(normalized, "length_bucket", min_posts=2)),
        "hour_bucket": ("franja horaria", summarize_group(normalized, "hour_bucket", min_posts=2)),
        "visual_style": ("estilo visual", summarize_group(normalized, "visual_style", min_posts=2)),
    }

    winning_lines: list[str] = []
    losing_lines: list[str] = []
    for label, rows in dimensions.values():
        if len(rows) < 2:
            continue
        leader = rows[0]
        trailer = rows[-1]
        # Only mention if there's a meaningful gap (>30% relative diff)
        if leader["avg_engagement_rate"] > 0 and trailer["avg_engagement_rate"] >= 0:
            relative_gap = (
                (leader["avg_engagement_rate"] - trailer["avg_engagement_rate"])
                / max(leader["avg_engagement_rate"], 0.0001)
            )
            if relative_gap >= 0.3:
                winning_lines.append(
                    f"- {label.capitalize()} '{leader['key']}' lidera con {leader['avg_engagement_rate'] * 100:.1f}% engagement promedio."
                )
                losing_lines.append(
                    f"- {label.capitalize()} '{trailer['key']}' rinde peor ({trailer['avg_engagement_rate'] * 100:.1f}%) — evítalo o renuévalo."
                )

    by_engagement = sorted(normalized, key=lambda item: -item["engagement_rate"])
    top_topics = by_engagement[: max(1, max_examples // 2)]
    flop_topics = list(reversed(by_engagement[-max(1, max_examples // 2):]))

    top_topics_lines = [
        f"- '{post.get('topic', 'sin tema')[:90]}' ({post['engagement_rate'] * 100:.1f}% engagement)"
        for post in top_topics
    ]
    flop_topics_lines = [
        f"- '{post.get('topic', 'sin tema')[:90]}' ({post['engagement_rate'] * 100:.1f}% engagement)"
        for post in flop_topics
        if post not in top_topics
    ]

    sections: list[str] = []
    if winning_lines:
        sections.append("PATRONES QUE ESTÁN FUNCIONANDO:\n" + "\n".join(winning_lines))
    if losing_lines:
        sections.append("PATRONES A EVITAR:\n" + "\n".join(losing_lines))
    if top_topics_lines:
        sections.append("ÁNGULOS GANADORES (úsalos como referencia, no los copies literal):\n" + "\n".join(top_topics_lines))
    if flop_topics_lines:
        sections.append("ÁNGULOS QUE NO CONECTARON (no los repitas):\n" + "\n".join(flop_topics_lines))

    if not sections:
        return ""
    header = (
        "RETROALIMENTACIÓN DERIVADA DE MÉTRICAS REALES "
        f"(basada en {len(normalized)} posts publicados con datos):"
    )
    return header + "\n\n" + "\n\n".join(sections)
