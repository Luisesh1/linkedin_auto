"""
Background metrics collector.

Iterates over published posts whose engagement metrics are stale (or missing)
and refreshes them using `linkedin.scrape_post_metrics`. Designed to be called
on a schedule from `src.scheduler` and from a manual API endpoint.

Status of the latest run is exposed via the `current_run` dict so the UI can
display it.
"""

from __future__ import annotations

from datetime import UTC, datetime

from src.logging_utils import get_logger

logger = get_logger(__name__)

current_run: dict = {
    "status": "idle",
    "message": "",
    "started_at": "",
    "finished_at": "",
    "processed": 0,
    "updated": 0,
    "errors": 0,
}


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _set_status(message: str, *, status: str | None = None) -> None:
    current_run["message"] = message
    if status:
        current_run["status"] = status
    logger.info(message, extra={"event": "metrics_collector.status"})


def collect_metrics_cycle(
    db,
    linkedin_mod,
    *,
    max_posts: int = 10,
    stale_after_hours: int = 24,
    max_age_days: int = 30,
) -> dict:
    """Run a single collection cycle.

    Returns a summary dict regardless of how many posts were processed:
        {
          "status": "ok" | "skipped" | "error",
          "processed": int,
          "updated": int,
          "errors": int,
          "details": [{"post_id", "topic", "result": "updated"|"empty"|"error", "message": str}],
          "started_at": iso, "finished_at": iso,
        }
    """
    started = _utc_now()
    current_run.update(
        {
            "status": "running",
            "message": "Buscando posts con métricas pendientes...",
            "started_at": started,
            "finished_at": "",
            "processed": 0,
            "updated": 0,
            "errors": 0,
        }
    )

    if getattr(linkedin_mod, "is_login_in_progress", lambda: False)():
        _set_status("Saltando ciclo: hay un login manual de LinkedIn en progreso.", status="skipped")
        finished = _utc_now()
        current_run["finished_at"] = finished
        return {
            "status": "skipped",
            "processed": 0,
            "updated": 0,
            "errors": 0,
            "details": [],
            "started_at": started,
            "finished_at": finished,
            "message": current_run["message"],
        }

    try:
        if not linkedin_mod.is_session_valid(verify_browser=False, log=lambda msg: _set_status(msg)):
            _set_status("Saltando ciclo: no hay sesión válida de LinkedIn.", status="skipped")
            finished = _utc_now()
            current_run["finished_at"] = finished
            return {
                "status": "skipped",
                "processed": 0,
                "updated": 0,
                "errors": 0,
                "details": [],
                "started_at": started,
                "finished_at": finished,
                "message": current_run["message"],
            }
    except Exception as exc:
        logger.warning(
            "No se pudo verificar la sesión de LinkedIn",
            extra={"event": "metrics_collector.session_check_failed"},
            exc_info=exc,
        )

    try:
        pending = db.get_posts_pending_metrics(
            stale_after_hours=stale_after_hours,
            max_posts=max_posts,
            max_age_days=max_age_days,
        )
    except Exception as exc:
        logger.exception(
            "Error consultando posts pendientes de métricas",
            extra={"event": "metrics_collector.query_failed"},
        )
        finished = _utc_now()
        current_run.update({"status": "error", "message": f"Error consultando BD: {exc}", "finished_at": finished})
        return {
            "status": "error",
            "processed": 0,
            "updated": 0,
            "errors": 1,
            "details": [],
            "started_at": started,
            "finished_at": finished,
            "message": str(exc),
        }

    if not pending:
        finished = _utc_now()
        _set_status("No hay posts con métricas pendientes en este ciclo.", status="idle")
        current_run["finished_at"] = finished
        try:
            db.update_metrics_collection_run(finished)
        except Exception:
            pass
        return {
            "status": "ok",
            "processed": 0,
            "updated": 0,
            "errors": 0,
            "details": [],
            "started_at": started,
            "finished_at": finished,
            "message": current_run["message"],
        }

    details: list[dict] = []
    updated = 0
    errors = 0
    for post in pending:
        post_id = post["id"]
        topic = post.get("topic", "")
        url = post.get("linkedin_url", "")
        _set_status(f"Scrapeando métricas de '{topic[:60]}'...", status="running")
        try:
            scraped = linkedin_mod.scrape_post_metrics(url, log=lambda msg: _set_status(msg, status="running"))
        except Exception as exc:
            logger.exception(
                "Error en scrape_post_metrics durante el ciclo",
                extra={"event": "metrics_collector.scrape_failed", "post_id": post_id},
            )
            details.append({"post_id": post_id, "topic": topic, "result": "error", "message": str(exc)})
            errors += 1
            continue

        if not scraped or not any((scraped or {}).get(k, 0) for k in ("impressions", "reactions", "comments", "reposts", "saves", "link_clicks", "profile_visits")):
            details.append(
                {"post_id": post_id, "topic": topic, "result": "empty", "message": "Sin datos detectados"}
            )
            continue

        try:
            db.save_post_metrics(
                post_id,
                impressions=int(scraped.get("impressions") or 0),
                reactions=int(scraped.get("reactions") or 0),
                comments=int(scraped.get("comments") or 0),
                reposts=int(scraped.get("reposts") or 0),
                profile_visits=int(scraped.get("profile_visits") or 0),
                link_clicks=int(scraped.get("link_clicks") or 0),
                saves=int(scraped.get("saves") or 0),
            )
            updated += 1
            details.append({"post_id": post_id, "topic": topic, "result": "updated", "message": "Métricas actualizadas"})
        except Exception as exc:
            logger.exception(
                "Error guardando métricas en BD",
                extra={"event": "metrics_collector.save_failed", "post_id": post_id},
            )
            details.append({"post_id": post_id, "topic": topic, "result": "error", "message": str(exc)})
            errors += 1

    finished = _utc_now()
    current_run.update(
        {
            "status": "idle",
            "message": f"Ciclo terminado. Actualizados: {updated} · Errores: {errors}",
            "processed": len(pending),
            "updated": updated,
            "errors": errors,
            "finished_at": finished,
        }
    )
    try:
        db.update_metrics_collection_run(finished)
    except Exception:
        pass
    return {
        "status": "ok",
        "processed": len(pending),
        "updated": updated,
        "errors": errors,
        "details": details,
        "started_at": started,
        "finished_at": finished,
        "message": current_run["message"],
    }
