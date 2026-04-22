"""
Automated post scheduler.
Runs in a background daemon thread, checks every 30 s whether it's time
to execute the full pipeline with feedback gates before publishing.
"""

import threading
import time
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from src import pipeline
from src.config import get_setting
from src.logging_utils import get_logger

_thread: threading.Thread | None = None
_stop_event = threading.Event()
logger = get_logger(__name__)

# Current run status exposed to the Flask app
current_run: dict = {"status": "idle", "message": "", "topic": ""}


# ─── Public API ───────────────────────────────────────────────────────────────

def start():
    """Start the scheduler background thread (idempotent)."""
    global _thread
    if _thread and _thread.is_alive():
        return
    _stop_event.clear()
    _thread = threading.Thread(target=_loop, daemon=True, name="scheduler")
    _thread.start()


def stop():
    _stop_event.set()


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _schedule_timezone_name() -> str:
    configured = str(get_setting("app", "timezone", "") or "").strip()
    if configured:
        return configured
    browser_timezone = str(get_setting("linkedin_browser", "timezone_id", "UTC") or "").strip()
    return browser_timezone or "UTC"


def _schedule_timezone() -> ZoneInfo:
    timezone_name = _schedule_timezone_name()
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        logger.warning(
            "Timezone configurada inválida; usando UTC",
            extra={"event": "scheduler.invalid_timezone", "timezone": timezone_name},
        )
        return ZoneInfo("UTC")


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def compute_next_run(cfg: dict) -> str | None:
    """Return ISO string of next scheduled run time, or None if disabled."""
    nxt, _ = compute_next_run_with_category(cfg)
    return nxt


def compute_next_run_with_category(cfg: dict) -> tuple[str | None, str]:
    """Return (next_run_iso, category) tuple. category is '' unless mode == 'rules'."""
    if not cfg.get("enabled"):
        return None, ""
    now_utc = _utc_now()
    schedule_tz = _schedule_timezone()
    now_local = now_utc.astimezone(schedule_tz)

    mode = cfg.get("mode", "interval")

    if mode == "rules":
        rules = cfg.get("rules") or []
        candidates: list[tuple[datetime, str]] = []
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            rule_days = rule.get("days") or []
            rule_times = rule.get("times") or []
            rule_category = str(rule.get("category") or "").strip()
            if not rule_times:
                continue
            for day_offset in range(8):
                check_date = now_local + timedelta(days=day_offset)
                if rule_days and check_date.weekday() not in rule_days:
                    continue
                for t in rule_times:
                    try:
                        h, m = map(int, str(t).split(":"))
                        candidate_local = check_date.replace(hour=h, minute=m, second=0, microsecond=0)
                        if candidate_local > now_local:
                            candidates.append((candidate_local.astimezone(UTC), rule_category))
                    except Exception:
                        continue
        if not candidates:
            return None, ""
        candidates.sort(key=lambda x: x[0])
        winner = candidates[0]
        return winner[0].isoformat(), winner[1]

    days_of_week = cfg.get("days_of_week", [])  # [] = all days allowed

    def _day_allowed(dt: datetime) -> bool:
        return not days_of_week or dt.weekday() in days_of_week

    if mode == "interval":
        hours = float(cfg.get("interval_hours", 24))
        last = _parse_iso_datetime(cfg.get("last_run_at"))
        if last:
            nxt_utc = last + timedelta(hours=hours)
            if nxt_utc <= now_utc:
                nxt_utc = now_utc + timedelta(hours=hours)
        else:
            nxt_utc = now_utc + timedelta(hours=hours)
        nxt_local = nxt_utc.astimezone(schedule_tz)
        # Advance day-by-day until we land on an allowed weekday
        if days_of_week:
            for _ in range(14):
                if _day_allowed(nxt_local):
                    break
                nxt_local += timedelta(days=1)
        return nxt_local.astimezone(UTC).isoformat(), ""

    # mode == "times"
    times = cfg.get("times_of_day", [])
    if not times:
        return None, ""
    candidates_times: list[datetime] = []
    # Search up to 8 days ahead so day filtering can always find a match
    for day_offset in range(8):
        check_date = now_local + timedelta(days=day_offset)
        if not _day_allowed(check_date):
            continue
        for t in times:
            try:
                h, m = map(int, t.split(":"))
                candidate_local = check_date.replace(hour=h, minute=m, second=0, microsecond=0)
                if candidate_local > now_local:
                    candidates_times.append(candidate_local.astimezone(UTC))
            except Exception:
                continue
    if not candidates_times:
        return None, ""
    return min(candidates_times).isoformat(), ""


# ─── Internal loop ────────────────────────────────────────────────────────────

def _loop():
    # Import here to avoid circular imports at module load time
    from src import db, linkedin

    while not _stop_event.is_set():
        try:
            _tick(db, linkedin)
        except Exception as e:
            logger.exception("Unexpected error in scheduler tick", extra={"event": "scheduler.tick_error"})
        try:
            _metrics_tick(db, linkedin)
        except Exception as e:
            logger.exception(
                "Unexpected error in metrics-collection tick",
                extra={"event": "scheduler.metrics_tick_error"},
            )
        _stop_event.wait(30)  # check every 30 seconds


def _metrics_tick(db, linkedin_mod):
    """Run the metrics-collection cycle when its interval has elapsed.

    Reuses the same daemon thread as the post-publishing tick. Honors the
    `metrics_collection_enabled` flag and `metrics_collection_interval_hours`
    column on schedule_config.
    """
    cfg = db.get_schedule()
    if not cfg.get("metrics_collection_enabled"):
        return

    last_collected_iso = cfg.get("metrics_last_collected_at") or ""
    interval_hours = float(cfg.get("metrics_collection_interval_hours") or 6)
    if last_collected_iso:
        try:
            last_collected = _parse_iso_datetime(last_collected_iso)
        except Exception:
            last_collected = None
        if last_collected and (_utc_now() - last_collected) < timedelta(hours=interval_hours):
            return

    from src import metrics_collector

    if metrics_collector.current_run.get("status") == "running":
        return

    logger.info(
        "Iniciando ciclo automático de recolección de métricas",
        extra={"event": "scheduler.metrics_tick_start"},
    )
    metrics_collector.collect_metrics_cycle(db, linkedin_mod)


def _tick(db, linkedin_mod):
    cfg = db.get_schedule()

    if not cfg.get("enabled"):
        return

    next_run = cfg.get("next_run_at")
    if not next_run:
        # First boot with enabled=True but no next_run computed
        nxt, nxt_category = compute_next_run_with_category(cfg)
        try:
            db.update_schedule_run_times(cfg.get("last_run_at", ""), nxt, next_run_category=nxt_category)
        except TypeError:
            db.update_schedule_run_times(cfg.get("last_run_at", ""), nxt)
        return

    try:
        next_dt = _parse_iso_datetime(next_run)
    except Exception:
        return

    if not next_dt:
        return

    if _utc_now() < next_dt:
        return  # Not yet

    # ── It's time to run ──────────────────────────────────────────────────────
    current_run.update({"status": "running", "message": "Iniciando pipeline programado...", "topic": ""})
    started = _utc_now().isoformat()
    run_id = db.log_schedule_run(started, "running")

    def log(msg):
        current_run["message"] = msg
        logger.info(msg, extra={"event": "scheduler.status"})

    try:
        if getattr(linkedin_mod, "is_login_in_progress", lambda: False)():
            raise PermissionError("Hay un inicio de sesion manual de LinkedIn en progreso. Reintentando en el siguiente ciclo.")

        if not linkedin_mod.is_session_valid(verify_browser=True, log=log):
            raise PermissionError("No hay una sesión válida de LinkedIn para ejecutar la publicación programada.")

        if cfg.get("mode") == "rules":
            requested_category_name = str(cfg.get("next_run_category", "") or "").strip()
        else:
            requested_category_name = str(cfg.get("category_name", "") or "").strip()
        category_cfg, _ = db.resolve_pipeline_category_choice(requested_category_name or None)
        if not category_cfg:
            if requested_category_name and requested_category_name != db.RANDOM_CATEGORY_NAME:
                raise RuntimeError("La categoría programada ya no existe.")
            raise RuntimeError("No hay categorías configuradas para ejecutar la automatización.")

        current_run["message"] = f"Iniciando pipeline programado con categoría {category_cfg['name']}..."

        stage_messages = {
            "candidate_research": "Investigando señales...",
            "candidate_scoring": "Midiendo novedad del tema...",
            "brief_generation": "Preparando brief editorial...",
            "copy_validation": "Generando y validando copy...",
            "visual_validation": "Generando imagen con control de diversidad...",
            "publish_gate": "Ejecutando gate final de calidad...",
        }

        payload = pipeline.run_feedback_pipeline(
            category_cfg=category_cfg,
            history_fetcher=linkedin_mod.get_recent_posts_local,
            emit=lambda event: log(stage_messages.get(event.get("stage", ""), event.get("message", "Procesando...")))
            if event.get("status") == "running"
            else None,
        )
        current_run["topic"] = payload.get("topic", "")

        log("Publicando en LinkedIn...")
        linkedin_mod.publish_post(
            post_text=payload["post_text"],
            image_path=payload["image_path"],
            log=log,
        )

        db.save_post(
            topic=payload["topic"],
            post_text=payload["post_text"],
            category=category_cfg["name"],
            image_path=payload.get("image_path", ""),
            image_url=payload.get("image_url", ""),
            image_desc=payload.get("image_desc", ""),
            prompt_used=payload.get("prompt_used", ""),
            pillar=payload.get("content_brief", {}).get("pillar", ""),
            topic_signature=payload.get("selected_candidate", {}).get("topic_signature", ""),
            angle_signature=payload.get("angle_signature", ""),
            content_format=payload.get("content_brief", {}).get("content_format", ""),
            cta_type=payload.get("cta_type", ""),
            hook_type=payload.get("hook_type", ""),
            visual_style=payload.get("visual_style", ""),
            composition_type=payload.get("composition_type", ""),
            color_direction=payload.get("color_direction", ""),
            quality_score=float(payload.get("quality_score", 0) or 0),
            published=True,
        )

        db.finish_schedule_run(run_id, "done", payload["topic"], "Publicado correctamente.")
        current_run.update({"status": "idle", "message": f"Último post: {payload['topic']}", "topic": payload["topic"]})
        log(f"¡Post programado publicado! Tema: {payload['topic']}")

    except Exception as e:
        msg = str(e)
        db.finish_schedule_run(run_id, "error", current_run.get("topic", ""), msg)
        current_run.update({"status": "error", "message": msg})
        log(f"Error en publicación programada: {msg}")

    finally:
        # Always advance next_run regardless of success/failure
        now_iso = _utc_now().isoformat()
        cfg_fresh = db.get_schedule()
        nxt, nxt_category = compute_next_run_with_category(cfg_fresh)
        try:
            db.update_schedule_run_times(now_iso, nxt, next_run_category=nxt_category)
        except TypeError:
            # Backward compat: older db.update_schedule_run_times without the kwarg (e.g. test fakes)
            db.update_schedule_run_times(now_iso, nxt)
