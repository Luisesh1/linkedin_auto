"""
Automated post scheduler.
Runs in a background daemon thread, checks every 30 s whether it's time
to execute the full pipeline (trends → content → image → publish).
"""

import threading
import time
from datetime import datetime, timedelta

_thread: threading.Thread | None = None
_stop_event = threading.Event()

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


def compute_next_run(cfg: dict) -> str | None:
    """Return ISO string of next scheduled run time, or None if disabled."""
    if not cfg.get("enabled"):
        return None
    now = datetime.utcnow()
    days_of_week = cfg.get("days_of_week", [])  # [] = all days allowed

    def _day_allowed(dt: datetime) -> bool:
        return not days_of_week or dt.weekday() in days_of_week

    if cfg["mode"] == "interval":
        hours = float(cfg.get("interval_hours", 24))
        last = cfg.get("last_run_at")
        if last:
            base = datetime.fromisoformat(last)
            nxt = base + timedelta(hours=hours)
            if nxt <= now:
                nxt = now + timedelta(hours=hours)
        else:
            nxt = now + timedelta(hours=hours)
        # Advance day-by-day until we land on an allowed weekday
        if days_of_week:
            for _ in range(14):
                if _day_allowed(nxt):
                    break
                nxt += timedelta(days=1)
        return nxt.isoformat()

    # mode == "times"
    times = cfg.get("times_of_day", [])
    if not times:
        return None
    candidates = []
    # Search up to 8 days ahead so day filtering can always find a match
    for day_offset in range(8):
        check_date = now + timedelta(days=day_offset)
        if not _day_allowed(check_date):
            continue
        for t in times:
            try:
                h, m = map(int, t.split(":"))
                candidate = check_date.replace(hour=h, minute=m, second=0, microsecond=0)
                if candidate > now:
                    candidates.append(candidate)
            except Exception:
                continue
    if not candidates:
        return None
    return min(candidates).isoformat()


# ─── Internal loop ────────────────────────────────────────────────────────────

def _loop():
    # Import here to avoid circular imports at module load time
    from src import db, trends, content, image_gen, linkedin

    while not _stop_event.is_set():
        try:
            _tick(db, trends, content, image_gen, linkedin)
        except Exception as e:
            print(f"[scheduler] Unexpected error in tick: {e}")
        _stop_event.wait(30)  # check every 30 seconds


def _tick(db, trends_mod, content_mod, image_gen_mod, linkedin_mod):
    cfg = db.get_schedule()

    if not cfg.get("enabled"):
        return

    next_run = cfg.get("next_run_at")
    if not next_run:
        # First boot with enabled=True but no next_run computed
        nxt = compute_next_run(cfg)
        db.update_schedule_run_times(cfg.get("last_run_at", ""), nxt)
        return

    try:
        next_dt = datetime.fromisoformat(next_run)
    except Exception:
        return

    if datetime.utcnow() < next_dt:
        return  # Not yet

    # ── It's time to run ──────────────────────────────────────────────────────
    current_run.update({"status": "running", "message": "Iniciando pipeline programado...", "topic": ""})
    started = datetime.utcnow().isoformat()
    run_id = db.log_schedule_run(started, "running")

    def log(msg):
        current_run["message"] = msg
        print(f"[scheduler] {msg}")

    try:
        category_cfg = db.get_default_pipeline_category()
        if not category_cfg:
            raise RuntimeError("No hay una categoría predeterminada configurada.")

        log("Obteniendo tendencias...")
        topic_list = trends_mod.get_trending_topics(category_cfg=category_cfg)

        log("Revisando historial...")
        history = linkedin_mod.get_recent_posts_local(5)

        log("Generando contenido con Grok...")
        post_data = content_mod.generate_post(topic_list, history, category_cfg=category_cfg)
        current_run["topic"] = post_data["topic"]

        log(f"Generando imagen para: {post_data['topic']}...")
        img_data = image_gen_mod.generate_image(post_data["topic"], category_cfg=category_cfg)

        log("Publicando en LinkedIn...")
        linkedin_mod.publish_post(
            post_text=post_data["post_text"],
            image_path=img_data["image_path"],
            log=log,
        )

        db.save_post(
            topic=post_data["topic"],
            post_text=post_data["post_text"],
            category=category_cfg["name"],
            image_path=img_data.get("image_path", ""),
            image_url=img_data.get("image_url", ""),
            image_desc=img_data.get("image_desc", ""),
            prompt_used=img_data.get("prompt_used", ""),
            published=True,
        )
        linkedin_mod.save_to_history(
            post_data["topic"],
            post_data["post_text"],
            category=category_cfg["name"],
        )

        db.finish_schedule_run(run_id, "done", post_data["topic"], "Publicado correctamente.")
        current_run.update({"status": "idle", "message": f"Último post: {post_data['topic']}", "topic": post_data["topic"]})
        log(f"¡Post programado publicado! Tema: {post_data['topic']}")

    except Exception as e:
        msg = str(e)
        db.finish_schedule_run(run_id, "error", current_run.get("topic", ""), msg)
        current_run.update({"status": "error", "message": msg})
        log(f"Error en publicación programada: {msg}")

    finally:
        # Always advance next_run regardless of success/failure
        now_iso = datetime.utcnow().isoformat()
        cfg_fresh = db.get_schedule()
        nxt = compute_next_run(cfg_fresh)
        db.update_schedule_run_times(now_iso, nxt)
