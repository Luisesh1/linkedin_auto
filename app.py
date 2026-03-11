"""
LinkedIn Auto-Poster — Flask Application
Browser automation via Playwright (no LinkedIn API).
"""

import json
import os
import threading
import uuid

import yaml
from flask import Flask, Response, jsonify, render_template, request

from src import content, db, image_gen, linkedin, scheduler, trends

# ─── App setup ────────────────────────────────────────────────────────────────

app = Flask(__name__)


def load_config() -> dict:
    with open("config.yaml") as f:
        return yaml.safe_load(f)


# In-memory session store: session_id -> pipeline result
pipeline_results: dict = {}

# Login job store: job_id -> {status, message}
login_jobs: dict = {}


# ─── SSE helper ───────────────────────────────────────────────────────────────

def sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


# ─── Main page ────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/automation")
def automation():
    return render_template("automation.html")


@app.route("/settings")
def settings():
    return render_template("settings.html")


# ─── Auth routes (Playwright-based) ──────────────────────────────────────────

@app.route("/auth/status")
def auth_status():
    valid = linkedin.is_session_valid()
    days = linkedin.session_days_left()
    return jsonify({
        "authenticated": valid,
        "days_left": days,
        "needs_reconnect": 0 < days < 7,
    })


@app.route("/auth/login", methods=["POST"])
def auth_login():
    """
    Start a LinkedIn login job in background thread.
    Returns a job_id to poll via /auth/login_status/<job_id>.
    The Playwright browser will open (headed) so the user can
    complete 2FA / CAPTCHA if needed.
    """
    cfg = load_config()
    email = cfg["linkedin"].get("email", "")
    password = cfg["linkedin"].get("password", "")

    if not email or not password:
        return jsonify({"error": "Configura email y password de LinkedIn en config.yaml"}), 400

    job_id = str(uuid.uuid4())
    login_jobs[job_id] = {"status": "running", "message": "Abriendo navegador..."}

    def run_login():
        def log(msg):
            login_jobs[job_id]["message"] = msg

        success = linkedin.login(email, password, log=log)
        if success:
            login_jobs[job_id]["status"] = "done"
            login_jobs[job_id]["message"] = "Sesión iniciada correctamente."
        else:
            login_jobs[job_id]["status"] = "error"
            login_jobs[job_id]["message"] = (
                login_jobs[job_id].get("message") or "No se pudo iniciar sesión."
            )

    t = threading.Thread(target=run_login, daemon=True)
    t.start()

    return jsonify({"job_id": job_id})


@app.route("/auth/login_status/<job_id>")
def auth_login_status(job_id: str):
    job = login_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job no encontrado"}), 404
    return jsonify(job)


@app.route("/auth/disconnect", methods=["POST"])
def auth_disconnect():
    linkedin._clear_session()
    return jsonify({"success": True})


# ─── Pipeline SSE ─────────────────────────────────────────────────────────────

@app.route("/api/run")
def api_run():
    test_mode = request.args.get("test") == "1"
    category_name = request.args.get("category") or None
    from_step = int(request.args.get("from_step", 1))
    prev_session = request.args.get("session_id", "")

    def generate():
        # Reuse session when resuming mid-pipeline, create new one for full runs
        session_id = prev_session if prev_session and from_step > 1 else str(uuid.uuid4())
        existing = pipeline_results.get(session_id, {})

        # Resolve category (reuse stored one when resuming)
        cat_name = category_name or existing.get("category_name") or None
        category_cfg = db.get_pipeline_category(cat_name)
        if not category_cfg:
            yield sse({"step": 0, "status": "error", "message": "No hay categorías configuradas."})
            return

        # Announce session_id so the frontend can enable per-step regen buttons
        yield sse({"type": "init", "session_id": session_id})

        if not test_mode and not linkedin.is_session_valid():
            yield sse({"step": 0, "status": "error",
                       "message": "No hay sesión de LinkedIn activa. Inicia sesión primero."})
            return

        # ── Step 1: Trending topics ──────────────────────────────────────────
        if from_step <= 1:
            yield sse({"step": 1, "status": "running", "message": "Investigando temas de tendencia..."})
            try:
                topic_list = trends.get_trending_topics(category_cfg=category_cfg)
                pipeline_results.setdefault(session_id, {}).update({
                    "topic_list": topic_list,
                    "category_name": category_cfg["name"],
                })
                yield sse({"step": 1, "status": "done", "result": topic_list})
            except Exception as e:
                yield sse({"step": 1, "status": "error", "message": f"Error al obtener tendencias: {e}"})
                return
        else:
            topic_list = existing.get("topic_list", [])
            yield sse({"step": 1, "status": "done", "result": topic_list, "skipped": True})

        # ── Step 2: Post history ─────────────────────────────────────────────
        if from_step <= 2:
            yield sse({"step": 2, "status": "running", "message": "Revisando publicaciones recientes..."})
            try:
                history = linkedin.get_recent_posts_local(5)
                pipeline_results.setdefault(session_id, {})["history"] = history
                yield sse({"step": 2, "status": "done",
                           "result": [h.get("topic", "") for h in history]})
            except Exception as e:
                yield sse({"step": 2, "status": "error", "message": f"Error al leer historial: {e}"})
                return
        else:
            history = existing.get("history", [])
            yield sse({"step": 2, "status": "done",
                       "result": [h.get("topic", "") for h in history], "skipped": True})

        # ── Step 3: Generate post ────────────────────────────────────────────
        if from_step <= 3:
            yield sse({"step": 3, "status": "running", "message": "Generando contenido con Claude AI..."})
            try:
                post_data = content.generate_post(topic_list, history, category_cfg=category_cfg)
                pipeline_results.setdefault(session_id, {}).update({
                    "topic": post_data["topic"],
                    "post_text": post_data["post_text"],
                    "reasoning": post_data.get("reasoning", ""),
                })
                yield sse({"step": 3, "status": "done", "result": {
                    "topic": post_data["topic"],
                    "reasoning": post_data.get("reasoning", ""),
                }})
            except Exception as e:
                yield sse({"step": 3, "status": "error", "message": f"Error al generar contenido: {e}"})
                return
        else:
            post_data = {
                "topic": existing.get("topic", ""),
                "post_text": existing.get("post_text", ""),
                "reasoning": existing.get("reasoning", ""),
            }
            yield sse({"step": 3, "status": "done", "result": {
                "topic": post_data["topic"],
                "reasoning": post_data["reasoning"],
            }, "skipped": True})

        # ── Step 4: Generate image ───────────────────────────────────────────
        if from_step <= 4:
            yield sse({"step": 4, "status": "running",
                       "message": "Generando imagen con Grok (estilo anime)..."})
            try:
                img_data = image_gen.generate_image(post_data["topic"], category_cfg=category_cfg)
                pipeline_results.setdefault(session_id, {}).update({
                    "image_path": img_data["image_path"],
                    "image_url": img_data["image_url"],
                    "image_desc": img_data.get("image_desc", ""),
                    "prompt_used": img_data.get("prompt_used", ""),
                })
                yield sse({"step": 4, "status": "done",
                           "result": {"image_url": img_data["image_url"]}})
            except Exception as e:
                yield sse({"step": 4, "status": "error", "message": f"Error al generar imagen: {e}"})
                return
        else:
            img_data = {
                "image_path": existing.get("image_path", ""),
                "image_url": existing.get("image_url", ""),
                "image_desc": existing.get("image_desc", ""),
                "prompt_used": existing.get("prompt_used", ""),
            }
            yield sse({"step": 4, "status": "done",
                       "result": {"image_url": img_data["image_url"]}, "skipped": True})

        # ── Step 5: Preview ──────────────────────────────────────────────────
        stored = pipeline_results.get(session_id, {})
        yield sse({
            "step": 5,
            "status": "preview",
            "session_id": session_id,
            "post_text": stored.get("post_text", post_data.get("post_text", "")),
            "image_url": stored.get("image_url", img_data.get("image_url", "")),
            "topic": stored.get("topic", post_data.get("topic", "")),
            "category": category_cfg["name"],
            "reasoning": stored.get("reasoning", post_data.get("reasoning", "")),
            "test_mode": test_mode,
        })

    return Response(
        generate(),
        content_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ─── Publish (Playwright) ─────────────────────────────────────────────────────

@app.route("/api/publish", methods=["POST"])
def api_publish():
    data = request.get_json()
    session_id = data.get("session_id")
    result = pipeline_results.get(session_id)

    if not result:
        return jsonify({"error": "Sesión expirada o no encontrada. Genera el post nuevamente."}), 404

    if not linkedin.is_session_valid():
        return jsonify({"error": "Sesión de LinkedIn expirada. Inicia sesión nuevamente."}), 401

    post_text = data.get("post_text_override") or result["post_text"]

    # Run in background thread so SSE connection doesn't time out
    job_id = str(uuid.uuid4())
    publish_jobs[job_id] = {"status": "running", "message": "Abriendo navegador...", "screenshots": []}

    def run_publish():
        def log(msg):
            publish_jobs[job_id]["message"] = msg

        def on_screenshot(url):
            publish_jobs[job_id]["screenshots"].append(url)

        try:
            pub_result = linkedin.publish_post(
                post_text=post_text,
                image_path=result["image_path"],
                log=log,
                on_screenshot=on_screenshot,
            )
            # Persist to SQLite
            db.save_post(
                topic=result["topic"],
                post_text=post_text,
                category=result.get("category", "default"),
                image_path=result.get("image_path", ""),
                image_url=result.get("image_url", ""),
                image_desc=result.get("image_desc", ""),
                prompt_used=result.get("prompt_used", ""),
                published=True,
            )
            linkedin.save_to_history(
                result["topic"],
                post_text,
                category=result.get("category", "default"),
            )
            pipeline_results.pop(session_id, None)
            publish_jobs[job_id]["status"] = "done"
            publish_jobs[job_id]["message"] = "Post publicado exitosamente."
            publish_jobs[job_id]["screenshots"] = pub_result.get("screenshots", [])
        except Exception as e:
            publish_jobs[job_id]["status"] = "error"
            publish_jobs[job_id]["message"] = str(e)
            publish_jobs[job_id]["screenshots"] = publish_jobs[job_id].get("screenshots", [])

    t = threading.Thread(target=run_publish, daemon=True)
    t.start()
    return jsonify({"job_id": job_id})


publish_jobs: dict = {}


@app.route("/api/publish_status/<job_id>")
def api_publish_status(job_id: str):
    job = publish_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job no encontrado"}), 404
    return jsonify(job)


# ─── Headless setting ─────────────────────────────────────────────────────────

@app.route("/api/headless", methods=["GET"])
def get_headless():
    cfg = load_config()
    return jsonify({"headless": bool(cfg.get("app", {}).get("headless", True))})


@app.route("/api/headless", methods=["POST"])
def set_headless():
    headless = request.get_json().get("headless", True)
    # Read → update → write config.yaml
    with open("config.yaml") as f:
        text = f.read()
    import re
    # Replace the headless value in-place
    text = re.sub(
        r"(headless\s*:\s*)(true|false)",
        lambda m: m.group(1) + ("true" if headless else "false"),
        text,
    )
    with open("config.yaml", "w") as f:
        f.write(text)
    return jsonify({"headless": headless})


# ─── Debug ────────────────────────────────────────────────────────────────────

@app.route("/debug/screenshot")
def debug_screenshot():
    """Serve the last captured debug screenshot."""
    import glob as _glob
    screenshots = sorted(_glob.glob("static/debug/*.png"))
    if not screenshots:
        return "No hay capturas de debug todavía.", 404
    latest = screenshots[-1]
    name = os.path.basename(latest)
    html_file = latest.replace(".png", ".html")
    has_html = os.path.exists(html_file)
    links = "".join(
        f'<li><a href="/static/debug/{os.path.basename(s)}">{os.path.basename(s)}</a>'
        + (f' | <a href="/static/debug/{os.path.basename(s.replace(".png",".html"))}">HTML</a>' if os.path.exists(s.replace(".png",".html")) else "")
        + "</li>"
        for s in sorted(_glob.glob("static/debug/*.png"))
    )
    return f"""<html><body>
<h2>Debug Screenshots</h2><ul>{links}</ul>
<h3>Latest: {name}</h3>
<img src="/static/debug/{name}" style="max-width:100%">
</body></html>"""


# ─── Schedule ─────────────────────────────────────────────────────────────────

@app.route("/api/schedule", methods=["GET"])
def get_schedule():
    cfg = db.get_schedule()
    runs = db.get_schedule_runs(limit=5)
    return jsonify({
        "config": cfg,
        "current_run": scheduler.current_run,
        "recent_runs": runs,
    })


@app.route("/api/schedule", methods=["POST"])
def save_schedule():
    data = request.get_json()
    enabled = bool(data.get("enabled", False))
    mode = data.get("mode", "interval")
    interval_hours = float(data.get("interval_hours", 24))
    times_of_day = data.get("times_of_day", [])
    days_of_week = data.get("days_of_week", [])

    cfg = {
        "enabled": enabled,
        "mode": mode,
        "interval_hours": interval_hours,
        "times_of_day": times_of_day,
        "days_of_week": days_of_week,
        "last_run_at": db.get_schedule().get("last_run_at"),
    }
    next_run = scheduler.compute_next_run(cfg)
    db.save_schedule(enabled, mode, interval_hours, times_of_day, next_run, days_of_week)
    return jsonify({"ok": True, "next_run_at": next_run})


@app.route("/api/schedule/run_now", methods=["POST"])
def schedule_run_now():
    """Trigger an immediate scheduled run (for testing)."""
    if scheduler.current_run.get("status") == "running":
        return jsonify({"error": "Ya hay una publicación programada en curso."}), 409

    def run():
        from src import trends as t, content as c, image_gen as ig, linkedin as li
        from src.scheduler import _tick
        from src import db as _db
        # Force next_run to now so _tick fires
        _db.update_schedule_run_times(
            _db.get_schedule().get("last_run_at", ""),
            "2000-01-01T00:00:00"
        )
        _tick(_db, t, c, ig, li)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True})


# ─── Post history ─────────────────────────────────────────────────────────────

@app.route("/api/history")
def api_history():
    posts = db.get_posts(limit=20, published_only=True)
    return jsonify({"posts": posts})


@app.route("/api/history/<int:post_id>")
def api_history_detail(post_id: int):
    posts = db.get_posts(limit=1000)
    match = next((p for p in posts if p["id"] == post_id), None)
    if not match:
        return jsonify({"error": "Post no encontrado"}), 404
    return jsonify(match)


@app.route("/api/categories", methods=["GET"])
def api_categories():
    categories = db.get_pipeline_categories()
    default = db.get_default_pipeline_category()
    return jsonify({
        "categories": categories,
        "default_category": default["name"] if default else "",
    })


@app.route("/api/categories", methods=["POST"])
def api_save_category():
    data = request.get_json() or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "El nombre de la categoría es obligatorio."}), 400

    try:
        category = db.save_pipeline_category(
            category_id=data.get("id"),
            name=name,
            description=(data.get("description") or "").strip(),
            trends_prompt=(data.get("trends_prompt") or "").strip(),
            history_prompt=(data.get("history_prompt") or "").strip(),
            content_prompt=(data.get("content_prompt") or "").strip(),
            image_prompt=(data.get("image_prompt") or "").strip(),
            is_default=bool(data.get("is_default", False)),
        )
        return jsonify({"ok": True, "category": category})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/categories/<int:category_id>", methods=["DELETE"])
def api_delete_category(category_id: int):
    try:
        db.delete_pipeline_category(category_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


# ─── Run ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not os.path.exists("config.yaml"):
        import shutil
        shutil.copy("config.yaml.example", "config.yaml")
        print("⚠  config.yaml creado desde el ejemplo. Rellena tus credenciales.")

    os.makedirs(os.path.join("static", "generated"), exist_ok=True)
    os.makedirs(os.path.join("static", "debug"), exist_ok=True)
    db.init_db()
    scheduler.start()

    cfg = load_config()
    port = cfg["app"].get("port", 5000)
    debug = cfg["app"].get("debug", False)
    app.secret_key = cfg["app"].get("secret_key", "dev-secret")

    print(f"\n LinkedIn Auto-Poster corriendo en http://localhost:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=debug, threaded=True)
