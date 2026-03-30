"""
LinkedIn Auto-Poster Flask application.
Browser automation via Playwright (no LinkedIn API).
"""

from __future__ import annotations

import hmac
import json
import os
import secrets
import threading
import time
from datetime import timedelta

from flask import Flask, Response, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash

from src import db, linkedin, pipeline, scheduler
from src.config import ensure_local_config, get_settings, reload_settings, update_yaml_setting
from src.logging_utils import configure_logging, get_logger
from src.validation import (
    ValidationError,
    ensure_dict,
    parse_bool,
    parse_float,
    parse_int,
    parse_string,
    parse_string_list,
    parse_times_of_day,
    parse_weekdays,
)

configure_logging()
logger = get_logger(__name__)
app = Flask(__name__)
_runtime_ready = False
_pipeline_workers: dict[str, threading.Thread] = {}
_pipeline_worker_lock = threading.Lock()
_SESSION_META_KEYS = {"events", "last_event", "preview_data", "test_mode"}
_LOGIN_ATTEMPTS: dict[str, dict] = {}
_UNSAFE_HTTP_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
_AUTH_EXEMPT_ENDPOINTS = {"static", "login_page", "login_submit"}


def sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


def _settings() -> dict:
    return get_settings()


def _security_settings() -> dict:
    return _settings().get("security", {})


def _admin_username() -> str:
    return str(_security_settings().get("admin_username", "admin") or "admin").strip() or "admin"


def _admin_password_hash() -> str:
    return str(_security_settings().get("admin_password_hash", "") or "").strip()


def _admin_password_plain() -> str:
    return str(_security_settings().get("admin_password", "") or "")


def _security_ready() -> bool:
    return bool(_admin_password_hash() or _admin_password_plain())


def _session_timeout_seconds() -> int:
    minutes = int(_security_settings().get("session_timeout_minutes", 480) or 480)
    return max(300, minutes * 60)


def _login_rate_limit_settings() -> tuple[int, int, int]:
    sec = _security_settings()
    max_attempts = int(sec.get("max_login_attempts", 5) or 5)
    window_seconds = max(60, int(sec.get("login_window_minutes", 15) or 15) * 60)
    lockout_seconds = max(60, int(sec.get("lockout_minutes", 15) or 15) * 60)
    return max_attempts, window_seconds, lockout_seconds


def _client_identifier() -> str:
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.remote_addr or "unknown"


def _safe_redirect_target(value: str | None) -> str:
    target = str(value or "").strip()
    if not target or not target.startswith("/") or target.startswith("//"):
        return url_for("index")
    return target


def _is_authenticated() -> bool:
    if not session.get("admin_authenticated"):
        return False
    if session.get("admin_username") != _admin_username():
        return False
    last_seen = float(session.get("last_seen_at", 0) or 0)
    if not last_seen or time.time() - last_seen > _session_timeout_seconds():
        _clear_admin_session()
        return False
    return True


def _issue_csrf_token() -> str:
    token = str(session.get("csrf_token", "") or "")
    if not token:
        token = secrets.token_urlsafe(32)
        session["csrf_token"] = token
    return token


def _clear_admin_session() -> None:
    session.clear()
    _issue_csrf_token()


def _mark_authenticated() -> None:
    session.clear()
    session["admin_authenticated"] = True
    session["admin_username"] = _admin_username()
    session["last_seen_at"] = time.time()
    session.permanent = True
    _issue_csrf_token()


def _is_json_request() -> bool:
    return request.path.startswith("/api/") or request.path.startswith("/auth/") or request.path.startswith("/debug/")


def _validate_csrf() -> bool:
    session_token = str(session.get("csrf_token", "") or "")
    if not session_token:
        return False
    header_token = request.headers.get("X-CSRF-Token", "")
    form_token = request.form.get("csrf_token", "")
    body_token = ""
    if request.is_json:
        payload = request.get_json(silent=True) or {}
        if isinstance(payload, dict):
            body_token = str(payload.get("csrf_token", "") or "")
    provided = header_token or form_token or body_token
    return bool(provided) and hmac.compare_digest(session_token, provided)


def _record_failed_login() -> tuple[bool, str]:
    client_id = _client_identifier()
    now = time.time()
    max_attempts, window_seconds, lockout_seconds = _login_rate_limit_settings()
    state = _LOGIN_ATTEMPTS.setdefault(client_id, {"failed": [], "locked_until": 0.0})
    state["failed"] = [stamp for stamp in state["failed"] if now - stamp <= window_seconds]
    state["failed"].append(now)
    if len(state["failed"]) >= max_attempts:
        state["locked_until"] = now + lockout_seconds
        return False, "Demasiados intentos fallidos. Espera unos minutos antes de reintentar."
    remaining = max_attempts - len(state["failed"])
    return True, f"Credenciales inválidas. Intentos restantes antes del bloqueo: {remaining}."


def _clear_login_failures() -> None:
    _LOGIN_ATTEMPTS.pop(_client_identifier(), None)


def _login_rate_limit_message() -> str | None:
    client_id = _client_identifier()
    state = _LOGIN_ATTEMPTS.get(client_id)
    if not state:
        return None
    locked_until = float(state.get("locked_until", 0) or 0)
    if locked_until <= time.time():
        return None
    seconds_left = max(1, int(locked_until - time.time()))
    minutes = max(1, round(seconds_left / 60))
    return f"Acceso temporalmente bloqueado por seguridad. Reintenta en aproximadamente {minutes} minuto(s)."


def _render_login(*, error: str = "", status: int = 200):
    next_target = _safe_redirect_target(request.args.get("next") or request.form.get("next"))
    return (
        render_template(
            "login.html",
            login_error=error,
            next_target=next_target,
            security_ready=_security_ready(),
            csrf_token=_issue_csrf_token(),
        ),
        status,
    )


def _template_bootstrap() -> dict:
    return {
        "csrfToken": _issue_csrf_token(),
        "currentUser": session.get("admin_username", ""),
        "logoutUrl": url_for("logout"),
        "loginUrl": url_for("login_page"),
    }


def initialize_runtime(*, start_scheduler: bool = True) -> None:
    global _runtime_ready
    if _runtime_ready:
        return
    ensure_local_config()
    os.makedirs(os.path.join("static", "generated"), exist_ok=True)
    os.makedirs(os.path.join("static", "debug"), exist_ok=True)
    db.init_db()
    db.cleanup_expired_state()
    settings = _settings()
    app.secret_key = settings["app"].get("secret_key", "dev-secret")
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = bool(_security_settings().get("require_https_cookies", False))
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(seconds=_session_timeout_seconds())
    app.config["SESSION_REFRESH_EACH_REQUEST"] = True
    app.config["MAX_CONTENT_LENGTH"] = int(float(_security_settings().get("max_content_length_mb", 2) or 2) * 1024 * 1024)
    if start_scheduler:
        scheduler.start()
    _runtime_ready = True


def _json_error(message: str, status: int = 400):
    return jsonify({"error": message}), status


def _parse_json_body() -> dict:
    try:
        return ensure_dict(request.get_json(silent=False), label="body")
    except Exception as exc:
        raise ValidationError("El body debe ser JSON válido.") from exc


def _serialize_job(job: dict) -> dict:
    return {
        "id": job["id"],
        "kind": job["kind"],
        "status": job["status"],
        "message": job["message"],
        **job.get("result", {}),
    }


def _get_session_payload(session_id: str) -> tuple[dict | None, dict | None]:
    session = db.get_pipeline_session(session_id)
    if not session:
        return None, None
    return session, session.get("payload", {})


def _pipeline_execution_payload(payload: dict | None) -> dict:
    clean = dict(payload or {})
    for key in _SESSION_META_KEYS:
        clean.pop(key, None)
    return clean


def _append_pipeline_event(session_id: str, event: dict, *, status: str | None = None, payload_updates: dict | None = None) -> dict | None:
    session = db.get_pipeline_session(session_id)
    if not session:
        return None
    payload = dict(session.get("payload", {}))
    events = list(payload.get("events") or [])
    clean_event = {key: value for key, value in event.items() if value is not None}
    events.append(clean_event)
    payload["events"] = events[-50:]
    payload["last_event"] = clean_event
    if payload_updates:
        payload.update(payload_updates)
    return db.upsert_pipeline_session(session_id, status=status or session.get("status"), payload=payload)


def _serialize_pipeline_session(session: dict) -> dict:
    payload = dict(session.get("payload", {}))
    return {
        "id": session["id"],
        "category": session.get("category", ""),
        "status": session.get("status", ""),
        "updated_at": session.get("updated_at", ""),
        "test_mode": bool(payload.get("test_mode", False)),
        "events": list(payload.get("events") or []),
        "preview": payload.get("preview_data"),
    }


def _run_pipeline_session(session_id: str, category_cfg: dict, *, from_step: int, test_mode: bool) -> None:
    try:
        if not test_mode and not linkedin.is_session_valid(verify_browser=True):
            _append_pipeline_event(
                session_id,
                {
                    "step": 0,
                    "status": "error",
                    "message": "No hay sesión de LinkedIn activa. Inicia sesión primero.",
                },
                status="error",
            )
            return

        stored = (db.get_pipeline_session(session_id) or {}).get("payload", {})

        def emit(event: dict) -> None:
            _append_pipeline_event(session_id, event, status="running")

        payload = pipeline.run_feedback_pipeline(
            category_cfg=category_cfg,
            history_fetcher=linkedin.get_recent_posts_local,
            existing_payload=_pipeline_execution_payload(stored),
            from_step=from_step,
            emit=emit,
        )
        preview_event = {
            "step": 7,
            "status": "preview",
            "session_id": session_id,
            "stage": "preview",
            "post_text": payload.get("post_text", ""),
            "image_url": payload.get("image_url", ""),
            "topic": payload.get("topic", ""),
            "category": category_cfg["name"],
            "reasoning": payload.get("reasoning", ""),
            "content_brief": payload.get("content_brief", {}),
            "quality_checks": payload.get("quality_checks", {}),
            "publish_readiness": payload.get("publish_readiness", {}),
            "test_mode": test_mode,
        }
        db.upsert_pipeline_session(
            session_id,
            status="ready",
            payload={
                **payload,
                "category": category_cfg["name"],
                "category_name": category_cfg["name"],
                "test_mode": test_mode,
                "preview_data": preview_event,
            },
        )
        _append_pipeline_event(session_id, preview_event, status="ready")
    except pipeline.PipelineStageError as exc:
        logger.exception("Error en pipeline con retroalimentación", extra={"event": "pipeline.feedback_error"})
        _append_pipeline_event(
            session_id,
            {"step": exc.step, "status": "error", "message": str(exc)},
            status="needs_regeneration",
        )
    except Exception as exc:
        logger.exception("Error inesperado en pipeline", extra={"event": "pipeline.unexpected_error"})
        _append_pipeline_event(
            session_id,
            {"step": 0, "status": "error", "message": f"Error inesperado del pipeline: {exc}"},
            status="error",
        )
    finally:
        with _pipeline_worker_lock:
            current = _pipeline_workers.get(session_id)
            if current is threading.current_thread():
                _pipeline_workers.pop(session_id, None)


def _ensure_pipeline_worker(session_id: str, category_cfg: dict, *, from_step: int, test_mode: bool) -> None:
    with _pipeline_worker_lock:
        current = _pipeline_workers.get(session_id)
        if current and current.is_alive():
            return
        worker = threading.Thread(
            target=_run_pipeline_session,
            args=(session_id, category_cfg),
            kwargs={"from_step": from_step, "test_mode": test_mode},
            daemon=True,
        )
        _pipeline_workers[session_id] = worker
        worker.start()


def _start_login_job(email: str, password: str) -> str:
    job_id = db.create_job("login", message="Abriendo navegador...")
    db.update_job(job_id, status="running")

    def run_login():
        def log(msg):
            db.update_job(job_id, message=msg)

        try:
            success = linkedin.login(email, password, log=log)
            if success:
                db.update_job(job_id, status="done", message="Sesión iniciada correctamente.")
            else:
                current = db.get_job(job_id) or {}
                db.update_job(
                    job_id,
                    status="error",
                    message=current.get("message") or "No se pudo iniciar sesión.",
                )
        except Exception as exc:
            logger.exception("Error en login de LinkedIn", extra={"event": "auth.login_error"})
            db.update_job(job_id, status="error", message=str(exc))

    threading.Thread(target=run_login, daemon=True).start()
    return job_id


def _start_publish_job(session_id: str, result: dict, post_text: str) -> str:
    job_id = db.create_job(
        "publish",
        message="Abriendo navegador...",
        payload={"session_id": session_id},
    )
    db.update_job(job_id, status="running")

    def run_publish():
        screenshots: list[str] = []

        def log(msg):
            db.update_job(job_id, message=msg)

        def on_screenshot(url):
            screenshots.append(url)
            db.update_job(job_id, result={"screenshots": screenshots})

        try:
            pub_result = linkedin.publish_post(
                post_text=post_text,
                image_path=result["image_path"],
                log=log,
                on_screenshot=on_screenshot,
            )
            db.save_post(
                topic=result["topic"],
                post_text=post_text,
                category=result.get("category", "default"),
                image_path=result.get("image_path", ""),
                image_url=result.get("image_url", ""),
                image_desc=result.get("image_desc", ""),
                prompt_used=result.get("prompt_used", ""),
                pillar=result.get("content_brief", {}).get("pillar", result.get("pillar", "")),
                topic_signature=result.get("selected_candidate", {}).get("topic_signature", result.get("topic_signature", "")),
                angle_signature=result.get("angle_signature", ""),
                content_format=result.get("content_brief", {}).get("content_format", result.get("content_format", "")),
                cta_type=result.get("cta_type", ""),
                hook_type=result.get("hook_type", ""),
                visual_style=result.get("visual_style", ""),
                composition_type=result.get("composition_type", ""),
                color_direction=result.get("color_direction", ""),
                quality_score=float(result.get("quality_score", 0) or 0),
                published=True,
            )
            db.delete_pipeline_session(session_id)
            db.update_job(
                job_id,
                status="done",
                message="Post publicado exitosamente.",
                result={"screenshots": pub_result.get("screenshots", screenshots)},
            )
        except Exception as exc:
            logger.exception("Error publicando post", extra={"event": "publish.error"})
            db.update_job(
                job_id,
                status="error",
                message=str(exc),
                result={"screenshots": screenshots},
            )

    threading.Thread(target=run_publish, daemon=True).start()
    return job_id


initialize_runtime(start_scheduler=False)


@app.context_processor
def inject_template_state():
    return {"app_bootstrap": _template_bootstrap()}


@app.before_request
def enforce_security():
    if request.endpoint in _AUTH_EXEMPT_ENDPOINTS:
        if request.endpoint == "login_page":
            _issue_csrf_token()
            if _is_authenticated():
                return redirect(_safe_redirect_target(request.args.get("next")))
        return None

    if request.endpoint == "login_submit":
        _issue_csrf_token()
        return None

    if not _is_authenticated():
        if _is_json_request():
            return _json_error("Autenticación requerida.", 401)
        return redirect(url_for("login_page", next=request.full_path if request.query_string else request.path))

    session.permanent = True
    session["last_seen_at"] = time.time()
    _issue_csrf_token()

    if request.method in _UNSAFE_HTTP_METHODS and not _validate_csrf():
        logger.warning(
            "Petición bloqueada por CSRF",
            extra={"event": "security.csrf_blocked", "path": request.path, "client": _client_identifier()},
        )
        if _is_json_request():
            return _json_error("Token CSRF inválido o ausente.", 403)
        return ("Token CSRF inválido o ausente.", 403)
    return None


@app.after_request
def apply_security_headers(response):
    if not request.path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-store"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "same-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["Cross-Origin-Opener-Policy"] = "same-origin"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' https://cdn.jsdelivr.net 'unsafe-inline'; "
        "style-src 'self' https://cdn.jsdelivr.net 'unsafe-inline'; "
        "img-src 'self' data:; "
        "font-src 'self' https://cdn.jsdelivr.net data:; "
        "connect-src 'self'; "
        "object-src 'none'; "
        "base-uri 'self'; "
        "form-action 'self'; "
        "frame-ancestors 'none'"
    )
    return response


@app.errorhandler(ValidationError)
def handle_validation_error(exc: ValidationError):
    return _json_error(str(exc), 400)


@app.route("/login", methods=["GET"])
def login_page():
    return _render_login()


@app.route("/login", methods=["POST"])
def login_submit():
    if not _validate_csrf():
        return _render_login(error="No se pudo validar la solicitud de inicio de sesión.", status=403)

    if not _security_ready():
        return _render_login(
            error="Falta configurar ADMIN_PASSWORD_HASH o security.admin_password_hash antes de habilitar el acceso.",
            status=503,
        )

    locked_message = _login_rate_limit_message()
    if locked_message:
        return _render_login(error=locked_message, status=429)

    username = parse_string(request.form.get("username"), label="username", required=True, max_length=120)
    password = parse_string(request.form.get("password"), label="password", required=True, max_length=200)
    next_target = _safe_redirect_target(request.form.get("next"))

    valid_username = hmac.compare_digest(username, _admin_username())
    password_hash = _admin_password_hash()
    valid_password = check_password_hash(password_hash, password) if password_hash else hmac.compare_digest(
        _admin_password_plain(),
        password,
    )
    if not valid_username or not valid_password:
        _, message = _record_failed_login()
        logger.warning(
            "Intento de login fallido",
            extra={"event": "security.login_failed", "client": _client_identifier(), "username": username},
        )
        return _render_login(error=message, status=401)

    _clear_login_failures()
    _mark_authenticated()
    logger.info(
        "Login administrativo exitoso",
        extra={"event": "security.login_success", "client": _client_identifier(), "username": username},
    )
    return redirect(next_target)


@app.route("/logout", methods=["POST"])
def logout():
    _clear_admin_session()
    if _is_json_request():
        return jsonify({"ok": True})
    return redirect(url_for("login_page"))


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/automation")
def automation():
    return render_template("automation.html")


@app.route("/settings")
def settings():
    return render_template("settings.html")


@app.route("/auth/status")
def auth_status():
    valid = linkedin.is_session_valid(verify_browser=True)
    days = linkedin.session_days_left()
    return jsonify(
        {
            "authenticated": valid,
            "days_left": days,
            "needs_reconnect": 0 < days < 7,
        }
    )


@app.route("/auth/login", methods=["POST"])
def auth_login():
    settings = _settings()
    email = settings["linkedin"].get("email", "")
    password = settings["linkedin"].get("password", "")
    if not email or not password:
        return _json_error(
            "Configura LINKEDIN_EMAIL/LINKEDIN_PASSWORD o completa linkedin.email/password en config.yaml."
        )
    return jsonify({"job_id": _start_login_job(email, password)})


@app.route("/auth/login_status/<job_id>")
def auth_login_status(job_id: str):
    job = db.get_job(job_id)
    if not job or job["kind"] != "login":
        return _json_error("Job no encontrado", 404)
    return jsonify(_serialize_job(job))


@app.route("/auth/disconnect", methods=["POST"])
def auth_disconnect():
    linkedin._clear_session()
    return jsonify({"success": True})


@app.route("/api/run")
def api_run():
    test_mode = parse_bool(request.args.get("test"), default=False)
    category_name = parse_string(request.args.get("category"), label="category", default="", max_length=120)
    from_step = parse_int(request.args.get("from_step"), label="from_step", default=1, minimum=1, maximum=7)
    prev_session = parse_string(request.args.get("session_id"), label="session_id", default="", max_length=64)

    def generate():
        existing_session = db.get_pipeline_session(prev_session) if prev_session and from_step > 1 else None
        if prev_session and from_step > 1 and not existing_session:
            yield sse(
                {
                    "step": 0,
                    "status": "error",
                    "message": "La sesión del pipeline expiró. Genera el post nuevamente.",
                }
            )
            return

        existing_payload = existing_session["payload"] if existing_session else {}
        cat_name = category_name or existing_payload.get("category_name") or None
        category_cfg = db.get_pipeline_category(cat_name)
        if not category_cfg:
            yield sse({"step": 0, "status": "error", "message": "No hay categorías configuradas."})
            return

        if existing_session:
            session_id = existing_session["id"]
            base_payload = _pipeline_execution_payload(existing_session.get("payload", {}))
            db.upsert_pipeline_session(
                session_id,
                category=category_cfg["name"],
                status="running",
                payload={
                    **base_payload,
                    "category_name": category_cfg["name"],
                    "test_mode": test_mode,
                    "events": [],
                    "last_event": None,
                    "preview_data": None,
                },
            )
        else:
            session_id = db.create_pipeline_session(
                category_cfg["name"],
                payload={
                    "category_name": category_cfg["name"],
                    "test_mode": test_mode,
                    "events": [],
                    "last_event": None,
                    "preview_data": None,
                },
            )
        yield sse({"type": "init", "session_id": session_id})
        _ensure_pipeline_worker(session_id, category_cfg, from_step=from_step, test_mode=test_mode)

        sent_count = 0
        while True:
            session = db.get_pipeline_session(session_id)
            if not session:
                yield sse({"step": 0, "status": "error", "message": "La sesión del pipeline expiró."})
                return
            events = list((session.get("payload", {}) or {}).get("events", []))
            while sent_count < len(events):
                yield sse(events[sent_count])
                sent_count += 1
            if session.get("status") in {"ready", "needs_regeneration", "error"}:
                return
            time.sleep(0.4)

    return Response(
        generate(),
        content_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/pipeline_sessions/<session_id>")
def api_pipeline_session(session_id: str):
    session_id = parse_string(session_id, label="session_id", required=True, max_length=64)
    session = db.get_pipeline_session(session_id)
    if not session:
        return _json_error("Sesión expirada o no encontrada.", 404)
    return jsonify(_serialize_pipeline_session(session))


@app.route("/api/publish", methods=["POST"])
def api_publish():
    data = _parse_json_body()
    session_id = parse_string(data.get("session_id"), label="session_id", required=True, max_length=64)
    session, result = _get_session_payload(session_id)
    if not session:
        return _json_error("Sesión expirada o no encontrada. Genera el post nuevamente.", 404)
    if not linkedin.is_session_valid(verify_browser=True):
        return _json_error("Sesión de LinkedIn expirada. Inicia sesión nuevamente.", 401)

    post_text = parse_string(
        data.get("post_text_override") or result.get("post_text", ""),
        label="post_text_override",
        required=True,
        max_length=4000,
    )
    if not result.get("image_path"):
        return _json_error("La sesión del pipeline no tiene imagen asociada.", 400)
    job_id = _start_publish_job(session_id, result, post_text)
    return jsonify({"job_id": job_id})


@app.route("/api/publish_status/<job_id>")
def api_publish_status(job_id: str):
    job = db.get_job(job_id)
    if not job or job["kind"] != "publish":
        return _json_error("Job no encontrado", 404)
    return jsonify(_serialize_job(job))


@app.route("/api/headless", methods=["GET"])
def get_headless():
    return jsonify({"headless": bool(_settings().get("app", {}).get("headless", True))})


@app.route("/api/headless", methods=["POST"])
def set_headless():
    data = _parse_json_body()
    headless = parse_bool(data.get("headless"), default=True)
    settings = update_yaml_setting("app", "headless", headless)
    return jsonify({"headless": bool(settings.get("app", {}).get("headless", True))})


@app.route("/debug/screenshot")
def debug_screenshot():
    import glob as _glob

    screenshots = sorted(_glob.glob("static/debug/*.png"))
    if not screenshots:
        return "No hay capturas de debug todavía.", 404
    latest = screenshots[-1]
    name = os.path.basename(latest)
    links = "".join(
        f'<li><a href="/static/debug/{os.path.basename(s)}">{os.path.basename(s)}</a>'
        + (
            f' | <a href="/static/debug/{os.path.basename(s.replace(".png",".html"))}">HTML</a>'
            if os.path.exists(s.replace(".png", ".html"))
            else ""
        )
        + "</li>"
        for s in sorted(_glob.glob("static/debug/*.png"))
    )
    return f"""<html><body>
<h2>Debug Screenshots</h2><ul>{links}</ul>
<h3>Latest: {name}</h3>
<img src="/static/debug/{name}" style="max-width:100%">
</body></html>"""


@app.route("/api/schedule", methods=["GET"])
def get_schedule():
    cfg = db.get_schedule()
    runs = db.get_schedule_runs(limit=5)
    return jsonify({"config": cfg, "current_run": scheduler.current_run, "recent_runs": runs})


@app.route("/api/schedule", methods=["POST"])
def save_schedule():
    data = _parse_json_body()
    enabled = parse_bool(data.get("enabled"), default=False)
    mode = parse_string(
        data.get("mode"),
        label="mode",
        default="interval",
        allowed={"interval", "times"},
    )
    interval_hours = parse_float(
        data.get("interval_hours"),
        label="interval_hours",
        default=24,
        minimum=1,
        maximum=24 * 30,
    )
    times_of_day = parse_times_of_day(data.get("times_of_day"))
    days_of_week = parse_weekdays(data.get("days_of_week"))

    if mode == "times" and not times_of_day:
        raise ValidationError("Debes indicar al menos una hora cuando el modo es 'times'.")

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
    if scheduler.current_run.get("status") == "running":
        return _json_error("Ya hay una publicación programada en curso.", 409)

    def run():
        from src import linkedin as li
        from src.scheduler import _tick

        db.update_schedule_run_times(db.get_schedule().get("last_run_at", ""), "2000-01-01T00:00:00")
        _tick(db, li)

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"ok": True})


@app.route("/api/history")
def api_history():
    limit = parse_int(request.args.get("limit"), label="limit", default=20, minimum=1, maximum=100)
    page = parse_int(request.args.get("page"), label="page", default=1, minimum=1, maximum=10000)
    search = parse_string(request.args.get("search"), label="search", default="", max_length=120)
    offset = (page - 1) * limit
    posts = db.get_posts(limit=limit, offset=offset, published_only=True, search=search)
    total = db.count_posts(published_only=True, search=search)
    return jsonify(
        {
            "posts": posts,
            "pagination": {
                "page": page,
                "limit": limit,
                "total": total,
                "pages": max(1, (total + limit - 1) // limit),
            },
        }
    )


@app.route("/api/history/<int:post_id>")
def api_history_detail(post_id: int):
    match = db.get_post(post_id)
    if not match:
        return _json_error("Post no encontrado", 404)
    return jsonify(match)


@app.route("/api/categories", methods=["GET"])
def api_categories():
    categories = db.get_pipeline_categories()
    default = db.get_default_pipeline_category()
    return jsonify({"categories": categories, "default_category": default["name"] if default else ""})


@app.route("/api/categories", methods=["POST"])
def api_save_category():
    data = _parse_json_body()
    topic_keywords = parse_string_list(
        data.get("topic_keywords"),
        label="topic_keywords",
        max_items=20,
        max_length=40,
    )
    fallback_topics = parse_string_list(
        data.get("fallback_topics"),
        label="fallback_topics",
        max_items=20,
        max_length=160,
    )
    preferred_formats = parse_string_list(
        data.get("preferred_formats"),
        label="preferred_formats",
        max_items=6,
        max_length=40,
    )
    preferred_visual_styles = parse_string_list(
        data.get("preferred_visual_styles"),
        label="preferred_visual_styles",
        max_items=6,
        max_length=40,
    )
    category = db.save_pipeline_category(
        category_id=parse_int(data.get("id"), label="id", minimum=1, maximum=1000000, default=None)
        if data.get("id")
        else None,
        name=parse_string(data.get("name"), label="name", required=True, max_length=80),
        description=parse_string(data.get("description"), label="description", max_length=240),
        trends_prompt=parse_string(data.get("trends_prompt"), label="trends_prompt", max_length=2000),
        history_prompt=parse_string(data.get("history_prompt"), label="history_prompt", max_length=2000),
        content_prompt=parse_string(data.get("content_prompt"), label="content_prompt", max_length=3000),
        image_prompt=parse_string(data.get("image_prompt"), label="image_prompt", max_length=2000),
        is_default=parse_bool(data.get("is_default"), default=False),
        post_length=parse_int(data.get("post_length"), label="post_length", default=200, minimum=80, maximum=400),
        language=parse_string(
            data.get("language"),
            label="language",
            default="auto",
            allowed={"auto", "es", "en"},
        ),
        hashtag_count=parse_int(
            data.get("hashtag_count"),
            label="hashtag_count",
            default=4,
            minimum=0,
            maximum=10,
        ),
        use_emojis=parse_bool(data.get("use_emojis"), default=False),
        topic_keywords=topic_keywords,
        negative_prompt=parse_string(
            data.get("negative_prompt"),
            label="negative_prompt",
            max_length=1200,
        ),
        fallback_topics=fallback_topics,
        originality_level=parse_int(
            data.get("originality_level"),
            label="originality_level",
            default=3,
            minimum=1,
            maximum=5,
        ),
        evidence_mode=parse_string(
            data.get("evidence_mode"),
            label="evidence_mode",
            default="balanced",
            allowed={"balanced", "examples", "data", "story"},
        ),
        hook_style=parse_string(
            data.get("hook_style"),
            label="hook_style",
            default="auto",
            allowed={"auto", "question", "contrarian", "story", "bold"},
        ),
        cta_style=parse_string(
            data.get("cta_style"),
            label="cta_style",
            default="auto",
            allowed={"auto", "question", "debate", "reflection", "action"},
        ),
        audience_focus=parse_string(
            data.get("audience_focus"),
            label="audience_focus",
            max_length=80,
        ),
        preferred_formats=preferred_formats,
        preferred_visual_styles=preferred_visual_styles,
    )
    return jsonify({"ok": True, "category": category})


@app.route("/api/categories/<int:category_id>", methods=["DELETE"])
def api_delete_category(category_id: int):
    try:
        db.delete_pipeline_category(category_id)
        return jsonify({"ok": True})
    except Exception as exc:
        return _json_error(str(exc), 400)


if __name__ == "__main__":
    ensure_local_config()
    settings = reload_settings()
    scheduler.start()
    port = int(settings["app"].get("port", 5000))
    debug = bool(settings["app"].get("debug", False))
    print(f"\n LinkedIn Auto-Poster corriendo en http://localhost:{port}\n")
    app.run(host="0.0.0.0", port=port, debug=debug, threaded=True)
