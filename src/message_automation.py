"""
Automated LinkedIn inbox processing.
"""

from __future__ import annotations

import threading
import time
import uuid
from datetime import UTC, datetime

from src import messages
from src.logging_utils import get_logger

SIMULATED_THREAD_PREFIX = "sim-"
SIMULATED_THREAD_URL_PREFIX = "simulated://thread/"

logger = get_logger(__name__)

_thread: threading.Thread | None = None
_stop_event = threading.Event()
current_run: dict = {"status": "idle", "message": "", "processed": 0}


def start():
    global _thread
    if _thread and _thread.is_alive():
        return
    _stop_event.clear()
    _thread = threading.Thread(target=_loop, daemon=True, name="message-automation")
    _thread.start()


def stop():
    _stop_event.set()


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _log_status(message: str, *, status: str | None = None, processed: int | None = None):
    current_run["message"] = message
    if status:
        current_run["status"] = status
    if processed is not None:
        current_run["processed"] = processed
    logger.info(message, extra={"event": "messages.status"})


def _loop():
    from src import db, linkedin

    while not _stop_event.is_set():
        try:
            cfg = db.get_message_automation_config()
            interval_minutes = max(1, int(cfg.get("poll_interval_minutes", 5) or 5))
            _tick(db, linkedin)
            _stop_event.wait(interval_minutes * 60)
        except Exception:
            logger.exception("Unexpected error in message automation loop", extra={"event": "messages.loop_error"})
            _stop_event.wait(60)


def _sync_thread_from_inbox(db, thread_meta: dict) -> dict:
    return db.upsert_message_thread(
        thread_key=thread_meta.get("thread_key", ""),
        thread_url=thread_meta.get("thread_url", ""),
        contact_name=thread_meta.get("contact_name", ""),
        contact_profile_url=thread_meta.get("contact_profile_url", ""),
        latest_snippet=thread_meta.get("latest_snippet", ""),
        last_message_at=thread_meta.get("last_message_at", ""),
        unread_count=int(thread_meta.get("unread_count", 0) or 0),
    )


def _ingest_messages(db, thread_id: int, thread_messages: list[dict]) -> dict | None:
    latest_inbound = None
    for item in thread_messages:
        text = str(item.get("text", "")).strip()
        if not text:
            continue
        sender_role = str(item.get("sender_role", "contact") or "contact")
        happened_at = str(item.get("happened_at", "") or "")
        message_hash = messages.build_message_hash(sender_role, text, happened_at)
        db.save_message_event(
            thread_id,
            event_type="message",
            sender_role=sender_role,
            text=text,
            message_hash=message_hash,
            happened_at=happened_at or _utc_now(),
            meta={"sender_name": item.get("sender_name", ""), "external_message_id": item.get("external_message_id", "")},
        )
        if sender_role == "contact":
            latest_inbound = {"text": text, "message_hash": message_hash, "happened_at": happened_at or _utc_now()}
    return latest_inbound


def _booking_link(cfg: dict, thread_id: int, contact_name: str) -> str:
    return messages.build_booking_link(
        cfg.get("public_base_url", ""),
        cfg.get("booking_token", ""),
        thread_id=thread_id,
        contact_name=contact_name,
    )


def _process_thread(db, linkedin_mod, cfg: dict, thread: dict) -> bool:
    if int(thread.get("paused", 0) or 0) or int(thread.get("closed", 0) or 0):
        return False

    try:
        detail = linkedin_mod.fetch_conversation(thread.get("thread_url", ""), log=lambda msg: _log_status(msg, status="running"))
        if not detail:
            return False

        db.upsert_message_thread(
            thread_key=thread.get("thread_key", ""),
            thread_url=detail.get("thread_url", thread.get("thread_url", "")),
            contact_name=detail.get("contact_name", thread.get("contact_name", "")),
            contact_profile_url=detail.get("contact_profile_url", thread.get("contact_profile_url", "")),
            latest_snippet=detail.get("latest_snippet", thread.get("latest_snippet", "")),
            last_message_at=detail.get("last_message_at", thread.get("last_message_at", "")),
            unread_count=int(detail.get("unread_count", thread.get("unread_count", 0)) or 0),
        )

        thread = db.get_message_thread(thread["id"]) or thread
        thread_messages = detail.get("messages", [])
        latest_inbound = _ingest_messages(db, thread["id"], thread_messages)
        if not latest_inbound:
            return False
        if latest_inbound["message_hash"] == str(thread.get("last_processed_hash", "") or ""):
            return False

        classification = messages.classify_conversation(thread.get("contact_name", ""), thread_messages)
        summary = messages.summarize_contact(thread.get("contact_name", ""), thread_messages, classification)
        db.upsert_contact_profile(
            thread["id"],
            contact_name=thread.get("contact_name", ""),
            profile_url=thread.get("contact_profile_url", ""),
            intent=classification["intent"],
            current_stage=classification["state"],
            summary=summary,
            next_action="Responder automáticamente" if not classification.get("sensitive") else "Revisión manual",
        )
        db.update_message_thread_state(
            thread["id"],
            intent=classification["intent"],
            state=classification["state"],
            last_inbound_hash=latest_inbound["message_hash"],
            crm_summary=summary,
            assigned_review=False,
            next_action="Procesando",
        )

        escalate, reason = messages.should_escalate(classification, latest_inbound)
        if escalate:
            db.create_message_review_item(thread["id"], reason, suggested_reply="")
            db.update_message_thread_state(
                thread["id"],
                state=messages.STATE_AWAITING_USER,
                assigned_review=True,
                next_action=reason,
                last_processed_hash=latest_inbound["message_hash"],
                last_error=reason,
            )
            return False

        booking_link = _booking_link(cfg, thread["id"], thread.get("contact_name", ""))
        reply = messages.generate_reply(
            thread.get("contact_name", ""),
            thread_messages,
            classification,
            booking_link=booking_link if classification.get("meeting_requested") or classification.get("job_related") else "",
            meeting_location=cfg.get("meeting_location", ""),
        )

        if not bool(cfg.get("auto_send_default", True)):
            db.create_message_review_item(thread["id"], "Autoenvío desactivado", suggested_reply=reply["reply_text"])
            db.update_message_thread_state(
                thread["id"],
                state=messages.STATE_AWAITING_USER,
                assigned_review=True,
                next_action="Borrador listo para aprobación",
                last_processed_hash=latest_inbound["message_hash"],
            )
            return False

        try:
            linkedin_mod.send_message_reply(thread.get("thread_url", ""), reply["reply_text"], log=lambda msg: _log_status(msg, status="running"))
        except Exception as exc:
            reason = f"No se pudo enviar automáticamente: {exc}"
            db.create_message_review_item(thread["id"], reason, suggested_reply=reply["reply_text"])
            db.update_message_thread_state(
                thread["id"],
                state=messages.STATE_AWAITING_USER,
                assigned_review=True,
                next_action=reason,
                last_processed_hash=latest_inbound["message_hash"],
                last_error=str(exc),
            )
            return False

        outbound_hash = messages.build_message_hash("self", reply["reply_text"], _utc_now())
        db.save_message_event(
            thread["id"],
            event_type="message",
            sender_role="self",
            text=reply["reply_text"],
            message_hash=outbound_hash,
            happened_at=_utc_now(),
            meta={"reasoning": reply.get("reasoning", "")},
        )
        db.update_message_thread_state(
            thread["id"],
            state=reply.get("next_state", messages.STATE_AWAITING_CONTACT),
            last_processed_hash=latest_inbound["message_hash"],
            next_action="Esperando respuesta del contacto",
            last_auto_reply_at=_utc_now(),
            assigned_review=False,
            last_error="",
        )
        db.update_message_reviews_for_thread(thread["id"], status="approved")
        return True

    except Exception as exc:
        logger.exception(
            "Error inesperado procesando thread %s",
            thread.get("id"),
            extra={"event": "messages.process_error"},
        )
        db.update_message_thread_state(
            thread["id"],
            last_error=f"Error interno: {exc}",
            next_action="Error interno — revisar logs",
        )
        return False


def _tick(db, linkedin_mod, *, force: bool = False):
    cfg = db.get_message_automation_config()
    if not cfg.get("enabled") and not force:
        current_run.update({"status": "idle", "message": "Automatización de mensajes desactivada.", "processed": 0})
        return

    _log_status(
        "Sincronizando inbox de LinkedIn..." if not force else "Sincronización manual del inbox en progreso...",
        status="running",
        processed=0,
    )
    if getattr(linkedin_mod, "is_login_in_progress", lambda: False)():
        raise PermissionError("Hay un login manual de LinkedIn en progreso.")
    if not linkedin_mod.is_session_valid(verify_browser=True, log=lambda msg: _log_status(msg, status="running")):
        raise PermissionError("No hay una sesión válida de LinkedIn para procesar mensajes.")

    inbox = linkedin_mod.fetch_inbox_threads(limit=int(cfg.get("sync_limit", 15) or 15), log=lambda msg: _log_status(msg, status="running"))
    synced_threads = []
    for item in inbox:
        try:
            synced_threads.append(_sync_thread_from_inbox(db, item))
        except Exception:
            logger.exception(
                "Error sincronizando thread %s",
                item.get("thread_key"),
                extra={"event": "messages.sync_error"},
            )
    processed = 0
    for thread in synced_threads[: int(cfg.get("max_threads_per_cycle", 5) or 5)]:
        try:
            if _process_thread(db, linkedin_mod, cfg, thread):
                processed += 1
        except Exception:
            logger.exception(
                "Error procesando thread %s",
                thread.get("id"),
                extra={"event": "messages.thread_error"},
            )
    _log_status(f"Mensajes procesados: {processed}", status="idle", processed=processed)


# ─── Simulated inbox (for testing the auto-reply pipeline locally) ────────────


class SimulatedLinkedIn:
    """In-memory adapter that mimics src.linkedin for the auto-reply pipeline.

    Lets us run _tick(db, fake_li, force=True) without a real LinkedIn session.
    The simulated messages go through the exact same classify → escalate →
    generate_reply → save flow as real ones.
    """

    def __init__(self, simulations: list[dict] | None = None):
        # Each simulation: {thread_key, thread_url, contact_name, contact_profile_url,
        #                    text, happened_at, history}
        self.simulations = list(simulations or [])
        self.sent_messages: list[dict] = []

    def is_login_in_progress(self) -> bool:
        return False

    def is_session_valid(self, **kwargs) -> bool:
        return True

    def fetch_inbox_threads(self, limit: int = 15, log=print) -> list[dict]:
        log("Inbox simulado: cargando hilos en memoria")
        threads = []
        for sim in self.simulations[:limit]:
            threads.append(
                {
                    "thread_key": sim["thread_key"],
                    "thread_url": sim["thread_url"],
                    "contact_name": sim.get("contact_name", "Contacto simulado"),
                    "contact_profile_url": sim.get("contact_profile_url", ""),
                    "latest_snippet": (sim.get("text", "") or "")[:200],
                    "last_message_at": sim.get("happened_at", ""),
                    "unread_count": 1,
                }
            )
        return threads

    def fetch_conversation(self, thread_url: str, log=print, limit: int = 30) -> dict | None:
        sim = next((item for item in self.simulations if item["thread_url"] == thread_url), None)
        if not sim:
            return None
        log(f"Conversación simulada: {sim.get('contact_name', '')}")
        history = list(sim.get("history", []) or [])
        # Append the new simulated inbound message at the end
        history.append(
            {
                "sender_role": "contact",
                "sender_name": sim.get("contact_name", "Contacto simulado"),
                "text": sim["text"],
                "happened_at": sim.get("happened_at", _utc_now()),
                "external_message_id": sim.get("external_message_id", f"sim-{uuid.uuid4().hex[:12]}"),
            }
        )
        return {
            "thread_url": thread_url,
            "contact_name": sim.get("contact_name", "Contacto simulado"),
            "contact_profile_url": sim.get("contact_profile_url", ""),
            "latest_snippet": (sim.get("text", "") or "")[:200],
            "last_message_at": sim.get("happened_at", ""),
            "unread_count": 1,
            "messages": history[-limit:],
        }

    def send_message_reply(self, thread_url: str, message_text: str, log=print) -> None:
        # Don't actually send anywhere — just record so tests/UI can display it.
        log("Reply simulada: no se envía a LinkedIn real")
        self.sent_messages.append({"thread_url": thread_url, "text": message_text})


def _events_to_history(events: list[dict]) -> list[dict]:
    """Convert persisted message_events into the shape fetch_conversation returns."""
    history = []
    for event in events:
        meta = event.get("meta") or {}
        history.append(
            {
                "sender_role": event.get("sender_role", "contact"),
                "sender_name": meta.get("sender_name", ""),
                "text": event.get("text", ""),
                "happened_at": event.get("happened_at", ""),
                "external_message_id": meta.get("external_message_id", ""),
            }
        )
    return history


def simulate_incoming_message(
    db,
    *,
    text: str,
    contact_name: str = "Contacto simulado",
    thread_id: int | None = None,
    contact_profile_url: str = "",
    happened_at: str | None = None,
) -> dict:
    """Inject a simulated incoming LinkedIn message and run the full auto-reply pipeline.

    If `thread_id` is provided, the message is appended to that existing thread
    (history is loaded from DB so the LLM has context for follow-ups).
    Otherwise a brand-new simulated thread is created.

    Returns a dict with the thread, persisted events, the simulated reply (if any),
    and the run status — useful for both UI rendering and tests.
    """
    text = str(text or "").strip()
    if not text:
        raise ValueError("El texto del mensaje simulado no puede estar vacío.")

    when = happened_at or _utc_now()
    history: list[dict] = []
    contact_name_clean = str(contact_name or "Contacto simulado").strip() or "Contacto simulado"

    if thread_id is not None:
        existing = db.get_message_thread(int(thread_id))
        if not existing:
            raise ValueError(f"No existe el hilo con id {thread_id}.")
        thread_key = existing["thread_key"]
        thread_url = existing.get("thread_url") or f"{SIMULATED_THREAD_URL_PREFIX}{thread_key}"
        contact_name_clean = existing.get("contact_name") or contact_name_clean
        contact_profile_url = existing.get("contact_profile_url", "") or contact_profile_url
        history = _events_to_history(db.list_message_events(int(thread_id), limit=200))
    else:
        thread_key = f"{SIMULATED_THREAD_PREFIX}{uuid.uuid4().hex[:12]}"
        thread_url = f"{SIMULATED_THREAD_URL_PREFIX}{thread_key}"

    simulation = {
        "thread_key": thread_key,
        "thread_url": thread_url,
        "contact_name": contact_name_clean,
        "contact_profile_url": contact_profile_url,
        "text": text,
        "happened_at": when,
        "history": history,
    }

    fake_li = SimulatedLinkedIn(simulations=[simulation])
    _tick(db, fake_li, force=True)

    # Reload the thread (was upserted by _tick) and the events that landed.
    thread = db.get_message_thread_by_key(thread_key) or {}
    events = db.list_message_events(int(thread["id"]), limit=200) if thread.get("id") else []
    profile = db.get_contact_profile(int(thread["id"])) if thread.get("id") else {}

    # The most recent self-event is the bot reply (if one was generated)
    bot_reply = next(
        (event for event in reversed(events) if event.get("sender_role") == "self"),
        None,
    )

    return {
        "thread": thread,
        "events": events,
        "profile": profile or {},
        "bot_reply": bot_reply,
        "auto_sent": fake_li.sent_messages,
        "run": dict(current_run),
    }
