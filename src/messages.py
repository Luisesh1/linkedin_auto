"""
Message automation helpers for LinkedIn inbox conversations.
"""

from __future__ import annotations

import hashlib
import json
import re
from urllib.parse import urlencode

from src.llm import get_text_model, get_xai_client
from src.logging_utils import get_logger

logger = get_logger(__name__)

INTENT_RECRUITER = "recruiter"
INTENT_MEETING = "meeting"
INTENT_NETWORKING = "networking"
INTENT_GENERAL = "general"
INTENT_SENSITIVE = "sensitive"
INTENT_UNKNOWN = "unknown"

STATE_NEW = "new"
STATE_ACTIVE = "active"
STATE_AWAITING_CONTACT = "awaiting_contact"
STATE_AWAITING_USER = "awaiting_user"
STATE_MEETING_PENDING = "meeting_pending"
STATE_MEETING_BOOKED = "meeting_booked"
STATE_JOB_FOLLOWUP = "job_followup"
STATE_CLOSED = "closed"

SENSITIVE_KEYWORDS = (
    "salary",
    "compensation",
    "sueldo",
    "salario",
    "visa",
    "contrato",
    "legal",
    "lawsuit",
    "demanda",
    "confidential",
    "confidencial",
    "acoso",
    "harassment",
    "nda",
    "equity",
    "acciones",
)

RECRUITER_KEYWORDS = (
    "recruiter",
    "recruiting",
    "talent",
    "headhunter",
    "vacante",
    "oportunidad",
    "position",
    "role",
    "opening",
    "hiring",
    "interview",
    "entrevista",
)

MEETING_KEYWORDS = (
    "meeting",
    "meet",
    "call",
    "schedule",
    "availability",
    "calendar",
    "agendar",
    "agenda",
    "reunion",
    "llamada",
    "horario",
)

NETWORKING_KEYWORDS = (
    "network",
    "coffee chat",
    "connect",
    "introduce",
    "intro",
    "referral",
    "refer",
    "networking",
    "conectar",
    "referido",
)


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip().lower())


def build_message_hash(sender_role: str, text: str, happened_at: str = "") -> str:
    raw = f"{sender_role}|{_normalize(text)}|{str(happened_at or '').strip()}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def find_latest_inbound(messages: list[dict]) -> dict | None:
    for item in reversed(messages):
        if item.get("sender_role") == "contact" and str(item.get("text", "")).strip():
            return item
    return None


def conversation_excerpt(messages: list[dict], limit: int = 8) -> str:
    lines = []
    for item in messages[-limit:]:
        role = "Contacto" if item.get("sender_role") == "contact" else "Yo"
        text = str(item.get("text", "")).strip()
        if text:
            lines.append(f"{role}: {text}")
    return "\n".join(lines)


def build_booking_link(public_base_url: str, booking_token: str, *, thread_id: int | None = None, contact_name: str = "") -> str:
    base = str(public_base_url or "").rstrip("/")
    token = str(booking_token or "").strip()
    params = {}
    if thread_id:
        params["thread"] = str(thread_id)
    if contact_name:
        params["name"] = contact_name
    query = f"?{urlencode(params)}" if params else ""
    return f"{base}/book/{token}{query}"


def classify_conversation(contact_name: str, messages: list[dict]) -> dict:
    latest = find_latest_inbound(messages) or {}
    latest_text = _normalize(latest.get("text", ""))
    transcript = _normalize(conversation_excerpt(messages, limit=10))

    sensitive = any(token in transcript for token in SENSITIVE_KEYWORDS)
    if sensitive:
        return {
            "intent": INTENT_SENSITIVE,
            "state": STATE_AWAITING_USER,
            "sensitive": True,
            "confidence": 0.95,
            "reasons": ["Se detectó un tema sensible o de alto riesgo."],
            "meeting_requested": any(token in transcript for token in MEETING_KEYWORDS),
            "job_related": any(token in transcript for token in RECRUITER_KEYWORDS),
        }

    job_related = any(token in transcript for token in RECRUITER_KEYWORDS)
    meeting_requested = any(token in transcript for token in MEETING_KEYWORDS)
    networking = any(token in transcript for token in NETWORKING_KEYWORDS)

    if job_related and meeting_requested:
        intent = INTENT_RECRUITER
        state = STATE_MEETING_PENDING
    elif job_related:
        intent = INTENT_RECRUITER
        state = STATE_JOB_FOLLOWUP
    elif meeting_requested:
        intent = INTENT_MEETING
        state = STATE_MEETING_PENDING
    elif networking:
        intent = INTENT_NETWORKING
        state = STATE_ACTIVE
    elif latest_text:
        intent = INTENT_GENERAL
        state = STATE_ACTIVE
    else:
        intent = INTENT_UNKNOWN
        state = STATE_AWAITING_USER

    reasons = []
    if job_related:
        reasons.append("El hilo parece venir de un recruiter u oportunidad laboral.")
    if meeting_requested:
        reasons.append("El hilo muestra intención de reunión o coordinación.")
    if networking and not reasons:
        reasons.append("La conversación parece orientada a networking o referral.")
    if not reasons:
        reasons.append("Se trata como conversación general de seguimiento.")

    return {
        "intent": intent,
        "state": state,
        "sensitive": False,
        "confidence": 0.78 if intent != INTENT_UNKNOWN else 0.45,
        "reasons": reasons,
        "meeting_requested": meeting_requested,
        "job_related": job_related,
    }


def should_escalate(classification: dict, latest_inbound: dict | None) -> tuple[bool, str]:
    latest_text = _normalize((latest_inbound or {}).get("text", ""))
    if classification.get("sensitive"):
        return True, "Tema sensible o de alto riesgo."
    if not latest_text:
        return True, "No hay mensaje inbound claro para responder."
    if classification.get("intent") == INTENT_UNKNOWN:
        return True, "La intención de la conversación es ambigua."
    if any(token in latest_text for token in ("salary", "sueldo", "salario", "equity", "acciones", "visa", "contract", "contrato")):
        return True, "La conversación pide datos sensibles de compensación o contrato."
    return False, ""


def summarize_contact(contact_name: str, messages: list[dict], classification: dict) -> str:
    latest = find_latest_inbound(messages) or {}
    latest_text = str(latest.get("text", "")).strip()
    if classification.get("intent") == INTENT_RECRUITER:
        return f"{contact_name}: recruiter u oportunidad laboral activa. Último interés: {latest_text[:140]}"
    if classification.get("intent") == INTENT_MEETING:
        return f"{contact_name}: conversación orientada a reunión. Último mensaje: {latest_text[:140]}"
    if classification.get("intent") == INTENT_NETWORKING:
        return f"{contact_name}: networking/referral en curso. Último mensaje: {latest_text[:140]}"
    return f"{contact_name}: conversación general activa. Último mensaje: {latest_text[:140]}"


def build_reply_context(contact_name: str, messages: list[dict], classification: dict, booking_link: str = "", meeting_location: str = "") -> dict:
    latest = find_latest_inbound(messages) or {}
    latest_text = str(latest.get("text", "")).strip()
    context = {
        "contact_name": contact_name,
        "latest_inbound": latest_text,
        "intent": classification.get("intent", INTENT_GENERAL),
        "meeting_requested": bool(classification.get("meeting_requested")),
        "job_related": bool(classification.get("job_related")),
        "booking_link": booking_link,
        "meeting_location": meeting_location,
    }
    return context


def _fallback_reply(context: dict) -> dict:
    name = context.get("contact_name") or "Hola"
    latest_text = context.get("latest_inbound", "")
    if context.get("intent") == INTENT_RECRUITER:
        body = (
            f"Hola {name}, gracias por escribir. Me interesa conocer mejor la oportunidad y entender el rol, "
            f"el contexto del equipo y el stack. "
        )
        if context.get("meeting_requested") and context.get("booking_link"):
            body += f"Si te sirve, puedes reservar un espacio aquí: {context['booking_link']}"
            next_state = STATE_MEETING_PENDING
        else:
            body += "Si puedes, compárteme más detalles del puesto, stack, seniority y modalidad para revisarlo con contexto."
            next_state = STATE_JOB_FOLLOWUP
        return {"reply_text": body, "next_state": next_state, "reasoning": "Respuesta fallback para recruiter."}

    if context.get("meeting_requested") and context.get("booking_link"):
        return {
            "reply_text": (
                f"Gracias, {name}. Para que nos coordinemos mejor, aquí tienes mi enlace de reserva: {context['booking_link']}. "
                "Cuando elijas horario, te quedará confirmado automáticamente."
            ),
            "next_state": STATE_MEETING_PENDING,
            "reasoning": "Respuesta fallback con booking link.",
        }

    if context.get("intent") == INTENT_NETWORKING:
        return {
            "reply_text": (
                f"Hola {name}, gracias por escribir. Encantado de seguir la conversación. "
                f"Leí tu mensaje sobre '{latest_text[:80]}' y me parece un buen punto para profundizar. "
                "Cuéntame un poco más de tu contexto y con gusto seguimos."
            ),
            "next_state": STATE_AWAITING_CONTACT,
            "reasoning": "Respuesta fallback de networking.",
        }

    return {
        "reply_text": (
            f"Hola {name}, gracias por escribir. Ya leí tu mensaje y con gusto seguimos por aquí. "
            "Si quieres, comparte un poco más de contexto y te respondo con más precisión."
        ),
        "next_state": STATE_AWAITING_CONTACT,
        "reasoning": "Respuesta fallback general.",
    }


def generate_reply(contact_name: str, messages: list[dict], classification: dict, *, booking_link: str = "", meeting_location: str = "") -> dict:
    context = build_reply_context(
        contact_name,
        messages,
        classification,
        booking_link=booking_link,
        meeting_location=meeting_location,
    )
    fallback = _fallback_reply(context)

    try:
        response = get_xai_client().chat.completions.create(
            model=get_text_model(),
            messages=[
                {
                    "role": "user",
                    "content": (
                        "Eres un asistente de LinkedIn que responde mensajes de forma profesional, humana y breve. "
                        "No inventes datos. No prometas salario. No negocies temas legales. "
                        "Debes escribir en el idioma predominante del hilo.\n\n"
                        f"Contexto del contacto: {contact_name}\n"
                        f"Clasificación: {json.dumps(classification, ensure_ascii=False)}\n"
                        f"Booking link: {booking_link or 'N/A'}\n"
                        f"Meeting location: {meeting_location or 'N/A'}\n"
                        f"Transcripción reciente:\n{conversation_excerpt(messages, limit=10)}\n\n"
                        "Devuelve SOLO JSON con keys: reply_text, next_state, reasoning."
                    ),
                }
            ],
            max_tokens=320,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content.strip()
        data = json.loads(re.sub(r"^```(?:json)?\s*|\s*```$", "", raw).strip())
        reply_text = str(data.get("reply_text", "")).strip()
        if not reply_text:
            return fallback
        return {
            "reply_text": reply_text,
            "next_state": str(data.get("next_state", fallback["next_state"]) or fallback["next_state"]),
            "reasoning": str(data.get("reasoning", "") or fallback["reasoning"]),
        }
    except Exception as exc:
        logger.warning("Fallo al generar respuesta con LLM; usando fallback", extra={"event": "messages.reply_fallback"}, exc_info=exc)
        return fallback
