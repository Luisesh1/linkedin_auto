from __future__ import annotations

from src import messages


def test_classify_recruiter_meeting_conversation():
    result = messages.classify_conversation(
        "Maria",
        [
            {"sender_role": "contact", "text": "Hola, soy recruiter y tengo una oportunidad para ti. Te va una llamada?", "happened_at": "2026-03-30T10:00:00+00:00"},
        ],
    )

    assert result["intent"] == messages.INTENT_RECRUITER
    assert result["state"] == messages.STATE_MEETING_PENDING
    assert result["job_related"] is True
    assert result["meeting_requested"] is True


def test_sensitive_conversation_escalates():
    conversation = [
        {"sender_role": "contact", "text": "Antes de avanzar necesito hablar de salario, equity y visa.", "happened_at": "2026-03-30T10:00:00+00:00"},
    ]
    classification = messages.classify_conversation("Ana", conversation)
    escalate, reason = messages.should_escalate(classification, messages.find_latest_inbound(conversation))

    assert classification["intent"] == messages.INTENT_SENSITIVE
    assert escalate is True
    assert "sensible" in reason.lower() or "riesgo" in reason.lower()


def test_build_booking_link_includes_context():
    url = messages.build_booking_link(
        "https://example.com",
        "token-123",
        thread_id=42,
        contact_name="Luis",
    )

    assert url == "https://example.com/book/token-123?thread=42&name=Luis"
