from __future__ import annotations

from src import message_automation


class FakeLinkedIn:
    def __init__(self):
        self.sent_messages: list[str] = []

    def is_login_in_progress(self):
        return False

    def is_session_valid(self, **kwargs):
        return True

    def fetch_inbox_threads(self, limit=15, log=print):
        return [
            {
                "thread_key": "thread-1",
                "thread_url": "https://linkedin.test/thread-1",
                "contact_name": "Recruiter Jane",
                "latest_snippet": "Necesito hablar de salario y equity.",
                "last_message_at": "2026-03-30T10:00:00+00:00",
                "unread_count": 1,
                "contact_profile_url": "https://linkedin.test/in/jane",
            }
        ]

    def fetch_conversation(self, thread_url, log=print, limit=30):
        return {
            "thread_url": thread_url,
            "contact_name": "Recruiter Jane",
            "latest_snippet": "Necesito hablar de salario y equity.",
            "last_message_at": "2026-03-30T10:00:00+00:00",
            "contact_profile_url": "https://linkedin.test/in/jane",
            "unread_count": 1,
            "messages": [
                {
                    "sender_role": "contact",
                    "sender_name": "Recruiter Jane",
                    "text": "Hola, antes de avanzar necesito hablar de salario y equity.",
                    "happened_at": "2026-03-30T10:00:00+00:00",
                    "external_message_id": "msg-1",
                }
            ],
        }

    def send_message_reply(self, thread_url, message_text, log=print):
        self.sent_messages.append(message_text)


def test_tick_deduplicates_pending_review_items(app_env):
    db = app_env["db"]
    linkedin = FakeLinkedIn()
    db.save_message_automation_config(
        enabled=True,
        poll_interval_minutes=5,
        auto_send_default=True,
        public_base_url="http://127.0.0.1:5000",
        meeting_location="Meet",
        sync_limit=10,
        max_threads_per_cycle=5,
    )

    message_automation._tick(db, linkedin)
    message_automation._tick(db, linkedin)

    reviews = db.list_message_review_items()
    threads = db.list_message_threads()

    assert len(reviews) == 1
    assert len(threads) == 1
    assert threads[0]["assigned_review"] == 1
    assert threads[0]["last_processed_hash"] != ""
    assert linkedin.sent_messages == []


def test_tick_force_runs_even_when_automation_is_disabled(app_env):
    db = app_env["db"]
    linkedin = FakeLinkedIn()
    db.save_message_automation_config(
        enabled=False,
        poll_interval_minutes=5,
        auto_send_default=True,
        public_base_url="http://127.0.0.1:5000",
        meeting_location="Meet",
        sync_limit=10,
        max_threads_per_cycle=5,
    )

    message_automation._tick(db, linkedin, force=True)

    threads = db.list_message_threads(limit=10, include_closed=True)
    assert len(threads) == 1


def _force_llm_fallback(monkeypatch):
    """Make generate_reply use its fallback path so tests don't need a real API key."""
    from src import messages

    class _Boom:
        class chat:
            class completions:
                @staticmethod
                def create(**kwargs):
                    raise RuntimeError("simulated LLM outage")

    monkeypatch.setattr(messages, "get_xai_client", lambda: _Boom())


def test_simulate_incoming_message_creates_thread_and_generates_reply(app_env, monkeypatch):
    db = app_env["db"]
    db.save_message_automation_config(
        enabled=False,
        poll_interval_minutes=5,
        auto_send_default=True,
        public_base_url="http://127.0.0.1:5000",
        meeting_location="Meet",
        sync_limit=10,
        max_threads_per_cycle=5,
    )
    _force_llm_fallback(monkeypatch)

    # NOTE: avoid words like "agendar" (contains "nda") and "revisar" (contains "visa"),
    # which would trip the sensitive substring filter in classify_conversation.
    result = message_automation.simulate_incoming_message(
        db,
        text="Hola, ¿podemos coordinar una llamada la próxima semana para conversar sobre el cliente?",
        contact_name="Carla Ops",
    )

    thread = result["thread"]
    events = result["events"]
    bot_reply = result["bot_reply"]

    assert thread["id"]
    assert thread["contact_name"] == "Carla Ops"
    assert thread["thread_key"].startswith(message_automation.SIMULATED_THREAD_PREFIX)
    # Both inbound and bot reply got persisted
    roles = [event["sender_role"] for event in events]
    assert "contact" in roles
    assert "self" in roles
    assert bot_reply is not None
    assert bot_reply["text"]
    # Meeting intent → state should be meeting_pending after the bot's reply
    assert thread["state"] in {"meeting_pending", "awaiting_contact"}
    assert thread["intent"] in {"meeting", "recruiter"}
    # No real LinkedIn send happened
    assert all(item["thread_url"].startswith(message_automation.SIMULATED_THREAD_URL_PREFIX) for item in result["auto_sent"])


def test_simulate_sensitive_message_routes_to_review_queue(app_env, monkeypatch):
    db = app_env["db"]
    db.save_message_automation_config(
        enabled=False,
        poll_interval_minutes=5,
        auto_send_default=True,
        public_base_url="http://127.0.0.1:5000",
        meeting_location="Meet",
        sync_limit=10,
        max_threads_per_cycle=5,
    )
    _force_llm_fallback(monkeypatch)

    result = message_automation.simulate_incoming_message(
        db,
        text="Antes de avanzar necesito hablar de salario y equity para el rol.",
        contact_name="HR Internal",
    )

    thread = result["thread"]
    assert thread["assigned_review"] == 1
    # Bot must NOT have replied to a sensitive message
    assert result["bot_reply"] is None
    reviews = db.list_message_review_items()
    assert len(reviews) == 1
    assert reviews[0]["thread_id"] == thread["id"]


def test_simulate_followup_appends_to_existing_thread(app_env, monkeypatch):
    db = app_env["db"]
    db.save_message_automation_config(
        enabled=False,
        poll_interval_minutes=5,
        auto_send_default=True,
        public_base_url="http://127.0.0.1:5000",
        meeting_location="Meet",
        sync_limit=10,
        max_threads_per_cycle=5,
    )
    _force_llm_fallback(monkeypatch)

    first = message_automation.simulate_incoming_message(
        db,
        text="Hola, vi tu perfil y quería saber si te interesa una vacante de Senior Engineer en una startup B2B.",
        contact_name="Recruiter Jane",
    )
    thread_id = first["thread"]["id"]
    assert thread_id

    second = message_automation.simulate_incoming_message(
        db,
        text="Genial, te paso más detalles del rol mañana. ¿Tienes alguna preferencia de stack?",
        thread_id=thread_id,
    )

    assert second["thread"]["id"] == thread_id
    # Second simulation should see the full history (first inbound + first reply + second inbound + second reply)
    assert len(second["events"]) >= 4
    assert second["bot_reply"] is not None


def test_simulated_linkedin_adapter_returns_history_in_fetch_conversation():
    sim = {
        "thread_key": "sim-test-1",
        "thread_url": "simulated://thread/sim-test-1",
        "contact_name": "Test Contact",
        "contact_profile_url": "",
        "text": "Mensaje nuevo",
        "happened_at": "2026-04-07T10:00:00+00:00",
        "history": [
            {"sender_role": "contact", "text": "Hola previo", "happened_at": "2026-04-06T10:00:00+00:00"},
            {"sender_role": "self", "text": "Respuesta previa", "happened_at": "2026-04-06T10:05:00+00:00"},
        ],
    }
    fake = message_automation.SimulatedLinkedIn(simulations=[sim])

    inbox = fake.fetch_inbox_threads()
    assert len(inbox) == 1
    assert inbox[0]["thread_key"] == "sim-test-1"

    conv = fake.fetch_conversation("simulated://thread/sim-test-1")
    assert conv is not None
    texts = [item["text"] for item in conv["messages"]]
    assert texts == ["Hola previo", "Respuesta previa", "Mensaje nuevo"]

    fake.send_message_reply("simulated://thread/sim-test-1", "Reply de prueba")
    assert fake.sent_messages == [{"thread_url": "simulated://thread/sim-test-1", "text": "Reply de prueba"}]


def test_simulate_endpoint_via_http(app_env, authed_client, monkeypatch):
    _force_llm_fallback(monkeypatch)
    db = app_env["db"]
    db.save_message_automation_config(
        enabled=False,
        poll_interval_minutes=5,
        auto_send_default=True,
        public_base_url="http://127.0.0.1:5000",
        meeting_location="Meet",
        sync_limit=10,
        max_threads_per_cycle=5,
    )
    response = authed_client.post(
        "/api/messages/simulate",
        headers={"X-CSRF-Token": "test-csrf-token"},
        json={
            "text": "Hola, me encantaría hacer networking y tomar un coffee chat sobre IA aplicada.",
            "contact_name": "Diego Networking",
        },
    )
    assert response.status_code == 200
    data = response.get_json()
    assert data["ok"] is True
    assert data["thread"]["contact_name"] == "Diego Networking"
    assert data["thread"]["intent"] in {"networking", "general"}
    assert data["bot_reply"] is not None


def test_simulate_endpoint_rejects_empty_text(app_env, authed_client):
    response = authed_client.post(
        "/api/messages/simulate",
        headers={"X-CSRF-Token": "test-csrf-token"},
        json={"text": "   "},
    )
    assert response.status_code == 400
    data = response.get_json()
    assert data.get("error")
