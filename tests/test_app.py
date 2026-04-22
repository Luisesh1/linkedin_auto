from __future__ import annotations

from datetime import UTC, datetime


def test_login_page_is_public(client):
    response = client.get("/login")

    assert response.status_code == 200
    assert "Acceso administrativo" in response.get_data(as_text=True)


def test_dashboard_redirects_to_login_when_unauthenticated(client):
    response = client.get("/", follow_redirects=False)

    assert response.status_code == 302
    assert "/login" in response.headers["Location"]


def test_auth_status_skips_browser_probe_while_login_is_in_progress(authed_client, app_env, monkeypatch):
    app_module = app_env["app_module"]
    captured: dict[str, bool] = {}

    def fake_is_session_valid(**kwargs):
        captured["verify_browser"] = kwargs.get("verify_browser")
        return False

    monkeypatch.setattr(app_module.linkedin, "is_login_in_progress", lambda: True)
    monkeypatch.setattr(app_module.linkedin, "is_session_valid", fake_is_session_valid)
    monkeypatch.setattr(app_module.linkedin, "session_days_left", lambda: 0)

    response = authed_client.get("/auth/status")
    data = response.get_json()

    assert response.status_code == 200
    assert captured["verify_browser"] is False
    assert data["login_in_progress"] is True


def test_schedule_endpoint_validates_mode(authed_client):
    response = authed_client.post(
        "/api/schedule",
        headers={"X-CSRF-Token": "test-csrf-token"},
        json={
            "enabled": True,
            "mode": "times",
            "interval_hours": 24,
            "times_of_day": [],
            "days_of_week": [],
        },
    )

    assert response.status_code == 400
    assert "al menos una hora" in response.get_json()["error"]


def test_schedule_endpoint_persists_selected_category(authed_client, app_env):
    response = authed_client.post(
        "/api/schedule",
        headers={"X-CSRF-Token": "test-csrf-token"},
        json={
            "enabled": True,
            "mode": "interval",
            "interval_hours": 12,
            "times_of_day": [],
            "days_of_week": [],
            "category_name": "random",
        },
    )

    assert response.status_code == 200
    assert app_env["db"].get_schedule()["category_name"] == "random"


def test_category_endpoint_validates_name(authed_client):
    response = authed_client.post(
        "/api/categories",
        headers={"X-CSRF-Token": "test-csrf-token"},
        json={
            "name": "",
            "description": "desc",
        },
    )

    assert response.status_code == 400
    assert "obligatorio" in response.get_json()["error"]


def test_publish_requires_active_session(authed_client, app_env, monkeypatch):
    app_module = app_env["app_module"]
    session_id = app_env["db"].create_pipeline_session(
        "default",
        payload={
            "topic": "IA",
            "post_text": "Texto",
            "image_path": "/tmp/image.jpg",
        },
    )
    monkeypatch.setattr(app_module.linkedin, "is_session_valid", lambda **kwargs: False)

    response = authed_client.post(
        "/api/publish",
        headers={"X-CSRF-Token": "test-csrf-token"},
        json={"session_id": session_id, "post_text_override": "Texto listo"},
    )

    assert response.status_code == 401
    assert "expirada" in response.get_json()["error"]


def test_history_endpoint_supports_pagination(authed_client, app_env):
    db = app_env["db"]
    for idx in range(3):
        db.save_post(topic=f"Tema {idx}", post_text="Texto", published=True)

    response = authed_client.get("/api/history?limit=2&page=2")
    data = response.get_json()

    assert response.status_code == 200
    assert data["pagination"]["page"] == 2
    assert data["pagination"]["limit"] == 2
    assert len(data["posts"]) == 1


def test_pipeline_session_endpoint_returns_persisted_state(authed_client, app_env):
    db = app_env["db"]
    session_id = db.create_pipeline_session(
        "default",
        payload={
            "test_mode": True,
            "events": [
                {"step": 1, "status": "done", "result": ["Tema A", "Tema B"]},
                {"step": 2, "status": "running", "message": "Midiendo novedad..."},
            ],
            "preview_data": {
                "step": 7,
                "status": "preview",
                "topic": "Tema A",
                "post_text": "Texto",
                "image_url": "/static/generated/test.jpg",
                "test_mode": True,
            },
        },
    )

    response = authed_client.get(f"/api/pipeline_sessions/{session_id}")
    data = response.get_json()

    assert response.status_code == 200
    assert data["id"] == session_id
    assert data["category"] == "default"
    assert data["status"] == "running"
    assert data["test_mode"] is True
    assert len(data["events"]) == 2
    assert data["preview"]["topic"] == "Tema A"


def test_pipeline_session_endpoint_returns_requested_category(authed_client, app_env):
    db = app_env["db"]
    session_id = db.create_pipeline_session(
        "aiRadar",
        payload={
            "requested_category_name": "random",
            "resolved_category_name": "aiRadar",
        },
    )

    response = authed_client.get(f"/api/pipeline_sessions/{session_id}")
    data = response.get_json()

    assert response.status_code == 200
    assert data["requested_category"] == "random"
    assert data["resolved_category"] == "aiRadar"


def test_api_requires_csrf_for_post_requests(authed_client):
    response = authed_client.post(
        "/api/schedule",
        json={
            "enabled": True,
            "mode": "interval",
            "interval_hours": 24,
            "times_of_day": [],
            "days_of_week": [],
        },
    )

    assert response.status_code == 403
    assert "CSRF" in response.get_json()["error"]


def test_post_metrics_can_be_saved_and_analytics_returned(authed_client, app_env):
    db = app_env["db"]
    post_id = db.save_post(
        topic="Tema medido",
        post_text="Texto de prueba",
        category="aiRadar",
        pillar="ai",
        content_format="insight",
        hook_type="contrarian",
        cta_type="debate",
        visual_style="diagram",
        published=True,
    )

    save_response = authed_client.post(
        f"/api/history/{post_id}/metrics",
        headers={"X-CSRF-Token": "test-csrf-token"},
        json={
            "impressions": 1400,
            "reactions": 96,
            "comments": 18,
            "reposts": 5,
            "profile_visits": 12,
            "link_clicks": 7,
            "saves": 15,
        },
    )
    analytics_response = authed_client.get("/api/analytics/summary")

    assert save_response.status_code == 200
    assert save_response.get_json()["metrics"]["impressions"] == 1400
    assert analytics_response.status_code == 200
    assert analytics_response.get_json()["summary"]["tracked_posts"] >= 1


def test_resolve_requested_category_keeps_existing_random_resolution(app_env):
    app_module = app_env["app_module"]

    category, requested = app_module._resolve_requested_category(
        "random",
        existing_payload={
            "requested_category_name": "random",
            "resolved_category_name": "aiRadar",
            "category_name": "aiRadar",
        },
    )

    assert requested == "random"
    assert category is not None
    assert category["name"] == "aiRadar"


def test_api_run_init_event_includes_resolved_random_category(authed_client, app_env, monkeypatch):
    app_module = app_env["app_module"]
    db = app_env["db"]

    monkeypatch.setattr(
        app_module.db,
        "resolve_pipeline_category_choice",
        lambda category_name=None: ({"name": "aiRadar"}, "random"),
    )

    def fake_ensure_pipeline_worker(session_id, category_cfg, *, from_step, test_mode):
        session = db.get_pipeline_session(session_id)
        payload = dict((session or {}).get("payload", {}))
        payload["events"] = [{"step": 0, "status": "error", "message": "stop"}]
        db.upsert_pipeline_session(session_id, status="error", payload=payload)

    monkeypatch.setattr(app_module, "_ensure_pipeline_worker", fake_ensure_pipeline_worker)

    response = authed_client.get("/api/run?test=1&category=random")
    body = response.get_data(as_text=True)

    assert response.status_code == 200
    assert '"type": "init"' in body
    assert '"requested_category": "random"' in body
    assert '"resolved_category": "aiRadar"' in body


def test_message_automation_config_roundtrip(authed_client):
    response = authed_client.post(
        "/api/messages/automation",
        headers={"X-CSRF-Token": "test-csrf-token"},
        json={
            "enabled": True,
            "poll_interval_minutes": 7,
            "auto_send_default": False,
            "public_base_url": "https://example.com",
            "meeting_location": "Google Meet",
            "sync_limit": 20,
            "max_threads_per_cycle": 8,
        },
    )
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["config"]["enabled"] == 1
    assert payload["config"]["poll_interval_minutes"] == 7
    assert payload["config"]["auto_send_default"] == 0
    assert payload["config"]["public_base_url"] == "https://example.com"


def test_message_reply_route_clears_review_queue(authed_client, app_env, monkeypatch):
    db = app_env["db"]
    thread = db.upsert_message_thread(
        thread_key="thread-123",
        thread_url="https://linkedin.test/thread-123",
        contact_name="Lucia",
    )
    db.create_message_review_item(thread["id"], "Revisar manualmente", suggested_reply="Hola Lucia")
    db.update_message_thread_state(thread["id"], assigned_review=True, state="awaiting_user")
    monkeypatch.setattr(app_env["app_module"].linkedin, "send_message_reply", lambda *args, **kwargs: None)

    response = authed_client.post(
        f"/api/messages/conversations/{thread['id']}/reply",
        headers={"X-CSRF-Token": "test-csrf-token"},
        json={"text": "Hola Lucia, gracias por escribir."},
    )

    assert response.status_code == 200
    assert db.list_message_review_items() == []
    updated = db.get_message_thread(thread["id"])
    assert updated["assigned_review"] == 0
    assert updated["state"] == "awaiting_contact"


def test_booking_page_is_public_and_accepts_valid_slot(client, app_env):
    db = app_env["db"]
    app_module = app_env["app_module"]
    thread = db.upsert_message_thread(
        thread_key="thread-booking",
        thread_url="https://linkedin.test/thread-booking",
        contact_name="Luis Recruiter",
    )
    db.replace_calendar_availability(
        [
            {
                "weekday": datetime.now(UTC).weekday(),
                "start_time": "00:00",
                "end_time": "23:30",
                "timezone": "UTC",
            }
        ]
    )
    token = db.get_message_automation_config()["booking_token"]
    slot = app_module._calendar_slots(db.get_calendar_availability(), db.list_calendar_bookings(limit=20))[0]

    get_response = client.get(f"/book/{token}?thread={thread['id']}&name=Luis")
    post_response = client.post(
        f"/book/{token}",
        data={
            "thread_id": str(thread["id"]),
            "contact_name": "Luis",
            "contact_message": "Me interesa conversar.",
            "start_at": slot["start_at"],
            "end_at": slot["end_at"],
        },
    )

    assert get_response.status_code == 200
    assert "Reserva una" in get_response.get_data(as_text=True)
    assert post_response.status_code == 200
    assert "Reserva confirmada" in post_response.get_data(as_text=True)
    bookings = db.list_calendar_bookings(limit=10)
    assert len(bookings) == 1
    assert bookings[0]["contact_name"] == "Luis"
    assert db.get_message_thread(thread["id"])["state"] == "meeting_booked"


def test_booking_rejects_slot_outside_availability(client, app_env):
    db = app_env["db"]
    token = db.get_message_automation_config()["booking_token"]
    db.replace_calendar_availability(
        [
            {
                "weekday": datetime.now(UTC).weekday(),
                "start_time": "09:00",
                "end_time": "10:00",
                "timezone": "UTC",
            }
        ]
    )

    response = client.post(
        f"/book/{token}",
        data={
            "contact_name": "Luis",
            "contact_message": "Fuera de agenda",
            "start_at": "2030-01-01T09:00:00+00:00",
            "end_at": "2030-01-01T09:30:00+00:00",
        },
    )

    assert response.status_code == 400
    assert "Ese horario" in response.get_data(as_text=True)
