from __future__ import annotations


def test_login_page_is_public(client):
    response = client.get("/login")

    assert response.status_code == 200
    assert "Acceso administrativo" in response.get_data(as_text=True)


def test_dashboard_redirects_to_login_when_unauthenticated(client):
    response = client.get("/", follow_redirects=False)

    assert response.status_code == 302
    assert "/login" in response.headers["Location"]


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
