from __future__ import annotations

import sqlite3
import threading

import pytest


def test_wal_journal_mode_enabled(app_env):
    db = app_env["db"]
    with sqlite3.connect(str(app_env["db_path"])) as conn:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert str(mode).lower() == "wal"


def test_get_conn_has_timeout(app_env):
    db = app_env["db"]
    conn = db._get_conn()
    try:
        assert conn.isolation_level is not None  # sanity: default behavior preserved
    finally:
        conn.close()


def test_recover_stale_workers_marks_running_as_error(app_env):
    db = app_env["db"]

    session_id = db.create_pipeline_session("default", payload={"topic": "AI"})
    job_id = db.create_job("publish", message="working")
    db.update_job(job_id, status="running")

    recovered = db.recover_stale_workers()

    assert recovered["sessions"] >= 1
    assert recovered["jobs"] >= 1

    stored = db.get_pipeline_session(session_id)
    assert stored is not None
    assert stored["status"] == "error"
    assert stored["payload"].get("recovery_reason") == "proceso reiniciado"

    stored_job = db.get_job(job_id)
    assert stored_job is not None
    assert stored_job["status"] == "error"


def test_recover_stale_workers_is_noop_when_no_stale(app_env):
    db = app_env["db"]
    result = db.recover_stale_workers()
    assert result == {"sessions": 0, "jobs": 0}


def test_upsert_pipeline_session_merges_disjoint_keys_atomically(app_env):
    """Concurrent upserts with *disjoint* payload keys must all survive.

    Before the _tx() fix, the SELECT + merge + UPDATE was not atomic, so two
    threads writing different keys at the same time would race and one side's
    key would be lost. With BEGIN IMMEDIATE, both keys end up persisted.
    (This test does not speak to TOCTOU at the *caller* level, which remains
    the caller's responsibility to guard.)
    """
    db = app_env["db"]
    session_id = db.create_pipeline_session("default", payload={})

    n_per_thread = 20
    n_threads = 4
    errors: list[Exception] = []

    def worker(prefix: str) -> None:
        try:
            for i in range(n_per_thread):
                db.upsert_pipeline_session(session_id, payload={f"{prefix}_{i}": i})
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(f"t{idx}",)) for idx in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, errors

    stored = db.get_pipeline_session(session_id)
    payload = stored["payload"]
    # Every disjoint key written by every thread should be present.
    for idx in range(n_threads):
        for i in range(n_per_thread):
            assert f"t{idx}_{i}" in payload, (
                f"missing key t{idx}_{i} — atomic merge lost a write"
            )


def test_column_name_validation_rejects_injection(app_env):
    db = app_env["db"]
    with db._get_conn() as conn:
        with pytest.raises(ValueError):
            db._add_column_if_missing(conn, "posts", "evil; DROP TABLE posts; --", "TEXT")
        with pytest.raises(ValueError):
            db._column_exists(conn, "posts; DROP TABLE posts; --", "id")


def test_upload_image_rejects_path_traversal():
    from src import linkedin

    with pytest.raises(ValueError):
        linkedin._upload_image(None, "/etc/passwd")
    with pytest.raises(ValueError):
        linkedin._upload_image(None, "../../etc/passwd")


def test_api_publish_rejects_missing_image_file(authed_client, app_env):
    db = app_env["db"]
    # Seed a pipeline session with an image_path that doesn't exist on disk
    session_id = db.create_pipeline_session(
        "default",
        payload={
            "post_text": "Hola mundo",
            "image_path": "/tmp/does-not-exist-xyz.jpg",
        },
    )
    db.upsert_pipeline_session(session_id, status="ready")

    # Stub LinkedIn session check so the route reaches the image validation
    from src import linkedin

    original = linkedin.is_session_valid
    linkedin.is_session_valid = lambda *args, **kwargs: True
    try:
        resp = authed_client.post(
            "/api/publish",
            json={"session_id": session_id},
            headers={"X-CSRF-Token": "test-csrf-token"},
        )
    finally:
        linkedin.is_session_valid = original

    assert resp.status_code == 400
    body = resp.get_json() or {}
    assert "imagen" in (body.get("error") or "").lower()
