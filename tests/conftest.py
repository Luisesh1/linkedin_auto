from __future__ import annotations

import importlib
import sys
import time
from pathlib import Path

import pytest
from werkzeug.security import generate_password_hash

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


@pytest.fixture()
def app_env(monkeypatch, tmp_path):
    db_path = tmp_path / "test_posts.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("APP_SECRET_KEY", "test-secret")
    monkeypatch.setenv("APP_LOG_LEVEL", "CRITICAL")
    monkeypatch.setenv("ADMIN_USERNAME", "admin")
    monkeypatch.setenv("ADMIN_PASSWORD_HASH", generate_password_hash("test-password"))

    for module_name in ["app", "src.config", "src.db", "src.scheduler"]:
        if module_name in sys.modules:
            del sys.modules[module_name]

    config = importlib.import_module("src.config")
    config.reload_settings()
    db = importlib.import_module("src.db")
    db.init_db()
    app_module = importlib.import_module("app")
    app_module.initialize_runtime(start_scheduler=False)
    return {"app_module": app_module, "db": db, "db_path": db_path}


@pytest.fixture()
def client(app_env):
    return app_env["app_module"].app.test_client()


@pytest.fixture()
def authed_client(client):
    with client.session_transaction() as sess:
        sess["admin_authenticated"] = True
        sess["admin_username"] = "admin"
        sess["csrf_token"] = "test-csrf-token"
        sess["last_seen_at"] = time.time()
    return client
