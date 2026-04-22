from __future__ import annotations

from pathlib import Path

from src import linkedin


def test_is_session_valid_verifies_browser_once_per_cache_window(monkeypatch, tmp_path):
    session_dir = tmp_path / "linkedin_session"
    session_flag = session_dir / "session_ok.json"
    monkeypatch.setattr(linkedin, "SESSION_DIR", str(session_dir))
    monkeypatch.setattr(linkedin, "SESSION_FLAG", str(session_flag))
    monkeypatch.setattr(linkedin, "_SESSION_PROBE_CACHE", {"checked_at": 0.0, "valid": False})

    calls = {"count": 0}

    def fake_probe(log=print):
        calls["count"] += 1
        return True

    monkeypatch.setattr(linkedin, "_probe_session_via_browser", fake_probe)
    linkedin._write_session_flag()
    linkedin._SESSION_PROBE_CACHE["checked_at"] = 0.0
    linkedin._SESSION_PROBE_CACHE["valid"] = False

    assert linkedin.is_session_valid(verify_browser=True) is True
    assert linkedin.is_session_valid(verify_browser=True) is True
    assert calls["count"] == 1


def test_cleanup_stale_profile_locks_removes_orphaned_files(monkeypatch, tmp_path):
    session_dir = tmp_path / "linkedin_session"
    default_dir = session_dir / "Default"
    default_dir.mkdir(parents=True)

    (default_dir / "LOCK").write_text("", encoding="utf-8")
    (session_dir / ".org.chromium.Chromium.dead").write_text("lock", encoding="utf-8")
    (session_dir / "SingletonLock").symlink_to("old-container-702")
    (session_dir / "SingletonCookie").symlink_to("123")
    (session_dir / "SingletonSocket").symlink_to("/tmp/fake-socket")

    monkeypatch.setattr(linkedin, "SESSION_DIR", str(session_dir))
    monkeypatch.setattr(linkedin.socket, "gethostname", lambda: "current-host")

    removed = linkedin._cleanup_stale_profile_locks(log=lambda _: None)

    assert removed is True
    assert not Path(session_dir / "SingletonLock").exists()
    assert not Path(session_dir / "SingletonCookie").exists()
    assert not Path(session_dir / "SingletonSocket").exists()
    assert not Path(default_dir / "LOCK").exists()
    assert not Path(session_dir / ".org.chromium.Chromium.dead").exists()
