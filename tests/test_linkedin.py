from __future__ import annotations

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
