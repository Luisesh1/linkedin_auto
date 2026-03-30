from __future__ import annotations

from datetime import UTC, datetime

from src import scheduler


def test_compute_next_run_times_uses_configured_timezone(monkeypatch):
    monkeypatch.setattr(scheduler, "_utc_now", lambda: datetime(2026, 3, 30, 15, 0, tzinfo=UTC))
    monkeypatch.setattr(
        scheduler,
        "get_setting",
        lambda section, key, default=None: "America/Phoenix" if (section, key) == ("app", "timezone") else default,
    )

    nxt = scheduler.compute_next_run(
        {
            "enabled": True,
            "mode": "times",
            "interval_hours": 24,
            "times_of_day": ["09:30"],
            "days_of_week": [],
        }
    )

    assert nxt is not None
    assert datetime.fromisoformat(nxt) == datetime(2026, 3, 30, 16, 30, tzinfo=UTC)
