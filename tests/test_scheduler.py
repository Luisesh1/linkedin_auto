from __future__ import annotations

from datetime import UTC, datetime

from src import scheduler


def test_compute_next_run_interval_uses_future_time():
    cfg = {
        "enabled": True,
        "mode": "interval",
        "interval_hours": 6,
        "days_of_week": [],
        "last_run_at": "2026-03-10T00:00:00",
    }

    nxt = scheduler.compute_next_run(cfg)

    assert nxt is not None
    assert datetime.fromisoformat(nxt) > datetime.now(UTC)


def test_compute_next_run_times_respects_allowed_days(monkeypatch):
    monkeypatch.setattr(
        scheduler,
        "get_setting",
        lambda section, key, default=None: "UTC" if (section, key) == ("app", "timezone") else default,
    )

    cfg = {
        "enabled": True,
        "mode": "times",
        "interval_hours": 24,
        "times_of_day": ["09:00", "18:30"],
        "days_of_week": [0],
    }

    nxt = scheduler.compute_next_run(cfg)

    assert nxt is not None
    assert datetime.fromisoformat(nxt).weekday() == 0
