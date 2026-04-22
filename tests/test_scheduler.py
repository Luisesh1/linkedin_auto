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


def test_compute_next_run_with_category_rules_mode(monkeypatch):
    monkeypatch.setattr(scheduler, "_utc_now", lambda: datetime(2026, 3, 30, 8, 0, tzinfo=UTC))
    monkeypatch.setattr(
        scheduler,
        "get_setting",
        lambda section, key, default=None: "UTC" if (section, key) == ("app", "timezone") else default,
    )

    cfg = {
        "enabled": True,
        "mode": "rules",
        "rules": [
            {"days": [0], "times": ["10:00"], "category": "tech"},
            {"days": [4], "times": ["18:00"], "category": "networking"},
        ],
    }

    nxt, category = scheduler.compute_next_run_with_category(cfg)

    assert nxt is not None
    assert category == "tech"
    parsed = datetime.fromisoformat(nxt)
    assert parsed.weekday() == 0
    assert parsed.hour == 10


def test_tick_uses_random_category_from_schedule(monkeypatch):
    class FakeDB:
        def __init__(self):
            self.saved_post = None
            self.finished = None
            self.updated = None

        def get_schedule(self):
            return {
                "enabled": True,
                "mode": "interval",
                "interval_hours": 24,
                "times_of_day": [],
                "days_of_week": [],
                "category_name": "random",
                "last_run_at": "",
                "next_run_at": "2000-01-01T00:00:00+00:00",
            }

        def log_schedule_run(self, started_at, status, topic="", message=""):
            return 1

        def resolve_pipeline_category_choice(self, category_name):
            assert category_name == "random"
            return {"name": "aiRadar"}, "random"

        def save_post(self, **kwargs):
            self.saved_post = kwargs

        def finish_schedule_run(self, run_id, status, topic="", message=""):
            self.finished = (run_id, status, topic, message)

        def update_schedule_run_times(self, last_run_at, next_run_at):
            self.updated = (last_run_at, next_run_at)

    class FakeLinkedIn:
        @staticmethod
        def is_login_in_progress():
            return False

        @staticmethod
        def is_session_valid(**kwargs):
            return True

        @staticmethod
        def get_recent_posts_local():
            return []

        @staticmethod
        def publish_post(**kwargs):
            return None

    fake_db = FakeDB()

    monkeypatch.setattr(
        scheduler.pipeline,
        "run_feedback_pipeline",
        lambda **kwargs: {
            "topic": "Tema aleatorio",
            "post_text": "Texto",
            "image_path": "/tmp/test.jpg",
            "image_url": "/static/generated/test.jpg",
            "image_desc": "",
            "prompt_used": "",
            "content_brief": {"pillar": "ai", "content_format": "insight"},
            "selected_candidate": {"topic_signature": "tema aleatorio"},
            "angle_signature": "angulo aleatorio",
            "cta_type": "question",
            "hook_type": "clarity",
            "visual_style": "diagram",
            "composition_type": "abstract",
            "color_direction": "blue",
            "quality_score": 0.9,
        },
    )
    monkeypatch.setattr(scheduler, "compute_next_run", lambda cfg: "2030-01-01T00:00:00+00:00")

    scheduler._tick(fake_db, FakeLinkedIn())

    assert fake_db.saved_post is not None
    assert fake_db.saved_post["category"] == "aiRadar"
    assert fake_db.finished is not None
    assert fake_db.finished[1] == "done"
