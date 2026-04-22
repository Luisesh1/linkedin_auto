from __future__ import annotations

from src import metrics_collector


class FakeLinkedIn:
    """Mocks src.linkedin for the metrics collector cycle.

    Records every URL it was asked to scrape so tests can verify which
    posts were touched, and returns a deterministic payload.
    """

    def __init__(self, payload: dict | None = None, fail_for_url: str | None = None, session_valid: bool = True):
        self.scraped_urls: list[str] = []
        self.payload = payload or {
            "impressions": 1234,
            "reactions": 50,
            "comments": 8,
            "reposts": 2,
            "saves": 5,
            "link_clicks": 3,
            "profile_visits": 12,
        }
        self.fail_for_url = fail_for_url
        self.session_valid = session_valid

    def is_login_in_progress(self):
        return False

    def is_session_valid(self, **kwargs):
        return self.session_valid

    def scrape_post_metrics(self, url: str, *, log=print):
        self.scraped_urls.append(url)
        if self.fail_for_url and url == self.fail_for_url:
            return None
        return dict(self.payload)


def _seed_pending_post(db, *, topic: str, url: str) -> int:
    post_id = db.save_post(topic=topic, post_text=f"texto de {topic}", published=True)
    db.update_post_linkedin_url(post_id, url)
    return post_id


def test_collect_metrics_cycle_skips_fresh_posts(app_env):
    db = app_env["db"]
    fresh_id = _seed_pending_post(db, topic="Post fresco", url="https://lnkd.in/fresh")
    db.save_post_metrics(fresh_id, impressions=900, reactions=30)

    fake = FakeLinkedIn()
    result = metrics_collector.collect_metrics_cycle(db, fake)

    assert result["status"] == "ok"
    assert result["processed"] == 0
    assert fake.scraped_urls == []


def test_collect_metrics_cycle_persists_scraped_metrics(app_env):
    db = app_env["db"]
    pending_id = _seed_pending_post(db, topic="Pending", url="https://lnkd.in/pending")

    fake = FakeLinkedIn()
    result = metrics_collector.collect_metrics_cycle(db, fake)

    assert result["status"] == "ok"
    assert result["processed"] == 1
    assert result["updated"] == 1
    assert fake.scraped_urls == ["https://lnkd.in/pending"]
    saved = db.get_post_metrics(pending_id)
    assert saved is not None
    assert saved["impressions"] == 1234
    assert saved["saves"] == 5
    assert saved["link_clicks"] == 3
    assert saved["profile_visits"] == 12


def test_collect_metrics_cycle_handles_scraper_returning_none(app_env):
    db = app_env["db"]
    pending_id = _seed_pending_post(db, topic="Pending", url="https://lnkd.in/empty")

    fake = FakeLinkedIn(fail_for_url="https://lnkd.in/empty")
    result = metrics_collector.collect_metrics_cycle(db, fake)

    assert result["processed"] == 1
    assert result["updated"] == 0
    assert result["details"][0]["result"] == "empty"
    assert db.get_post_metrics(pending_id) is None


def test_collect_metrics_cycle_skips_when_no_session(app_env):
    db = app_env["db"]
    _seed_pending_post(db, topic="Pending", url="https://lnkd.in/foo")

    fake = FakeLinkedIn(session_valid=False)
    result = metrics_collector.collect_metrics_cycle(db, fake)

    assert result["status"] == "skipped"
    assert fake.scraped_urls == []


def test_collect_metrics_cycle_records_last_collected_at(app_env):
    db = app_env["db"]
    _seed_pending_post(db, topic="Pending", url="https://lnkd.in/foo")

    fake = FakeLinkedIn()
    metrics_collector.collect_metrics_cycle(db, fake)

    sched = db.get_schedule()
    assert sched["metrics_last_collected_at"]


def test_pipeline_feedback_endpoint_returns_string(app_env, authed_client):
    db = app_env["db"]
    # Seed two posts with metrics so the feedback builder has signal
    for i, hook in enumerate(("contrarian", "clarity")):
        post_id = db.save_post(
            topic=f"Tema {i}",
            post_text=" ".join(["palabra"] * 120),
            published=True,
            hook_type=hook,
            cta_type="debate" if hook == "contrarian" else "question",
            content_format="opinion" if hook == "contrarian" else "insight",
            visual_style="editorial",
        )
        db.save_post_metrics(
            post_id,
            impressions=1500 if hook == "contrarian" else 800,
            reactions=120 if hook == "contrarian" else 8,
            comments=20 if hook == "contrarian" else 1,
            saves=10 if hook == "contrarian" else 1,
        )
    # Add a second post with the leading hook so it has 2 posts in its bucket.
    extra_id = db.save_post(
        topic="Extra ganador",
        post_text=" ".join(["palabra"] * 120),
        published=True,
        hook_type="contrarian",
        cta_type="debate",
        content_format="opinion",
        visual_style="editorial",
    )
    db.save_post_metrics(extra_id, impressions=1700, reactions=130, comments=22, saves=11)
    extra_id_2 = db.save_post(
        topic="Extra perdedor",
        post_text=" ".join(["palabra"] * 120),
        published=True,
        hook_type="clarity",
        cta_type="question",
        content_format="insight",
        visual_style="editorial",
    )
    db.save_post_metrics(extra_id_2, impressions=900, reactions=10, comments=1, saves=1)

    response = authed_client.get("/api/analytics/pipeline_feedback")
    assert response.status_code == 200
    data = response.get_json()
    assert "feedback" in data
    assert data["based_on_posts"] >= 4
    assert "PATRONES" in data["feedback"]


def test_diagnosis_endpoint_returns_verdict(app_env, authed_client):
    db = app_env["db"]
    target_id = db.save_post(
        topic="Diagnostico target",
        post_text=" ".join(["palabra"] * 120),
        published=True,
        hook_type="contrarian",
        cta_type="debate",
        content_format="opinion",
        visual_style="editorial",
    )
    db.save_post_metrics(target_id, impressions=2000, reactions=250, comments=40, saves=20)
    # peers
    for i in range(5):
        peer_id = db.save_post(
            topic=f"Peer {i}",
            post_text=" ".join(["palabra"] * 120),
            published=True,
            hook_type="clarity",
            cta_type="question",
            content_format="insight",
            visual_style="minimal",
        )
        db.save_post_metrics(peer_id, impressions=1000 + i * 50, reactions=10 + i, comments=1, saves=1)

    response = authed_client.get(f"/api/history/{target_id}/diagnosis")
    assert response.status_code == 200
    data = response.get_json()
    assert data["diagnosis"]["verdict"] in {"top", "above"}
    assert data["diagnosis"]["score"] >= 6.0
    assert data["post"]["id"] == target_id


def test_collection_settings_endpoint_persists(app_env, authed_client):
    response = authed_client.post(
        "/api/metrics/collection_settings",
        headers={"X-CSRF-Token": "test-csrf-token"},
        json={"enabled": True, "interval_hours": 4},
    )
    assert response.status_code == 200
    data = response.get_json()
    assert data["ok"] is True
    assert data["settings"]["enabled"] is True
    assert int(data["settings"]["interval_hours"]) == 4

    db = app_env["db"]
    sched = db.get_schedule()
    assert sched["metrics_collection_enabled"] is True
    assert int(sched["metrics_collection_interval_hours"]) == 4
