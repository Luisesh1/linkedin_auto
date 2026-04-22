from __future__ import annotations

from src import metrics


def _make_post(**overrides) -> dict:
    base = {
        "id": 0,
        "topic": "tema",
        "category": "default",
        "pillar": "ai",
        "content_format": "insight",
        "hook_type": "clarity",
        "cta_type": "question",
        "visual_style": "editorial",
        "post_text": " ".join(["palabra"] * 120),
        "created_at": "2026-04-01T10:00:00+00:00",
        "impressions": 1000,
        "reactions": 50,
        "comments": 10,
        "reposts": 3,
        "profile_visits": 8,
        "link_clicks": 4,
        "saves": 6,
    }
    base.update(overrides)
    return base


def test_analyze_posts_returns_recommendations_and_top_patterns():
    posts = [
        {
            "id": 1,
            "topic": "Tema A",
            "category": "aiRadar",
            "pillar": "ai",
            "content_format": "insight",
            "hook_type": "contrarian",
            "cta_type": "debate",
            "visual_style": "diagram",
            "post_text": " ".join(["palabra"] * 140),
            "created_at": "2026-03-30T15:00:00+00:00",
            "impressions": 2000,
            "reactions": 120,
            "comments": 25,
            "reposts": 6,
            "profile_visits": 30,
            "link_clicks": 20,
            "saves": 18,
        },
        {
            "id": 2,
            "topic": "Tema B",
            "category": "aiRadar",
            "pillar": "ai",
            "content_format": "insight",
            "hook_type": "contrarian",
            "cta_type": "question",
            "visual_style": "diagram",
            "post_text": " ".join(["palabra"] * 150),
            "created_at": "2026-03-29T16:00:00+00:00",
            "impressions": 1800,
            "reactions": 90,
            "comments": 21,
            "reposts": 4,
            "profile_visits": 20,
            "link_clicks": 16,
            "saves": 14,
        },
        {
            "id": 3,
            "topic": "Tema C",
            "category": "liderazgoReal",
            "pillar": "leadership",
            "content_format": "storytelling",
            "hook_type": "story",
            "cta_type": "reflection",
            "visual_style": "editorial",
            "post_text": " ".join(["palabra"] * 70),
            "created_at": "2026-03-28T22:00:00+00:00",
            "impressions": 900,
            "reactions": 30,
            "comments": 4,
            "reposts": 1,
            "profile_visits": 5,
            "link_clicks": 2,
            "saves": 3,
        },
    ]

    analysis = metrics.analyze_posts(posts)

    assert analysis["summary"]["tracked_posts"] == 3
    assert analysis["top_posts"][0]["topic"] in {"Tema A", "Tema B"}
    assert analysis["insights"]["hook_type"][0]["key"] == "contrarian"
    assert analysis["recommendations"]


def test_diagnose_post_marks_top_when_above_p80():
    target = _make_post(id=1, topic="Top", impressions=2000, reactions=200, comments=40, saves=20)
    peers = [
        _make_post(id=2, impressions=1000, reactions=20, comments=2, saves=1),
        _make_post(id=3, impressions=1100, reactions=18, comments=1, saves=1),
        _make_post(id=4, impressions=900, reactions=15, comments=1, saves=1),
        _make_post(id=5, impressions=1200, reactions=22, comments=2, saves=1),
        _make_post(id=6, impressions=1050, reactions=25, comments=3, saves=2),
    ]
    diagnosis = metrics.diagnose_post(target, [target, *peers])

    assert diagnosis["verdict"] == "top"
    assert diagnosis["score"] >= 8.0
    assert diagnosis["comparison_pool_size"] == len(peers)
    assert diagnosis["highlights"]


def test_diagnose_post_returns_no_data_when_zero_impressions():
    target = _make_post(id=1, impressions=0, reactions=0, comments=0, saves=0)
    diagnosis = metrics.diagnose_post(target, [target])
    assert diagnosis["verdict"] == "no_data"
    assert diagnosis["score"] == 0.0
    assert diagnosis["highlights"] == []


def test_diagnose_post_neutral_when_no_peer_pool():
    target = _make_post(id=1)
    # Only one post in the system → no comparison pool.
    diagnosis = metrics.diagnose_post(target, [target])
    assert diagnosis["verdict"] == "average"
    assert diagnosis["comparison_pool_size"] == 0


def test_build_pipeline_feedback_summarizes_winning_patterns():
    posts = [
        _make_post(id=1, hook_type="contrarian", cta_type="debate", impressions=2000, reactions=200, comments=40, saves=20),
        _make_post(id=2, hook_type="contrarian", cta_type="debate", impressions=1500, reactions=150, comments=30, saves=15),
        _make_post(id=3, hook_type="clarity", cta_type="question", impressions=800, reactions=10, comments=2, saves=1),
        _make_post(id=4, hook_type="clarity", cta_type="question", impressions=900, reactions=12, comments=1, saves=1),
    ]
    feedback = metrics.build_pipeline_feedback(posts)

    assert "PATRONES QUE ESTÁN FUNCIONANDO" in feedback
    assert "PATRONES A EVITAR" in feedback
    # The "contrarian" hook should appear as the leader
    assert "contrarian" in feedback
    # The flop hook "clarity" should be flagged
    assert "clarity" in feedback


def test_build_pipeline_feedback_returns_empty_when_no_history():
    assert metrics.build_pipeline_feedback([]) == ""
    assert metrics.build_pipeline_feedback([_make_post(impressions=0)]) == ""
