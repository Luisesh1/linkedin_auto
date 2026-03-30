from __future__ import annotations

from src import trends


def test_trends_fallback_prefers_category_topics(monkeypatch):
    class BrokenClient:
        class chat:
            class completions:
                @staticmethod
                def create(**kwargs):
                    raise RuntimeError("boom")

    monkeypatch.setattr(trends, "get_xai_client", lambda: BrokenClient())
    monkeypatch.setattr(trends, "_fetch_google_news_signals", lambda max_items=12: [])
    monkeypatch.setattr(trends, "_fetch_linkedin_signals", lambda max_items=6: [])
    monkeypatch.setattr(trends, "_fetch_x_signals", lambda max_items=6: [])

    result = trends.get_trending_topics(category_cfg={"fallback_topics": ["Tema A", "Tema B"]})

    assert result[:2] == ["Tema A", "Tema B"]
