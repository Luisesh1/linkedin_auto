from __future__ import annotations


def test_save_and_decode_category_json_fields(app_env):
    db = app_env["db"]

    category = db.save_pipeline_category(
        name="qa-category",
        description="QA",
        topic_keywords=["python", "tests"],
        fallback_topics=["Fallback A", "Fallback B"],
        preferred_formats=["insight", "opinion"],
        preferred_visual_styles=["diagram", "editorial"],
    )

    loaded = db.get_pipeline_category_by_id(category["id"])

    assert loaded is not None
    assert loaded["topic_keywords"] == ["python", "tests"]
    assert loaded["fallback_topics"] == ["Fallback A", "Fallback B"]
    assert loaded["preferred_formats"] == ["insight", "opinion"]
    assert loaded["preferred_visual_styles"] == ["diagram", "editorial"]


def test_init_db_seeds_multiple_configured_categories(app_env):
    db = app_env["db"]

    categories = db.get_pipeline_categories()
    names = {category["name"] for category in categories}

    assert {"default", "historyTime", "aiRadar", "liderazgoReal", "careerCompass", "creatorBrand", "buildInPublic"} <= names

    ai_radar = db.get_pipeline_category("aiRadar")

    assert ai_radar is not None
    assert ai_radar["language"] == "es"
    assert ai_radar["evidence_mode"] == "data"
    assert "agentes" in ai_radar["topic_keywords"]
    assert ai_radar["preferred_formats"] == ["insight", "opinion", "case-study"]


def test_init_db_backfills_missing_seeded_categories_without_touching_custom_ones(app_env):
    db = app_env["db"]

    custom = db.save_pipeline_category(
        name="customOps",
        description="Custom",
        topic_keywords=["ops"],
    )

    ai_radar = db.get_pipeline_category("aiRadar")
    assert ai_radar is not None

    db.delete_pipeline_category(ai_radar["id"])
    assert db.get_pipeline_category("aiRadar")["name"] == "default"

    db.init_db()

    restored = db.get_pipeline_category("aiRadar")
    categories = db.get_pipeline_categories()
    names = {category["name"] for category in categories}

    assert restored is not None
    assert restored["name"] == "aiRadar"
    assert custom["name"] in names


def test_pipeline_session_persists_payload(app_env):
    db = app_env["db"]

    session_id = db.create_pipeline_session("default", payload={"topic": "AI"})
    db.upsert_pipeline_session(session_id, payload={"image_url": "/static/generated/test.jpg"})
    stored = db.get_pipeline_session(session_id)

    assert stored is not None
    assert stored["payload"]["topic"] == "AI"
    assert stored["payload"]["image_url"] == "/static/generated/test.jpg"


def test_save_post_persists_editorial_metadata(app_env):
    db = app_env["db"]

    post_id = db.save_post(
        topic="AI agents",
        post_text="Texto",
        pillar="ai",
        topic_signature="ai agents enterprise",
        angle_signature="adopcion operativa",
        content_format="insight",
        cta_type="question",
        hook_type="contrarian",
        visual_style="diagram",
        composition_type="abstract systems diagram",
        color_direction="cyan and graphite",
        quality_score=0.91,
        published=True,
    )

    stored = db.get_post(post_id)

    assert stored is not None
    assert stored["pillar"] == "ai"
    assert stored["content_format"] == "insight"
    assert stored["visual_style"] == "diagram"
    assert stored["quality_score"] == 0.91


def test_save_post_metrics_are_joined_into_post_detail(app_env):
    db = app_env["db"]

    post_id = db.save_post(
        topic="AI agents",
        post_text="Texto",
        published=True,
    )
    db.save_post_metrics(
        post_id,
        impressions=1200,
        reactions=89,
        comments=14,
        saves=21,
        link_clicks=9,
    )

    stored = db.get_post(post_id)

    assert stored is not None
    assert stored["impressions"] == 1200
    assert stored["comments"] == 14
    assert stored["saves"] == 21
    assert stored["engagement_rate"] > 0


def test_init_db_seeds_multiple_content_categories(app_env):
    db = app_env["db"]

    names = {item["name"] for item in db.get_pipeline_categories()}

    assert {"default", "historyTime", "aiRadar", "productSense", "salesSignals", "opsPlaybook", "securityDecoded"} <= names


def test_seeded_categories_include_richer_editorial_constraints(app_env):
    db = app_env["db"]
    categories = {item["name"]: item for item in db.get_pipeline_categories()}

    for name in {
        "default",
        "historyTime",
        "aiRadar",
        "liderazgoReal",
        "careerCompass",
        "creatorBrand",
        "buildInPublic",
        "productSense",
        "salesSignals",
        "opsPlaybook",
        "securityDecoded",
    }:
        category = categories[name]
        assert category["description"]
        assert category["trends_prompt"]
        assert category["history_prompt"]
        assert category["content_prompt"]
        assert category["image_prompt"]
        assert category["negative_prompt"]
        assert len(category["fallback_topics"]) >= 3
        assert len(category["topic_keywords"]) >= 4
        assert category["hook_style"] in {"auto", "clarity", "question", "contrarian", "story", "bold"}
        assert category["cta_style"] in {"auto", "question", "debate", "reflection", "action"}

    assert categories["historyTime"]["hashtag_count"] == 0


def test_resolve_pipeline_category_choice_supports_random(app_env, monkeypatch):
    db = app_env["db"]

    monkeypatch.setattr(
        db.random,
        "choice",
        lambda categories: next(item for item in categories if item["name"] == "aiRadar"),
    )

    category, requested = db.resolve_pipeline_category_choice("random")

    assert requested == "random"
    assert category is not None
    assert category["name"] == "aiRadar"


def test_refresh_seeded_pipeline_categories_preserves_custom_categories(app_env):
    db = app_env["db"]
    db.save_pipeline_category(name="customOps", description="Custom ops", topic_keywords=["ops"])

    refreshed = db.refresh_seeded_pipeline_categories()
    names = {item["name"] for item in db.get_pipeline_categories()}

    assert refreshed
    assert "customOps" in names
    assert db.get_pipeline_category("default")["negative_prompt"]
