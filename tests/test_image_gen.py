from __future__ import annotations

from src import image_gen


def test_build_image_brief_fallback_prioritizes_copy_thesis():
    brief = image_gen._build_image_brief_fallback(
        {
            "topic": "IA en operaciones",
            "angle": "La automatizacion sin criterio crea mas retrabajo",
            "post_text": (
                "Automatizar una tarea no mejora el sistema si nadie redefine decisiones.\n\n"
                "La velocidad aparente tapa deuda operativa."
            ),
            "visual_style": "editorial",
            "content_format": "insight",
            "audience": "ops leaders",
        },
        category_cfg={"image_prompt": "Escena editorial de operaciones.", "negative_prompt": "Evita robots."},
    )

    assert "automatizacion sin criterio" in brief["core_idea"].lower()
    assert brief["visual_style"] == "editorial"
    assert brief["negative_prompt"] == "Evita robots."
    assert brief["supporting_points"]


def test_build_prompt_variants_are_distinct_but_coherent():
    brief = {
        "topic": "IA en operaciones",
        "angle": "Automatizar sin criterio genera friccion",
        "core_idea": "Automatizar sin criterio genera mas retrabajo",
        "scene": "Un equipo revisa un flujo roto tras una automatizacion mal definida",
        "subject": "Una lider de operaciones frente a un tablero fisico y colegas",
        "setting": "Oficina contemporanea con ambiente profesional",
        "focal_point": "La decision pendiente en el centro de la escena",
        "mood": "sobrio y claro",
        "social_goal": "detener el scroll con una escena creible",
        "visual_metaphor": "un cuello de botella visible",
        "supporting_points": ["velocidad aparente", "deuda operativa"],
        "category_instruction": "Visual de procesos y coordinacion",
        "negative_prompt": "Evita robots y sci-fi.",
        "visual_style": "diagram",
        "audience": "ops leaders",
    }

    variants = [image_gen._build_prompt_variant(brief, family) for family in image_gen.PROMPT_FAMILIES]

    assert len(variants) == 3
    assert len({item["family"] for item in variants}) == 3
    assert all("linkedin" in item["prompt"].lower() for item in variants)
    assert all("automatizar sin criterio" in item["prompt"].lower() for item in variants)
    assert len({item["prompt"] for item in variants}) == 3


def test_select_best_candidate_uses_family_priority():
    candidates = [
        {"family": "symbolic_grounded", "family_label": "symbolic but grounded", "composition_type": "editorial portrait", "color_direction": "blue", "remote_url": "https://img/3"},
        {"family": "hybrid_editorial_conceptual", "family_label": "hybrid editorial-conceptual", "composition_type": "editorial portrait", "color_direction": "blue", "remote_url": "https://img/2"},
        {"family": "literal_editorial", "family_label": "literal editorial", "composition_type": "editorial portrait", "color_direction": "blue", "remote_url": "https://img/1"},
    ]

    result = image_gen._select_best_candidate(candidates, {"topic": "tema"})

    assert result["image_prompt_family"] == "literal_editorial"
    assert result["image_alignment_score"] == 6.4


def test_generate_image_single_selection_pass(monkeypatch):
    selection_calls = {"count": 0}

    monkeypatch.setattr(
        image_gen,
        "_build_image_brief",
        lambda content_input, category_cfg=None: {
            "topic": "IA en operaciones",
            "angle": "Automatizar sin criterio crea deuda",
            "core_idea": "Automatizar sin criterio crea deuda",
            "scene": "Equipo revisando un flujo roto",
            "subject": "Lider de operaciones",
            "setting": "Oficina moderna",
            "focal_point": "Cuello de botella",
            "mood": "sobrio",
            "social_goal": "feed clarity",
            "visual_metaphor": "",
            "supporting_points": ["criterio", "flujo"],
            "category_instruction": "Escena editorial",
            "negative_prompt": "Evita robots",
            "visual_style": "editorial",
            "audience": "ops leaders",
        },
    )
    monkeypatch.setattr(
        image_gen,
        "_generate_image_candidates",
        lambda variants: [
            {**variants[0], "remote_url": "https://img/1"},
            {**variants[1], "remote_url": "https://img/2"},
            {**variants[2], "remote_url": "https://img/3"},
        ],
    )
    monkeypatch.setattr(image_gen, "get_vision_model", lambda: "vision-test-model")

    def fake_select(candidates, brief):
        selection_calls["count"] += 1
        return {
            "selected_candidate": candidates[0],
            "image_alignment_score": 6.5,
            "image_selection_reason": "acepta la mejor disponible",
            "image_prompt_family": candidates[0]["family"],
            "candidate_scores": [],
        }

    monkeypatch.setattr(image_gen, "_select_best_candidate", fake_select)
    monkeypatch.setattr(image_gen, "_download_selected_image", lambda remote_url: ("/static/generated/test.jpg", "/tmp/test.jpg"))

    result = image_gen.generate_image({"topic": "IA en operaciones", "visual_style": "editorial"})

    assert selection_calls["count"] == 1
    assert result["image_alignment_score"] == 6.5
    assert result["image_path"] == "/tmp/test.jpg"
    assert "IA en operaciones" in result["image_desc"]
