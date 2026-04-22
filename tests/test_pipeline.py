from __future__ import annotations

import pytest

from src import pipeline


def test_score_topic_candidates_penalizes_repetition():
    history = [
        {
            "topic": "AI agents transformando flujos de trabajo empresariales",
            "post_text": "AI agents transformando flujos de trabajo empresariales con automatizacion.",
            "pillar": "ai",
            "angle_signature": "automatizacion empresarial",
        }
    ]
    candidates = [
        {
            "topic": "AI agents transformando flujos de trabajo empresariales",
            "why_now": "La conversacion sobre automatizacion crece.",
            "pillar": "ai",
            "freshness_score": 0.95,
        },
        {
            "topic": "Liderazgo en equipos distribuidos con IA",
            "why_now": "Los managers buscan nuevos rituales de colaboracion.",
            "pillar": "leadership",
            "freshness_score": 0.7,
        },
    ]

    scored = pipeline.score_topic_candidates(candidates, history, category_cfg={"topic_keywords": ["leadership"]})

    assert scored[0]["topic"] == "Liderazgo en equipos distribuidos con IA"
    repeated = next(item for item in scored if item["topic"].startswith("AI agents"))
    assert repeated["repetition_score"] > 0.7


def test_run_feedback_pipeline_retries_candidates_when_topic_is_repetitive(monkeypatch):
    calls = {"count": 0}

    def fake_candidates(category_cfg=None, diversify_hint=""):
        calls["count"] += 1
        if calls["count"] == 1:
            return {
                "evidence": [],
                "topic_candidates": [
                    {
                        "topic": "AI agents transformando flujos de trabajo empresariales",
                        "why_now": "Tema repetido.",
                        "pillar": "ai",
                        "freshness_score": 0.9,
                    }
                ],
            }
        return {
            "evidence": [],
            "topic_candidates": [
                {
                    "topic": "Liderazgo en equipos distribuidos con IA",
                    "why_now": "Tema nuevo.",
                    "pillar": "leadership",
                    "freshness_score": 0.8,
                }
            ],
        }

    monkeypatch.setattr(pipeline.trends, "get_topic_candidates", fake_candidates)
    monkeypatch.setattr(
        pipeline.content,
        "generate_post",
        lambda brief, history, category_cfg=None, feedback="": {
            "topic": brief["topic"],
            "reasoning": "ok",
                "post_text": (
                    "Los equipos distribuidos no necesitan mas reuniones, necesitan mejores decisiones.\n\n"
                    "Cuando los lideres documentan contexto, aclaran prioridades y crean rituales asincornos, "
                    "la colaboracion mejora incluso con zonas horarias distintas y objetivos compartidos.\n\n"
                    "Que habito operativo te ha funcionado mejor en equipos remotos?"
                ),
            "hook_type": "clarity",
            "cta_type": brief["cta_type"],
            "angle_signature": "liderazgo distribuido",
        },
    )
    monkeypatch.setattr(
        pipeline.image_gen,
        "generate_image",
        lambda brief, category_cfg=None, progress_callback=None: {
            "image_path": "/tmp/test.jpg",
            "image_url": "/static/generated/test.jpg",
            "image_desc": "desc",
            "prompt_used": "prompt",
            "visual_style": brief["visual_style"],
            "composition_type": "editorial portrait",
            "color_direction": "deep blues",
            "image_alignment_score": 8.7,
            "image_selection_reason": "La variante elegida representa mejor el argumento.",
            "image_prompt_family": "literal_editorial",
            "image_brief": {"core_idea": "Liderazgo distribuido con claridad"},
        },
    )

    payload = pipeline.run_feedback_pipeline(
        category_cfg={"name": "default", "post_length": 120, "language": "es"},
        history_fetcher=lambda limit: [
            {
                "topic": "AI agents transformando flujos de trabajo empresariales",
                "post_text": "AI agents transformando flujos de trabajo empresariales con automatizacion.",
                "pillar": "ai",
                "visual_style": "editorial",
            }
        ],
    )

    assert calls["count"] == 2
    assert payload["topic"] == "Liderazgo en equipos distribuidos con IA"
    assert payload["image_alignment_score"] == 8.7
    assert payload["image_prompt_family"] == "literal_editorial"


def test_run_feedback_pipeline_regenerates_copy_until_it_passes(monkeypatch):
    calls = {"count": 0}

    monkeypatch.setattr(
        pipeline.trends,
        "get_topic_candidates",
        lambda category_cfg=None, diversify_hint="": {
            "evidence": [],
            "topic_candidates": [
                {
                    "topic": "Developer productivity con IA",
                    "why_now": "Tema actual.",
                    "pillar": "engineering",
                    "freshness_score": 0.85,
                }
            ],
        },
    )

    def fake_generate_post(brief, history, category_cfg=None, feedback=""):
        calls["count"] += 1
        if calls["count"] == 1:
            return {
                "topic": brief["topic"],
                "reasoning": "primer intento",
                "post_text": "El futuro ya esta aqui.\n\nLa IA cambia las reglas del juego.\n\nQue opinas?",
                "hook_type": "story",
                "cta_type": brief["cta_type"],
                "angle_signature": "mensaje generico",
            }
        return {
            "topic": brief["topic"],
            "reasoning": "segundo intento",
            "post_text": (
                "La productividad del desarrollador no mejora por sumar prompts.\n\n"
                "Mejora cuando el equipo rediseña el flujo, elimina pasos repetidos "
                "y decide donde la IA agrega contexto real. Tambien mejora cuando la documentacion, "
                "los handoffs y la definicion de calidad dejan de depender de memoria individual.\n\n"
                "En tu equipo, que tarea deberia redisenarse antes de automatizarla? "
                "#IA #DeveloperProductivity"
            ),
            "hook_type": "contrarian",
            "cta_type": brief["cta_type"],
            "angle_signature": "rediseno del flujo antes de automatizar",
        }

    monkeypatch.setattr(pipeline.content, "generate_post", fake_generate_post)
    monkeypatch.setattr(
        pipeline.image_gen,
        "generate_image",
        lambda brief, category_cfg=None, progress_callback=None: {
            "image_path": "/tmp/test.jpg",
            "image_url": "/static/generated/test.jpg",
            "image_desc": "desc",
            "prompt_used": "prompt",
            "visual_style": brief["visual_style"],
            "composition_type": "editorial portrait",
            "color_direction": "deep blues",
        },
    )

    payload = pipeline.run_feedback_pipeline(
        category_cfg={"name": "default", "post_length": 80, "language": "es", "hashtag_count": 2},
        history_fetcher=lambda limit: [
            {
                "topic": "Tema viejo",
                "post_text": "Antiguo post con otro enfoque y otro cierre.",
                "pillar": "engineering",
                "visual_style": "minimal",
            }
        ],
    )

    assert calls["count"] == 2
    assert payload["quality_checks"]["copy_validation"]["passed"] is True
    assert "productividad del desarrollador" in payload["post_text"].lower()


def test_run_feedback_pipeline_rotates_visual_style_when_recent_history_is_saturated(monkeypatch):
    styles_seen: list[str] = []

    monkeypatch.setattr(
        pipeline.trends,
        "get_topic_candidates",
        lambda category_cfg=None, diversify_hint="": {
            "evidence": [],
            "topic_candidates": [
                {
                    "topic": "Automatizacion de procesos internos",
                    "why_now": "Tema actual.",
                    "pillar": "productivity",
                    "freshness_score": 0.8,
                }
            ],
        },
    )
    monkeypatch.setattr(
        pipeline.content,
        "generate_post",
        lambda brief, history, category_cfg=None, feedback="": {
            "topic": brief["topic"],
            "reasoning": "ok",
            "post_text": (
                "Automatizar no siempre acelera.\n\n"
                "Cuando un proceso esta mal diseñado, solo replica errores a mayor velocidad. "
                "Por eso conviene revisar dependencias, decisiones manuales y cuellos de botella antes "
                "de agregar agentes o workflows.\n\n"
                "Que paso de tu operacion revisarias primero? #Automation #Ops"
            ),
            "hook_type": "clarity",
            "cta_type": brief["cta_type"],
            "angle_signature": "automatizar sin redisenar",
        },
    )

    def fake_generate_image(brief, category_cfg=None, progress_callback=None):
        styles_seen.append(brief["visual_style"])
        return {
            "image_path": "/tmp/test.jpg",
            "image_url": "/static/generated/test.jpg",
            "image_desc": "desc",
            "prompt_used": "prompt",
            "visual_style": brief["visual_style"],
            "composition_type": "editorial portrait",
            "color_direction": "deep blues",
        }

    monkeypatch.setattr(pipeline.image_gen, "generate_image", fake_generate_image)

    payload = pipeline.run_feedback_pipeline(
        category_cfg={"name": "default", "post_length": 80, "language": "es"},
        history_fetcher=lambda limit: [
            {"topic": "Tema 1", "post_text": "Texto 1", "visual_style": "editorial"},
            {"topic": "Tema 2", "post_text": "Texto 2", "visual_style": "editorial"},
            {"topic": "Tema 3", "post_text": "Texto 3", "visual_style": "minimal"},
        ],
    )

    assert styles_seen
    assert styles_seen[0] != "editorial"
    assert payload["visual_style"] != "editorial"


def test_validate_topic_coherence_accepts_semantic_rephrase():
    brief = {
        "topic": "La diferencia entre incorporar IA a un flujo y depender de ella sin criterio",
    }
    post = {
        "topic": "La diferencia entre integrar IA en flujos de trabajo y depender de ella sin criterio",
        "post_text": (
            "La diferencia entre integrar IA en flujos de trabajo y depender de ella sin criterio "
            "aparece cuando el equipo delega criterio, no solo tareas."
        ),
    }

    result = pipeline.validate_topic_coherence(brief, post)

    assert result["passed"] is True
    assert result["score"] >= 0.5
    assert result["keywords"]


def test_validate_topic_coherence_rejects_unrelated_topic():
    brief = {
        "topic": "La diferencia entre incorporar IA a un flujo y depender de ella sin criterio",
    }
    post = {
        "topic": "Como construir marca personal en LinkedIn sin sonar generico",
        "post_text": (
            "Construir marca personal exige consistencia, ejemplos concretos y una voz reconocible "
            "para destacar sin repetir formulas vacias."
        ),
    }

    result = pipeline.validate_topic_coherence(brief, post)

    assert result["passed"] is False
    assert result["issues"]
    assert result["issues"][0].startswith("incluye literalmente")


def test_run_feedback_pipeline_accepts_paraphrased_topic_in_publish_gate(monkeypatch):
    monkeypatch.setattr(
        pipeline.trends,
        "get_topic_candidates",
        lambda category_cfg=None, diversify_hint="": {
            "evidence": [],
            "topic_candidates": [
                {
                    "topic": "La diferencia entre incorporar IA a un flujo y depender de ella sin criterio",
                    "why_now": "Muchos equipos confunden velocidad con claridad operativa.",
                    "pillar": "ai",
                    "freshness_score": 0.86,
                }
            ],
        },
    )
    monkeypatch.setattr(
        pipeline.content,
        "generate_post",
        lambda brief, history, category_cfg=None, feedback="": {
            "topic": "La diferencia entre integrar IA en flujos de trabajo y depender de ella sin criterio",
            "reasoning": "reformula el tema sin cambiar la tesis",
            "post_text": (
                "Integrar IA al flujo no es lo mismo que entregarle el criterio.\n\n"
                "Cuando un equipo automatiza sin definir que decision sigue siendo humana, "
                "la velocidad aparente tapa deuda operativa, contexto difuso y errores que luego "
                "cuestan mas corregir.\n\n"
                "Que decision de tu flujo nunca delegarias sin un marco claro? #IA #Ops"
            ),
            "hook_type": "clarity",
            "cta_type": brief["cta_type"],
            "angle_signature": "automatizar sin delegar el criterio",
        },
    )
    monkeypatch.setattr(
        pipeline.image_gen,
        "generate_image",
        lambda brief, category_cfg=None, progress_callback=None: {
            "image_path": "/tmp/test.jpg",
            "image_url": "/static/generated/test.jpg",
            "image_desc": "desc",
            "prompt_used": "prompt",
            "visual_style": brief["visual_style"],
            "composition_type": "editorial portrait",
            "color_direction": "deep blues",
        },
    )

    payload = pipeline.run_feedback_pipeline(
        category_cfg={"name": "default", "post_length": 80, "language": "es", "hashtag_count": 2},
        history_fetcher=lambda limit: [],
    )

    assert payload["publish_readiness"]["passed"] is True
    assert payload["publish_readiness"]["topic_coherence"]["passed"] is True
    assert "integrar ia en flujos de trabajo" in payload["topic"].lower()


def test_run_feedback_pipeline_reports_gate_issues_without_raising(monkeypatch):
    monkeypatch.setattr(
        pipeline.trends,
        "get_topic_candidates",
        lambda category_cfg=None, diversify_hint="": {
            "evidence": [],
            "topic_candidates": [
                {
                    "topic": "Tema nuevo",
                    "why_now": "Tema actual.",
                    "pillar": "productivity",
                    "freshness_score": 0.8,
                }
            ],
        },
    )
    monkeypatch.setattr(
        pipeline.content,
        "generate_post",
        lambda brief, history, category_cfg=None, feedback="": {
            "topic": brief["topic"],
            "reasoning": "ok",
            "post_text": (
                "Tema nuevo con un enfoque especifico.\n\n"
                "Hay una diferencia entre automatizar una tarea y redisenar una capacidad. "
                "La primera ahorra minutos; la segunda cambia como trabaja el equipo, como comparte contexto "
                "y como decide que calidad espera de cada entrega.\n\n"
                "Como lo ves? #Ops #AI"
            ),
            "hook_type": "clarity",
            "cta_type": brief["cta_type"],
            "angle_signature": "diferencia entre automatizar y redisenar",
        },
    )
    monkeypatch.setattr(
        pipeline.image_gen,
        "generate_image",
        lambda brief, category_cfg=None, progress_callback=None: {
            "image_path": "",
            "image_url": "",
            "image_desc": "desc",
            "prompt_used": "prompt",
            "visual_style": brief["visual_style"],
            "composition_type": "editorial portrait",
            "color_direction": "deep blues",
        },
    )

    payload = pipeline.run_feedback_pipeline(
        category_cfg={"name": "default", "post_length": 80, "language": "es"},
        history_fetcher=lambda limit: [],
    )
    readiness = payload.get("publish_readiness", {})
    assert readiness.get("passed") is False
    assert any("imagen" in issue.lower() for issue in readiness.get("issues", []))


def test_run_feedback_pipeline_raises_clear_error_when_image_generation_fails(monkeypatch):
    monkeypatch.setattr(
        pipeline.trends,
        "get_topic_candidates",
        lambda category_cfg=None, diversify_hint="": {
            "evidence": [],
            "topic_candidates": [
                {
                    "topic": "Tema nuevo",
                    "why_now": "Tema actual.",
                    "pillar": "productivity",
                    "freshness_score": 0.8,
                }
            ],
        },
    )
    monkeypatch.setattr(
        pipeline.content,
        "generate_post",
        lambda brief, history, category_cfg=None, feedback="": {
            "topic": brief["topic"],
            "reasoning": "ok",
            "post_text": (
                "Tema nuevo con un enfoque especifico.\n\n"
                "Una buena decision operativa reduce friccion antes de sumar herramientas. "
                "Cuando el equipo ordena criterio, ownership y handoffs, la velocidad mejora sin maquillaje.\n\n"
                "Que friccion revisarias primero? #Ops #AI"
            ),
            "hook_type": "clarity",
            "cta_type": brief["cta_type"],
            "angle_signature": "operacion antes que herramienta",
        },
    )
    monkeypatch.setattr(
        pipeline.image_gen,
        "generate_image",
        lambda brief, category_cfg=None, progress_callback=None: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    with pytest.raises(pipeline.PipelineStageError) as exc:
        pipeline.run_feedback_pipeline(
            category_cfg={"name": "default", "post_length": 80, "language": "es"},
            history_fetcher=lambda limit: [],
        )

    assert exc.value.step == 5
    assert "imagen válida" in str(exc.value)


def test_run_feedback_pipeline_injects_metrics_feedback_into_generate_post(monkeypatch):
    """The copy-generation step must receive metrics-derived feedback when posts
    with metrics exist in the DB."""
    captured_feedback: list[str] = []

    monkeypatch.setattr(
        pipeline.trends,
        "get_topic_candidates",
        lambda category_cfg=None, diversify_hint="": {
            "evidence": [],
            "topic_candidates": [
                {
                    "topic": "Decisiones de producto bajo presión real",
                    "why_now": "Las empresas piden criterio mas que frameworks.",
                    "pillar": "leadership",
                    "freshness_score": 0.9,
                }
            ],
        },
    )

    def fake_generate_post(brief, history, category_cfg=None, feedback=""):
        captured_feedback.append(feedback)
        return {
            "topic": brief["topic"],
            "reasoning": "ok",
            "post_text": (
                "Las decisiones de producto que recordamos son las que se sostuvieron sin aplausos en su momento.\n\n"
                "Cuando un equipo confunde velocidad con criterio, los siguientes seis meses se vuelven retrabajo "
                "disfrazado de progreso. La señal real no es cuantos features lanzamos, es cuantos pudimos sostener.\n\n"
                "Que decision incomoda evitaste y cuanto te costo despues?"
            ),
            "hook_type": "clarity",
            "cta_type": brief["cta_type"],
            "angle_signature": "criterio bajo presion",
        }

    monkeypatch.setattr(pipeline.content, "generate_post", fake_generate_post)
    monkeypatch.setattr(
        pipeline.image_gen,
        "generate_image",
        lambda brief, category_cfg=None, progress_callback=None: {
            "image_path": "/tmp/test.jpg",
            "image_url": "/static/generated/test.jpg",
            "image_desc": "desc",
            "prompt_used": "prompt",
            "visual_style": brief["visual_style"],
            "composition_type": "editorial portrait",
            "color_direction": "deep blues",
            "image_alignment_score": 8.7,
            "image_selection_reason": "ok",
            "image_prompt_family": "literal_editorial",
            "image_brief": {"core_idea": "criterio"},
        },
    )

    # Patch the loader directly so this test stays focused on the *wiring*
    # between _load_metrics_feedback and content.generate_post. The contents
    # of the feedback string are exhaustively covered in tests/test_metrics.py.
    sentinel_feedback = (
        "RETROALIMENTACIÓN DERIVADA DE MÉTRICAS REALES (basada en 4 posts):\n\n"
        "PATRONES QUE ESTÁN FUNCIONANDO:\n- Hook 'contrarian' lidera.\n\n"
        "PATRONES A EVITAR:\n- Hook 'clarity' rinde peor."
    )
    monkeypatch.setattr(pipeline, "_load_metrics_feedback", lambda: sentinel_feedback)

    payload = pipeline.run_feedback_pipeline(
        category_cfg={"name": "default", "post_length": 120, "language": "es"},
        history_fetcher=lambda limit: [],
    )

    assert captured_feedback, "generate_post should have been called at least once"
    first_feedback = captured_feedback[0]
    assert sentinel_feedback in first_feedback
    assert "PATRONES QUE ESTÁN FUNCIONANDO" in first_feedback
    # The payload should also expose the metrics feedback for the UI to render.
    assert payload.get("metrics_feedback") == sentinel_feedback


def test_build_content_brief_respects_new_category_preferences():
    brief = pipeline.build_content_brief(
        {
            "topic": "AI en reclutamiento",
            "why_now": "Las empresas estan redefiniendo hiring.",
            "pillar": "careers",
        },
        history=[
            {"content_format": "insight", "cta_type": "question", "visual_style": "editorial", "hook_type": "story"},
            {"content_format": "storytelling", "cta_type": "reflection", "visual_style": "diagram", "hook_type": "clarity"},
        ],
        category_cfg={
            "language": "es",
            "hook_style": "contrarian",
            "cta_style": "debate",
            "audience_focus": "recruiters tech",
            "preferred_formats": ["opinion", "case-study"],
            "preferred_visual_styles": ["cinematic", "illustrated"],
            "originality_level": 5,
            "evidence_mode": "data",
        },
    )

    assert brief["content_format"] in {"opinion", "case-study"}
    assert brief["visual_style"] in {"cinematic", "illustrated"}
    assert brief["hook_goal"] == "contrarian"
    assert brief["cta_type"] == "debate"
    assert brief["audience"] == "recruiters tech"
    assert brief["originality_level"] == 5
    assert brief["evidence_mode"] == "data"
