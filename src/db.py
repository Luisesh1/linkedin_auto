"""
SQLite persistence for posts, scheduler state and background jobs.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime, timedelta

from src.config import get_setting


def _db_path() -> str:
    return str(get_setting("storage", "db_path"))


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _future_iso(*, hours: int) -> str:
    return (datetime.now(UTC) + timedelta(hours=hours)).isoformat()


DEFAULT_PIPELINE_CATEGORIES = [
    {
        "name": "default",
        "description": "Configuración base para publicaciones profesionales de tecnología con tono claro y accionable.",
        "trends_prompt": (
            "Prioriza temas actuales con relevancia profesional en tecnología, IA, "
            "mercado laboral, producto digital y liderazgo."
        ),
        "history_prompt": (
            "Evita repetir enfoques recientes y mantén coherencia con un tono "
            "profesional, útil y confiable."
        ),
        "content_prompt": (
            "Escribe una publicación de LinkedIn concreta y valiosa con hook claro, "
            "2 o 3 ideas accionables y cierre con pregunta que invite conversación."
        ),
        "image_prompt": (
            "Genera una imagen editorial profesional, sobria y conceptual, apta para LinkedIn."
        ),
        "is_default": 1,
        "post_length": 180,
        "language": "es",
        "hashtag_count": 4,
        "use_emojis": False,
        "topic_keywords": ["ia", "tecnologia", "liderazgo", "productividad"],
        "negative_prompt": "Evita humo, frases motivacionales vacías, exageraciones y promesas irreales.",
        "fallback_topics": [
            "Lo que cambia en equipos de tecnologia con IA generativa",
            "Errores comunes al adoptar automatizacion en empresas",
            "Como liderar cambio digital sin desgastar al equipo",
        ],
        "originality_level": 3,
        "evidence_mode": "balanced",
        "hook_style": "clarity",
        "cta_style": "question",
        "audience_focus": "profesionales tech y negocio",
        "preferred_formats": ["insight", "opinion", "case-study"],
        "preferred_visual_styles": ["editorial", "minimal", "diagram"],
    },
    {
        "name": "historyTime",
        "description": "Historias en primera persona con estructura narrativa y aprendizaje profesional.",
        "trends_prompt": (
            "Busca situaciones repetitivas en ambientes laborales, cambios de carrera "
            "y momentos de tension profesional con relevancia social."
        ),
        "history_prompt": (
            "Crea una historia alineada con experiencias previas sin contradecir el "
            "personaje ni repetir el mismo conflicto."
        ),
        "content_prompt": (
            "Genera una historia breve en primera persona con etapas claras: contexto, "
            "conflicto, decision, aprendizaje y cierre reflexivo."
        ),
        "image_prompt": (
            "Genera una ilustracion narrativa estilo historieta editorial que exprese la problematica y su resolucion."
        ),
        "is_default": 0,
        "post_length": 220,
        "language": "es",
        "hashtag_count": 3,
        "use_emojis": False,
        "topic_keywords": ["historia laboral", "aprendizaje", "liderazgo humano", "carrera"],
        "negative_prompt": "Evita moralejas obvias, victimizacion, drama forzado y frases de autoayuda.",
        "fallback_topics": [
            "La conversacion dificil que cambio mi forma de liderar",
            "El error pequeno que expuso un problema grande en el equipo",
            "La vez que elegi escuchar antes de responder",
        ],
        "originality_level": 4,
        "evidence_mode": "story",
        "hook_style": "story",
        "cta_style": "reflection",
        "audience_focus": "profesionales que lideran personas",
        "preferred_formats": ["storytelling", "insight"],
        "preferred_visual_styles": ["illustrated", "editorial"],
    },
    {
        "name": "aiRadar",
        "description": "Analisis de tendencias de IA, automatizacion y adopcion real en empresas.",
        "trends_prompt": (
            "Enfocate en IA aplicada, agentes, automatizacion, operaciones, producto y "
            "casos reales con impacto reciente en negocios."
        ),
        "history_prompt": (
            "Evita repetir la misma herramienta o hype. Prioriza cambios operativos, "
            "trade-offs y aprendizajes practicos."
        ),
        "content_prompt": (
            "Explica la tendencia con lenguaje ejecutivo-tecnico. Incluye contexto, "
            "impacto para equipos y una recomendacion concreta para actuar hoy."
        ),
        "image_prompt": (
            "Visual de sistemas de IA y automatizacion empresarial. Estilo diagrama editorial, limpio y moderno."
        ),
        "is_default": 0,
        "post_length": 190,
        "language": "es",
        "hashtag_count": 4,
        "use_emojis": False,
        "topic_keywords": ["agentes", "automatizacion", "llm", "ia aplicada", "operaciones"],
        "negative_prompt": "Evita futurismo vacio, jerga innecesaria y afirmaciones sin contexto de negocio.",
        "fallback_topics": [
            "Donde si aporta valor un agente de IA y donde todavia no",
            "Como medir ROI en automatizacion antes de escalar",
            "Riesgos invisibles al integrar IA en procesos internos",
        ],
        "originality_level": 4,
        "evidence_mode": "data",
        "hook_style": "contrarian",
        "cta_style": "debate",
        "audience_focus": "leaders de tecnologia, operaciones y producto",
        "preferred_formats": ["insight", "opinion", "case-study"],
        "preferred_visual_styles": ["diagram", "minimal", "editorial"],
    },
    {
        "name": "liderazgoReal",
        "description": "Contenido sobre liderazgo practico, cultura y toma de decisiones en equipos.",
        "trends_prompt": (
            "Detecta conversaciones sobre managers, cultura, feedback, rendimiento, "
            "delegacion y tensiones reales en equipos."
        ),
        "history_prompt": (
            "No repitas consejos genericos. Prioriza situaciones concretas y decisiones "
            "que muestren criterio de liderazgo."
        ),
        "content_prompt": (
            "Escribe como un lider experimentado: directo, humano y util. Explica una "
            "decision, el criterio detras y la leccion aplicable para otros managers."
        ),
        "image_prompt": (
            "Escena editorial de colaboracion, decision y liderazgo de equipo. Estilo profesional y humano."
        ),
        "is_default": 0,
        "post_length": 170,
        "language": "es",
        "hashtag_count": 3,
        "use_emojis": False,
        "topic_keywords": ["liderazgo", "management", "feedback", "cultura", "equipos"],
        "negative_prompt": "Evita frases de coach vacias, autoridad impostada y consejos sin contexto.",
        "fallback_topics": [
            "Como dar feedback sin romper la confianza",
            "Que delegar primero cuando un equipo crece",
            "La diferencia entre urgencia y prioridad para un manager",
        ],
        "originality_level": 3,
        "evidence_mode": "examples",
        "hook_style": "clarity",
        "cta_style": "reflection",
        "audience_focus": "managers y lideres de equipo",
        "preferred_formats": ["insight", "case-study", "opinion"],
        "preferred_visual_styles": ["editorial", "minimal"],
    },
    {
        "name": "careerCompass",
        "description": "Publicaciones sobre carrera profesional, empleabilidad y crecimiento en el mercado tech.",
        "trends_prompt": (
            "Prioriza temas de hiring, entrevistas, CV, empleabilidad, upskilling y "
            "cambios recientes del mercado laboral tech."
        ),
        "history_prompt": (
            "Evita reciclar consejos clasicos. Busca senales nuevas del mercado y "
            "recomendaciones accionables para candidatos."
        ),
        "content_prompt": (
            "Escribe para personas que quieren avanzar en su carrera. Combina realidad "
            "del mercado, consejo practico y un siguiente paso claro."
        ),
        "image_prompt": (
            "Imagen editorial de carrera profesional, crecimiento y decision laboral. Estilo limpio y aspiracional."
        ),
        "is_default": 0,
        "post_length": 200,
        "language": "es",
        "hashtag_count": 5,
        "use_emojis": True,
        "topic_keywords": ["empleabilidad", "entrevistas", "cv", "reclutamiento", "carrera tech"],
        "negative_prompt": "Evita prometer empleo facil, consejos vacios y generalidades sin contexto.",
        "fallback_topics": [
            "Senales de que tu CV no esta comunicando seniority",
            "Como prepararte para entrevistas cuando el mercado esta mas exigente",
            "Que habilidades si aumentan tu empleabilidad en 2026",
        ],
        "originality_level": 4,
        "evidence_mode": "examples",
        "hook_style": "question",
        "cta_style": "action",
        "audience_focus": "job seekers y talento tech",
        "preferred_formats": ["tutorial", "listicle", "insight"],
        "preferred_visual_styles": ["editorial", "illustrated", "minimal"],
    },
    {
        "name": "creatorBrand",
        "description": "Marca personal y posicionamiento para creadores y profesionales que publican en LinkedIn.",
        "trends_prompt": (
            "Busca debates sobre crecimiento organico, posicionamiento, construccion de "
            "audiencia y diferenciacion profesional en LinkedIn."
        ),
        "history_prompt": (
            "No repitas formulas virales. Prioriza ideas con experiencia real, claridad "
            "de posicionamiento y observaciones poco obvias."
        ),
        "content_prompt": (
            "Comparte una postura clara sobre marca personal o contenido. Tono cercano, "
            "preciso y sin vender humo. Cierra invitando a conversar."
        ),
        "image_prompt": (
            "Concepto visual de identidad profesional, visibilidad y reputacion digital. Estilo editorial contemporaneo."
        ),
        "is_default": 0,
        "post_length": 160,
        "language": "es",
        "hashtag_count": 4,
        "use_emojis": False,
        "topic_keywords": ["linkedin", "marca personal", "contenido", "audiencia", "creadores"],
        "negative_prompt": "Evita hacks virales, formulas de gurú y promesas de crecimiento instantaneo.",
        "fallback_topics": [
            "La diferencia entre publicar seguido y construir posicionamiento",
            "Por que una opinion clara atrae mejores oportunidades que el contenido neutro",
            "Que hace que un perfil se vea experto sin parecer arrogante",
        ],
        "originality_level": 5,
        "evidence_mode": "balanced",
        "hook_style": "contrarian",
        "cta_style": "debate",
        "audience_focus": "profesionales que quieren visibilidad en LinkedIn",
        "preferred_formats": ["opinion", "insight", "storytelling"],
        "preferred_visual_styles": ["editorial", "cinematic", "illustrated"],
    },
    {
        "name": "buildInPublic",
        "description": "Lecciones de producto, aprendizaje tecnico y construccion publica para founders y builders.",
        "trends_prompt": (
            "Enfocate en producto digital, experimentacion, aprendizaje rapido, ventas "
            "B2B y decisiones de construccion con senales del mercado."
        ),
        "history_prompt": (
            "Evita repetir el mismo experimento o logro. Cuenta avances, bloqueos y "
            "aprendizajes con honestidad."
        ),
        "content_prompt": (
            "Escribe como alguien que esta construyendo en tiempo real. Comparte contexto, "
            "decision tomada, resultado inicial y aprendizaje transferible."
        ),
        "image_prompt": (
            "Visual contemporaneo de producto digital, experimentacion y construccion iterativa. Estilo minimal con diagramas suaves."
        ),
        "is_default": 0,
        "post_length": 175,
        "language": "es",
        "hashtag_count": 3,
        "use_emojis": False,
        "topic_keywords": ["producto", "startup", "mvp", "experimentos", "founders"],
        "negative_prompt": "Evita presumir metricas sin contexto, storytelling falso y triunfalismo.",
        "fallback_topics": [
            "Lo que aprendes cuando lanzas antes de sentirte listo",
            "Como priorizar feedback sin convertirte en roadmap infinito",
            "Una decision pequena de producto que cambio una metrica clave",
        ],
        "originality_level": 5,
        "evidence_mode": "examples",
        "hook_style": "bold",
        "cta_style": "question",
        "audience_focus": "founders, PMs y builders",
        "preferred_formats": ["case-study", "storytelling", "insight"],
        "preferred_visual_styles": ["diagram", "minimal", "cinematic"],
    },
    {
        "name": "productSense",
        "description": "Contenido sobre estrategia de producto, discovery y toma de decisiones para equipos digitales.",
        "trends_prompt": (
            "Busca senales recientes sobre product discovery, roadmaps, priorizacion, "
            "research, experimentacion y adopcion de producto digital."
        ),
        "history_prompt": (
            "Evita repetir frameworks conocidos sin contexto. Prioriza trade-offs, "
            "decisiones imperfectas y aprendizajes aplicables."
        ),
        "content_prompt": (
            "Escribe para PMs y equipos de producto. Explica una decision, el criterio "
            "utilizado y la implicacion real para producto, negocio o usuario."
        ),
        "image_prompt": (
            "Visual de producto digital, iteracion y descubrimiento. Estilo diagrama editorial con interfaces abstractas."
        ),
        "is_default": 0,
        "post_length": 185,
        "language": "es",
        "hashtag_count": 4,
        "use_emojis": False,
        "topic_keywords": ["producto", "discovery", "roadmap", "experimentos", "research"],
        "negative_prompt": "Evita dogmas de producto, frameworks vacios y consejos sin trade-offs.",
        "fallback_topics": [
            "Como saber si un roadmap esta resolviendo ruido o estrategia",
            "Que cambia cuando discovery deja de ser solo entrevistas",
            "La metrica equivocada puede empujar a construir lo incorrecto",
        ],
        "originality_level": 4,
        "evidence_mode": "examples",
        "hook_style": "contrarian",
        "cta_style": "debate",
        "audience_focus": "PMs, founders y equipos de producto",
        "preferred_formats": ["insight", "case-study", "opinion"],
        "preferred_visual_styles": ["diagram", "editorial", "minimal"],
    },
    {
        "name": "salesSignals",
        "description": "Contenido para ventas B2B, GTM y relacion con clientes desde una perspectiva estrategica.",
        "trends_prompt": (
            "Prioriza cambios en ventas B2B, ciclos de compra, prospeccion, confianza, "
            "posicionamiento y colaboracion entre marketing, producto y revenue."
        ),
        "history_prompt": (
            "No repitas frases de vendedor. Prioriza observaciones reales sobre como "
            "compran hoy los clientes y como cambia el proceso comercial."
        ),
        "content_prompt": (
            "Escribe con tono consultivo. Aporta una lectura clara del mercado, una "
            "implicacion para equipos comerciales y una accion concreta."
        ),
        "image_prompt": (
            "Escena editorial de pipeline, reuniones B2B y momentum comercial. Estilo profesional y contemporaneo."
        ),
        "is_default": 0,
        "post_length": 170,
        "language": "es",
        "hashtag_count": 4,
        "use_emojis": False,
        "topic_keywords": ["ventas b2b", "gtm", "clientes", "pipeline", "revenue"],
        "negative_prompt": "Evita copy de vendedor agresivo, promesas faciles y frases de cierre manipuladoras.",
        "fallback_topics": [
            "El error de vender antes de entender el riesgo del cliente",
            "Por que un pipeline lleno no siempre significa demanda real",
            "Que cambia en ventas cuando el comprador llega mas informado",
        ],
        "originality_level": 4,
        "evidence_mode": "balanced",
        "hook_style": "bold",
        "cta_style": "question",
        "audience_focus": "equipos comerciales, founders y GTM leaders",
        "preferred_formats": ["opinion", "insight", "case-study"],
        "preferred_visual_styles": ["editorial", "cinematic", "minimal"],
    },
    {
        "name": "opsPlaybook",
        "description": "Operacion, procesos, productividad y escalamiento interno para equipos que quieren ejecutar mejor.",
        "trends_prompt": (
            "Enfocate en senales sobre operaciones internas, procesos, automatizacion, "
            "SOPs, eficiencia y colaboracion entre equipos."
        ),
        "history_prompt": (
            "Evita simplificar la operacion. Busca puntos de friccion concretos, "
            "cuellos de botella y mejoras con impacto real."
        ),
        "content_prompt": (
            "Explica un problema operativo, la causa raiz y una mejora concreta para "
            "ejecutar mejor. Tono claro, practico y sin jerga innecesaria."
        ),
        "image_prompt": (
            "Visual de sistemas operativos internos, flujos y coordinacion. Estilo diagrama minimal con energia moderna."
        ),
        "is_default": 0,
        "post_length": 175,
        "language": "es",
        "hashtag_count": 3,
        "use_emojis": False,
        "topic_keywords": ["operaciones", "procesos", "automatizacion", "eficiencia", "sops"],
        "negative_prompt": "Evita productivity porn, hacks irreales y simplificaciones sin contexto operativo.",
        "fallback_topics": [
            "La diferencia entre proceso documentado y proceso adoptado",
            "Que automatizar primero cuando una operacion ya esta saturada",
            "Por que algunos cuellos de botella no se resuelven con mas herramientas",
        ],
        "originality_level": 4,
        "evidence_mode": "examples",
        "hook_style": "clarity",
        "cta_style": "action",
        "audience_focus": "operaciones, project leads y equipos de ejecucion",
        "preferred_formats": ["tutorial", "insight", "case-study"],
        "preferred_visual_styles": ["diagram", "minimal", "editorial"],
    },
    {
        "name": "securityDecoded",
        "description": "Ciberseguridad explicada para negocio y tecnologia sin alarmismo ni humo.",
        "trends_prompt": (
            "Busca incidentes, cambios regulatorios, practicas de seguridad, identidad, "
            "riesgo y cultura de ciberseguridad con relevancia profesional reciente."
        ),
        "history_prompt": (
            "No repitas miedo ni headlines. Prioriza contexto, impacto, mitigacion y "
            "lo que equipos reales deberian revisar."
        ),
        "content_prompt": (
            "Escribe con claridad ejecutiva-tecnica. Explica el riesgo, por que importa "
            "y que accion concreta deberia evaluar una empresa."
        ),
        "image_prompt": (
            "Visual de seguridad digital, identidad y defensa operativa. Estilo cinematico sobrio con elementos de red abstracta."
        ),
        "is_default": 0,
        "post_length": 185,
        "language": "es",
        "hashtag_count": 4,
        "use_emojis": False,
        "topic_keywords": ["ciberseguridad", "riesgo", "identidad", "compliance", "seguridad"],
        "negative_prompt": "Evita fearmongering, tecnicismos opacos y consejos vagos sin accion.",
        "fallback_topics": [
            "Lo que un incidente menor revela sobre la cultura de seguridad",
            "Por que el riesgo de identidad ya no es solo un tema de TI",
            "Como hablar de seguridad sin paralizar a la operacion",
        ],
        "originality_level": 5,
        "evidence_mode": "data",
        "hook_style": "question",
        "cta_style": "reflection",
        "audience_focus": "leaders de tecnologia, seguridad y negocio",
        "preferred_formats": ["insight", "opinion", "case-study"],
        "preferred_visual_styles": ["cinematic", "diagram", "editorial"],
    },
]


def _get_conn():
    conn = sqlite3.connect(_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(row["name"]) == column_name for row in rows)


def _add_column_if_missing(conn: sqlite3.Connection, table_name: str, column_name: str, column_def: str) -> None:
    if _column_exists(conn, table_name, column_name):
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")


def init_db():
    """Create tables if they don't exist."""
    with _get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS posts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at  TEXT NOT NULL,
                topic       TEXT NOT NULL,
                category    TEXT NOT NULL DEFAULT 'default',
                post_text   TEXT NOT NULL,
                image_path  TEXT,
                image_url   TEXT,
                image_desc  TEXT,
                prompt_used TEXT,
                pillar      TEXT NOT NULL DEFAULT '',
                topic_signature TEXT NOT NULL DEFAULT '',
                angle_signature TEXT NOT NULL DEFAULT '',
                content_format  TEXT NOT NULL DEFAULT '',
                cta_type        TEXT NOT NULL DEFAULT '',
                hook_type       TEXT NOT NULL DEFAULT '',
                visual_style    TEXT NOT NULL DEFAULT '',
                composition_type TEXT NOT NULL DEFAULT '',
                color_direction  TEXT NOT NULL DEFAULT '',
                quality_score    REAL NOT NULL DEFAULT 0,
                published   INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        _add_column_if_missing(conn, "posts", "category", "TEXT NOT NULL DEFAULT 'default'")
        for col_name, col_def in [
            ("pillar", "TEXT NOT NULL DEFAULT ''"),
            ("topic_signature", "TEXT NOT NULL DEFAULT ''"),
            ("angle_signature", "TEXT NOT NULL DEFAULT ''"),
            ("content_format", "TEXT NOT NULL DEFAULT ''"),
            ("cta_type", "TEXT NOT NULL DEFAULT ''"),
            ("hook_type", "TEXT NOT NULL DEFAULT ''"),
            ("visual_style", "TEXT NOT NULL DEFAULT ''"),
            ("composition_type", "TEXT NOT NULL DEFAULT ''"),
            ("color_direction", "TEXT NOT NULL DEFAULT ''"),
            ("quality_score", "REAL NOT NULL DEFAULT 0"),
        ]:
            _add_column_if_missing(conn, "posts", col_name, col_def)

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_config (
                id             INTEGER PRIMARY KEY CHECK (id = 1),
                enabled        INTEGER NOT NULL DEFAULT 0,
                mode           TEXT    NOT NULL DEFAULT 'interval',
                interval_hours REAL    NOT NULL DEFAULT 24,
                times_of_day   TEXT    NOT NULL DEFAULT '[]',
                days_of_week   TEXT    NOT NULL DEFAULT '[]',
                last_run_at    TEXT,
                next_run_at    TEXT
            )
            """
        )
        _add_column_if_missing(conn, "schedule_config", "days_of_week", "TEXT NOT NULL DEFAULT '[]'")
        conn.execute(
            """
            INSERT OR IGNORE INTO schedule_config
                (id, enabled, mode, interval_hours, times_of_day)
            VALUES (1, 0, 'interval', 24, '[]')
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_runs (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                ended_at   TEXT,
                status     TEXT NOT NULL DEFAULT 'running',
                topic      TEXT,
                message    TEXT
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_categories (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                name            TEXT NOT NULL UNIQUE,
                description     TEXT NOT NULL DEFAULT '',
                trends_prompt   TEXT NOT NULL DEFAULT '',
                history_prompt  TEXT NOT NULL DEFAULT '',
                content_prompt  TEXT NOT NULL DEFAULT '',
                image_prompt    TEXT NOT NULL DEFAULT '',
                is_default      INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL,
                post_length     INTEGER NOT NULL DEFAULT 200,
                language        TEXT    NOT NULL DEFAULT 'auto',
                hashtag_count   INTEGER NOT NULL DEFAULT 4,
                use_emojis      INTEGER NOT NULL DEFAULT 0,
                topic_keywords  TEXT    NOT NULL DEFAULT '[]',
                negative_prompt TEXT    NOT NULL DEFAULT '',
                fallback_topics TEXT    NOT NULL DEFAULT '[]',
                originality_level INTEGER NOT NULL DEFAULT 3,
                evidence_mode     TEXT    NOT NULL DEFAULT 'balanced',
                hook_style        TEXT    NOT NULL DEFAULT 'auto',
                cta_style         TEXT    NOT NULL DEFAULT 'auto',
                audience_focus    TEXT    NOT NULL DEFAULT '',
                preferred_formats TEXT    NOT NULL DEFAULT '[]',
                preferred_visual_styles TEXT NOT NULL DEFAULT '[]'
            )
            """
        )
        for col_name, col_def in [
            ("post_length", "INTEGER NOT NULL DEFAULT 200"),
            ("language", "TEXT NOT NULL DEFAULT 'auto'"),
            ("hashtag_count", "INTEGER NOT NULL DEFAULT 4"),
            ("use_emojis", "INTEGER NOT NULL DEFAULT 0"),
            ("topic_keywords", "TEXT NOT NULL DEFAULT '[]'"),
            ("negative_prompt", "TEXT NOT NULL DEFAULT ''"),
            ("fallback_topics", "TEXT NOT NULL DEFAULT '[]'"),
            ("originality_level", "INTEGER NOT NULL DEFAULT 3"),
            ("evidence_mode", "TEXT NOT NULL DEFAULT 'balanced'"),
            ("hook_style", "TEXT NOT NULL DEFAULT 'auto'"),
            ("cta_style", "TEXT NOT NULL DEFAULT 'auto'"),
            ("audience_focus", "TEXT NOT NULL DEFAULT ''"),
            ("preferred_formats", "TEXT NOT NULL DEFAULT '[]'"),
            ("preferred_visual_styles", "TEXT NOT NULL DEFAULT '[]'"),
        ]:
            _add_column_if_missing(conn, "pipeline_categories", col_name, col_def)
        _seed_default_categories(conn)

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id         TEXT PRIMARY KEY,
                kind       TEXT NOT NULL,
                status     TEXT NOT NULL,
                message    TEXT NOT NULL DEFAULT '',
                payload    TEXT NOT NULL DEFAULT '{}',
                result     TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_sessions (
                id          TEXT PRIMARY KEY,
                category    TEXT NOT NULL DEFAULT 'default',
                status      TEXT NOT NULL DEFAULT 'running',
                payload     TEXT NOT NULL DEFAULT '{}',
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL,
                expires_at  TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_status_expires ON jobs(status, expires_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pipeline_sessions_expires ON pipeline_sessions(expires_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at DESC)"
        )
        conn.commit()


def cleanup_expired_state() -> None:
    now = _utc_now()
    with _get_conn() as conn:
        conn.execute(
            "UPDATE jobs SET status='expired', updated_at=? WHERE expires_at < ? AND status IN ('queued', 'running')",
            (now, now),
        )
        conn.execute("DELETE FROM pipeline_sessions WHERE expires_at < ?", (now,))
        conn.commit()


def _seed_default_categories(conn):
    now = _utc_now()
    existing_rows = conn.execute("SELECT name, is_default FROM pipeline_categories").fetchall()
    existing_names = {str(row["name"]) for row in existing_rows}
    default_exists = any(int(row["is_default"]) == 1 for row in existing_rows)

    for item in DEFAULT_PIPELINE_CATEGORIES:
        if item["name"] in existing_names:
            continue
        conn.execute(
            """
            INSERT INTO pipeline_categories
                (name, description, trends_prompt, history_prompt, content_prompt,
                 image_prompt, is_default, created_at, updated_at, post_length,
                 language, hashtag_count, use_emojis, topic_keywords, negative_prompt,
                 fallback_topics, originality_level, evidence_mode, hook_style,
                 cta_style, audience_focus, preferred_formats, preferred_visual_styles)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item["name"],
                item["description"],
                item["trends_prompt"],
                item["history_prompt"],
                item["content_prompt"],
                item["image_prompt"],
                1 if item["is_default"] and not default_exists else 0,
                now,
                now,
                item["post_length"],
                item["language"],
                item["hashtag_count"],
                1 if item["use_emojis"] else 0,
                json.dumps(item["topic_keywords"]),
                item["negative_prompt"],
                json.dumps(item["fallback_topics"]),
                item["originality_level"],
                item["evidence_mode"],
                item["hook_style"],
                item["cta_style"],
                item["audience_focus"],
                json.dumps(item["preferred_formats"]),
                json.dumps(item["preferred_visual_styles"]),
            ),
        )
        if item["is_default"] and not default_exists:
            default_exists = True


def _decode_category(row) -> dict:
    d = dict(row)
    for json_field in ("topic_keywords", "fallback_topics", "preferred_formats", "preferred_visual_styles"):
        try:
            d[json_field] = json.loads(d.get(json_field) or "[]")
        except Exception:
            d[json_field] = []
    return d


def get_pipeline_categories() -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM pipeline_categories ORDER BY is_default DESC, name COLLATE NOCASE ASC"
        ).fetchall()
    return [_decode_category(r) for r in rows]


def get_pipeline_category(category_name: str | None) -> dict | None:
    if not category_name:
        return get_default_pipeline_category()
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM pipeline_categories WHERE name = ?",
            (category_name,),
        ).fetchone()
    return _decode_category(row) if row else get_default_pipeline_category()


def get_default_pipeline_category() -> dict | None:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM pipeline_categories ORDER BY is_default DESC, id ASC LIMIT 1"
        ).fetchone()
    return _decode_category(row) if row else None


def save_pipeline_category(
    *,
    name: str,
    description: str = "",
    trends_prompt: str = "",
    history_prompt: str = "",
    content_prompt: str = "",
    image_prompt: str = "",
    is_default: bool = False,
    post_length: int = 200,
    language: str = "auto",
    hashtag_count: int = 4,
    use_emojis: bool = False,
    topic_keywords: list | None = None,
    negative_prompt: str = "",
    fallback_topics: list | None = None,
    originality_level: int = 3,
    evidence_mode: str = "balanced",
    hook_style: str = "auto",
    cta_style: str = "auto",
    audience_focus: str = "",
    preferred_formats: list | None = None,
    preferred_visual_styles: list | None = None,
    category_id: int | None = None,
) -> dict:
    topic_keywords_json = json.dumps(topic_keywords or [])
    fallback_topics_json = json.dumps(fallback_topics or [])
    preferred_formats_json = json.dumps(preferred_formats or [])
    preferred_visual_styles_json = json.dumps(preferred_visual_styles or [])
    now = _utc_now()
    with _get_conn() as conn:
        if is_default:
            conn.execute("UPDATE pipeline_categories SET is_default = 0")

        if category_id:
            conn.execute(
                """
                UPDATE pipeline_categories SET
                    name=?, description=?, trends_prompt=?, history_prompt=?,
                    content_prompt=?, image_prompt=?, is_default=?,
                    post_length=?, language=?, hashtag_count=?, use_emojis=?,
                    topic_keywords=?, negative_prompt=?, fallback_topics=?,
                    originality_level=?, evidence_mode=?, hook_style=?, cta_style=?,
                    audience_focus=?, preferred_formats=?, preferred_visual_styles=?,
                    updated_at=?
                WHERE id=?
                """,
                (
                    name,
                    description,
                    trends_prompt,
                    history_prompt,
                    content_prompt,
                    image_prompt,
                    1 if is_default else 0,
                    post_length,
                    language,
                    hashtag_count,
                    1 if use_emojis else 0,
                    topic_keywords_json,
                    negative_prompt,
                    fallback_topics_json,
                    originality_level,
                    evidence_mode,
                    hook_style,
                    cta_style,
                    audience_focus,
                    preferred_formats_json,
                    preferred_visual_styles_json,
                    now,
                    category_id,
                ),
            )
        else:
            cur = conn.execute(
                """
                INSERT INTO pipeline_categories
                    (name, description, trends_prompt, history_prompt, content_prompt,
                     image_prompt, is_default, post_length, language, hashtag_count,
                     use_emojis, topic_keywords, negative_prompt, fallback_topics,
                     originality_level, evidence_mode, hook_style, cta_style,
                     audience_focus, preferred_formats, preferred_visual_styles,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    name,
                    description,
                    trends_prompt,
                    history_prompt,
                    content_prompt,
                    image_prompt,
                    1 if is_default else 0,
                    post_length,
                    language,
                    hashtag_count,
                    1 if use_emojis else 0,
                    topic_keywords_json,
                    negative_prompt,
                    fallback_topics_json,
                    originality_level,
                    evidence_mode,
                    hook_style,
                    cta_style,
                    audience_focus,
                    preferred_formats_json,
                    preferred_visual_styles_json,
                    now,
                    now,
                ),
            )
            category_id = cur.lastrowid
        conn.commit()

    return get_pipeline_category_by_id(category_id)


def get_pipeline_category_by_id(category_id: int) -> dict | None:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM pipeline_categories WHERE id = ?",
            (category_id,),
        ).fetchone()
    return _decode_category(row) if row else None


def delete_pipeline_category(category_id: int) -> None:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT name, is_default FROM pipeline_categories WHERE id = ?",
            (category_id,),
        ).fetchone()
        if not row:
            return
        if int(row["is_default"]) == 1:
            raise ValueError("No se puede eliminar la categoría predeterminada.")

        conn.execute("DELETE FROM pipeline_categories WHERE id = ?", (category_id,))
        conn.commit()


def get_schedule() -> dict:
    with _get_conn() as conn:
        row = conn.execute("SELECT * FROM schedule_config WHERE id=1").fetchone()
    d = dict(row)
    d["times_of_day"] = json.loads(d["times_of_day"] or "[]")
    d["days_of_week"] = json.loads(d.get("days_of_week") or "[]")
    return d


def save_schedule(
    enabled: bool,
    mode: str,
    interval_hours: float,
    times_of_day: list,
    next_run_at: str | None = None,
    days_of_week: list | None = None,
):
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE schedule_config SET
                enabled=?, mode=?, interval_hours=?, times_of_day=?, days_of_week=?, next_run_at=?
            WHERE id=1
            """,
            (
                1 if enabled else 0,
                mode,
                interval_hours,
                json.dumps(times_of_day),
                json.dumps(days_of_week or []),
                next_run_at,
            ),
        )
        conn.commit()


def update_schedule_run_times(last_run_at: str, next_run_at: str | None):
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE schedule_config SET last_run_at=?, next_run_at=? WHERE id=1
            """,
            (last_run_at, next_run_at),
        )
        conn.commit()


def log_schedule_run(started_at: str, status: str, topic: str = "", message: str = "") -> int:
    with _get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO schedule_runs (started_at, status, topic, message)
            VALUES (?, ?, ?, ?)
            """,
            (started_at, status, topic, message),
        )
        conn.commit()
        return cur.lastrowid


def finish_schedule_run(run_id: int, status: str, topic: str = "", message: str = ""):
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE schedule_runs SET ended_at=?, status=?, topic=?, message=?
            WHERE id=?
            """,
            (_utc_now(), status, topic, message, run_id),
        )
        conn.commit()


def get_schedule_runs(limit: int = 10) -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM schedule_runs ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def create_job(kind: str, message: str = "", payload: dict | None = None, ttl_hours: int | None = None) -> str:
    job_id = str(uuid.uuid4())
    now = _utc_now()
    expires_at = _future_iso(hours=ttl_hours or int(get_setting("app", "job_ttl_hours", 24)))
    with _get_conn() as conn:
        conn.execute(
            """
            INSERT INTO jobs (id, kind, status, message, payload, result, created_at, updated_at, expires_at)
            VALUES (?, ?, 'queued', ?, ?, '{}', ?, ?, ?)
            """,
            (job_id, kind, message, json.dumps(payload or {}), now, now, expires_at),
        )
        conn.commit()
    return job_id


def update_job(
    job_id: str,
    *,
    status: str | None = None,
    message: str | None = None,
    payload: dict | None = None,
    result: dict | None = None,
) -> dict | None:
    current = get_job(job_id)
    if not current:
        return None

    merged_payload = current["payload"]
    if payload:
        merged_payload = {**merged_payload, **payload}

    merged_result = current["result"]
    if result:
        merged_result = {**merged_result, **result}

    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE jobs
            SET status=?, message=?, payload=?, result=?, updated_at=?
            WHERE id=?
            """,
            (
                status or current["status"],
                message if message is not None else current["message"],
                json.dumps(merged_payload),
                json.dumps(merged_result),
                _utc_now(),
                job_id,
            ),
        )
        conn.commit()
    return get_job(job_id)


def get_job(job_id: str) -> dict | None:
    cleanup_expired_state()
    with _get_conn() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if not row:
        return None
    data = dict(row)
    data["payload"] = json.loads(data.get("payload") or "{}")
    data["result"] = json.loads(data.get("result") or "{}")
    return data


def create_pipeline_session(category: str, payload: dict | None = None, ttl_hours: int | None = None) -> str:
    session_id = str(uuid.uuid4())
    now = _utc_now()
    expires_at = _future_iso(hours=ttl_hours or int(get_setting("app", "session_ttl_hours", 24)))
    with _get_conn() as conn:
        conn.execute(
            """
            INSERT INTO pipeline_sessions (id, category, status, payload, created_at, updated_at, expires_at)
            VALUES (?, ?, 'running', ?, ?, ?, ?)
            """,
            (session_id, category, json.dumps(payload or {}), now, now, expires_at),
        )
        conn.commit()
    return session_id


def upsert_pipeline_session(
    session_id: str,
    *,
    category: str | None = None,
    status: str | None = None,
    payload: dict | None = None,
) -> dict | None:
    current = get_pipeline_session(session_id)
    if not current:
        return None
    merged_payload = current["payload"]
    if payload:
        merged_payload = {**merged_payload, **payload}
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE pipeline_sessions
            SET category=?, status=?, payload=?, updated_at=?, expires_at=?
            WHERE id=?
            """,
            (
                category or current["category"],
                status or current["status"],
                json.dumps(merged_payload),
                _utc_now(),
                _future_iso(hours=int(get_setting("app", "session_ttl_hours", 24))),
                session_id,
            ),
        )
        conn.commit()
    return get_pipeline_session(session_id)


def get_pipeline_session(session_id: str) -> dict | None:
    cleanup_expired_state()
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM pipeline_sessions WHERE id = ?",
            (session_id,),
        ).fetchone()
    if not row:
        return None
    data = dict(row)
    data["payload"] = json.loads(data.get("payload") or "{}")
    return data


def delete_pipeline_session(session_id: str) -> None:
    with _get_conn() as conn:
        conn.execute("DELETE FROM pipeline_sessions WHERE id = ?", (session_id,))
        conn.commit()


def save_post(
    topic: str,
    post_text: str,
    category: str = "default",
    image_path: str = "",
    image_url: str = "",
    image_desc: str = "",
    prompt_used: str = "",
    pillar: str = "",
    topic_signature: str = "",
    angle_signature: str = "",
    content_format: str = "",
    cta_type: str = "",
    hook_type: str = "",
    visual_style: str = "",
    composition_type: str = "",
    color_direction: str = "",
    quality_score: float = 0,
    published: bool = True,
) -> int:
    with _get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO posts
                (created_at, topic, category, post_text, image_path, image_url,
                 image_desc, prompt_used, pillar, topic_signature, angle_signature,
                 content_format, cta_type, hook_type, visual_style, composition_type,
                 color_direction, quality_score, published)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _utc_now(),
                topic,
                category,
                post_text,
                image_path,
                image_url,
                image_desc,
                prompt_used,
                pillar,
                topic_signature,
                angle_signature,
                content_format,
                cta_type,
                hook_type,
                visual_style,
                composition_type,
                color_direction,
                quality_score,
                1 if published else 0,
            ),
        )
        conn.commit()
        return cur.lastrowid


def get_posts(limit: int = 50, published_only: bool = False, offset: int = 0, search: str = "") -> list[dict]:
    query = "SELECT * FROM posts"
    params: list = []
    clauses = []
    if published_only:
        clauses.append("published = 1")
    if search:
        clauses.append("(topic LIKE ? OR post_text LIKE ? OR category LIKE ?)")
        search_like = f"%{search}%"
        params.extend([search_like, search_like, search_like])
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    with _get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def count_posts(*, published_only: bool = False, search: str = "") -> int:
    query = "SELECT COUNT(*) FROM posts"
    params: list = []
    clauses = []
    if published_only:
        clauses.append("published = 1")
    if search:
        clauses.append("(topic LIKE ? OR post_text LIKE ? OR category LIKE ?)")
        search_like = f"%{search}%"
        params.extend([search_like, search_like, search_like])
    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    with _get_conn() as conn:
        return int(conn.execute(query, params).fetchone()[0])


def get_post(post_id: int) -> dict | None:
    with _get_conn() as conn:
        row = conn.execute("SELECT * FROM posts WHERE id = ?", (post_id,)).fetchone()
    return dict(row) if row else None


def get_recent_topics(n: int = 5) -> list[str]:
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT topic FROM posts WHERE published=1 ORDER BY id DESC LIMIT ?",
            (n,),
        ).fetchall()
    return [r["topic"] for r in rows]


def get_recent_posts(n: int = 5) -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                created_at AS date,
                topic,
                category,
                substr(post_text, 1, 300) AS post_text,
                id AS linkedin_post_id,
                pillar,
                topic_signature,
                angle_signature,
                content_format,
                cta_type,
                hook_type,
                visual_style,
                composition_type,
                color_direction,
                quality_score
            FROM posts
            WHERE published = 1
            ORDER BY id DESC
            LIMIT ?
            """,
            (n,),
        ).fetchall()
    return [dict(r) for r in rows]
