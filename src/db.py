"""
SQLite persistence for published posts.
Stores full post data including image description.
"""

import os
import sqlite3
from datetime import datetime

DB_PATH = os.path.abspath("posts.db")


def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create tables if they don't exist."""
    with _get_conn() as conn:
        conn.execute("""
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
                published   INTEGER NOT NULL DEFAULT 1
            )
        """)
        try:
            conn.execute("ALTER TABLE posts ADD COLUMN category TEXT NOT NULL DEFAULT 'default'")
            conn.commit()
        except Exception:
            pass
        conn.execute("""
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
        """)
        # Migration: add days_of_week column to existing databases
        try:
            conn.execute("ALTER TABLE schedule_config ADD COLUMN days_of_week TEXT NOT NULL DEFAULT '[]'")
            conn.commit()
        except Exception:
            pass  # Column already exists
        # Seed singleton row if absent
        conn.execute("""
            INSERT OR IGNORE INTO schedule_config
                (id, enabled, mode, interval_hours, times_of_day)
            VALUES (1, 0, 'interval', 24, '[]')
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schedule_runs (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                ended_at   TEXT,
                status     TEXT NOT NULL DEFAULT 'running',
                topic      TEXT,
                message    TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_categories (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                name           TEXT NOT NULL UNIQUE,
                description    TEXT NOT NULL DEFAULT '',
                trends_prompt  TEXT NOT NULL DEFAULT '',
                history_prompt TEXT NOT NULL DEFAULT '',
                content_prompt TEXT NOT NULL DEFAULT '',
                image_prompt   TEXT NOT NULL DEFAULT '',
                is_default     INTEGER NOT NULL DEFAULT 0,
                created_at     TEXT NOT NULL,
                updated_at     TEXT NOT NULL
            )
        """)
        _seed_default_categories(conn)
        conn.commit()


def _seed_default_categories(conn):
    now = datetime.utcnow().isoformat()
    existing = conn.execute("SELECT COUNT(*) FROM pipeline_categories").fetchone()[0]
    if existing:
        return

    defaults = [
        {
            "name": "default",
            "description": "Configuración base para publicaciones profesionales de tecnología.",
            "trends_prompt": (
                "Prioriza temas actuales con relevancia profesional en tecnología, IA, "
                "mercado laboral y liderazgo."
            ),
            "history_prompt": (
                "Evita repetir enfoques recientes y mantén coherencia con el tono general "
                "de publicaciones anteriores."
            ),
            "content_prompt": (
                "Escribe una publicación profesional, concreta y útil para LinkedIn, con "
                "insights accionables y cierre con pregunta."
            ),
            "image_prompt": (
                "Genera una imagen editorial profesional, sobria y conceptual, apta para LinkedIn."
            ),
            "is_default": 1,
        },
        {
            "name": "historyTime",
            "description": "Historias en primera persona con estructura narrativa y aprendizaje profesional.",
            "trends_prompt": (
                "Buscar situaciones repetitivas en ambientes laborales comentadas recientemente "
                "que tengan relevancia social y profesional."
            ),
            "history_prompt": (
                "Crea una historia que vaya acorde con las otras historias que he comentado. "
                "No tiene que tener relación directa, pero procura no contradecirte."
            ),
            "content_prompt": (
                "Genera una breve historia narrada en primera persona relacionada al tema "
                "seleccionado y que siga etapas claras de storytelling: contexto, conflicto, "
                "aprendizaje y cierre."
            ),
            "image_prompt": (
                "Genera una imagen estilo historieta que exprese la problemática y su resolución."
            ),
            "is_default": 0,
        },
    ]
    for item in defaults:
        conn.execute(
            """
            INSERT INTO pipeline_categories
                (name, description, trends_prompt, history_prompt, content_prompt,
                 image_prompt, is_default, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item["name"],
                item["description"],
                item["trends_prompt"],
                item["history_prompt"],
                item["content_prompt"],
                item["image_prompt"],
                item["is_default"],
                now,
                now,
            ),
        )


def get_pipeline_categories() -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM pipeline_categories ORDER BY is_default DESC, name COLLATE NOCASE ASC"
        ).fetchall()
    return [dict(r) for r in rows]


def get_pipeline_category(category_name: str | None) -> dict | None:
    if not category_name:
        return get_default_pipeline_category()
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM pipeline_categories WHERE name = ?",
            (category_name,),
        ).fetchone()
    return dict(row) if row else get_default_pipeline_category()


def get_default_pipeline_category() -> dict | None:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM pipeline_categories ORDER BY is_default DESC, id ASC LIMIT 1"
        ).fetchone()
    return dict(row) if row else None


def save_pipeline_category(
    *,
    name: str,
    description: str = "",
    trends_prompt: str = "",
    history_prompt: str = "",
    content_prompt: str = "",
    image_prompt: str = "",
    is_default: bool = False,
    category_id: int | None = None,
) -> dict:
    now = datetime.utcnow().isoformat()
    with _get_conn() as conn:
        if is_default:
            conn.execute("UPDATE pipeline_categories SET is_default = 0")

        if category_id:
            conn.execute(
                """
                UPDATE pipeline_categories SET
                    name=?, description=?, trends_prompt=?, history_prompt=?,
                    content_prompt=?, image_prompt=?, is_default=?, updated_at=?
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
                    now,
                    category_id,
                ),
            )
        else:
            cur = conn.execute(
                """
                INSERT INTO pipeline_categories
                    (name, description, trends_prompt, history_prompt, content_prompt,
                     image_prompt, is_default, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    name,
                    description,
                    trends_prompt,
                    history_prompt,
                    content_prompt,
                    image_prompt,
                    1 if is_default else 0,
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
    return dict(row) if row else None


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


# ─── Schedule config helpers ──────────────────────────────────────────────────

def get_schedule() -> dict:
    import json
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
    next_run_at: str = None,
    days_of_week: list = None,
):
    import json
    with _get_conn() as conn:
        conn.execute("""
            UPDATE schedule_config SET
                enabled=?, mode=?, interval_hours=?, times_of_day=?, days_of_week=?, next_run_at=?
            WHERE id=1
        """, (1 if enabled else 0, mode, interval_hours,
              json.dumps(times_of_day), json.dumps(days_of_week or []), next_run_at))
        conn.commit()


def update_schedule_run_times(last_run_at: str, next_run_at: str):
    with _get_conn() as conn:
        conn.execute("""
            UPDATE schedule_config SET last_run_at=?, next_run_at=? WHERE id=1
        """, (last_run_at, next_run_at))
        conn.commit()


def log_schedule_run(started_at: str, status: str, topic: str = "", message: str = "") -> int:
    with _get_conn() as conn:
        cur = conn.execute("""
            INSERT INTO schedule_runs (started_at, status, topic, message)
            VALUES (?, ?, ?, ?)
        """, (started_at, status, topic, message))
        conn.commit()
        return cur.lastrowid


def finish_schedule_run(run_id: int, status: str, topic: str = "", message: str = ""):
    with _get_conn() as conn:
        conn.execute("""
            UPDATE schedule_runs SET ended_at=?, status=?, topic=?, message=?
            WHERE id=?
        """, (datetime.utcnow().isoformat(), status, topic, message, run_id))
        conn.commit()


def get_schedule_runs(limit: int = 10) -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute("""
            SELECT * FROM schedule_runs ORDER BY id DESC LIMIT ?
        """, (limit,)).fetchall()
    return [dict(r) for r in rows]


def save_post(
    topic: str,
    post_text: str,
    category: str = "default",
    image_path: str = "",
    image_url: str = "",
    image_desc: str = "",
    prompt_used: str = "",
    published: bool = True,
) -> int:
    """Insert a new post record. Returns the new row id."""
    with _get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO posts
                (created_at, topic, category, post_text, image_path, image_url,
                 image_desc, prompt_used, published)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.utcnow().isoformat(),
                topic,
                category,
                post_text,
                image_path,
                image_url,
                image_desc,
                prompt_used,
                1 if published else 0,
            ),
        )
        conn.commit()
        return cur.lastrowid


def get_posts(limit: int = 50, published_only: bool = False) -> list[dict]:
    """Return posts ordered by newest first."""
    query = "SELECT * FROM posts"
    params = []
    if published_only:
        query += " WHERE published = 1"
    query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    with _get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]


def get_recent_topics(n: int = 5) -> list[str]:
    """Return topics of the last n published posts (to avoid repetition)."""
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT topic FROM posts WHERE published=1 ORDER BY id DESC LIMIT ?", (n,)
        ).fetchall()
    return [r["topic"] for r in rows]
