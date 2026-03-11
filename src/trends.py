"""
Trending topics research module.
Collects external signals from news and social sources, then asks Grok to
synthesize professional LinkedIn-ready topics from that evidence.
"""

import json
import re
import xml.etree.ElementTree as ET
from html import unescape
from urllib.parse import quote_plus

from openai import OpenAI
import requests

from src import linkedin

REQUEST_TIMEOUT = 20
NEWS_QUERIES = [
    "artificial intelligence site:reuters.com OR site:techcrunch.com OR site:theverge.com OR site:wired.com when:7d",
    "cybersecurity site:reuters.com OR site:bloomberg.com OR site:therecord.media when:7d",
    "software engineering hiring site:reuters.com OR site:techcrunch.com OR site:ft.com when:7d",
    "future of work productivity leadership site:reuters.com OR site:fortune.com OR site:hbr.org when:7d",
    "startups venture capital digital transformation site:reuters.com OR site:techcrunch.com OR site:ft.com when:7d",
]
X_SEARCH_QUERIES = [
    "AI productivity",
    "software engineering jobs",
    "cybersecurity",
]
XCANCEL_RSS_BASES = [
    "https://xcancel.com",
    "https://nitter.net",
]


def _load_api_key() -> str:
    import yaml
    with open("config.yaml") as f:
        cfg = yaml.safe_load(f)
    return cfg["grok"]["api_key"]


def _client() -> OpenAI:
    return OpenAI(api_key=_load_api_key(), base_url="https://api.x.ai/v1")


def _strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    return re.sub(r"\s+", " ", unescape(text)).strip()


def _unique(items: list[str], limit: int | None = None) -> list[str]:
    seen = set()
    out = []
    for item in items:
        normalized = re.sub(r"\s+", " ", item).strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(normalized)
        if limit and len(out) >= limit:
            break
    return out


def _get(url: str, timeout: int = REQUEST_TIMEOUT) -> str:
    response = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.text


def _fetch_google_news_signals(max_items: int = 12) -> list[str]:
    signals = []
    for query in NEWS_QUERIES:
        url = (
            "https://news.google.com/rss/search?q="
            f"{quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
        )
        try:
            xml_text = _get(url)
            root = ET.fromstring(xml_text)
            for item in root.findall("./channel/item")[:4]:
                title = (item.findtext("title") or "").strip()
                source = (item.findtext("source") or "").strip()
                if title:
                    signals.append(f"{title} [{source or 'Google News'}]")
        except Exception:
            continue
    return _unique(signals, limit=max_items)


def _parse_rss_items(xml_text: str, max_items: int = 5) -> list[str]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    items = []
    for item in root.findall("./channel/item")[:max_items]:
        title = _strip_html(item.findtext("title") or "")
        desc = _strip_html(item.findtext("description") or "")
        parts = [p for p in (title, desc) if p]
        if parts:
            items.append(" | ".join(parts))
    return items


def _fetch_x_signals(max_items: int = 6) -> list[str]:
    signals = []
    for query in X_SEARCH_QUERIES:
        encoded = quote_plus(query)
        for base in XCANCEL_RSS_BASES:
            if "xcancel.com" in base:
                url = f"{base}/search/rss?f=tweets&q={encoded}"
            else:
                url = f"{base}/search/rss?f=tweets&q={encoded}"
            try:
                items = _parse_rss_items(_get(url, timeout=6), max_items=3)
                blocked = any("whitelist" in item.lower() for item in items)
                if items and not blocked:
                    signals.extend(f"{item} [X]" for item in items)
                    break
            except Exception:
                continue
    return _unique(signals, limit=max_items)


def _fetch_linkedin_signals(max_items: int = 6) -> list[str]:
    raw_items = linkedin.collect_feed_signals(limit=max_items)
    clipped = []
    for item in raw_items:
        text = re.sub(r"\s+", " ", item).strip()
        if text:
            clipped.append(f"{text[:280]} [LinkedIn]")
    return _unique(clipped, limit=max_items)


def _category_text(category_cfg: dict | None, key: str, fallback: str) -> str:
    if not category_cfg:
        return fallback
    value = (category_cfg.get(key) or "").strip()
    return value or fallback


def _build_prompt(evidence: dict[str, list[str]], category_cfg: dict | None = None) -> str:
    sections = []
    for source, items in evidence.items():
        if items:
            lines = "\n".join(f"- {item}" for item in items)
            sections.append(f"## {source}\n{lines}")

    evidence_text = "\n\n".join(sections)
    category_name = category_cfg.get("name", "default") if category_cfg else "default"
    trends_instruction = _category_text(
        category_cfg,
        "trends_prompt",
        "Prioriza tendencias con relevancia profesional y conversación real en LinkedIn.",
    )
    return f"""Eres un investigador de tendencias para redes sociales profesionales.

Tu tarea es detectar 10 temas actuales y relevantes para publicar en LinkedIn
basándote SOLO en la evidencia externa recopilada abajo.

Categoría activa: {category_name}

Objetivo específico para esta categoría:
{trends_instruction}

Prioriza:
- Coincidencias entre varias fuentes
- Relevancia social y profesional
- Tecnología, IA, mercado laboral, liderazgo, productividad, ciberseguridad,
  startups, desarrollo de software y transformación digital
- Temas con potencial de conversación real en LinkedIn

Descarta:
- Clickbait
- Temas demasiado nicho
- Duplicados o variaciones del mismo tema

EVIDENCIA:
{evidence_text}

Responde SOLO con un array JSON de 10 strings, sin markdown, sin explicaciones:
["tema 1", "tema 2", ..., "tema 10"]"""


def get_trending_topics(category_cfg: dict | None = None) -> list:
    evidence = {
        "Noticias internacionales": _fetch_google_news_signals(),
        "LinkedIn": _fetch_linkedin_signals(),
        "X/Twitter": _fetch_x_signals(),
    }
    prompt = _build_prompt(evidence, category_cfg=category_cfg)

    try:
        response = _client().chat.completions.create(
            model="grok-3",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=600,
        )
        raw = response.choices[0].message.content.strip()

        # Strip markdown code fences if present
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        raw = raw.strip()

        topics = json.loads(raw)
        if isinstance(topics, list) and topics:
            return [str(t) for t in topics[:10]]
    except Exception:
        pass

    # Fallback: static list used only if evidence gathering and synthesis fail.
    fallback = [
        "AI agents transformando flujos de trabajo empresariales",
        "Semana laboral de cuatro días: nuevas investigaciones",
        "Productividad del desarrollador con IA generativa",
        "Liderazgo en equipos distribuidos y trabajo remoto",
        "Innovación en green tech y oportunidades laborales",
        "Modelos de lenguaje en el desarrollo de software",
        "Tendencias de ciberseguridad en 2026",
        "Herramientas no-code democratizando la creación de software",
        "Salud mental y burnout en la industria tech",
        "Modelos de IA open source cambiando el panorama",
    ]
    return fallback


if __name__ == "__main__":
    print("Fetching trending topics with Grok...")
    topics = get_trending_topics()
    for i, t in enumerate(topics, 1):
        print(f"{i}. {t}")
