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

import requests

from src import linkedin
from src.llm import get_text_model, get_xai_client
from src.logging_utils import get_logger

REQUEST_TIMEOUT = 20
logger = get_logger(__name__)
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
        except Exception as exc:
            logger.info(
                "No se pudo leer Google News",
                extra={"event": "trends.google_news_error"},
                exc_info=exc,
            )
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
            except Exception as exc:
                logger.info(
                    "No se pudo leer RSS de X",
                    extra={"event": "trends.x_rss_error"},
                    exc_info=exc,
                )
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
    import json as _json
    topic_keywords = []
    if category_cfg:
        try:
            kw = category_cfg.get("topic_keywords", [])
            topic_keywords = kw if isinstance(kw, list) else _json.loads(kw or "[]")
        except Exception:
            topic_keywords = []
    keywords_note = ""
    if topic_keywords:
        keywords_note = f"\n\nKEYWORDS FOCO: Asegúrate de que varios temas estén relacionados con alguna de estas palabras clave: {', '.join(topic_keywords)}."
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
- Duplicados o variaciones del mismo tema{keywords_note}

EVIDENCIA:
{evidence_text}

Responde SOLO con un array JSON de 10 strings, sin markdown, sin explicaciones:
["tema 1", "tema 2", ..., "tema 10"]"""


def _infer_pillar(text: str) -> str:
    normalized = (text or "").lower()
    if any(token in normalized for token in ("ai", "ia", "llm", "model", "agent")):
        return "ai"
    if any(token in normalized for token in ("security", "cyber", "ransomware", "privacy")):
        return "cybersecurity"
    if any(token in normalized for token in ("hiring", "career", "job", "talent", "recruit")):
        return "careers"
    if any(token in normalized for token in ("leader", "management", "team", "culture", "strategy")):
        return "leadership"
    if any(token in normalized for token in ("startup", "founder", "venture", "saas")):
        return "startups"
    if any(token in normalized for token in ("developer", "software", "engineering", "code", "api")):
        return "engineering"
    return "productivity"


def _flatten_evidence(evidence: dict[str, list[str]], category_cfg: dict | None = None) -> list[dict]:
    keywords = [str(item).lower() for item in (category_cfg or {}).get("topic_keywords", [])]
    category_name = category_cfg.get("name", "default") if category_cfg else "default"
    records: list[dict] = []
    for source, items in evidence.items():
        for item in items:
            lowered = item.lower()
            records.append(
                {
                    "source": source,
                    "signal_text": item,
                    "recency": "7d",
                    "keyword_match": any(keyword in lowered for keyword in keywords),
                    "category": category_name,
                }
            )
    return records


def _candidate_from_topic(topic: str, evidence: list[dict]) -> dict:
    related_sources = [
        record["source"]
        for record in evidence
        if any(token in record["signal_text"].lower() for token in topic.lower().split()[:4])
    ]
    unique_sources = _unique(related_sources)
    freshness = min(1.0, 0.45 + 0.1 * len(unique_sources))
    return {
        "topic": topic,
        "why_now": f"El tema conecta con señales recientes de {', '.join(unique_sources[:2]) or 'múltiples fuentes'}.",
        "source_support": unique_sources or ["Síntesis"],
        "pillar": _infer_pillar(topic),
        "freshness_score": round(freshness, 3),
    }


def _fallback_topics(category_cfg: dict | None = None) -> list[str]:
    if category_cfg:
        try:
            kw = category_cfg.get("fallback_topics", [])
            custom = kw if isinstance(kw, list) else json.loads(kw or "[]")
            if custom:
                return [str(t) for t in custom[:10]]
        except Exception:
            pass

    return [
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


def get_topic_candidates(category_cfg: dict | None = None, diversify_hint: str = "") -> dict:
    evidence = {
        "Noticias internacionales": _fetch_google_news_signals(),
        "LinkedIn": _fetch_linkedin_signals(),
        "X/Twitter": _fetch_x_signals(),
    }
    evidence_records = _flatten_evidence(evidence, category_cfg=category_cfg)
    prompt = _build_prompt(evidence, category_cfg=category_cfg)
    if diversify_hint:
        prompt += (
            "\n\nInstrucción extra de diversidad:\n"
            + diversify_hint
            + "\n\nDevuelve un array JSON de objetos con esta forma exacta:\n"
            + '[{"topic":"...","why_now":"...","source_support":["..."],"pillar":"...","freshness_score":0.0}]'
        )
    else:
        prompt += (
            "\n\nAdemás de detectar temas, devuelve un array JSON de objetos con esta forma exacta:\n"
            '[{"topic":"...","why_now":"...","source_support":["..."],"pillar":"...","freshness_score":0.0}]'
        )

    try:
        response = get_xai_client().chat.completions.create(
            model=get_text_model(),
            messages=[{"role": "user", "content": prompt}],
            max_tokens=900,
        )
        raw = response.choices[0].message.content.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        raw = raw.strip()
        candidates = json.loads(raw)
        if isinstance(candidates, list) and candidates:
            normalized = []
            for item in candidates[:10]:
                if not isinstance(item, dict):
                    continue
                topic = str(item.get("topic", "")).strip()
                if not topic:
                    continue
                normalized.append(
                    {
                        "topic": topic,
                        "why_now": str(item.get("why_now", "")).strip() or f"{topic} está generando conversación profesional.",
                        "source_support": item.get("source_support")
                        if isinstance(item.get("source_support"), list)
                        else [],
                        "pillar": str(item.get("pillar", "")).strip() or _infer_pillar(topic),
                        "freshness_score": float(item.get("freshness_score", 0.65) or 0.65),
                    }
                )
            if normalized:
                return {"evidence": evidence_records, "topic_candidates": normalized}
    except Exception as exc:
        logger.warning(
            "Fallo la síntesis de candidatos, usando fallback",
            extra={"event": "trends.candidates_fallback"},
            exc_info=exc,
        )

    fallback_topics = _fallback_topics(category_cfg=category_cfg)
    return {
        "evidence": evidence_records,
        "topic_candidates": [_candidate_from_topic(topic, evidence_records) for topic in fallback_topics[:10]],
    }


def get_trending_topics(category_cfg: dict | None = None) -> list:
    try:
        candidates = get_topic_candidates(category_cfg=category_cfg)
        topics = [item.get("topic", "") for item in candidates.get("topic_candidates", []) if item.get("topic")]
        if topics:
            return topics[:10]
    except Exception as exc:
        logger.warning(
            "Fallo la síntesis de tendencias, usando fallback",
            extra={"event": "trends.synthesis_fallback"},
            exc_info=exc,
        )

    return _fallback_topics(category_cfg=category_cfg)


if __name__ == "__main__":
    print("Fetching trending topics with Grok...")
    topics = get_trending_topics()
    for i, t in enumerate(topics, 1):
        print(f"{i}. {t}")
