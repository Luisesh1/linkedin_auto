"""
SQLite persistence for posts, scheduler state and background jobs.
"""

from __future__ import annotations

import json
import random
import re
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

from src.config import get_setting

_SAFE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

RANDOM_CATEGORY_NAME = "random"


def _db_path() -> str:
    return str(get_setting("storage", "db_path"))


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _future_iso(*, hours: int) -> str:
    return (datetime.now(UTC) + timedelta(hours=hours)).isoformat()


SOCIAL_IMAGE_GUIDANCE = (
    " Pensada para feed de LinkedIn: una sola idea central, sujeto o escena claros, composición limpia y legible en miniatura, "
    "contraste útil, fondo controlado y sensación editorial premium. Debe representar la tesis del copy más que el tema en abstracto. "
    "Evita texto incrustado, dashboards falsos, logos, manos deformes, poses rígidas, humo visual y clichés de IA tipo robots, hologramas o circuitos brillando si no son imprescindibles."
)


def _with_image_guidance(prompt: str) -> str:
    base = str(prompt or "").strip()
    if not base:
        return SOCIAL_IMAGE_GUIDANCE.strip()
    if SOCIAL_IMAGE_GUIDANCE.strip() in base:
        return base
    return base + SOCIAL_IMAGE_GUIDANCE


DEFAULT_PIPELINE_CATEGORIES = [
    {
        "name": "default",
        "description": "Categoría editorial general para tecnología, trabajo y negocio con observaciones útiles, aterrizadas y conversacionales.",
        "trends_prompt": (
            "Detecta cambios que ya se sienten en equipos reales: decisiones, fricciones, trade-offs, hábitos de trabajo, "
            "adopción tecnológica y señales de mercado con impacto práctico. "
            "Prefiere temas donde una observación concreta aporte más valor que repetir una noticia obvia, "
            "y donde haya una decisión real que un profesional pueda revisar esta semana."
        ),
        "history_prompt": (
            "No repitas la misma tesis, el mismo villano ni la misma moraleja del historial. "
            "Si el historial ya tocó IA, rota a criterio, coordinación, carrera, ejecución o riesgo; conserva una voz sobria, humana y útil."
        ),
        "content_prompt": (
            "Escribe como alguien que entiende el trabajo real. Abre con una observación nítida, "
            "baja el tema a una escena, decisión o patrón reconocible, desarrolla 2 o 3 ideas con ejemplos plausibles "
            "y cierra con una pregunta honesta que invite conversación, no engagement bait. "
            "La pieza debe sonar fresca, específica y conversacional sin perder rigor."
        ),
        "image_prompt": _with_image_guidance(
            "Imagen editorial contemporánea con sensación profesional y humana. Escenas de trabajo real, "
            "sistemas abstractos o símbolos de coordinación y criterio. Sin texto, sin poses artificiales, "
            "sin look genérico de stock."
        ),
        "is_default": 1,
        "post_length": 190,
        "language": "es",
        "hashtag_count": 3,
        "use_emojis": False,
        "topic_keywords": ["ia aplicada", "trabajo", "liderazgo", "producto", "operaciones", "carrera"],
        "negative_prompt": (
            "Evita lenguaje de gurú, predicciones grandilocuentes y moralejas fáciles. "
            "Nada de 'esto cambiará todo', tono corporativo robótico, hooks de pregunta retórica vacía "
            "ni cierres tipo '¿qué opinas?' sin contexto."
        ),
        "fallback_topics": [
            "La diferencia entre incorporar IA a un flujo y depender de ella sin criterio",
            "Por qué muchos equipos confunden velocidad con claridad operativa",
            "La reunión que parece de alineación pero oculta una decisión no tomada",
            "Qué revela una mala prioridad sobre la forma de liderar un equipo",
        ],
        "originality_level": 4,
        "evidence_mode": "balanced",
        "hook_style": "clarity",
        "cta_style": "question",
        "audience_focus": "profesionales de tecnología, producto, operaciones y negocio",
        "preferred_formats": ["insight", "opinion", "storytelling"],
        "preferred_visual_styles": ["editorial", "minimal", "diagram"],
        "forbidden_phrases": [
            "esto cambia todo",
            "el futuro es ahora",
            "la nueva normalidad",
            "game changer",
            "no te puedes quedar atrás",
        ],
        "voice_examples": [
            "Vi a un equipo presumir un agente nuevo durante una semana. La segunda semana ya estaban discutiendo quién era responsable cuando se equivocaba.",
            "Hay decisiones que parecen lentas pero ahorran tres meses de retrabajo. El problema es que nadie aplaude las decisiones que evitaron el problema.",
        ],
    },
    {
        "name": "historyTime",
        "description": "Historias profesionales en primera persona con tensión real, aprendizaje ganado y cierre memorable.",
        "trends_prompt": (
            "Busca microconflictos reconocibles en la vida laboral: conversaciones difíciles, errores pequeños con impacto grande, "
            "cambios de criterio, decisiones incómodas, inseguridad profesional y momentos de lucidez. "
            "Deben sentirse humanos y plausibles, no melodramáticos."
        ),
        "history_prompt": (
            "Mantén coherencia con una voz vulnerable pero lúcida. "
            "No repitas el mismo tipo de conflicto, el mismo aprendizaje ni el mismo personaje implícito; rota la dimensión emocional."
        ),
        "content_prompt": (
            "Escribe una historia breve en primera persona. Empieza dentro de la escena, muestra contexto, giro o tensión, "
            "la decisión tomada y un aprendizaje que se gane por lo ocurrido, no por una moraleja impuesta. "
            "Debe sonar íntima, visual y profesional al mismo tiempo."
        ),
        "image_prompt": _with_image_guidance(
            "Ilustración narrativa estilo anime editorial en 3 viñetas, con lenguaje visual maduro y expresivo. "
            "Cada viñeta debe mostrar un momento distinto del conflicto y su resolución."
        ),
        "is_default": 0,
        "post_length": 190,
        "language": "es",
        "hashtag_count": 0,
        "use_emojis": False,
        "topic_keywords": ["historia laboral", "feedback", "liderazgo humano", "carrera", "aprendizaje"],
        "negative_prompt": (
            "Evita drama forzado, victimismo, moralejas obvias, frases de autoayuda, personajes perfectos "
            "y finales demasiado ordenados o inspiracionales. Nada de 'y entonces entendí que…' como cierre."
        ),
        "fallback_topics": [
            "La frase aparentemente inocente que me hizo cambiar mi forma de dar feedback",
            "El error mínimo que me mostró un problema mucho más profundo del equipo",
            "La vez que confundí responsabilidad con control y casi rompo la confianza",
            "El silencio de una reunión me dijo más que cualquier dashboard",
        ],
        "originality_level": 4,
        "evidence_mode": "story",
        "hook_style": "story",
        "cta_style": "reflection",
        "audience_focus": "profesionales que lideran personas o atraviesan decisiones difíciles",
        "preferred_formats": ["storytelling", "insight"],
        "preferred_visual_styles": ["illustrated", "anime", "editorial"],
        "forbidden_phrases": [
            "y entonces entendí que",
            "esto me cambió la vida",
            "una lección que nunca olvidaré",
            "quería compartir contigo",
            "fue ahí cuando me di cuenta",
        ],
        "voice_examples": [
            "La reunión empezó tarde porque nadie quería decir lo que pensaba. Yo tampoco. Tardé tres meses en darme cuenta de cuánto costaba ese silencio.",
            "Le di un feedback que creí valiente. En realidad fue cómodo: dije lo difícil con frases blandas. Ella se fue de la reunión con la duda intacta.",
        ],
    },
    {
        "name": "aiRadar",
        "description": "Análisis de tendencias de IA, automatización y adopción real en empresas, con criterio operativo y sin hype.",
        "trends_prompt": (
            "Prioriza señales de IA aplicada que cambien cómo operan equipos, productos o procesos. "
            "Busca consecuencias de segundo orden, fricciones de adopción, límites reales y decisiones que importen hoy "
            "para negocio y ejecución, no solo titulares llamativos."
        ),
        "history_prompt": (
            "Evita repetir la misma herramienta, demo o hype cycle del historial. "
            "Prefiere ángulos sobre criterio, integración, cambios en roles, ROI, riesgo operacional o gobernanza ligera."
        ),
        "content_prompt": (
            "Explica la tendencia como alguien que separa hype de utilidad. "
            "Define qué está cambiando, qué se suele malinterpretar, dónde sí aporta valor real "
            "y qué decisión concreta debería tomar un equipo ahora. Tono ejecutivo-técnico, sin worship de herramientas."
        ),
        "image_prompt": _with_image_guidance(
            "Visual editorial de sistemas de IA en contexto empresarial: flujos, supervisión humana, automatización y decisión. "
            "Limpio, sobrio y contemporáneo; sin estética sci-fi vacía."
        ),
        "is_default": 0,
        "post_length": 190,
        "language": "es",
        "hashtag_count": 3,
        "use_emojis": False,
        "topic_keywords": ["agentes", "automatizacion", "llm", "ia aplicada", "operaciones"],
        "negative_prompt": (
            "Evita futurismo grandilocuente, worship de herramientas, absolutismos y jerga vacía. "
            "Nada de 'la IA reemplazará', 'es solo cuestión de tiempo' ni consejos que ignoren coste, riesgo o cambio organizacional."
        ),
        "fallback_topics": [
            "La parte menos visible de adoptar agentes es decidir dónde no usarlos",
            "Automatizar una tarea no siempre mejora el sistema que la rodea",
            "La mayor fricción de IA en empresas suele ser operativa, no técnica",
            "Antes de pedir más prompts conviene revisar el proceso que se quiere acelerar",
        ],
        "originality_level": 4,
        "evidence_mode": "data",
        "hook_style": "contrarian",
        "cta_style": "debate",
        "audience_focus": "líderes de tecnología, operaciones, producto y transformación",
        "preferred_formats": ["insight", "opinion", "case-study"],
        "preferred_visual_styles": ["diagram", "minimal", "editorial"],
        "forbidden_phrases": [
            "la IA va a reemplazar",
            "es solo cuestión de tiempo",
            "el futuro del trabajo",
            "revolucionario",
            "imprescindible para no quedarte atrás",
        ],
        "voice_examples": [
            "El equipo lanzó tres agentes en un mes. Lo difícil no fue construirlos: fue decidir quién responde cuando uno se equivoca con un cliente.",
            "Adoptar IA no es un problema técnico cuando llega a operaciones. Es un problema de criterio: qué decisiones siguen siendo humanas y por qué.",
        ],
    },
    {
        "name": "liderazgoReal",
        "description": "Liderazgo práctico, cultura y toma de decisiones en equipos, contado desde la experiencia y no desde el manual.",
        "trends_prompt": (
            "Detecta tensiones reales de liderazgo: feedback difícil, delegación, desempeño desigual, alineación, "
            "cultura bajo presión y decisiones impopulares que un manager no puede esquivar."
        ),
        "history_prompt": (
            "Evita consejos genéricos o tono de manual del historial. "
            "Prioriza escenas concretas, criterios de decisión y consecuencias humanas u operativas que otros líderes puedan reconocer."
        ),
        "content_prompt": (
            "Escribe como un líder que ya pagó el costo de equivocarse. Expón una situación concreta, el criterio usado, "
            "la tensión entre opciones y la lección útil sin sonar paternalista. "
            "Debe sentirse vivido y específico, no abstracto."
        ),
        "image_prompt": _with_image_guidance(
            "Escena editorial de liderazgo y coordinación: conversación difícil, alineación, criterio y responsabilidad compartida. "
            "Humana, sobria y creíble."
        ),
        "is_default": 0,
        "post_length": 165,
        "language": "es",
        "hashtag_count": 2,
        "use_emojis": False,
        "topic_keywords": ["liderazgo", "management", "feedback", "cultura", "equipos"],
        "negative_prompt": (
            "Evita tono de coach, frases solemnes, autoridad inflada y consejos sin contexto. "
            "Nada de héroes gerenciales perfectos, citas motivacionales ni 'el verdadero líder…' como apertura."
        ),
        "fallback_topics": [
            "Delegar no es soltar trabajo, es transferir criterio poco a poco",
            "El feedback más difícil suele empezar antes de la conversación formal",
            "Hay decisiones de liderazgo lentas que evitan meses de desgaste",
            "La confianza del equipo se gana más por consistencia que por carisma",
        ],
        "originality_level": 3,
        "evidence_mode": "examples",
        "hook_style": "clarity",
        "cta_style": "reflection",
        "audience_focus": "managers y líderes de equipo",
        "preferred_formats": ["insight", "case-study", "opinion"],
        "preferred_visual_styles": ["editorial", "minimal"],
        "forbidden_phrases": [
            "el verdadero líder",
            "los grandes líderes",
            "lidera con el ejemplo",
            "rodéate de los mejores",
            "el liderazgo es",
        ],
        "voice_examples": [
            "La conversación más difícil que tuve este año duró ocho minutos. La preparé durante dos semanas. La aplazaba porque sabía que iba a romper algo que ya estaba roto.",
            "Pedí más contexto antes de decidir. El equipo lo leyó como duda, no como criterio. Aprendí a explicar también el cómo decido, no solo el qué decido.",
        ],
    },
    {
        "name": "careerCompass",
        "description": "Carrera profesional y empleabilidad explicadas con realismo, señales concretas y sin consejos reciclados.",
        "trends_prompt": (
            "Prioriza señales nuevas sobre empleabilidad, entrevistas, movilidad, seniority, aprendizaje útil y cambios del mercado tech. "
            "Busca temas donde haya una decisión práctica para candidatos, managers o recruiters, no listas obvias."
        ),
        "history_prompt": (
            "Evita consejos clásicos sin contexto del historial. "
            "Prefiere observaciones sobre cómo se interpreta hoy la experiencia, qué señales generan confianza y qué errores siguen frenando buenas oportunidades."
        ),
        "content_prompt": (
            "Escribe con tono claro y cercano. Combina lectura honesta del mercado, una interpretación útil y un siguiente paso concreto. "
            "Debe sentirse como consejo de alguien que observa procesos reales de contratación, no como un hilo de tips."
        ),
        "image_prompt": _with_image_guidance(
            "Imagen editorial sobre decisiones de carrera, entrevistas y crecimiento profesional. "
            "Humana, limpia y aspiracional sin caer en iconografía genérica de CV o escaleras."
        ),
        "is_default": 0,
        "post_length": 200,
        "language": "es",
        "hashtag_count": 3,
        "use_emojis": False,
        "topic_keywords": ["empleabilidad", "entrevistas", "cv", "reclutamiento", "carrera tech"],
        "negative_prompt": (
            "Evita prometer resultados fáciles, listas vacías, optimism wash y frases de autoayuda. "
            "Nada de 'consigue tu trabajo soñado en X pasos' ni fórmulas universales sin contexto."
        ),
        "fallback_topics": [
            "Un CV fuerte no solo enumera experiencia, reduce incertidumbre",
            "La seniority se nota más en criterio y trade-offs que en años acumulados",
            "Muchas entrevistas fallan porque el candidato responde bien pero no traduce impacto",
            "Aprender una herramienta no pesa igual que aprender a decidir mejor con ella",
        ],
        "originality_level": 4,
        "evidence_mode": "examples",
        "hook_style": "clarity",
        "cta_style": "reflection",
        "audience_focus": "profesionales tech, recruiters y personas en transición de carrera",
        "preferred_formats": ["tutorial", "listicle", "insight"],
        "preferred_visual_styles": ["editorial", "illustrated", "minimal"],
        "forbidden_phrases": [
            "consigue tu trabajo soñado",
            "haz lo que amas",
            "el trabajo de tus sueños",
            "tip que nadie te dice",
            "el mejor consejo que recibí",
        ],
        "voice_examples": [
            "Los recruiters no leen CV, los escanean. Un buen CV no enumera experiencia: quita dudas en quince segundos sobre si vale la pena una entrevista.",
            "La seniority no es la cantidad de proyectos. Es saber qué decisiones tomaste cuando todo estaba ambiguo y nadie te iba a aplaudir por la elección correcta.",
        ],
    },
    {
        "name": "creatorBrand",
        "description": "Marca personal y posicionamiento para profesionales que publican en LinkedIn, sin fórmulas virales ni gurús.",
        "trends_prompt": (
            "Busca tensiones reales sobre visibilidad, reputación, diferenciación y construcción de audiencia profesional. "
            "Prioriza temas donde publicar tenga relación con confianza, posicionamiento y oportunidades, no solo alcance."
        ),
        "history_prompt": (
            "Evita fórmulas virales, consejos de gurú y obsesión con métricas superficiales del historial. "
            "Prefiere observaciones incómodas pero útiles sobre cómo se construye credibilidad con el tiempo."
        ),
        "content_prompt": (
            "Toma una postura clara sobre contenido o marca personal y defiéndela con ejemplos, matices y lenguaje humano. "
            "Debe sentirse como criterio propio, no como receta de crecimiento."
        ),
        "image_prompt": _with_image_guidance(
            "Concepto editorial de reputación, identidad profesional y visibilidad sostenible. "
            "Contemporáneo, elegante y con personalidad; sin clichés de influencers."
        ),
        "is_default": 0,
        "post_length": 160,
        "language": "es",
        "hashtag_count": 2,
        "use_emojis": False,
        "topic_keywords": ["linkedin", "marca personal", "contenido", "audiencia", "creadores"],
        "negative_prompt": (
            "Evita hacks, growth tricks, lenguaje de creador vendehumo y promesas de viralidad. "
            "Nada de 'el algoritmo premia X', 'la fórmula que cambió mi cuenta' ni listas de tips genéricos."
        ),
        "fallback_topics": [
            "Publicar más no corrige una idea de posicionamiento borrosa",
            "La gente recuerda una postura clara antes que un calendario perfecto",
            "Mucho contenido falla no por calidad sino por falta de criterio visible",
            "La marca personal crece cuando lo que dices y lo que demuestras empiezan a coincidir",
        ],
        "originality_level": 5,
        "evidence_mode": "balanced",
        "hook_style": "contrarian",
        "cta_style": "debate",
        "audience_focus": "profesionales que quieren visibilidad sostenible en LinkedIn",
        "preferred_formats": ["opinion", "insight", "storytelling"],
        "preferred_visual_styles": ["editorial", "cinematic", "illustrated"],
        "forbidden_phrases": [
            "el algoritmo premia",
            "la fórmula que cambió",
            "secretos del crecimiento",
            "hack viral",
            "no te puedes perder esto",
        ],
        "voice_examples": [
            "Publicar todos los días no arregla una idea borrosa. Solo la repite más rápido. La calidad de la postura pesa más que la frecuencia.",
            "Los perfiles que recuerdo no son los más activos. Son los que tienen una postura clara y la sostienen aunque el feed esté en otra ola.",
        ],
    },
    {
        "name": "buildInPublic",
        "description": "Lecciones de producto, aprendizaje y construcción pública para founders y builders, sin triunfalismo.",
        "trends_prompt": (
            "Enfócate en decisiones de producto, distribución, aprendizaje y construcción pública con tensión real. "
            "Busca avances, tropiezos, renuncias y hallazgos que otros builders puedan reutilizar."
        ),
        "history_prompt": (
            "Evita el triunfalismo y las historias demasiado redondas del historial. "
            "Prioriza aprendizaje en tiempo real, dudas razonables, cambios de criterio y señales pequeñas que alteraron una decisión."
        ),
        "content_prompt": (
            "Escribe como alguien que está construyendo mientras aprende. "
            "Explica qué estabas intentando resolver, qué descubriste, qué cambió en tu decisión "
            "y qué puede aprovechar otro builder sin copiarte. Tono honesto, sin pose de founder hero."
        ),
        "image_prompt": _with_image_guidance(
            "Visual contemporáneo de construcción iterativa: producto, prototipos, sistemas y decisión bajo incertidumbre. "
            "Minimalista pero vivo, con energía de progreso real."
        ),
        "is_default": 0,
        "post_length": 175,
        "language": "es",
        "hashtag_count": 2,
        "use_emojis": False,
        "topic_keywords": ["producto", "startup", "mvp", "experimentos", "founders"],
        "negative_prompt": (
            "Evita build in public performativo, métricas sin contexto, storytelling falso de founder hero y moralejas de ganador. "
            "Nada de 'pasamos de 0 a X en Y días' sin contexto real."
        ),
        "fallback_topics": [
            "A veces compartir progreso sirve menos para vender y más para pensar mejor",
            "Lanzar temprano no acelera si todavía no entendiste el problema",
            "Un buen experimento reduce dudas; uno malo solo produce actividad",
            "La mejor señal de producto a veces no es una métrica sino una objeción repetida",
        ],
        "originality_level": 5,
        "evidence_mode": "examples",
        "hook_style": "bold",
        "cta_style": "question",
        "audience_focus": "founders, PMs y builders",
        "preferred_formats": ["case-study", "storytelling", "insight"],
        "preferred_visual_styles": ["diagram", "minimal", "editorial"],
        "forbidden_phrases": [
            "de 0 a",
            "lo logramos",
            "el viaje de un emprendedor",
            "rompimos récord",
            "el secreto detrás de",
        ],
        "voice_examples": [
            "Lanzamos antes de estar listos. Aprendimos rápido, sí, pero también aprendimos lo que no sabíamos: el problema no era el producto, era nuestra hipótesis del cliente.",
            "Una objeción repetida cinco veces nos enseñó más que cualquier survey. La frustración del cliente era la métrica que faltaba en el dashboard.",
        ],
    },
    {
        "name": "productSense",
        "description": "Estrategia de producto, discovery y toma de decisiones para equipos digitales, con trade-offs reales.",
        "trends_prompt": (
            "Busca señales recientes sobre discovery, priorización, adopción, research y decisiones de producto "
            "con tensión real entre usuario, negocio y ejecución."
        ),
        "history_prompt": (
            "Evita repetir frameworks conocidos como verdad absoluta del historial. "
            "Prefiere trade-offs, decisiones imperfectas, costes de oportunidad y efectos colaterales de producto."
        ),
        "content_prompt": (
            "Escribe para PMs y equipos digitales. Explica una decisión o patrón, el criterio detrás "
            "y la implicación concreta para usuario, negocio y velocidad de ejecución. Debe sonar práctico, no doctrinario."
        ),
        "image_prompt": _with_image_guidance(
            "Visual editorial de producto digital, descubrimiento y toma de decisiones. "
            "Interfaces abstractas, tensión entre opciones y sensación de claridad estratégica."
        ),
        "is_default": 0,
        "post_length": 185,
        "language": "es",
        "hashtag_count": 3,
        "use_emojis": False,
        "topic_keywords": ["producto", "discovery", "roadmap", "experimentos", "research"],
        "negative_prompt": (
            "Evita dogma de producto, frases de framework, obviedades sobre usuarios y consejos que ignoren coste o capacidad. "
            "Nada de 'enamórate del problema, no de la solución' como muletilla."
        ),
        "fallback_topics": [
            "A veces un roadmap ordenado solo está maquillando una estrategia confusa",
            "El discovery útil no siempre produce certezas, pero sí mejores preguntas",
            "Una métrica puede mejorar mientras el producto se vuelve menos claro",
            "No toda deuda de producto es técnica; mucha se acumula en decisiones mal cerradas",
        ],
        "originality_level": 4,
        "evidence_mode": "examples",
        "hook_style": "contrarian",
        "cta_style": "debate",
        "audience_focus": "PMs, founders y equipos de producto",
        "preferred_formats": ["insight", "case-study", "opinion"],
        "preferred_visual_styles": ["diagram", "editorial", "minimal"],
        "forbidden_phrases": [
            "enamórate del problema",
            "el cliente siempre tiene la razón",
            "construye lo que tu usuario quiere",
            "fail fast",
            "data driven",
        ],
        "voice_examples": [
            "El roadmap se veía limpio. Las prioridades estaban claras. Nadie quería decir en voz alta que ese orden era el resultado de evitar conversaciones, no de elegirlas.",
            "Una métrica subió 12% el mes pasado. Lo celebramos en standup. Lo que no contamos: bajó la claridad del producto y aumentó el ruido en soporte.",
        ],
    },
    {
        "name": "salesSignals",
        "description": "Ventas B2B y GTM desde la perspectiva del comprador, con criterio comercial y sin postureo.",
        "trends_prompt": (
            "Prioriza cambios en comportamiento de compra, confianza, timing, evaluación de riesgo y colaboración GTM. "
            "Busca señales que expliquen cómo compran hoy los clientes, no solo cómo venden los equipos."
        ),
        "history_prompt": (
            "Evita frases de vendedor y consejos de cierre del historial. "
            "Prefiere observaciones concretas sobre objeciones, fricción de compra, calidad de pipeline y conversaciones que cambian una oportunidad."
        ),
        "content_prompt": (
            "Escribe con tono consultivo y criterio comercial. Nombra una señal del mercado, "
            "explica por qué importa para revenue y tradúcela en una acción práctica para ventas o GTM. "
            "Debe sentirse como observación de campo, no como manual de prospecting."
        ),
        "image_prompt": _with_image_guidance(
            "Escena editorial de proceso comercial B2B: evaluación, conversación, pipeline y confianza. "
            "Profesional, actual y sin estética de vendedor agresivo."
        ),
        "is_default": 0,
        "post_length": 170,
        "language": "es",
        "hashtag_count": 2,
        "use_emojis": False,
        "topic_keywords": ["ventas b2b", "gtm", "clientes", "pipeline", "revenue"],
        "negative_prompt": (
            "Evita urgencia falsa, copy agresivo, postureo de closers y clichés de prospecting. "
            "Nada de 'cierra como un pro', 'always be closing' ni simplificaciones del proceso de compra."
        ),
        "fallback_topics": [
            "Una objeción repetida suele revelar más del mercado que del pitch",
            "Pipeline lleno y pipeline sano no son la misma conversación",
            "Muchos deals se enfrían antes de la demo y el problema no es seguimiento",
            "El comprador informado no quiere más presión, quiere menos incertidumbre",
        ],
        "originality_level": 4,
        "evidence_mode": "balanced",
        "hook_style": "bold",
        "cta_style": "question",
        "audience_focus": "equipos comerciales, founders y GTM leaders",
        "preferred_formats": ["opinion", "insight", "case-study"],
        "preferred_visual_styles": ["editorial", "cinematic", "minimal"],
        "forbidden_phrases": [
            "cierra como un pro",
            "always be closing",
            "el arte de vender",
            "técnicas infalibles",
            "duplica tu pipeline",
        ],
        "voice_examples": [
            "Tres prospectos repitieron la misma objeción en una semana. No era una excusa: era la respuesta a una pregunta que el pitch nunca contestaba bien.",
            "El pipeline estaba lleno. Las reuniones se cerraban. Solo que ninguna avanzaba. Confundimos actividad con demanda durante dos trimestres.",
        ],
    },
    {
        "name": "opsPlaybook",
        "description": "Operaciones, procesos y escalamiento interno para equipos que quieren ejecutar mejor sin recetas mágicas.",
        "trends_prompt": (
            "Busca fricciones de operación que afecten velocidad, coordinación, claridad de ownership y calidad de ejecución. "
            "Prioriza problemas que se puedan explicar con causa raíz y mejora concreta."
        ),
        "history_prompt": (
            "Evita simplificar la operación en hacks del historial. "
            "Prefiere cuellos de botella, dependencias, ambigüedad y hábitos que drenan energía sin que el equipo lo note de inmediato."
        ),
        "content_prompt": (
            "Describe un problema operativo reconocible, explica por qué ocurre realmente "
            "y propone una mejora concreta que reduzca fricción. Debe sonar a experiencia de ejecución, no a teoría de productividad."
        ),
        "image_prompt": _with_image_guidance(
            "Visual editorial de operaciones internas, flujos, handoffs y coordinación entre equipos. "
            "Diagrama limpio con sensación de sistema vivo."
        ),
        "is_default": 0,
        "post_length": 175,
        "language": "es",
        "hashtag_count": 2,
        "use_emojis": False,
        "topic_keywords": ["operaciones", "procesos", "automatizacion", "eficiencia", "sops"],
        "negative_prompt": (
            "Evita productivity porn, hacks vacíos, soluciones mágicas y consejos que ignoren contexto, dependencia o adopción real. "
            "Nada de 'la herramienta que cambió mi vida' ni listas de productividad descontextualizadas."
        ),
        "fallback_topics": [
            "Hay procesos que fallan no por falta de documentación sino por falta de dueño claro",
            "Automatizar una fricción mal entendida solo la vuelve más rápida",
            "Cuando todo parece urgente normalmente el problema es de criterio compartido",
            "Mucho retrabajo nace en decisiones pequeñas que nadie cerró del todo",
        ],
        "originality_level": 4,
        "evidence_mode": "examples",
        "hook_style": "clarity",
        "cta_style": "action",
        "audience_focus": "operaciones, project leads y equipos de ejecución",
        "preferred_formats": ["tutorial", "insight", "case-study"],
        "preferred_visual_styles": ["diagram", "minimal", "editorial"],
        "forbidden_phrases": [
            "la herramienta que cambió",
            "10x tu productividad",
            "el truco que nadie",
            "rutina ganadora",
            "elimina las distracciones",
        ],
        "voice_examples": [
            "El proceso estaba documentado. Nadie lo seguía. No por rebeldía: porque ningún paso tenía dueño y todos asumían que el siguiente lo iba a verificar.",
            "Automatizamos una fricción y se volvió tres veces más rápida. También se volvió tres veces más ruidosa, porque el problema real estaba antes del paso que automatizamos.",
        ],
    },
    {
        "name": "securityDecoded",
        "description": "Ciberseguridad explicada para negocio y tecnología sin alarmismo, jerga ni espectáculo.",
        "trends_prompt": (
            "Prioriza incidentes, prácticas, cultura y decisiones de seguridad con impacto real para negocio y operación. "
            "Busca ángulos que ayuden a entender riesgo, responsabilidad y mitigación sin alarmismo."
        ),
        "history_prompt": (
            "No repitas headlines ni miedo del historial. "
            "Prefiere contexto, implicaciones concretas, errores comunes de interpretación y acciones que una empresa sí podría considerar."
        ),
        "content_prompt": (
            "Escribe con claridad ejecutiva y criterio técnico suficiente. Explica el riesgo, "
            "por qué importa en lenguaje comprensible y qué debería revisar una organización antes de reaccionar de forma superficial."
        ),
        "image_prompt": _with_image_guidance(
            "Visual sobrio de seguridad digital, identidad y defensa operativa. "
            "Más criterio y contexto que espectáculo; elegante, contemporáneo y sin estética hacker cliché."
        ),
        "is_default": 0,
        "post_length": 185,
        "language": "es",
        "hashtag_count": 3,
        "use_emojis": False,
        "topic_keywords": ["ciberseguridad", "riesgo", "identidad", "compliance", "seguridad"],
        "negative_prompt": (
            "Evita fearmongering, dramatización, tecnojerga innecesaria y consejos vagos. "
            "Nada de 'tu empresa es la siguiente', 'antes de que sea tarde' ni clichés de pantallas con código verde."
        ),
        "fallback_topics": [
            "Un incidente pequeño suele mostrar una debilidad cultural antes que una falla aislada",
            "Hablar de identidad ya no es hablar solo de credenciales",
            "La fatiga de alertas también es un problema de diseño operativo",
            "Seguridad madura no significa cero riesgo, significa mejores decisiones bajo presión",
        ],
        "originality_level": 5,
        "evidence_mode": "data",
        "hook_style": "question",
        "cta_style": "reflection",
        "audience_focus": "líderes de tecnología, seguridad y negocio",
        "preferred_formats": ["insight", "opinion", "case-study"],
        "preferred_visual_styles": ["cinematic", "diagram", "editorial"],
        "forbidden_phrases": [
            "tu empresa es la siguiente",
            "antes de que sea tarde",
            "no es si, es cuándo",
            "la nueva amenaza",
            "ataque sin precedentes",
        ],
        "voice_examples": [
            "El incidente parecía menor: una credencial filtrada en un repositorio. Lo que no era menor fue lo que reveló sobre cómo el equipo decidía qué considerar 'sensible'.",
            "La fatiga de alertas no se arregla con más dashboards. Se arregla decidiendo qué señales merecen interrumpir a una persona y cuáles deberían quedarse en background.",
        ],
    },
]


def _get_conn():
    conn = sqlite3.connect(_db_path(), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def _tx():
    """Run SELECT+UPDATE inside an atomic transaction (BEGIN IMMEDIATE)."""
    conn = _get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    if not _SAFE_IDENT.match(table_name):
        raise ValueError(f"invalid table name: {table_name!r}")
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(str(row["name"]) == column_name for row in rows)


def _add_column_if_missing(conn: sqlite3.Connection, table_name: str, column_name: str, column_def: str) -> None:
    if not _SAFE_IDENT.match(table_name):
        raise ValueError(f"invalid table name: {table_name!r}")
    if not _SAFE_IDENT.match(column_name):
        raise ValueError(f"invalid column name: {column_name!r}")
    if _column_exists(conn, table_name, column_name):
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")


def init_db():
    """Create tables if they don't exist."""
    with _get_conn() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
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
            ("linkedin_url", "TEXT NOT NULL DEFAULT ''"),
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
                category_name  TEXT    NOT NULL DEFAULT '',
                last_run_at    TEXT,
                next_run_at    TEXT
            )
            """
        )
        _add_column_if_missing(conn, "schedule_config", "days_of_week", "TEXT NOT NULL DEFAULT '[]'")
        _add_column_if_missing(conn, "schedule_config", "category_name", "TEXT NOT NULL DEFAULT ''")
        _add_column_if_missing(conn, "schedule_config", "rules", "TEXT NOT NULL DEFAULT '[]'")
        _add_column_if_missing(conn, "schedule_config", "next_run_category", "TEXT NOT NULL DEFAULT ''")
        _add_column_if_missing(conn, "schedule_config", "metrics_collection_enabled", "INTEGER NOT NULL DEFAULT 0")
        _add_column_if_missing(conn, "schedule_config", "metrics_collection_interval_hours", "REAL NOT NULL DEFAULT 6")
        _add_column_if_missing(conn, "schedule_config", "metrics_last_collected_at", "TEXT NOT NULL DEFAULT ''")
        conn.execute(
            """
            INSERT OR IGNORE INTO schedule_config
                (id, enabled, mode, interval_hours, times_of_day, category_name)
            VALUES (1, 0, 'interval', 24, '[]', '')
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
                preferred_visual_styles TEXT NOT NULL DEFAULT '[]',
                forbidden_phrases TEXT    NOT NULL DEFAULT '[]',
                voice_examples    TEXT    NOT NULL DEFAULT '[]'
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
            ("forbidden_phrases", "TEXT NOT NULL DEFAULT '[]'"),
            ("voice_examples", "TEXT NOT NULL DEFAULT '[]'"),
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
            """
            CREATE TABLE IF NOT EXISTS message_threads (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_key           TEXT NOT NULL UNIQUE,
                thread_url           TEXT NOT NULL DEFAULT '',
                contact_name         TEXT NOT NULL DEFAULT '',
                contact_profile_url  TEXT NOT NULL DEFAULT '',
                latest_snippet       TEXT NOT NULL DEFAULT '',
                last_message_at      TEXT NOT NULL DEFAULT '',
                last_inbound_at      TEXT NOT NULL DEFAULT '',
                last_outbound_at     TEXT NOT NULL DEFAULT '',
                unread_count         INTEGER NOT NULL DEFAULT 0,
                intent               TEXT NOT NULL DEFAULT 'unknown',
                state                TEXT NOT NULL DEFAULT 'new',
                paused               INTEGER NOT NULL DEFAULT 0,
                closed               INTEGER NOT NULL DEFAULT 0,
                assigned_review      INTEGER NOT NULL DEFAULT 0,
                last_inbound_hash    TEXT NOT NULL DEFAULT '',
                last_processed_hash  TEXT NOT NULL DEFAULT '',
                last_auto_reply_at   TEXT NOT NULL DEFAULT '',
                crm_summary          TEXT NOT NULL DEFAULT '',
                next_action          TEXT NOT NULL DEFAULT '',
                last_error           TEXT NOT NULL DEFAULT '',
                created_at           TEXT NOT NULL,
                updated_at           TEXT NOT NULL
            )
            """
        )
        for col_name, col_def in [
            ("contact_avatar_url", "TEXT NOT NULL DEFAULT ''"),
            ("last_synced_at", "TEXT NOT NULL DEFAULT ''"),
        ]:
            _add_column_if_missing(conn, "message_threads", col_name, col_def)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS message_events (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id     INTEGER NOT NULL,
                event_type    TEXT NOT NULL DEFAULT 'message',
                sender_role   TEXT NOT NULL DEFAULT '',
                text          TEXT NOT NULL DEFAULT '',
                message_hash  TEXT NOT NULL DEFAULT '',
                happened_at   TEXT NOT NULL,
                meta          TEXT NOT NULL DEFAULT '{}',
                created_at    TEXT NOT NULL,
                UNIQUE(thread_id, message_hash),
                FOREIGN KEY(thread_id) REFERENCES message_threads(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS contact_profiles (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id       INTEGER NOT NULL UNIQUE,
                contact_name    TEXT NOT NULL DEFAULT '',
                profile_url     TEXT NOT NULL DEFAULT '',
                intent          TEXT NOT NULL DEFAULT 'unknown',
                current_stage   TEXT NOT NULL DEFAULT 'new',
                summary         TEXT NOT NULL DEFAULT '',
                goals           TEXT NOT NULL DEFAULT '',
                next_action     TEXT NOT NULL DEFAULT '',
                last_memory_at  TEXT NOT NULL DEFAULT '',
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL,
                FOREIGN KEY(thread_id) REFERENCES message_threads(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS message_automation_config (
                id                    INTEGER PRIMARY KEY CHECK (id = 1),
                enabled               INTEGER NOT NULL DEFAULT 0,
                poll_interval_minutes INTEGER NOT NULL DEFAULT 5,
                auto_send_default     INTEGER NOT NULL DEFAULT 1,
                public_base_url       TEXT NOT NULL DEFAULT 'http://127.0.0.1:5000',
                booking_token         TEXT NOT NULL DEFAULT '',
                meeting_location      TEXT NOT NULL DEFAULT 'Enlace por confirmar',
                sync_limit            INTEGER NOT NULL DEFAULT 15,
                max_threads_per_cycle INTEGER NOT NULL DEFAULT 5,
                created_at            TEXT NOT NULL,
                updated_at            TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO message_automation_config
                (id, enabled, poll_interval_minutes, auto_send_default, public_base_url, booking_token, meeting_location, sync_limit, max_threads_per_cycle, created_at, updated_at)
            VALUES (1, 0, 5, 1, 'http://127.0.0.1:5000', ?, 'Enlace por confirmar', 15, 5, ?, ?)
            """,
            (str(uuid.uuid4()), _utc_now(), _utc_now()),
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS message_review_queue (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id        INTEGER NOT NULL,
                reason           TEXT NOT NULL DEFAULT '',
                suggested_reply  TEXT NOT NULL DEFAULT '',
                status           TEXT NOT NULL DEFAULT 'pending',
                created_at       TEXT NOT NULL,
                updated_at       TEXT NOT NULL,
                FOREIGN KEY(thread_id) REFERENCES message_threads(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS calendar_availability (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                weekday     INTEGER NOT NULL,
                start_time  TEXT NOT NULL,
                end_time    TEXT NOT NULL,
                timezone    TEXT NOT NULL DEFAULT 'UTC',
                is_active   INTEGER NOT NULL DEFAULT 1,
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS calendar_bookings (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                booking_public_id   TEXT NOT NULL UNIQUE,
                thread_id           INTEGER,
                contact_name        TEXT NOT NULL DEFAULT '',
                contact_profile_url TEXT NOT NULL DEFAULT '',
                contact_message     TEXT NOT NULL DEFAULT '',
                start_at            TEXT NOT NULL,
                end_at              TEXT NOT NULL,
                timezone            TEXT NOT NULL DEFAULT 'UTC',
                status              TEXT NOT NULL DEFAULT 'booked',
                created_at          TEXT NOT NULL,
                updated_at          TEXT NOT NULL,
                FOREIGN KEY(thread_id) REFERENCES message_threads(id) ON DELETE SET NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_jobs_status_expires ON jobs(status, expires_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pipeline_sessions_expires ON pipeline_sessions(expires_at)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_message_threads_updated_at ON message_threads(updated_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_message_events_thread_id ON message_events(thread_id, happened_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_message_review_queue_status ON message_review_queue(status, updated_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_calendar_bookings_start_at ON calendar_bookings(start_at)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts(created_at DESC)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS post_metrics (
                post_id          INTEGER PRIMARY KEY,
                impressions      INTEGER NOT NULL DEFAULT 0,
                reactions        INTEGER NOT NULL DEFAULT 0,
                comments         INTEGER NOT NULL DEFAULT 0,
                reposts          INTEGER NOT NULL DEFAULT 0,
                profile_visits   INTEGER NOT NULL DEFAULT 0,
                link_clicks      INTEGER NOT NULL DEFAULT 0,
                saves            INTEGER NOT NULL DEFAULT 0,
                engagement_rate  REAL,
                collected_at     TEXT NOT NULL,
                updated_at       TEXT NOT NULL,
                FOREIGN KEY(post_id) REFERENCES posts(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_post_metrics_updated_at ON post_metrics(updated_at DESC)"
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


def recover_stale_workers() -> dict:
    """Mark jobs/sessions left in running/queued/pending as 'error' after a restart."""
    now = _utc_now()
    with _tx() as conn:
        n_sessions = conn.execute(
            "UPDATE pipeline_sessions "
            "SET status='error', updated_at=?, "
            "payload=json_set(COALESCE(payload,'{}'), '$.recovery_reason', 'proceso reiniciado') "
            "WHERE status='running'",
            (now,),
        ).rowcount
        n_jobs = conn.execute(
            "UPDATE jobs SET status='error', updated_at=?, "
            "message='Interrumpido por reinicio del proceso' "
            "WHERE status IN ('queued', 'running', 'pending')",
            (now,),
        ).rowcount
    return {"sessions": int(n_sessions or 0), "jobs": int(n_jobs or 0)}


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
                 cta_style, audience_focus, preferred_formats, preferred_visual_styles,
                 forbidden_phrases, voice_examples)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                json.dumps(item.get("forbidden_phrases", [])),
                json.dumps(item.get("voice_examples", [])),
            ),
        )
        if item["is_default"] and not default_exists:
            default_exists = True


def _decode_category(row) -> dict:
    d = dict(row)
    for json_field in (
        "topic_keywords",
        "fallback_topics",
        "preferred_formats",
        "preferred_visual_styles",
        "forbidden_phrases",
        "voice_examples",
    ):
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


def find_pipeline_category(category_name: str | None) -> dict | None:
    clean_name = str(category_name or "").strip()
    if not clean_name:
        return None
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM pipeline_categories WHERE name = ?",
            (clean_name,),
        ).fetchone()
    return _decode_category(row) if row else None


def get_pipeline_category(category_name: str | None) -> dict | None:
    if not category_name:
        return get_default_pipeline_category()
    return find_pipeline_category(category_name) or get_default_pipeline_category()


def get_default_pipeline_category() -> dict | None:
    with _get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM pipeline_categories ORDER BY is_default DESC, id ASC LIMIT 1"
        ).fetchone()
    return _decode_category(row) if row else None


def refresh_seeded_pipeline_categories(*, preserve_default_assignment: bool = True) -> list[dict]:
    current_default = get_default_pipeline_category() if preserve_default_assignment else None
    refreshed: list[dict] = []
    for item in DEFAULT_PIPELINE_CATEGORIES:
        existing = find_pipeline_category(item["name"])
        refreshed.append(
            save_pipeline_category(
                category_id=existing["id"] if existing else None,
                name=item["name"],
                description=item["description"],
                trends_prompt=item["trends_prompt"],
                history_prompt=item["history_prompt"],
                content_prompt=item["content_prompt"],
                image_prompt=item["image_prompt"],
                is_default=(
                    item["name"] == current_default["name"]
                    if current_default
                    else bool(item.get("is_default"))
                ),
                post_length=int(item.get("post_length", 200) or 200),
                language=str(item.get("language", "auto") or "auto"),
                hashtag_count=int(item["hashtag_count"]) if item.get("hashtag_count") is not None else 4,
                use_emojis=bool(item.get("use_emojis", False)),
                topic_keywords=list(item.get("topic_keywords", []) or []),
                negative_prompt=str(item.get("negative_prompt", "") or ""),
                fallback_topics=list(item.get("fallback_topics", []) or []),
                originality_level=int(item.get("originality_level", 3) or 3),
                evidence_mode=str(item.get("evidence_mode", "balanced") or "balanced"),
                hook_style=str(item.get("hook_style", "auto") or "auto"),
                cta_style=str(item.get("cta_style", "auto") or "auto"),
                audience_focus=str(item.get("audience_focus", "") or ""),
                preferred_formats=list(item.get("preferred_formats", []) or []),
                preferred_visual_styles=list(item.get("preferred_visual_styles", []) or []),
                forbidden_phrases=list(item.get("forbidden_phrases", []) or []),
                voice_examples=list(item.get("voice_examples", []) or []),
            )
        )
    return refreshed


def resolve_pipeline_category_choice(category_name: str | None) -> tuple[dict | None, str]:
    requested = str(category_name or "").strip()
    if requested == RANDOM_CATEGORY_NAME:
        categories = get_pipeline_categories()
        if not categories:
            return None, requested
        return random.choice(categories), requested
    if not requested:
        return get_default_pipeline_category(), requested
    return find_pipeline_category(requested), requested


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
    forbidden_phrases: list | None = None,
    voice_examples: list | None = None,
    category_id: int | None = None,
) -> dict:
    topic_keywords_json = json.dumps(topic_keywords or [])
    fallback_topics_json = json.dumps(fallback_topics or [])
    preferred_formats_json = json.dumps(preferred_formats or [])
    preferred_visual_styles_json = json.dumps(preferred_visual_styles or [])
    forbidden_phrases_json = json.dumps(forbidden_phrases or [])
    voice_examples_json = json.dumps(voice_examples or [])
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
                    forbidden_phrases=?, voice_examples=?,
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
                    forbidden_phrases_json,
                    voice_examples_json,
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
                     forbidden_phrases, voice_examples,
                     created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    forbidden_phrases_json,
                    voice_examples_json,
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
    d["category_name"] = str(d.get("category_name") or "")
    try:
        d["rules"] = json.loads(d.get("rules") or "[]")
    except (TypeError, ValueError):
        d["rules"] = []
    if not isinstance(d["rules"], list):
        d["rules"] = []
    d["next_run_category"] = str(d.get("next_run_category") or "")
    d["metrics_collection_enabled"] = bool(int(d.get("metrics_collection_enabled") or 0))
    d["metrics_collection_interval_hours"] = float(d.get("metrics_collection_interval_hours") or 6)
    d["metrics_last_collected_at"] = str(d.get("metrics_last_collected_at") or "")
    return d


def save_metrics_collection_settings(*, enabled: bool, interval_hours: float) -> dict:
    """Update only the metrics-collection columns of schedule_config."""
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE schedule_config
            SET metrics_collection_enabled=?, metrics_collection_interval_hours=?
            WHERE id=1
            """,
            (1 if enabled else 0, max(0.5, float(interval_hours or 6))),
        )
        conn.commit()
    return get_schedule()


def update_metrics_collection_run(timestamp: str) -> None:
    with _get_conn() as conn:
        conn.execute(
            "UPDATE schedule_config SET metrics_last_collected_at=? WHERE id=1",
            (timestamp,),
        )
        conn.commit()


def save_schedule(
    enabled: bool,
    mode: str,
    interval_hours: float,
    times_of_day: list,
    next_run_at: str | None = None,
    days_of_week: list | None = None,
    category_name: str = "",
    rules: list | None = None,
    next_run_category: str = "",
):
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE schedule_config SET
                enabled=?, mode=?, interval_hours=?, times_of_day=?, days_of_week=?,
                category_name=?, rules=?, next_run_at=?, next_run_category=?
            WHERE id=1
            """,
            (
                1 if enabled else 0,
                mode,
                interval_hours,
                json.dumps(times_of_day),
                json.dumps(days_of_week or []),
                str(category_name or "").strip(),
                json.dumps(rules or []),
                next_run_at,
                str(next_run_category or "").strip(),
            ),
        )
        conn.commit()


def update_schedule_run_times(last_run_at: str, next_run_at: str | None, next_run_category: str = ""):
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE schedule_config SET last_run_at=?, next_run_at=?, next_run_category=? WHERE id=1
            """,
            (last_run_at, next_run_at, str(next_run_category or "").strip()),
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
    now = _utc_now()
    with _tx() as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if not row:
            return None
        current = dict(row)
        current["payload"] = json.loads(current.get("payload") or "{}")
        current["result"] = json.loads(current.get("result") or "{}")

        merged_payload = {**current["payload"], **payload} if payload else current["payload"]
        merged_result = {**current["result"], **result} if result else current["result"]
        new_status = status or current["status"]
        new_message = message if message is not None else current["message"]

        conn.execute(
            """
            UPDATE jobs
            SET status=?, message=?, payload=?, result=?, updated_at=?
            WHERE id=?
            """,
            (
                new_status,
                new_message,
                json.dumps(merged_payload),
                json.dumps(merged_result),
                now,
                job_id,
            ),
        )
        current.update(
            status=new_status,
            message=new_message,
            payload=merged_payload,
            result=merged_result,
            updated_at=now,
        )
        return current


def get_job(job_id: str) -> dict | None:
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
    now = _utc_now()
    expires_at = _future_iso(hours=int(get_setting("app", "session_ttl_hours", 24)))
    with _tx() as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM pipeline_sessions WHERE id = ?",
            (session_id,),
        ).fetchone()
        if not row:
            return None
        current = dict(row)
        current["payload"] = json.loads(current.get("payload") or "{}")

        merged_payload = {**current["payload"], **payload} if payload else current["payload"]
        new_category = category or current["category"]
        new_status = status or current["status"]

        conn.execute(
            """
            UPDATE pipeline_sessions
            SET category=?, status=?, payload=?, updated_at=?, expires_at=?
            WHERE id=?
            """,
            (
                new_category,
                new_status,
                json.dumps(merged_payload),
                now,
                expires_at,
                session_id,
            ),
        )
        current.update(
            category=new_category,
            status=new_status,
            payload=merged_payload,
            updated_at=now,
            expires_at=expires_at,
        )
        return current


def get_pipeline_session(session_id: str) -> dict | None:
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


def save_post_metrics(
    post_id: int,
    *,
    impressions: int = 0,
    reactions: int = 0,
    comments: int = 0,
    reposts: int = 0,
    profile_visits: int = 0,
    link_clicks: int = 0,
    saves: int = 0,
    collected_at: str | None = None,
) -> dict | None:
    now = _utc_now()
    collected = collected_at or now
    impressions = max(0, int(impressions or 0))
    reactions = max(0, int(reactions or 0))
    comments = max(0, int(comments or 0))
    reposts = max(0, int(reposts or 0))
    profile_visits = max(0, int(profile_visits or 0))
    link_clicks = max(0, int(link_clicks or 0))
    saves = max(0, int(saves or 0))
    engagement_total = reactions + comments + reposts + saves + link_clicks
    engagement_rate = round(engagement_total / impressions, 4) if impressions > 0 else None

    with _get_conn() as conn:
        conn.execute(
            """
            INSERT INTO post_metrics
                (post_id, impressions, reactions, comments, reposts, profile_visits,
                 link_clicks, saves, engagement_rate, collected_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(post_id) DO UPDATE SET
                impressions=excluded.impressions,
                reactions=excluded.reactions,
                comments=excluded.comments,
                reposts=excluded.reposts,
                profile_visits=excluded.profile_visits,
                link_clicks=excluded.link_clicks,
                saves=excluded.saves,
                engagement_rate=excluded.engagement_rate,
                collected_at=excluded.collected_at,
                updated_at=excluded.updated_at
            """,
            (
                post_id,
                impressions,
                reactions,
                comments,
                reposts,
                profile_visits,
                link_clicks,
                saves,
                engagement_rate,
                collected,
                now,
            ),
        )
        conn.commit()
    return get_post_metrics(post_id)


def get_post_metrics(post_id: int) -> dict | None:
    with _get_conn() as conn:
        row = conn.execute("SELECT * FROM post_metrics WHERE post_id = ?", (post_id,)).fetchone()
    return dict(row) if row else None


def _metric_defaults(row: dict) -> dict:
    return {
        **row,
        "impressions": int(row.get("impressions") or 0),
        "reactions": int(row.get("reactions") or 0),
        "comments": int(row.get("comments") or 0),
        "reposts": int(row.get("reposts") or 0),
        "profile_visits": int(row.get("profile_visits") or 0),
        "link_clicks": int(row.get("link_clicks") or 0),
        "saves": int(row.get("saves") or 0),
        "engagement_rate": float(row.get("engagement_rate") or 0),
    }


def get_posts(limit: int = 50, published_only: bool = False, offset: int = 0, search: str = "") -> list[dict]:
    query = """
        SELECT
            posts.*,
            pm.impressions,
            pm.reactions,
            pm.comments,
            pm.reposts,
            pm.profile_visits,
            pm.link_clicks,
            pm.saves,
            pm.engagement_rate,
            pm.collected_at AS metrics_collected_at,
            pm.updated_at AS metrics_updated_at
        FROM posts
        LEFT JOIN post_metrics pm ON pm.post_id = posts.id
    """
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
    return [_metric_defaults(dict(r)) for r in rows]


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
        row = conn.execute(
            """
            SELECT
                posts.*,
                pm.impressions,
                pm.reactions,
                pm.comments,
                pm.reposts,
                pm.profile_visits,
                pm.link_clicks,
                pm.saves,
                pm.engagement_rate,
                pm.collected_at AS metrics_collected_at,
                pm.updated_at AS metrics_updated_at
            FROM posts
            LEFT JOIN post_metrics pm ON pm.post_id = posts.id
            WHERE posts.id = ?
            """,
            (post_id,),
        ).fetchone()
    return _metric_defaults(dict(row)) if row else None


def update_post_linkedin_url(post_id: int, url: str) -> None:
    with _get_conn() as conn:
        conn.execute("UPDATE posts SET linkedin_url = ? WHERE id = ?", (url, post_id))


def get_posts_pending_metrics(
    *,
    stale_after_hours: int = 24,
    max_posts: int = 10,
    max_age_days: int = 30,
) -> list[dict]:
    """Return published posts that have a linkedin_url and need (re)scraping.

    A post is considered "pending" when:
      - It has been published in the last `max_age_days` days
      - It has a linkedin_url
      - Either no metrics have been collected yet OR
        post_metrics.updated_at is older than `stale_after_hours` hours.
    """
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                posts.*,
                pm.impressions,
                pm.reactions,
                pm.comments,
                pm.reposts,
                pm.profile_visits,
                pm.link_clicks,
                pm.saves,
                pm.engagement_rate,
                pm.collected_at AS metrics_collected_at,
                pm.updated_at AS metrics_updated_at
            FROM posts
            LEFT JOIN post_metrics pm ON pm.post_id = posts.id
            WHERE posts.published = 1
              AND posts.linkedin_url IS NOT NULL
              AND posts.linkedin_url != ''
              AND posts.created_at >= datetime('now', ?)
              AND (
                pm.updated_at IS NULL
                OR pm.updated_at <= datetime('now', ?)
              )
            ORDER BY
              CASE WHEN pm.updated_at IS NULL THEN 0 ELSE 1 END,
              pm.updated_at ASC,
              posts.id DESC
            LIMIT ?
            """,
            (f"-{int(max_age_days)} days", f"-{int(stale_after_hours)} hours", int(max_posts)),
        ).fetchall()
    return [_metric_defaults(dict(r)) for r in rows]


def get_posts_with_metrics(*, minimum_impressions: int = 1, limit: int = 200, days: int | None = None) -> list[dict]:
    date_clause = "AND posts.created_at >= datetime('now', ?)" if days is not None else ""
    params: tuple = (minimum_impressions, f"-{days} days", limit) if days is not None else (minimum_impressions, limit)
    with _get_conn() as conn:
        rows = conn.execute(
            f"""
            SELECT
                posts.*,
                pm.impressions,
                pm.reactions,
                pm.comments,
                pm.reposts,
                pm.profile_visits,
                pm.link_clicks,
                pm.saves,
                pm.engagement_rate,
                pm.collected_at AS metrics_collected_at,
                pm.updated_at AS metrics_updated_at
            FROM posts
            INNER JOIN post_metrics pm ON pm.post_id = posts.id
            WHERE posts.published = 1 AND pm.impressions >= ?
            {date_clause}
            ORDER BY posts.id DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    return [_metric_defaults(dict(r)) for r in rows]


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


def get_message_automation_config() -> dict:
    with _get_conn() as conn:
        row = conn.execute("SELECT * FROM message_automation_config WHERE id = 1").fetchone()
    return dict(row) if row else {}


def save_message_automation_config(
    *,
    enabled: bool,
    poll_interval_minutes: int,
    auto_send_default: bool,
    public_base_url: str,
    meeting_location: str,
    sync_limit: int,
    max_threads_per_cycle: int,
) -> dict:
    current = get_message_automation_config()
    booking_token = str(current.get("booking_token") or uuid.uuid4())
    now = _utc_now()
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE message_automation_config
            SET enabled=?, poll_interval_minutes=?, auto_send_default=?, public_base_url=?,
                booking_token=?, meeting_location=?, sync_limit=?, max_threads_per_cycle=?, updated_at=?
            WHERE id=1
            """,
            (
                1 if enabled else 0,
                int(poll_interval_minutes),
                1 if auto_send_default else 0,
                str(public_base_url or "").strip(),
                booking_token,
                str(meeting_location or "").strip(),
                int(sync_limit),
                int(max_threads_per_cycle),
                now,
            ),
        )
        conn.commit()
    return get_message_automation_config()


def regenerate_booking_token() -> dict:
    token = str(uuid.uuid4())
    with _get_conn() as conn:
        conn.execute(
            "UPDATE message_automation_config SET booking_token=?, updated_at=? WHERE id=1",
            (token, _utc_now()),
        )
        conn.commit()
    return get_message_automation_config()


def list_message_threads(*, limit: int = 50, query: str = "", state: str = "", include_closed: bool = False) -> list[dict]:
    sql = "SELECT * FROM message_threads"
    params: list = []
    clauses: list[str] = []
    if not include_closed:
        clauses.append("closed = 0")
    if query:
        clauses.append("(contact_name LIKE ? OR latest_snippet LIKE ? OR crm_summary LIKE ?)")
        search_like = f"%{query}%"
        params.extend([search_like, search_like, search_like])
    if state:
        clauses.append("state = ?")
        params.append(state)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY updated_at DESC LIMIT ?"
    params.append(limit)
    with _get_conn() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [dict(row) for row in rows]


def get_message_thread(thread_id: int) -> dict | None:
    with _get_conn() as conn:
        row = conn.execute("SELECT * FROM message_threads WHERE id = ?", (thread_id,)).fetchone()
    return dict(row) if row else None


def get_message_thread_by_key(thread_key: str) -> dict | None:
    with _get_conn() as conn:
        row = conn.execute("SELECT * FROM message_threads WHERE thread_key = ?", (str(thread_key or "").strip(),)).fetchone()
    return dict(row) if row else None


def upsert_message_thread(
    *,
    thread_key: str,
    thread_url: str = "",
    contact_name: str = "",
    contact_profile_url: str = "",
    contact_avatar_url: str = "",
    latest_snippet: str = "",
    last_message_at: str = "",
    unread_count: int = 0,
) -> dict:
    now = _utc_now()
    existing = get_message_thread_by_key(thread_key)
    with _get_conn() as conn:
        if existing:
            conn.execute(
                """
                UPDATE message_threads
                SET thread_url=?, contact_name=?, contact_profile_url=?, contact_avatar_url=?,
                    latest_snippet=?, last_message_at=?, unread_count=?, updated_at=?
                WHERE thread_key=?
                """,
                (
                    thread_url or existing["thread_url"],
                    contact_name or existing["contact_name"],
                    contact_profile_url or existing["contact_profile_url"],
                    contact_avatar_url or existing.get("contact_avatar_url", ""),
                    latest_snippet or existing["latest_snippet"],
                    last_message_at or existing["last_message_at"],
                    int(unread_count or 0),
                    now,
                    thread_key,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO message_threads
                    (thread_key, thread_url, contact_name, contact_profile_url, contact_avatar_url,
                     latest_snippet, last_message_at, unread_count, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    thread_key,
                    thread_url,
                    contact_name,
                    contact_profile_url,
                    contact_avatar_url,
                    latest_snippet,
                    last_message_at,
                    int(unread_count or 0),
                    now,
                    now,
                ),
            )
        conn.commit()
    return get_message_thread_by_key(thread_key)


def mark_message_thread_synced(thread_key: str) -> None:
    now = _utc_now()
    with _get_conn() as conn:
        conn.execute(
            "UPDATE message_threads SET last_synced_at=?, updated_at=? WHERE thread_key=?",
            (now, now, thread_key),
        )
        conn.commit()


def update_message_thread_state(
    thread_id: int,
    *,
    intent: str | None = None,
    state: str | None = None,
    paused: bool | None = None,
    closed: bool | None = None,
    assigned_review: bool | None = None,
    last_inbound_hash: str | None = None,
    last_processed_hash: str | None = None,
    crm_summary: str | None = None,
    next_action: str | None = None,
    last_auto_reply_at: str | None = None,
    last_error: str | None = None,
) -> dict | None:
    current = get_message_thread(thread_id)
    if not current:
        return None
    with _get_conn() as conn:
        conn.execute(
            """
            UPDATE message_threads
            SET intent=?, state=?, paused=?, closed=?, assigned_review=?, last_inbound_hash=?,
                last_processed_hash=?, crm_summary=?, next_action=?, last_auto_reply_at=?,
                last_error=?, updated_at=?
            WHERE id=?
            """,
            (
                intent if intent is not None else current["intent"],
                state if state is not None else current["state"],
                int(paused) if paused is not None else current["paused"],
                int(closed) if closed is not None else current["closed"],
                int(assigned_review) if assigned_review is not None else current["assigned_review"],
                last_inbound_hash if last_inbound_hash is not None else current["last_inbound_hash"],
                last_processed_hash if last_processed_hash is not None else current["last_processed_hash"],
                crm_summary if crm_summary is not None else current["crm_summary"],
                next_action if next_action is not None else current["next_action"],
                last_auto_reply_at if last_auto_reply_at is not None else current["last_auto_reply_at"],
                last_error if last_error is not None else current["last_error"],
                _utc_now(),
                thread_id,
            ),
        )
        conn.commit()
    return get_message_thread(thread_id)


def list_message_events(thread_id: int, *, limit: int = 100) -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM message_events WHERE thread_id = ? ORDER BY happened_at ASC, id ASC LIMIT ?",
            (thread_id, limit),
        ).fetchall()
    return [{**dict(row), "meta": json.loads(row["meta"] or "{}")} for row in rows]


def save_message_event(
    thread_id: int,
    *,
    event_type: str,
    sender_role: str,
    text: str,
    message_hash: str,
    happened_at: str,
    meta: dict | None = None,
) -> dict | None:
    now = _utc_now()
    with _get_conn() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO message_events
                (thread_id, event_type, sender_role, text, message_hash, happened_at, meta, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                thread_id,
                event_type,
                sender_role,
                text,
                message_hash,
                happened_at,
                json.dumps(meta or {}),
                now,
            ),
        )
        if sender_role == "contact":
            conn.execute(
                "UPDATE message_threads SET last_inbound_at=?, updated_at=? WHERE id=?",
                (happened_at, now, thread_id),
            )
        if sender_role == "self":
            conn.execute(
                "UPDATE message_threads SET last_outbound_at=?, updated_at=? WHERE id=?",
                (happened_at, now, thread_id),
            )
        conn.commit()
    rows = list_message_events(thread_id, limit=200)
    return next((row for row in rows if row["message_hash"] == message_hash), None)


def get_contact_profile(thread_id: int) -> dict | None:
    with _get_conn() as conn:
        row = conn.execute("SELECT * FROM contact_profiles WHERE thread_id = ?", (thread_id,)).fetchone()
    return dict(row) if row else None


def upsert_contact_profile(
    thread_id: int,
    *,
    contact_name: str = "",
    profile_url: str = "",
    intent: str = "unknown",
    current_stage: str = "new",
    summary: str = "",
    goals: str = "",
    next_action: str = "",
) -> dict:
    now = _utc_now()
    existing = get_contact_profile(thread_id)
    with _get_conn() as conn:
        if existing:
            conn.execute(
                """
                UPDATE contact_profiles
                SET contact_name=?, profile_url=?, intent=?, current_stage=?, summary=?, goals=?, next_action=?, last_memory_at=?, updated_at=?
                WHERE thread_id=?
                """,
                (
                    contact_name or existing["contact_name"],
                    profile_url or existing["profile_url"],
                    intent or existing["intent"],
                    current_stage or existing["current_stage"],
                    summary or existing["summary"],
                    goals or existing["goals"],
                    next_action or existing["next_action"],
                    now,
                    now,
                    thread_id,
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO contact_profiles
                    (thread_id, contact_name, profile_url, intent, current_stage, summary, goals, next_action, last_memory_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (thread_id, contact_name, profile_url, intent, current_stage, summary, goals, next_action, now, now, now),
            )
        conn.commit()
    return get_contact_profile(thread_id) or {}


def create_message_review_item(thread_id: int, reason: str, *, suggested_reply: str = "") -> int:
    now = _utc_now()
    with _get_conn() as conn:
        existing = conn.execute(
            """
            SELECT id
            FROM message_review_queue
            WHERE thread_id = ? AND status = 'pending' AND reason = ? AND suggested_reply = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (thread_id, reason, suggested_reply),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE message_review_queue SET updated_at = ? WHERE id = ?",
                (now, existing["id"]),
            )
            conn.commit()
            return int(existing["id"])
        cur = conn.execute(
            """
            INSERT INTO message_review_queue (thread_id, reason, suggested_reply, status, created_at, updated_at)
            VALUES (?, ?, ?, 'pending', ?, ?)
            """,
            (thread_id, reason, suggested_reply, now, now),
        )
        conn.commit()
        return cur.lastrowid


def list_message_review_items(*, status: str = "pending") -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute(
            """
            SELECT q.*, t.contact_name, t.thread_url, t.intent, t.state
            FROM message_review_queue q
            LEFT JOIN message_threads t ON t.id = q.thread_id
            WHERE q.status = ?
            ORDER BY q.updated_at DESC, q.id DESC
            """,
            (status,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_message_review_item(review_id: int) -> dict | None:
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT q.*, t.contact_name, t.thread_url, t.intent, t.state
            FROM message_review_queue q
            LEFT JOIN message_threads t ON t.id = q.thread_id
            WHERE q.id = ?
            """,
            (review_id,),
        ).fetchone()
    return dict(row) if row else None


def update_message_review_item(review_id: int, *, status: str) -> None:
    with _get_conn() as conn:
        conn.execute(
            "UPDATE message_review_queue SET status=?, updated_at=? WHERE id=?",
            (status, _utc_now(), review_id),
        )
        conn.commit()


def update_message_reviews_for_thread(thread_id: int, *, status: str) -> None:
    with _get_conn() as conn:
        conn.execute(
            "UPDATE message_review_queue SET status=?, updated_at=? WHERE thread_id=? AND status='pending'",
            (status, _utc_now(), thread_id),
        )
        conn.commit()


def get_calendar_availability() -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM calendar_availability WHERE is_active = 1 ORDER BY weekday ASC, start_time ASC"
        ).fetchall()
    return [dict(row) for row in rows]


def replace_calendar_availability(blocks: list[dict]) -> list[dict]:
    now = _utc_now()
    with _get_conn() as conn:
        conn.execute("DELETE FROM calendar_availability")
        for block in blocks:
            conn.execute(
                """
                INSERT INTO calendar_availability
                    (weekday, start_time, end_time, timezone, is_active, created_at, updated_at)
                VALUES (?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    int(block.get("weekday", 0)),
                    str(block.get("start_time", "09:00")),
                    str(block.get("end_time", "17:00")),
                    str(block.get("timezone", "UTC")),
                    now,
                    now,
                ),
            )
        conn.commit()
    return get_calendar_availability()


def list_calendar_bookings(*, limit: int = 100) -> list[dict]:
    with _get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM calendar_bookings ORDER BY start_at ASC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def has_calendar_conflict(start_at: str, end_at: str) -> bool:
    with _get_conn() as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM calendar_bookings
            WHERE status = 'booked'
              AND NOT (end_at <= ? OR start_at >= ?)
            LIMIT 1
            """,
            (start_at, end_at),
        ).fetchone()
    return bool(row)


def create_calendar_booking(
    *,
    thread_id: int | None,
    contact_name: str,
    contact_profile_url: str,
    contact_message: str,
    start_at: str,
    end_at: str,
    timezone: str,
) -> dict:
    if has_calendar_conflict(start_at, end_at):
        raise ValueError("Ese horario ya no está disponible.")
    now = _utc_now()
    public_id = str(uuid.uuid4())
    with _get_conn() as conn:
        conn.execute(
            """
            INSERT INTO calendar_bookings
                (booking_public_id, thread_id, contact_name, contact_profile_url, contact_message, start_at, end_at, timezone, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'booked', ?, ?)
            """,
            (public_id, thread_id, contact_name, contact_profile_url, contact_message, start_at, end_at, timezone, now, now),
        )
        conn.commit()
    return get_calendar_booking_by_public_id(public_id) or {}


def get_calendar_booking_by_public_id(public_id: str) -> dict | None:
    with _get_conn() as conn:
        row = conn.execute("SELECT * FROM calendar_bookings WHERE booking_public_id = ?", (public_id,)).fetchone()
    return dict(row) if row else None
