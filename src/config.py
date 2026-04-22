"""
Centralized application settings with precedence:
environment > local .env > config.yaml > defaults.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import yaml

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = Path(os.environ.get("APP_CONFIG_PATH", BASE_DIR / "config.yaml")).expanduser()
ENV_PATH = Path(os.environ.get("APP_ENV_PATH", BASE_DIR / ".env")).expanduser()

DEFAULTS = {
    "linkedin": {
        "email": "",
        "password": "",
    },
    "grok": {
        "api_key": "",
        "model": "grok-3",
        "image_model": "grok-imagine-image",
    },
    "app": {
        "port": 5000,
        "debug": False,
        "secret_key": "dev-secret",
        "headless": True,
        "timezone": "",
        "session_ttl_hours": 24,
        "job_ttl_hours": 24,
        "log_level": "INFO",
    },
    "security": {
        "admin_username": "admin",
        "admin_password": "",
        "admin_password_hash": "",
        "session_timeout_minutes": 43200,
        "max_login_attempts": 5,
        "login_window_minutes": 15,
        "lockout_minutes": 15,
        "require_https_cookies": False,
        "max_content_length_mb": 2,
    },
    "storage": {
        "db_path": str(BASE_DIR / "posts.db"),
        "linkedin_session_dir": str(BASE_DIR / "linkedin_session"),
        "linkedin_history_file": str(BASE_DIR / "post_history.json"),
    },
    "linkedin_browser": {
        "locale": "es-ES",
        "timezone_id": "America/Mexico_City",
        "feed_timeout_ms": 30000,
    },
}

ENV_MAP = {
    "LINKEDIN_EMAIL": ("linkedin", "email"),
    "LINKEDIN_PASSWORD": ("linkedin", "password"),
    "XAI_API_KEY": ("grok", "api_key"),
    "GROK_API_KEY": ("grok", "api_key"),
    "GROK_MODEL": ("grok", "model"),
    "GROK_IMAGE_MODEL": ("grok", "image_model"),
    "APP_PORT": ("app", "port"),
    "APP_DEBUG": ("app", "debug"),
    "APP_SECRET_KEY": ("app", "secret_key"),
    "APP_HEADLESS": ("app", "headless"),
    "APP_TIMEZONE": ("app", "timezone"),
    "APP_SESSION_TTL_HOURS": ("app", "session_ttl_hours"),
    "APP_JOB_TTL_HOURS": ("app", "job_ttl_hours"),
    "APP_LOG_LEVEL": ("app", "log_level"),
    "ADMIN_USERNAME": ("security", "admin_username"),
    "ADMIN_PASSWORD": ("security", "admin_password"),
    "ADMIN_PASSWORD_HASH": ("security", "admin_password_hash"),
    "SECURITY_SESSION_TIMEOUT_MINUTES": ("security", "session_timeout_minutes"),
    "SECURITY_MAX_LOGIN_ATTEMPTS": ("security", "max_login_attempts"),
    "SECURITY_LOGIN_WINDOW_MINUTES": ("security", "login_window_minutes"),
    "SECURITY_LOCKOUT_MINUTES": ("security", "lockout_minutes"),
    "SECURITY_REQUIRE_HTTPS_COOKIES": ("security", "require_https_cookies"),
    "SECURITY_MAX_CONTENT_LENGTH_MB": ("security", "max_content_length_mb"),
    "DB_PATH": ("storage", "db_path"),
    "LINKEDIN_SESSION_DIR": ("storage", "linkedin_session_dir"),
    "LINKEDIN_HISTORY_FILE": ("storage", "linkedin_history_file"),
    "LINKEDIN_LOCALE": ("linkedin_browser", "locale"),
    "LINKEDIN_TIMEZONE": ("linkedin_browser", "timezone_id"),
    "LINKEDIN_FEED_TIMEOUT_MS": ("linkedin_browser", "feed_timeout_ms"),
}


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _coerce(value: str, default):
    if isinstance(default, bool):
        return _parse_bool(value)
    if isinstance(default, int):
        return int(value)
    if isinstance(default, float):
        return float(value)
    return value


def _deep_merge(base: dict, incoming: dict) -> dict:
    merged = dict(base)
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _load_dotenv() -> dict[str, str]:
    if not ENV_PATH.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'").strip('"')
    return values


@lru_cache(maxsize=1)
def get_settings() -> dict:
    cfg = DEFAULTS
    if CONFIG_PATH.exists():
        loaded = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
        cfg = _deep_merge(cfg, loaded)

    env_values = _load_dotenv()
    env_values.update({k: v for k, v in os.environ.items() if v is not None})

    cfg = _deep_merge({}, cfg)
    for env_name, path in ENV_MAP.items():
        if env_name not in env_values:
            continue
        section, key = path
        default = DEFAULTS[section][key]
        cfg.setdefault(section, {})
        cfg[section][key] = _coerce(env_values[env_name], default)

    return cfg


def reload_settings() -> dict:
    get_settings.cache_clear()
    return get_settings()


def get_setting(section: str, key: str, default=None):
    return get_settings().get(section, {}).get(key, default)


def update_yaml_setting(section: str, key: str, value) -> dict:
    current = {}
    if CONFIG_PATH.exists():
        current = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    current.setdefault(section, {})
    current[section][key] = value
    CONFIG_PATH.write_text(
        yaml.safe_dump(current, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return reload_settings()


def ensure_local_config() -> None:
    example_path = BASE_DIR / "config.yaml.example"
    if not CONFIG_PATH.exists() and example_path.exists():
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_PATH.write_text(example_path.read_text(encoding="utf-8"), encoding="utf-8")
