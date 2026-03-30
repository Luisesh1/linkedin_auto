"""
Small request validation helpers for Flask endpoints.
"""

from __future__ import annotations


class ValidationError(ValueError):
    pass


def ensure_dict(value, *, label: str = "payload") -> dict:
    if not isinstance(value, dict):
        raise ValidationError(f"El {label} debe ser un objeto JSON.")
    return value


def parse_bool(value, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "on"}:
            return True
        if lowered in {"false", "0", "no", "off"}:
            return False
    raise ValidationError("Valor booleano inválido.")


def parse_int(value, *, label: str, minimum: int | None = None, maximum: int | None = None, default=None) -> int:
    if value is None:
        if default is None:
            raise ValidationError(f"{label} es obligatorio.")
        value = default
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{label} debe ser un entero.") from exc
    if minimum is not None and parsed < minimum:
        raise ValidationError(f"{label} debe ser >= {minimum}.")
    if maximum is not None and parsed > maximum:
        raise ValidationError(f"{label} debe ser <= {maximum}.")
    return parsed


def parse_float(value, *, label: str, minimum: float | None = None, maximum: float | None = None, default=None) -> float:
    if value is None:
        if default is None:
            raise ValidationError(f"{label} es obligatorio.")
        value = default
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValidationError(f"{label} debe ser numérico.") from exc
    if minimum is not None and parsed < minimum:
        raise ValidationError(f"{label} debe ser >= {minimum}.")
    if maximum is not None and parsed > maximum:
        raise ValidationError(f"{label} debe ser <= {maximum}.")
    return parsed


def parse_string(
    value,
    *,
    label: str,
    required: bool = False,
    max_length: int | None = None,
    allowed: set[str] | None = None,
    default: str = "",
) -> str:
    if value is None:
        value = default
    if not isinstance(value, str):
        raise ValidationError(f"{label} debe ser texto.")
    parsed = value.strip()
    if required and not parsed:
        raise ValidationError(f"{label} es obligatorio.")
    if max_length is not None and len(parsed) > max_length:
        raise ValidationError(f"{label} excede el máximo de {max_length} caracteres.")
    if allowed is not None and parsed not in allowed:
        raise ValidationError(f"{label} contiene un valor inválido.")
    return parsed


def parse_string_list(
    value,
    *,
    label: str,
    max_items: int,
    max_length: int,
    dedupe: bool = True,
) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValidationError(f"{label} debe ser una lista.")
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = parse_string(item, label=label, required=True, max_length=max_length)
        key = text.lower()
        if dedupe and key in seen:
            continue
        seen.add(key)
        out.append(text)
    if len(out) > max_items:
        raise ValidationError(f"{label} permite máximo {max_items} elementos.")
    return out


def parse_weekdays(value) -> list[int]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValidationError("days_of_week debe ser una lista.")
    out: list[int] = []
    for item in value:
        day = parse_int(item, label="days_of_week", minimum=0, maximum=6)
        if day not in out:
            out.append(day)
    return sorted(out)


def parse_times_of_day(value) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValidationError("times_of_day debe ser una lista.")
    out: list[str] = []
    for item in value:
        text = parse_string(item, label="times_of_day", required=True, max_length=5)
        parts = text.split(":")
        if len(parts) != 2:
            raise ValidationError("Las horas deben tener formato HH:MM.")
        hour = parse_int(parts[0], label="hora", minimum=0, maximum=23)
        minute = parse_int(parts[1], label="minuto", minimum=0, maximum=59)
        normalized = f"{hour:02d}:{minute:02d}"
        if normalized not in out:
            out.append(normalized)
    return sorted(out)
