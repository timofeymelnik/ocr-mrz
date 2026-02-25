from __future__ import annotations

import re
from re import Pattern


def canonical_from_placeholder(
    value: str,
    *,
    placeholder_re: Pattern[str],
    canonical_field_keys: set[str],
) -> str:
    """Resolve a single placeholder token to a canonical key."""
    match = placeholder_re.fullmatch((value or "").strip())
    if not match:
        return ""
    key = match.group(1).strip().lower()
    return key if key in canonical_field_keys else ""


def canonical_keys_from_placeholder_tokens(
    value: str,
    *,
    placeholder_token_re: Pattern[str],
    canonical_field_keys: set[str],
) -> tuple[list[str], list[str]]:
    """Extract unique known/unknown placeholder keys from a template string."""
    found = [
        match.group(1).strip().lower()
        for match in placeholder_token_re.finditer(value or "")
    ]
    if not found:
        return [], []
    known: list[str] = []
    unknown: list[str] = []
    seen_known: set[str] = set()
    seen_unknown: set[str] = set()
    for key in found:
        if key in canonical_field_keys:
            if key not in seen_known:
                known.append(key)
                seen_known.add(key)
            continue
        if key not in seen_unknown:
            unknown.append(key)
            seen_unknown.add(key)
    return known, unknown


def select_canonical_for_composite_placeholder(keys: list[str]) -> str:
    """Choose the best canonical key for a composite placeholder expression."""
    if not keys:
        return ""
    key_set = set(keys)
    if "domicilio_en_espana" in key_set or (
        "tipo_via" in key_set and "nombre_via" in key_set
    ):
        return "domicilio_en_espana"
    if "nombre_apellidos" in key_set or {"nombre", "primer_apellido"}.issubset(key_set):
        return "nombre_apellidos"
    return keys[0]


def rule_context(values: dict[str, str]) -> dict[str, str]:
    """Normalize runtime values into a stable context for mapping rules."""
    return {key: str(value or "").strip() for key, value in values.items()}


def eval_checked_when(rule: str, context: dict[str, str]) -> bool | None:
    """Evaluate a simple equality expression used by checkbox mapping rules."""
    expr = (rule or "").strip()
    if not expr:
        return None
    match = re.fullmatch(r"([a-z_]+)\s*==\s*['\"]([^'\"]+)['\"]", expr, re.I)
    if not match:
        return None
    key = match.group(1).strip().lower()
    expected = match.group(2).strip()
    return context.get(key, "") == expected
