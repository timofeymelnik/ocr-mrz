"""Parsers for Spanish EX-* immigration forms extracted from OCR text."""

from __future__ import annotations

import re
from typing import Any

from app.data_builder.normalizers import (
    clean_spaces as _clean_spaces,
    normalize_email as _normalize_email,
    normalize_sex_code as _normalize_sex_code,
    to_spanish_date as _to_spanish_date,
    upper_compact as _upper_compact,
)

_EX_FORM_RE = re.compile(r"\bEX\s*[-–]?\s*(\d{1,2})\b", re.I)


def _safe(value: Any) -> str:
    """Return trimmed string for nullable values."""
    if value is None:
        return ""
    return str(value).strip()


def normalize_ex_form_code(raw: str) -> str:
    """Normalize user or OCR EX form code to canonical `ex_NN`."""
    value = _safe(raw).lower()
    if not value:
        return ""
    match = re.search(r"\bex[_\-\s]?(\d{1,2})\b", value, re.I)
    if not match:
        return ""
    return f"ex_{int(match.group(1)):02d}"


def detect_ex_form_code(*, merged_text: str, tasa_code: str) -> str:
    """Detect EX form code using explicit tasa_code first, then OCR text markers."""
    by_code = normalize_ex_form_code(tasa_code)
    if by_code:
        return by_code
    match = _EX_FORM_RE.search(merged_text or "")
    if not match:
        return ""
    return f"ex_{int(match.group(1)):02d}"


def _extract_tail_values_for_ex17(merged_text: str) -> list[str]:
    """Extract compact tail values often present in PDF text-layer for EX-17."""
    lines = [_clean_spaces(line) for line in (merged_text or "").splitlines()]
    lines = [line for line in lines if line]
    start_idx = -1
    for idx, line in enumerate(lines):
        up = line.upper()
        if "ESPACIOS PARA SELLOS" in up or up == "DE REGISTRO":
            start_idx = idx
    if start_idx < 0 or start_idx + 1 >= len(lines):
        return []
    return [line for line in lines[start_idx + 1 :] if line]


def _parse_ex17_structured_tail(merged_text: str) -> dict[str, str]:
    """Parse EX-17 values from the known tail ordering used by editable PDFs."""
    values = _extract_tail_values_for_ex17(merged_text)
    if len(values) < 22:
        return {}
    passport = _upper_compact(values[0])
    nie = _upper_compact("".join(values[1:4]))
    day = re.sub(r"\D", "", values[6]).zfill(2)[:2]
    month = re.sub(r"\D", "", values[7]).zfill(2)[:2]
    year = re.sub(r"\D", "", values[8])[:4]
    birth_date = _to_spanish_date(f"{day}/{month}/{year}")
    return {
        "pasaporte": passport,
        "nif_nie": nie,
        "apellidos": values[4],
        "nombre": values[5],
        "fecha_nacimiento": birth_date,
        "lugar_nacimiento": values[9],
        "pais_nacimiento": values[10],
        "nacionalidad": values[11],
        "nombre_padre": values[12],
        "nombre_madre": values[13],
        "nombre_via_publica": values[14],
        "numero": values[15],
        "piso": values[16],
        "municipio": values[17],
        "codigo_postal": values[18],
        "provincia": values[19],
        "telefono": values[20],
        "email": _normalize_email(values[21]),
    }


def _tipo_via_from_street(name: str) -> tuple[str, str]:
    """Split street line into `tipo_via` and `nombre_via_publica` when possible."""
    value = _clean_spaces(name)
    if not value:
        return "", ""
    match = re.match(r"^([A-ZÁÉÍÓÚÑÜ][\wÁÉÍÓÚÑÜ.]*)\s+(.*)$", value, re.I)
    if not match:
        return "", value
    raw_type = _clean_spaces(match.group(1)).strip(".")
    street = _clean_spaces(match.group(2))
    mapping = {
        "C": "Calle",
        "CL": "Calle",
        "CALLE": "Calle",
        "AV": "Avenida",
        "AVDA": "Avenida",
        "AVENIDA": "Avenida",
        "PZ": "Plaza",
        "PLAZA": "Plaza",
    }
    return mapping.get(raw_type.upper(), raw_type.title()), street


def _merge_ex_fields(
    *,
    parsed: dict[str, str],
    fallback_fields: dict[str, Any],
    overrides: dict[str, Any],
) -> dict[str, Any]:
    """Build normalized field dictionary with parser -> fallback -> override priority."""
    fallback = {k: _safe(v) for k, v in fallback_fields.items()}
    over = {k: _safe(v) for k, v in overrides.items()}

    def pick(field: str) -> str:
        return over.get(field) or _safe(parsed.get(field)) or fallback.get(field, "")

    street = pick("nombre_via_publica")
    tipo_via = pick("tipo_via")
    if street and not tipo_via:
        tipo_via, street = _tipo_via_from_street(street)

    apellidos = pick("apellidos")
    nombre = pick("nombre")
    full_name = _clean_spaces(
        over.get("full_name")
        or _safe(parsed.get("full_name"))
        or f"{apellidos} {nombre}"
    )

    return {
        "nif_nie": _upper_compact(pick("nif_nie")),
        "pasaporte": _upper_compact(pick("pasaporte")),
        "apellidos": apellidos,
        "nombre": nombre,
        "full_name": full_name,
        "sexo": _normalize_sex_code(pick("sexo")),
        "estado_civil": pick("estado_civil"),
        "fecha_nacimiento": _to_spanish_date(pick("fecha_nacimiento")),
        "lugar_nacimiento": pick("lugar_nacimiento"),
        "pais_nacimiento": pick("pais_nacimiento"),
        "nacionalidad": pick("nacionalidad"),
        "nombre_padre": pick("nombre_padre"),
        "nombre_madre": pick("nombre_madre"),
        "tipo_via": tipo_via,
        "nombre_via_publica": street,
        "numero": pick("numero"),
        "escalera": pick("escalera"),
        "piso": pick("piso"),
        "puerta": pick("puerta"),
        "municipio": pick("municipio"),
        "provincia": pick("provincia"),
        "codigo_postal": pick("codigo_postal"),
        "telefono": pick("telefono"),
        "email": _normalize_email(pick("email")),
        "representante_legal": pick("representante_legal"),
        "representante_documento": pick("representante_documento"),
        "titulo_representante": pick("titulo_representante"),
    }


def parse_ex_form_fields(
    *,
    ex_form_code: str,
    merged_text: str,
    fallback_fields: dict[str, Any],
    overrides: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    """Parse EX-form fields and return `(fields, strategy)` for traceability."""
    parsed: dict[str, str] = {}
    strategy = "fallback"
    if ex_form_code == "ex_17":
        parsed = _parse_ex17_structured_tail(merged_text)
        if parsed:
            strategy = "ex17_structured_tail"
    fields = _merge_ex_fields(
        parsed=parsed, fallback_fields=fallback_fields, overrides=overrides
    )
    return fields, strategy
