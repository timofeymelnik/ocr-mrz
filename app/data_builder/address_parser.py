"""Address parsing helpers for document data builder."""

from __future__ import annotations

import re
from typing import Any

from app.data_builder.constants import (
    ADDRESS_ABBREVIATIONS,
    TIPO_VIA_CANONICAL,
    TIPO_VIA_CANONICAL_NORM,
    norm_tipo_token,
    postal_tipo_via_aliases,
)
from app.data_builder.normalizers import clean_spaces, normalize_puerta


def _safe(value: Any) -> str:
    """Convert optional value to trimmed string."""
    if value is None:
        return ""
    return str(value).strip()


def expand_abbrev(address: str) -> tuple[str, list[dict[str, str]]]:
    """Expand known Spanish street abbreviations in address text."""
    expanded = address
    used: list[dict[str, str]] = []
    for short, full in ADDRESS_ABBREVIATIONS.items():
        pattern = rf"\b{re.escape(short)}\b"
        if re.search(pattern, expanded, flags=re.I):
            expanded = re.sub(pattern, full, expanded, flags=re.I)
            used.append({"abbr": short, "expanded": full})
    return clean_spaces(expanded), used


def parse_address_parts(address: str, overrides: dict[str, Any]) -> dict[str, str]:
    """Parse structured address fields from freeform address and overrides."""
    fields = {
        "tipo_via": _safe(overrides.get("tipo_via")),
        "nombre_via_publica": _safe(overrides.get("nombre_via_publica")),
        "numero": _safe(overrides.get("numero")),
        "escalera": _safe(overrides.get("escalera")),
        "piso": _safe(overrides.get("piso")),
        "puerta": _safe(overrides.get("puerta")),
        "municipio": _safe(overrides.get("municipio")),
        "provincia": _safe(overrides.get("provincia")),
        "codigo_postal": _safe(overrides.get("codigo_postal")),
    }
    if not address:
        return fields

    upper = address.upper()
    type_match = re.match(r"^\s*([A-ZÁÉÍÓÚÑÜ]+)\b", upper)
    if type_match and not fields["tipo_via"]:
        token = type_match.group(1)
        token_norm = norm_tipo_token(token)
        alias_map = postal_tipo_via_aliases()
        canonical_norm = alias_map.get(token_norm) or token_norm
        if canonical_norm in TIPO_VIA_CANONICAL_NORM:
            pretty = next(
                (
                    value
                    for value in TIPO_VIA_CANONICAL
                    if norm_tipo_token(value) == canonical_norm
                ),
                canonical_norm,
            )
            fields["tipo_via"] = pretty.title()

    if fields["tipo_via"] and not fields["nombre_via_publica"]:
        candidates = [fields["tipo_via"].upper()]
        if type_match:
            candidates.append(type_match.group(1))
        for token in candidates:
            match = re.search(re.escape(token) + r"[.\s]+([^,\d]+)", upper)
            if match:
                fields["nombre_via_publica"] = clean_spaces(match.group(1)).title()
                break

    if not fields["numero"]:
        match = re.search(r"\b(\d{1,5}[A-Z]?)\b", upper)
        if match:
            fields["numero"] = match.group(1)

    if not fields["codigo_postal"]:
        match = re.search(r"\b(\d{5})\b", upper)
        if match:
            fields["codigo_postal"] = match.group(1)

    if not fields["municipio"] or not fields["provincia"]:
        match = re.search(
            r"\b([A-ZÁÉÍÓÚÑÜ]{3,})\s+([A-ZÁÉÍÓÚÑÜ]{3,})\s*-\s*ESP\b", upper
        )
        if match:
            if not fields["municipio"]:
                fields["municipio"] = match.group(1).title()
            if not fields["provincia"]:
                fields["provincia"] = match.group(2).title()

    if not fields["piso"]:
        match = re.search(r"\bPLANTA\s+([A-Z0-9]+)\b", upper)
        if match:
            fields["piso"] = match.group(1)

    if not fields["puerta"]:
        match = re.search(r"\bPUERTA\s+([A-Z0-9]+)\b", upper)
        if match:
            fields["puerta"] = match.group(1)

    if not fields["piso"] or not fields["puerta"]:
        match = re.search(r"\bP\s*0?(\d{1,2})\s+(\d{2,5}[A-Z]?)\b", upper)
        if match:
            if not fields["piso"]:
                fields["piso"] = match.group(1)
            if not fields["puerta"]:
                fields["puerta"] = match.group(2)

    fields["puerta"] = normalize_puerta(fields["puerta"])
    return fields
