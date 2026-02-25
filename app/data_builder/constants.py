"""Constants and catalog helpers for tasa document parsing."""

from __future__ import annotations

import json
import os
import re
from functools import lru_cache
from pathlib import Path

REQUIRED_FIELDS_790_012 = [
    "nif_nie",
    "apellidos_nombre_razon_social",
    "tipo_via",
    "nombre_via_publica",
    "numero",
    "municipio",
    "provincia",
    "codigo_postal",
    "localidad_declaracion",
    "fecha",
    "forma_pago",
]

REQUIRED_FIELDS_MI_T = [
    "nif_nie",
    "apellidos",
    "nombre",
]

REQUIRED_FIELDS_VISUAL_GENERIC: list[str] = []

ADDRESS_ABBREVIATIONS = {
    "C": "Calle",
    "CL": "Calle",
    "AV": "Avenida",
    "AVDA": "Avenida",
    "PZ": "Plaza",
    "PL": "Plaza",
    "PS": "Paseo",
    "PSO": "Paseo",
    "CR": "Carretera",
    "CTRA": "Carretera",
    "CM": "Camino",
    "CMNO": "Camino",
    "TR": "Travesia",
    "TRV": "Travesia",
    "URB": "Urbanizacion",
    "PJE": "Pasaje",
    "PJ": "Pasaje",
    "GL": "Glorieta",
    "GTA": "Glorieta",
    "POL": "Poligono",
    "PG": "Poligono",
    "RDA": "Ronda",
    "BARR": "Barrio",
    "BAR": "Barrio",
    "PB": "Planta Baja",
    "PBJ": "Planta Baja",
    "BJ": "Bajo",
    "ENT": "Entresuelo",
    "PRAL": "Principal",
    "PISO": "Piso",
    "PTA": "Puerta",
    "IZQ": "Izquierda",
    "DCHA": "Derecha",
    "ESC": "Escalera",
}

TIPO_VIA_CANONICAL = {
    "CALLE",
    "AVENIDA",
    "PLAZA",
    "PASEO",
    "PASAJE",
    "CARRETERA",
    "CAMINO",
    "TRAVESIA",
    "URBANIZACION",
    "GLORIETA",
    "RONDA",
    "POLIGONO",
    "BARRIO",
    "AUTOPISTA",
    "AUTOVIA",
    "CUESTA",
    "RAMBLA",
    "SENDA",
    "VEREDA",
}

MONTHS_ES = {
    "enero": "01",
    "febrero": "02",
    "marzo": "03",
    "abril": "04",
    "mayo": "05",
    "junio": "06",
    "julio": "07",
    "agosto": "08",
    "septiembre": "09",
    "setiembre": "09",
    "octubre": "10",
    "noviembre": "11",
    "diciembre": "12",
}

RU_TO_LAT = {
    "А": "A",
    "Б": "B",
    "В": "V",
    "Г": "G",
    "Д": "D",
    "Е": "E",
    "Ё": "E",
    "Ж": "ZH",
    "З": "Z",
    "И": "I",
    "Й": "I",
    "К": "K",
    "Л": "L",
    "М": "M",
    "Н": "N",
    "О": "O",
    "П": "P",
    "Р": "R",
    "С": "S",
    "Т": "T",
    "У": "U",
    "Ф": "F",
    "Х": "KH",
    "Ц": "TS",
    "Ч": "CH",
    "Ш": "SH",
    "Щ": "SHCH",
    "Ъ": "",
    "Ы": "Y",
    "Ь": "",
    "Э": "E",
    "Ю": "YU",
    "Я": "YA",
}


def norm_tipo_token(value: str) -> str:
    """Normalize Spanish street type token for dictionary matching."""
    normalized = (value or "").upper()
    normalized = re.sub(r"[ÁÀÂÄ]", "A", normalized)
    normalized = re.sub(r"[ÉÈÊË]", "E", normalized)
    normalized = re.sub(r"[ÍÌÎÏ]", "I", normalized)
    normalized = re.sub(r"[ÓÒÔÖ]", "O", normalized)
    normalized = re.sub(r"[ÚÙÛÜ]", "U", normalized)
    normalized = normalized.replace("Ñ", "N")
    normalized = re.sub(r"[^A-Z0-9]+", "", normalized)
    return normalized


TIPO_VIA_CANONICAL_NORM = {norm_tipo_token(value) for value in TIPO_VIA_CANONICAL}


@lru_cache(maxsize=1)
def postal_tipo_via_aliases() -> dict[str, str]:
    """Load optional aliases from JSON catalog for postal street types."""
    default_path = Path("runtime/catalogs/postal_street_types_es.json")
    configured = os.getenv("POSTAL_STREET_TYPE_DICT_PATH", "").strip()
    path = Path(configured) if configured else default_path
    if not path.exists():
        return {}

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    source = (
        raw.get("aliases")
        if isinstance(raw, dict) and isinstance(raw.get("aliases"), dict)
        else raw
    )
    if not isinstance(source, dict):
        return {}

    out: dict[str, str] = {}
    for alias, canonical in source.items():
        alias_key = norm_tipo_token(str(alias))
        canonical_value = norm_tipo_token(str(canonical))
        if alias_key and canonical_value:
            out[alias_key] = canonical_value
    return out
