"""Pure helper utilities extracted from target_autofill monolith."""

from __future__ import annotations

import re
import unicodedata
from typing import Any

import fitz


def strip_extra_spaces(value: str) -> str:
    """Collapse whitespace and trim punctuation around token values."""
    return re.sub(r"\s+", " ", (value or "")).strip(" ,.-")


def sanitize_floor_token(value: str) -> str:
    """Normalize floor token and drop noisy postal-code placeholders."""
    v = strip_extra_spaces(value).upper()
    if v in {"CP", "C.P", "C.P.", "CODIGO POSTAL", "CÓDIGO POSTAL"}:
        return ""
    return strip_extra_spaces(value)


def compose_floor_door_token(piso: str, puerta: str) -> str:
    """Build compact floor+door token while avoiding duplicated letter suffixes."""
    piso_clean = sanitize_floor_token(piso)
    puerta_clean = strip_extra_spaces(puerta)
    if piso_clean and puerta_clean:
        piso_norm = norm_text(piso_clean)
        puerta_norm = norm_text(puerta_clean)
        if puerta_norm and puerta_norm in piso_norm:
            return piso_clean
        return f"{piso_clean} {puerta_clean}".strip()
    return piso_clean or puerta_clean


def normalize_door_token(value: str) -> str:
    """Normalize OCR door token and transliterate common Cyrillic lookalikes."""
    raw = strip_extra_spaces(value).upper()
    if not raw:
        return ""
    translit = {
        "А": "A",
        "В": "B",
        "Е": "E",
        "К": "K",
        "М": "M",
        "Н": "H",
        "О": "O",
        "Р": "P",
        "С": "C",
        "Т": "T",
        "Х": "X",
    }
    return "".join(translit.get(ch, ch) for ch in raw)


def split_compact_floor_door(piso: str, puerta: str) -> tuple[str, str]:
    """Split compact floor tokens like ``5C`` into floor and door parts."""
    piso_clean = sanitize_floor_token(piso)
    puerta_clean = normalize_door_token(puerta)
    if piso_clean and puerta_clean:
        compact_with_door = re.fullmatch(
            r"(\d{1,3})\s*[ºª]?\s*([A-Z])", piso_clean.upper()
        )
        if compact_with_door and compact_with_door.group(2) == puerta_clean:
            return compact_with_door.group(1), puerta_clean
    if piso_clean and not puerta_clean:
        compact = re.fullmatch(r"(\d{1,3})\s*([A-Z])", piso_clean.upper())
        if compact:
            return compact.group(1), compact.group(2)
    return piso_clean, puerta_clean


def split_address_details(nombre_via: str) -> tuple[str, str, str, str, str]:
    """Extract numero/escalera/piso/puerta suffixes from freeform street value."""
    raw = strip_extra_spaces(nombre_via)
    if not raw:
        return "", "", "", "", ""

    work = f" {raw} "
    inferred_numero = ""
    inferred_escalera = ""
    inferred_piso = ""
    inferred_puerta = ""

    patterns = [
        (
            "numero",
            re.compile(
                r"\b(?:n[úu]m(?:ero)?\.?|num\.?)\s*([0-9A-Z][0-9A-Z\-]*)\b",
                re.I,
            ),
        ),
        (
            "escalera",
            re.compile(
                r"\b(?:escalera|esc\.?|portal|bloque)\s*([0-9A-Z][0-9A-Z\-]*)\b",
                re.I,
            ),
        ),
        (
            "piso",
            re.compile(r"\b(?:piso|planta)\s*([0-9A-Zºª][0-9A-Zºª\-]*)\b", re.I),
        ),
        (
            "puerta",
            re.compile(r"\b(?:puerta|pta\.?|casa)\s*([0-9A-Z][0-9A-Z\-]*)\b", re.I),
        ),
    ]

    for kind, pattern in patterns:
        matched = pattern.search(work)
        if not matched:
            continue
        token = (matched.group(1) or "").strip().upper()
        if kind == "numero":
            inferred_numero = token
        elif kind == "escalera":
            inferred_escalera = token
        elif kind == "piso":
            inferred_piso = token
        elif kind == "puerta":
            inferred_puerta = token
        work = work[: matched.start()] + " " + work[matched.end() :]

    cleaned = strip_extra_spaces(work)
    return cleaned, inferred_numero, inferred_escalera, inferred_piso, inferred_puerta


def normalize_signal(value: str) -> str:
    """Normalize signal-like value into lowercase alnum token."""
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def normalize_ascii_upper(value: str) -> str:
    """Normalize value to uppercase ASCII by stripping diacritics."""
    text = unicodedata.normalize("NFD", (value or "").strip().upper())
    return "".join(ch for ch in text if unicodedata.category(ch) != "Mn")


def infer_spanish_province_from_cp(cp_value: str) -> str:
    """Infer Spanish province from first two CP digits."""
    cp = re.sub(r"\D+", "", cp_value or "")
    if len(cp) < 2:
        return ""
    prefix = cp[:2]
    by_prefix = {
        "01": "ALAVA",
        "02": "ALBACETE",
        "03": "ALICANTE",
        "04": "ALMERIA",
        "05": "AVILA",
        "06": "BADAJOZ",
        "07": "BALEARES",
        "08": "BARCELONA",
        "09": "BURGOS",
        "10": "CACERES",
        "11": "CADIZ",
        "12": "CASTELLON",
        "13": "CIUDAD REAL",
        "14": "CORDOBA",
        "15": "A CORUNA",
        "16": "CUENCA",
        "17": "GIRONA",
        "18": "GRANADA",
        "19": "GUADALAJARA",
        "20": "GUIPUZCOA",
        "21": "HUELVA",
        "22": "HUESCA",
        "23": "JAEN",
        "24": "LEON",
        "25": "LLEIDA",
        "26": "LA RIOJA",
        "27": "LUGO",
        "28": "MADRID",
        "29": "MALAGA",
        "30": "MURCIA",
        "31": "NAVARRA",
        "32": "OURENSE",
        "33": "ASTURIAS",
        "34": "PALENCIA",
        "35": "LAS PALMAS",
        "36": "PONTEVEDRA",
        "37": "SALAMANCA",
        "38": "SANTA CRUZ DE TENERIFE",
        "39": "CANTABRIA",
        "40": "SEGOVIA",
        "41": "SEVILLA",
        "42": "SORIA",
        "43": "TARRAGONA",
        "44": "TERUEL",
        "45": "TOLEDO",
        "46": "VALENCIA",
        "47": "VALLADOLID",
        "48": "VIZCAYA",
        "49": "ZAMORA",
        "50": "ZARAGOZA",
        "51": "CEUTA",
        "52": "MELILLA",
    }
    return by_prefix.get(prefix, "")


def norm_text(value: str) -> str:
    """Return lowercased alphanumeric text for fuzzy comparisons."""
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def split_date_parts(value: str) -> tuple[str, str, str]:
    """Parse multiple date string formats into ``DD,MM,YYYY`` tuple."""
    raw = (value or "").strip()
    if not raw:
        return "", "", ""
    matched = re.fullmatch(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", raw)
    if matched:
        dd = matched.group(1).zfill(2)
        mm = matched.group(2).zfill(2)
        yy = matched.group(3)
        if len(yy) == 2:
            yy = f"20{yy}"
        return dd, mm, yy
    matched_iso = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", raw)
    if matched_iso:
        return (
            matched_iso.group(3).zfill(2),
            matched_iso.group(2).zfill(2),
            matched_iso.group(1),
        )
    digits = re.sub(r"\D+", "", raw)
    if len(digits) == 8:
        return digits[0:2], digits[2:4], digits[4:8]
    return "", "", ""


def build_date_split_field_values(
    doc: fitz.Document,
    explicit_by_field: dict[str, str],
    value_map: dict[str, str],
) -> dict[str, str]:
    """Map split day/month/year values to PDF field names by row geometry."""
    out: dict[str, str] = {}
    for date_key in ("fecha_nacimiento", "fecha"):
        dd, mm, yy = split_date_parts(value_map.get(date_key, ""))
        if not (dd and mm and yy):
            continue
        candidates: list[dict[str, Any]] = []
        for page in doc:
            for widget in page.widgets() or []:
                field_name = str((widget.field_name or "")).strip()
                if not field_name:
                    continue
                if explicit_by_field.get(field_name) != date_key:
                    continue
                if (
                    "check"
                    in str(getattr(widget, "field_type_string", "") or "").lower()
                ):
                    continue
                rect = widget.rect
                candidates.append(
                    {
                        "name": field_name,
                        "x0": float(rect.x0),
                        "y0": float(rect.y0),
                    }
                )
        if len(candidates) < 3:
            continue
        candidates.sort(key=lambda c: (c["y0"], c["x0"]))
        row = [c for c in candidates if abs(c["y0"] - candidates[0]["y0"]) <= 25.0]
        if len(row) < 3:
            row = candidates[:3]
        row.sort(key=lambda c: c["x0"])
        out[str(row[0]["name"])] = dd
        out[str(row[1]["name"])] = mm
        out[str(row[2]["name"])] = yy
    return out
