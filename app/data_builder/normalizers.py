"""Shared normalization helpers for data builder extraction flows."""

from __future__ import annotations

import re
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any

try:
    from dateutil import parser as date_parser  # type: ignore[import-untyped]
except Exception:  # pragma: no cover
    date_parser = None

try:
    from rapidfuzz import fuzz
except Exception:  # pragma: no cover
    fuzz = None  # type: ignore[assignment]

from app.core.validators import normalize_date
from app.data_builder.constants import RU_TO_LAT


def _similarity_ratio(left: str, right: str) -> float:
    """Return normalized string similarity ratio in range [0, 100]."""
    if fuzz is not None:
        return float(fuzz.ratio(left, right))
    return SequenceMatcher(None, left, right).ratio() * 100.0


def clean_spaces(value: str) -> str:
    """Normalize whitespace and trim string boundaries."""
    return re.sub(r"\s+", " ", (value or "").strip())


def upper_compact(value: str) -> str:
    """Uppercase text and remove all whitespace."""
    return re.sub(r"\s+", "", (value or "")).upper()


def contains_cyrillic(value: str) -> bool:
    """Return ``True`` when text contains Cyrillic letters."""
    return bool(re.search(r"[А-Яа-яЁё]", value or ""))


def transliterate_ru(value: str) -> str:
    """Transliterate Cyrillic text to latin using project mapping."""
    raw = str(value or "")
    if not raw:
        return ""

    out: list[str] = []
    for char in raw:
        upper_char = char.upper()
        if upper_char in RU_TO_LAT:
            replacement = RU_TO_LAT[upper_char]
            if char.islower():
                replacement = replacement.lower()
            out.append(replacement)
        else:
            out.append(char)

    return clean_spaces("".join(out))


def cleanup_nameish(value: str) -> str:
    """Trim common OCR separators around name-like fragments."""
    normalized = clean_spaces(str(value or ""))
    if not normalized:
        return ""
    normalized = re.sub(r"\s*/\s*$", "", normalized)
    normalized = re.sub(r"^\s*/\s*", "", normalized)
    return clean_spaces(normalized)


def normalize_sex_code(value: str) -> str:
    """Normalize sex value into document-compatible code."""
    normalized = upper_compact(value)
    if not normalized:
        return ""
    if normalized in {"H", "M", "X"}:
        return normalized
    if normalized in {"F", "FEMALE", "WOMAN", "MUJER"}:
        return "M"
    if normalized in {"MALE", "MAN", "HOMBRE"}:
        return "H"
    return ""


def normalize_document_sex_code(value: str) -> str:
    """Normalize sex for passport/MRZ conventions."""
    normalized = upper_compact(value)
    if not normalized:
        return ""
    if normalized in {"F", "FEMALE", "WOMAN", "MUJER"}:
        return "M"
    if normalized in {"M", "MALE", "MAN", "HOMBRE", "H"}:
        return "H"
    if normalized == "X":
        return "X"
    return ""


def normalize_puerta(value: str) -> str:
    """Normalize door identifier preserving alphanumeric values."""
    normalized = clean_spaces(value)
    if not normalized:
        return ""
    if re.fullmatch(r"\d+", normalized):
        return str(int(normalized))
    return normalized


def normalize_email(value: Any) -> str:
    """Normalize and validate email-like value."""
    email = clean_spaces(str(value or "")).lower()
    if not email:
        return ""
    if re.fullmatch(r"[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}", email):
        return email
    return ""


def to_spanish_date(value: str) -> str:
    """Convert different date representations into ``DD/MM/YYYY``."""
    normalized = clean_spaces(value)
    if not normalized:
        return ""
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", normalized):
        return normalized
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", normalized):
        return f"{normalized[8:10]}/{normalized[5:7]}/{normalized[0:4]}"

    iso = normalize_date(re.sub(r"[^0-9]", "", normalized))
    if iso and re.fullmatch(r"\d{4}-\d{2}-\d{2}", iso):
        return f"{iso[8:10]}/{iso[5:7]}/{iso[0:4]}"

    if date_parser is None:
        return ""

    try:
        parsed = date_parser.parse(normalized, dayfirst=True, yearfirst=False)
    except Exception:
        return ""
    if not isinstance(parsed, datetime):
        return ""

    return parsed.strftime("%d/%m/%Y")


def normalize_nationality(value: str) -> str:
    """Normalize nationality string and filter invalid tokens."""
    raw = clean_spaces(value)
    if not raw:
        return ""
    if to_spanish_date(raw) or re.search(r"\d", raw):
        return ""
    letters = re.sub(r"[^A-ZÁÉÍÓÚÑÜ ]", "", raw.upper())
    letters = clean_spaces(letters)
    if not letters:
        return ""
    return letters


def clean_address_freeform(value: str) -> str:
    """Remove noisy address labels from freeform OCR values."""
    normalized = clean_spaces(value)
    if not normalized:
        return ""
    normalized = re.sub(
        r"^(?:OBSERVACIONES/REMARKS\s+)+",
        "",
        normalized,
        flags=re.I,
    )
    normalized = re.sub(
        r"^(?:DOMICILIO/ADDRESS\s+)+",
        "",
        normalized,
        flags=re.I,
    )
    return clean_spaces(normalized)


def is_labelish_fragment(value: str) -> bool:
    """Detect whether text likely contains labels instead of actual data."""
    normalized = clean_spaces(value).upper()
    if not normalized:
        return True

    tokens = {token for token in re.findall(r"[A-ZÁÉÍÓÚÑÜ]+", normalized)}
    label_tokens = {
        "NOMBRE",
        "NAME",
        "APELLIDO",
        "APELLIDOS",
        "SURNAME",
        "SURNAMES",
        "NACIONALIDAD",
        "NATIONALITY",
        "DOMICILIO",
        "DOMICILI",
        "ADDRESS",
        "DIRECCION",
        "DIRECCIÓN",
        "MUNICIPIO",
        "LOCALIDAD",
        "PROVINCIA",
        "CODIGO",
        "CÓDIGO",
        "POSTAL",
        "DOCUMENTO",
        "DOCUMENT",
        "PASAPORTE",
        "PASSPORT",
        "LUGAR",
        "NACIMIENTO",
        "LLOC",
        "NAIXEMENT",
        "CITY",
        "BIRTH",
        "COUNTRY",
        "PAIS",
        "COGNOMS",
        "NOM",
    }

    if tokens and tokens.issubset(label_tokens):
        return True

    # OCR-safe matching for near-label typos.
    for token in tokens:
        if max(_similarity_ratio(token, label) for label in label_tokens) >= 88:
            if len(tokens) <= 4:
                return True

    if re.fullmatch(r"[*\-_/.: ]+", normalized):
        return True
    if (
        "/" in normalized
        and len(tokens) <= 5
        and any(token in label_tokens for token in tokens)
    ):
        return True

    noisy_phrases = [
        "LUGAR DE NACIMIENTO",
        "CIUDAD DE NACIMIENTO",
        "DATOS DEL",
        "HIJO",
        "MADRE",
        "PADRE",
    ]
    if any(phrase in normalized for phrase in noisy_phrases):
        return True

    return False


def is_invalid_place_of_birth(value: str) -> bool:
    """Return ``True`` when value looks like metadata/noise instead of birthplace."""
    normalized = clean_spaces(value)
    if not normalized:
        return True

    upper_value = normalized.upper()
    compact = upper_compact(normalized)

    if re.fullmatch(r"(?:X/)?[MFX]", compact):
        return True
    if to_spanish_date(normalized):
        return True

    blocked = [
        "УЧЕТНАЯ ЗАПИС",
        "UCHETNAYA ZAPIS",
        "ACCOUNT",
        "PLACE OF BIRT",
        "PLACE OF BIRTH",
        "CITY OF BIRTH",
    ]
    if any(token in upper_value for token in blocked):
        return True

    if is_labelish_fragment(normalized):
        return True

    return False
