"""Source-kind normalization and auto-detection helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass

CANONICAL_SOURCE_KINDS = {"anketa", "fmiliar", "passport", "nie_tie", "visa"}
SOURCE_KIND_ALIASES = {
    "familiar": "fmiliar",
    "form": "anketa",
    "formulario": "anketa",
    "questionnaire": "anketa",
}


@dataclass(frozen=True)
class SourceKindDetection:
    """Result of source-kind detection."""

    source_kind: str
    confidence: float
    requires_review: bool
    reason: str


def normalize_source_kind(value: str) -> str:
    """Normalize input source-kind to canonical value."""
    normalized = (value or "").strip().lower()
    if not normalized:
        return ""
    if normalized in CANONICAL_SOURCE_KINDS:
        return normalized
    return SOURCE_KIND_ALIASES.get(normalized, "")


def detect_source_kind(*, text: str, filename: str = "") -> SourceKindDetection:
    """Detect likely source-kind using OCR text and file-name hints."""
    upper_text = (text or "").upper()
    upper_name = (filename or "").upper()

    if _looks_like_visa(upper_text, upper_name):
        return SourceKindDetection(
            source_kind="visa",
            confidence=0.9,
            requires_review=False,
            reason="visa_markers",
        )
    if _looks_like_passport(upper_text, upper_name):
        return SourceKindDetection(
            source_kind="passport",
            confidence=0.9,
            requires_review=False,
            reason="passport_markers",
        )
    if _looks_like_nie_tie(upper_text, upper_name):
        return SourceKindDetection(
            source_kind="nie_tie",
            confidence=0.88,
            requires_review=False,
            reason="identity_markers",
        )
    if _looks_like_familiar_form(upper_text, upper_name):
        return SourceKindDetection(
            source_kind="fmiliar",
            confidence=0.76,
            requires_review=False,
            reason="family_form_markers",
        )
    if _looks_like_tasa_form(upper_text):
        return SourceKindDetection(
            source_kind="anketa",
            confidence=0.72,
            requires_review=False,
            reason="form_markers",
        )
    return SourceKindDetection(
        source_kind="anketa",
        confidence=0.35,
        requires_review=True,
        reason="fallback_default",
    )


def _looks_like_passport(text: str, filename: str) -> bool:
    """Detect passport markers from text/name."""
    mrz_line = re.search(r"(?:^|\n)P<[A-Z0-9<]{20,}", text)
    name_marker = "PASSPORT" in text or "ЗАГРАНПАСПОРТ" in text
    filename_marker = "PASSPORT" in filename
    return bool(mrz_line or name_marker or filename_marker)


def _looks_like_nie_tie(text: str, filename: str) -> bool:
    """Detect NIE/TIE/DNI markers from text/name."""
    markers = [" NIE ", " TIE ", " DNI ", "NÚMERO DE IDENTIDAD", "NUMERO DE IDENTIDAD"]
    filename_markers = ["NIE", "TIE", "DNI"]
    if any(marker in text for marker in markers) or any(
        marker in filename for marker in filename_markers
    ):
        return True
    # OCR often inserts line breaks/spaces inside "Identidad de Extranjero" labels.
    if re.search(
        r"IDENTIDAD\s+DE\s+EXTRANJER[OA]", text, flags=re.I | re.S
    ) or re.search(
        r"TARJETA\s+DE\s+IDENTIDAD\s+DE\s+EXTRANJER[OA]", text, flags=re.I | re.S
    ):
        return True
    # Common Spanish immigration form family markers (EX-17, EX17, EX 17...).
    if re.search(r"\bEX\s*[-–]?\s*(1[0-9]|2[0-9]|0?[1-9])\b", text, flags=re.I):
        return True
    return False


def _looks_like_visa(text: str, filename: str) -> bool:
    """Detect visa markers from text/name."""
    markers = [" VISA ", "SCHENGEN", "TYPE C", "TYPE D", "VISADO"]
    return any(marker in text for marker in markers) or "VISA" in filename


def _looks_like_familiar_form(text: str, filename: str) -> bool:
    """Detect familiar-form markers from text/name."""
    markers = [
        "FAMILIAR QUE DA DERECHO",
        "FAMILIAR",
        "REAGRUPACION",
        "REAGRUPACIÓN",
    ]
    return any(marker in text for marker in markers) or "FAMILIAR" in filename


def _looks_like_tasa_form(text: str) -> bool:
    """Detect generic tasa/anketa form markers."""
    markers = ["MODELO 790", "AUTOLIQUIDACION", "AUTOLIQUIDACIÓN", "DECLARANTE"]
    return any(marker in text for marker in markers)
