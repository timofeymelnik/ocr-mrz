from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

from validators import normalize_date, normalize_mrz_date, validate_spanish_document_number


@dataclass
class ParsedDocument:
    surname: str
    name: str
    document_number: str
    date_of_birth: str
    expiry_date: str
    nationality: str

    def to_row(self) -> Dict[str, str]:
        return {
            "surname": self.surname,
            "name": self.name,
            "document_number": self.document_number,
            "date_of_birth": self.date_of_birth,
            "expiry_date": self.expiry_date,
            "nationality": self.nationality,
        }


def _clean_field(value: str) -> str:
    return value.replace("<", " ").strip()


def _split_names(value: str) -> tuple[str, str]:
    if "<<" in value:
        surname, given = value.split("<<", 1)
        return _clean_field(surname), _clean_field(given).replace("  ", " ")
    parts = [p for p in value.replace("<", " ").split() if p]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return " ".join(parts[:-1]), parts[-1]


def _group_mrz_lines(candidates: List[str]) -> List[List[str]]:
    grouped: List[List[str]] = []
    clean = [c.strip() for c in candidates if c.strip()]
    for i in range(len(clean)):
        for size in (3, 2):
            chunk = clean[i : i + size]
            if len(chunk) != size:
                continue
            lengths = [len(x) for x in chunk]
            if size == 3 and all(28 <= x <= 31 for x in lengths):
                grouped.append(chunk)
            if size == 2 and all(40 <= x <= 45 for x in lengths):
                grouped.append(chunk)
    return grouped


def parse_mrz_lines(candidates: List[str]) -> Optional[ParsedDocument]:
    for block in _group_mrz_lines(candidates):
        if len(block) == 3:
            parsed = _parse_td1(block)
        else:
            parsed = _parse_td3_like(block)
        if parsed:
            return parsed
    return None


def _parse_td1(lines: List[str]) -> Optional[ParsedDocument]:
    l1, l2, l3 = [ln.ljust(30, "<")[:30] for ln in lines]
    document_number = re.sub(r"<", "", l1[5:14])
    dob = normalize_mrz_date(l2[0:6], is_expiry=False)
    expiry = normalize_mrz_date(l2[8:14], is_expiry=True)
    nationality = _clean_field(l2[15:18]).replace(" ", "")
    surname, name = _split_names(l3)

    if not validate_spanish_document_number(document_number):
        alt_doc = re.sub(r"<", "", l2[18:27])
        if validate_spanish_document_number(alt_doc):
            document_number = alt_doc

    if (
        surname
        and name
        and dob
        and expiry
        and nationality
        and validate_spanish_document_number(document_number)
    ):
        return ParsedDocument(
            surname=surname,
            name=name,
            document_number=document_number,
            date_of_birth=dob,
            expiry_date=expiry,
            nationality=nationality,
        )
    return None


def _parse_td3_like(lines: List[str]) -> Optional[ParsedDocument]:
    l1, l2 = [ln.ljust(44, "<")[:44] for ln in lines]
    surname, name = _split_names(l1[5:])
    document_number = re.sub(r"<", "", l2[0:9])
    nationality = _clean_field(l2[10:13]).replace(" ", "")
    dob = normalize_mrz_date(l2[13:19], is_expiry=False)
    expiry = normalize_mrz_date(l2[21:27], is_expiry=True)

    if (
        surname
        and name
        and dob
        and expiry
        and nationality
        and validate_spanish_document_number(document_number)
    ):
        return ParsedDocument(
            surname=surname,
            name=name,
            document_number=document_number,
            date_of_birth=dob,
            expiry_date=expiry,
            nationality=nationality,
        )
    return None


def parse_from_ocr_text(text: str) -> Optional[ParsedDocument]:
    normalized = re.sub(r"[ \t]+", " ", text.upper())
    lines = [ln.strip() for ln in normalized.splitlines() if ln.strip()]

    document_number = _find_document_number(normalized)
    if not document_number:
        return None

    surname, name = _find_names(lines)
    dob, expiry = _find_dates(normalized)
    nationality = _find_nationality(normalized)

    if not all([surname, name, dob, expiry]):
        return None

    return ParsedDocument(
        surname=surname,
        name=name,
        document_number=document_number,
        date_of_birth=dob,
        expiry_date=expiry,
        nationality=nationality,
    )


def _extract_after_label(lines: List[str], labels: List[str]) -> str:
    for i, line in enumerate(lines):
        for label in labels:
            if label in line:
                parts = re.split(rf"{label}\s*[:\-]?\s*", line, maxsplit=1)
                if len(parts) > 1 and parts[1].strip():
                    return parts[1].strip()
                if i + 1 < len(lines):
                    return lines[i + 1].strip()
    return ""


def _find_document_number(text: str) -> str:
    patterns = [r"\b[XYZ]\d{7}[A-Z]\b", r"\b\d{8}[A-Z]\b"]
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            candidate = match.group(0)
            if validate_spanish_document_number(candidate):
                return candidate
    return ""


def _find_names(lines: List[str]) -> tuple[str, str]:
    joined = " ".join(lines)

    combined_match = re.search(
        r"(?:SURNAMES?|APELLIDOS)\s+(?:FORENAMES?|NOMBRES?|NAME)\s+([A-Z][A-Z<\-']{1,})\s+([A-Z][A-Z<\-']{1,})",
        joined,
    )
    if combined_match:
        return _clean_field(combined_match.group(1)), _clean_field(combined_match.group(2))

    surname = _extract_after_label(lines, ["APELLIDOS", "SURNAME"])
    name = _extract_after_label(lines, ["NOMBRE", "NAME", "GIVEN NAMES"])
    surname = re.sub(r"[^A-Z< ]", "", surname).strip()
    name = re.sub(r"[^A-Z< ]", "", name).strip()

    if surname and name:
        return _clean_field(surname), _clean_field(name)

    for line in lines:
        if ("SURNAME" in line or "APELLIDOS" in line) and ("FORENAME" in line or "NOMBRE" in line):
            words = re.findall(r"[A-Z][A-Z<\-']+", line)
            filtered = [
                w
                for w in words
                if w
                not in {
                    "APELLIDOS",
                    "NOMBRES",
                    "NOMBRE",
                    "SURNAME",
                    "SURNAMES",
                    "FORENAME",
                    "FORENAMES",
                    "NAME",
                    "NAMES",
                }
            ]
            if len(filtered) >= 2:
                return _clean_field(filtered[-2]), _clean_field(filtered[-1])
    return "", ""


def _find_dates(text: str) -> tuple[str, str]:
    dob = ""
    expiry = ""
    dob_match = re.search(
        r"(DOB|FECHA DE NACIMIENTO|NACIMIENTO|BIRTH)[^\d]*(\d{2}[/-]\d{2}[/-]\d{4}|\d{8}|\d{2}\s+\d{2}\s+\d{4})",
        text,
    )
    exp_match = re.search(
        r"(CADUCIDAD|EXPIRY|VALID UNTIL|VENCE)[^\d]*(\d{2}[/-]\d{2}[/-]\d{4}|\d{8}|\d{2}\s+\d{2}\s+\d{4})",
        text,
    )
    if dob_match:
        dob = normalize_date(re.sub(r"[^0-9]", "", dob_match.group(2))) or ""
    if exp_match:
        expiry = normalize_date(re.sub(r"[^0-9]", "", exp_match.group(2))) or ""

    if dob and expiry:
        return dob, expiry

    date_candidates: list[datetime] = []
    for raw in re.findall(r"\b\d{2}[/-]\d{2}[/-]\d{4}\b|\b\d{8}\b|\b\d{2}\s+\d{2}\s+\d{4}\b", text):
        normalized = normalize_date(re.sub(r"[^0-9]", "", raw))
        if not normalized:
            continue
        try:
            date_candidates.append(datetime.strptime(normalized, "%Y-%m-%d"))
        except ValueError:
            continue

    if len(date_candidates) >= 2:
        date_candidates.sort()
        if not dob:
            dob = date_candidates[0].strftime("%Y-%m-%d")
        if not expiry:
            expiry = date_candidates[-1].strftime("%Y-%m-%d")
    return dob, expiry


def _find_nationality(text: str) -> str:
    match = re.search(
        r"\b(?:NACIONALIDAD|NATIONALITY)\s*[:\-]?\s*([A-Z]{3}|[A-Z ]{4,20})\b",
        text,
    )
    if match:
        return match.group(1).strip().replace(" ", "")
    return "ESP"
