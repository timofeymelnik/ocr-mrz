from __future__ import annotations

import json
import re
import uuid
from datetime import UTC, datetime
from typing import Any

from geocoding import fetch_geocode_candidates
from mrz_parser import parse_mrz_lines
from validators import normalize_date, validate_spanish_document_number

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
    "PJE": "Pasaje",
    "PJ": "Pasaje",
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


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _safe(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _clean_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def _upper_compact(value: str) -> str:
    return re.sub(r"\s+", "", (value or "")).upper()


def _normalize_puerta(value: str) -> str:
    v = _clean_spaces(value)
    if not v:
        return ""
    if re.fullmatch(r"\d+", v):
        return str(int(v))
    return v


def _normalize_email(value: Any) -> str:
    email = _clean_spaces(_safe(value)).lower()
    if not email:
        return ""
    if re.fullmatch(r"[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}", email):
        return email
    return ""


def _to_spanish_date(value: str) -> str:
    v = _clean_spaces(value)
    if not v:
        return ""
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", v):
        return v
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
        return f"{v[8:10]}/{v[5:7]}/{v[0:4]}"
    iso = normalize_date(re.sub(r"[^0-9]", "", v))
    if iso and re.fullmatch(r"\d{4}-\d{2}-\d{2}", iso):
        return f"{iso[8:10]}/{iso[5:7]}/{iso[0:4]}"
    return ""


def _clean_address_freeform(value: str) -> str:
    v = _clean_spaces(value)
    if not v:
        return ""
    # Remove frequent leading OCR labels that pollute parsing.
    v = re.sub(r"^(?:OBSERVACIONES/REMARKS\s+)+", "", v, flags=re.I)
    v = re.sub(r"^(?:DOMICILIO/ADDRESS\s+)+", "", v, flags=re.I)
    return _clean_spaces(v)


def _is_labelish_fragment(value: str) -> bool:
    v = _clean_spaces(value).upper()
    if not v:
        return True
    tokens = {t for t in re.findall(r"[A-ZÁÉÍÓÚÑÜ]+", v)}
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
    if re.fullmatch(r"[*\-_/.: ]+", v):
        return True
    if "/" in v and len(tokens) <= 5 and any(t in label_tokens for t in tokens):
        return True
    noisy_phrases = [
        "LUGAR DE NACIMIENTO",
        "CIUDAD DE NACIMIENTO",
        "DATOS DEL",
        "HIJO",
        "MADRE",
        "PADRE",
    ]
    if any(p in v for p in noisy_phrases):
        return True
    return False


def _extract_labeled_value(lines: list[str], labels: list[str]) -> str:
    label_re = "|".join(labels)
    pattern = re.compile(rf"\b(?:{label_re})\b\s*[:\-]?\s*(.*)$", re.I)
    for idx, raw in enumerate(lines):
        line = _clean_spaces(raw)
        m = pattern.search(line)
        if not m:
            continue
        tail = _clean_spaces(m.group(1))
        if tail and not _is_labelish_fragment(tail):
            return tail
        for j in range(idx + 1, min(len(lines), idx + 5)):
            nxt = _clean_spaces(lines[j])
            if not nxt:
                continue
            if pattern.search(nxt):
                continue
            if _is_labelish_fragment(nxt):
                continue
            return nxt
    return ""


def _extract_passport_candidate(lines: list[str], text: str) -> str:
    marker_re = re.compile(r"PASAPORTE|PASSPORT|N[ºO°]?\s*DOCUMENTO|DOCUMENT NUMBER|OTRO DOCUMENTO", re.I)
    number_re = re.compile(r"\b[A-Z]{0,2}\s*\d{6,9}\b")
    stop_re = re.compile(r"VISADO|VISA|DATOS DEL VIAJE|PAIS PROCEDENCIA|N[°ºO] VUELO|HORA", re.I)

    for i, line in enumerate(lines):
        if not marker_re.search(line):
            continue
        for j in range(max(0, i - 3), min(len(lines), i + 7)):
            candidate = _clean_spaces(lines[j])
            if stop_re.search(candidate):
                continue
            m = number_re.search(candidate.upper())
            if not m:
                continue
            value = re.sub(r"\s+", "", m.group(0).upper())
            if len(value) >= 7 and not re.fullmatch(r"\d{7,10}", value):
                return value
            if re.fullmatch(r"\d{8,9}", value):
                return value

    block_match = re.search(
        r"DATOS DEL DOCUMENTO:(.*?)(?:DATOS DEL VIAJE:|FIRMA|$)",
        text,
        re.I | re.S,
    )
    if block_match:
        block = block_match.group(1)
        m2 = re.search(r"\b\d{2}\s*\d{7}\b", block)
        if m2:
            return re.sub(r"\s+", "", m2.group(0))
    return ""


def _looks_like_name_noise(value: str) -> bool:
    v = _clean_spaces(value).upper()
    if not v:
        return True
    if any(tok in v for tok in ["SURNAME", "SURNAMES", "NAME", "NOMBRE", "APELLIDO", "APELLIDOS"]):
        if len(re.findall(r"[A-ZÁÉÍÓÚÑÜ]{2,}", v)) <= 2:
            return True
    if "/" in v and len(v) < 40:
        return True
    return False


def _extract_visual_fields(text: str) -> dict[str, str]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    all_text_up = text.upper()
    docs = _extract_doc_candidates(text)

    nif_nie = docs[0] if docs else ""
    if not nif_nie:
        raw = _extract_labeled_value(lines, ["NIE", "NIF", "NIF/NIE", "N\\.?I\\.?F\\.?/N\\.?I\\.?E\\.?", "DOCUMENTO"])
        m = re.search(r"(?:[XYZ]\d{7}[A-Z]|\d{8}[A-Z])", _upper_compact(raw))
        if m:
            nif_nie = m.group(0)

    apellidos = _extract_labeled_value(lines, ["APELLIDOS?", "APELLIDO", "SURNAMES?", "SURNAME"])
    nombre = _extract_labeled_value(lines, ["NOMBRE", "NOMBRES?", "NAME", "FORENAMES?"])
    full_name = _extract_labeled_value(
        lines, ["APELLIDOS?\\s+Y\\s+NOMBRE", "NOMBRE\\s+Y\\s+APELLIDOS?", "RAZ[ÓO]N\\s+SOCIAL"]
    )
    if not full_name:
        full_name = _clean_spaces(f"{apellidos} {nombre}".strip())

    dob_raw = _extract_labeled_value(lines, ["FECHA\\s+NAC", "FECHA\\s+DE\\s+NACIMIENTO", "BIRTH\\s+DATE", "DOB"])
    dob = _to_spanish_date(dob_raw)
    nat = _extract_labeled_value(lines, ["NACIONALIDAD", "NATIONALITY"])
    if re.fullmatch(r"[A-Za-z]{3}", nat):
        nat = nat.upper()
    father_name = _extract_labeled_value(lines, ["PADRE", "DAD", "FATHER", "NOMBRE\\s+DEL\\s+PADRE"])
    mother_name = _extract_labeled_value(lines, ["MADRE", "MOTHER", "NOMBRE\\s+DE\\s+LA\\s+MADRE"])
    if not father_name and not mother_name:
        for i, raw in enumerate(lines):
            up = raw.upper()
            if "HIJO/A DE" in up or "HIJO DE" in up or "HIJA DE" in up or "FILLJA DE" in up:
                cleaned = re.sub(r".*?(HIJO/A DE|HIJO DE|HIJA DE|FILLJA DE)\s*", "", raw, flags=re.I).strip()
                if not cleaned or _is_labelish_fragment(cleaned):
                    for j in range(i + 1, min(len(lines), i + 4)):
                        nxt = _clean_spaces(lines[j])
                        if not nxt:
                            continue
                        if _is_labelish_fragment(nxt):
                            continue
                        if "<<" in nxt:
                            continue
                        cleaned = nxt
                        break
                # Common DNI OCR format: "PEDRO / GREGORIA"
                if "/" in cleaned:
                    left, right = cleaned.split("/", 1)
                    father_name = father_name or _clean_spaces(left)
                    mother_name = mother_name or _clean_spaces(right)
                else:
                    father_name = father_name or _clean_spaces(cleaned)
                break
    place_of_birth = _extract_labeled_value(lines, ["LUGAR\\s+DE\\s+NACIMIENTO", "CITY\\s+OF\\s+BIRTH", "PLACE\\s+OF\\s+BIRTH"])
    if not place_of_birth:
        country_birth = _extract_labeled_value(lines, ["PAIS\\s+NACIMIENTO", "COUNTRY\\s+OF\\s+BIRTH"])
        place_of_birth = country_birth
    father_name = re.sub(r"^(?:/)?(?:PADRE|DAD|FATHER)\s*[:\-]?\s*", "", _clean_spaces(father_name), flags=re.I)
    mother_name = re.sub(r"^(?:/)?(?:MADRE|MOTHER)\s*[:\-]?\s*", "", _clean_spaces(mother_name), flags=re.I)
    place_of_birth = re.sub(
        r"^(?:/)?(?:LUGAR\s+DE\s+NACIMIENTO|LLOC\s+DE\s+NAIXEMENT|CITY\s+OF\s+BIRTH|PLACE\s+OF\s+BIRTH|PAIS\s+NACIMIENTO|COUNTRY\s+OF\s+BIRTH)\s*[:\-]?\s*",
        "",
        _clean_spaces(place_of_birth),
        flags=re.I,
    )

    telefono = _extract_labeled_value(lines, ["TEL[ÉE]FONO", "MOVIL", "M[ÓO]VIL", "PHONE"])
    tel_match = re.search(r"(\+?\d[\d \-]{6,})", telefono)
    telefono = _clean_spaces(tel_match.group(1)) if tel_match else ""

    email = ""
    for i, raw in enumerate(lines):
        up = raw.upper()
        if "E-MAIL" not in up and "CORREO" not in up:
            continue
        local = _clean_spaces(raw.split(":", 1)[-1] if ":" in raw else raw)
        local = re.sub(r"\s+", "", local)
        domain = ""
        for j in range(i + 1, min(len(lines), i + 6)):
            cand = _clean_spaces(lines[j]).lower()
            if re.fullmatch(r"[a-z0-9.\-]+\.[a-z]{2,}", cand):
                domain = cand
                break
        guess = f"{local}@{domain}" if local and domain and "@" not in local else local
        email = _normalize_email(guess)
        if email:
            break
    if not email:
        email = _normalize_email(_extract_labeled_value(lines, ["E-?MAIL", "CORREO(?:\\s+ELECTR[ÓO]NICO)?"]))
    if not email:
        em = re.search(r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}", all_text_up, re.I)
        if em:
            email = _normalize_email(em.group(0))

    domicilio = _extract_labeled_value(lines, ["DOMICILIO", "ADDRESS", "V[ÍI]A\\s+P[ÚU]BLICA"])
    municipio = _extract_labeled_value(lines, ["MUNICIPIO", "LOCALIDAD", "POBLACI[ÓO]N"])
    provincia = _extract_labeled_value(lines, ["PROVINCIA"])
    cp = _extract_labeled_value(lines, ["C[ÓO]DIGO\\s+POSTAL", "CODIGO\\s+POSTAL", "\\bCP\\b"])
    cp_match = re.search(r"\b\d{5}\b", cp)
    codigo_postal = cp_match.group(0) if cp_match else ""

    numero = _extract_labeled_value(lines, ["N[ÚU]MERO", "NUMERO", "N\\.?\\s*[ºO]"])
    numero = re.sub(r"[^0-9A-Z]", "", numero.upper())
    piso = _extract_labeled_value(lines, ["PISO", "PLANTA"])
    puerta = _extract_labeled_value(lines, ["PUERTA", "PTA"])
    escalera = _extract_labeled_value(lines, ["ESCALERA", "ESC"])

    localidad_declaracion = _extract_labeled_value(lines, ["LOCALIDAD\\s+DE\\s+DECLARACI[ÓO]N", "EN\\s+.*A\\s+\\d{1,2}"])
    fecha_raw = _extract_labeled_value(lines, ["FECHA", "DECLARACI[ÓO]N"])
    fecha = ""
    if fecha_raw:
        digits = re.sub(r"[^0-9]", "", fecha_raw)
        if len(digits) == 8:
            d = digits[:2]
            m = digits[2:4]
            y = digits[4:]
            fecha = f"{d}/{m}/{y}"

    forma_pago = _extract_labeled_value(lines, ["FORMA\\s+DE\\s+PAGO"])
    forma_pago_l = forma_pago.lower()
    if "efectivo" in forma_pago_l:
        forma_pago = "efectivo"
    elif "adeudo" in forma_pago_l:
        forma_pago = "adeudo"
    else:
        forma_pago = ""

    iban = _extract_labeled_value(lines, ["IBAN", "C[ÓO]DIGO\\s+IBAN"])
    iban = re.sub(r"\s+", "", iban).upper()
    pasaporte = _extract_passport_candidate(lines, text)

    return {
        "nif_nie": _upper_compact(nif_nie),
        "apellidos": _clean_spaces(apellidos.title()) if apellidos else "",
        "nombre": _clean_spaces(nombre.title()) if nombre else "",
        "full_name": full_name,
        "fecha_nacimiento": dob,
        "nacionalidad": nat,
        "lugar_nacimiento": place_of_birth,
        "nombre_padre": father_name,
        "nombre_madre": mother_name,
        "domicilio": domicilio,
        "numero": numero,
        "escalera": escalera,
        "piso": piso,
        "puerta": _normalize_puerta(puerta),
        "municipio": municipio.title() if municipio else "",
        "provincia": provincia.title() if provincia else "",
        "codigo_postal": codigo_postal,
        "telefono": telefono,
        "email": email,
        "localidad_declaracion": localidad_declaracion.title() if localidad_declaracion else "",
        "fecha": fecha,
        "forma_pago": forma_pago,
        "iban": iban,
        "pasaporte": pasaporte,
    }


def _find_mrz_candidates(text: str) -> list[str]:
    out: list[str] = []
    for raw in text.splitlines():
        line = "".join(ch for ch in raw.upper() if ch.isalnum() or ch == "<")
        if len(line) >= 20 and line.count("<") >= 2:
            out.append(line)
    return out


def _extract_from_mrz_candidates(candidates: list[str]) -> tuple[str, str, str, str]:
    """
    Lightweight MRZ fallback for TD1-like blocks (3 lines ~30 chars).
    Extracts surname, name, birth date and nationality even if document number is non-standard.
    """
    for i in range(len(candidates) - 2):
        block = [candidates[i], candidates[i + 1], candidates[i + 2]]
        if not all(28 <= len(x) <= 32 for x in block):
            continue
        l1, l2, l3 = [x.ljust(30, "<")[:30] for x in block]

        nat = re.sub(r"[^A-Z]", "", l2[15:18])
        dob_raw = l2[0:6]
        dob = ""
        if re.fullmatch(r"\d{6}", dob_raw):
            yy = int(dob_raw[:2])
            year = 1900 + yy if yy > 30 else 2000 + yy
            dob = normalize_date(f"{year}{dob_raw[2:]}", allow_two_digit_year=False) or ""

        surname = ""
        name = ""
        if "<<" in l3:
            parts = l3.split("<<", 1)
            surname = re.sub(r"<+", " ", parts[0]).strip().title()
            name = re.sub(r"<+", " ", parts[1]).strip().title()
        else:
            tokens = [t for t in re.sub(r"<+", " ", l3).split() if t]
            if len(tokens) >= 2:
                surname = " ".join(tokens[:-1]).title()
                name = tokens[-1].title()

        if surname or name or dob or nat:
            return surname, name, dob, nat
    return "", "", "", ""


def _extract_doc_candidates(text: str) -> list[str]:
    out: list[str] = []
    for pattern in [r"\b[XYZ]\d{7}[A-Z]\b", r"\b\d{8}[A-Z]\b"]:
        for match in re.finditer(pattern, text.upper()):
            c = _upper_compact(match.group(0))
            if c not in out:
                out.append(c)
    return out


def _pick_valid_doc(candidates: list[str]) -> str:
    for c in candidates:
        if validate_spanish_document_number(c):
            return c
    return ""


def _extract_keyed_tokens(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        k, v = line.split(":", 1)
        key = k.strip().upper()
        val = _clean_spaces(v)
        if key and val:
            out[key] = val
    return out


def _extract_checkbox_token_value(text: str, key: str, allowed: set[str]) -> str:
    """
    Parse repeated OCR key/value lines like:
      DEX_SEXO: Off
      DEX_SEXO: M
    and return the best non-Off allowed value.
    """
    values: list[str] = []
    prefix = f"{key.upper()}:"
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        up = line.upper()
        if not up.startswith(prefix):
            continue
        val = _clean_spaces(line.split(":", 1)[1]).upper()
        if not val:
            continue
        values.append(val)

    for v in values:
        if v in allowed:
            return v
    return ""


def _compose_fecha_from_fir(tokens: dict[str, str]) -> str:
    day = tokens.get("FIR_DIA", "").strip()
    month_raw = tokens.get("FIR_MES", "").strip().lower()
    year = tokens.get("FIR_ANYO", "").strip()
    if not (day and month_raw and year):
        return ""
    month_num = MONTHS_ES.get(month_raw, "")
    if not month_num:
        return ""
    dd = re.sub(r"\D", "", day).zfill(2)
    yyyy = re.sub(r"\D", "", year)
    if len(yyyy) != 4:
        return ""
    return f"{dd}/{month_num}/{yyyy}"


def _extract_idesp(text: str) -> str:
    for p in [
        r"\bIDESP[:\s]*([A-Z]{3}\d{6})",
        r"\bIDESP[:\s]*([A-Z0-9]{5,20})\b",
        r"\bN[ºO]\s*SOPORTE[:\s]*([A-Z0-9]{5,20})\b",
    ]:
        m = re.search(p, text.upper())
        if m:
            return m.group(1)
    return ""


def _extract_names(text: str) -> tuple[str, str, list[str]]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    candidates: list[str] = []

    label_tokens = {
        "APELLIDOS",
        "COGNOMS",
        "NOMBRE",
        "NOMBRES",
        "NOM",
        "NAME",
        "SURNAMES",
        "SURNAME",
        "FORENAME",
        "FORENAMES",
    }

    def _is_label_line(raw: str) -> bool:
        up = raw.upper()
        letters_only = re.sub(r"[^A-ZÁÉÍÓÚÑÜ ]", " ", up)
        tokens = [t for t in letters_only.split() if t]
        if not tokens:
            return False
        if any(tok in label_tokens for tok in tokens):
            # line mostly labels/separators
            non_labels = [tok for tok in tokens if tok not in label_tokens]
            return len(non_labels) <= 1
        return False

    # Structured DNI/TIE block extraction by line positions.
    surname_lines: list[str] = []
    name_line = ""
    surname_start = -1
    for i, line in enumerate(lines):
        up = line.upper()
        if "APELLIDOS" in up or "COGNOMS" in up or "SURNAMES" in up:
            surname_start = i + 1
            break

    if surname_start != -1:
        i = surname_start
        while i < len(lines):
            up = lines[i].upper()
            if any(k in up for k in ["NOMBRE", "NOM", "FORENAME", "NAME"]):
                i += 1
                while i < len(lines):
                    if not _is_label_line(lines[i]):
                        candidate = re.sub(r"[^A-ZÁÉÍÓÚÑÜ' -]", "", lines[i].upper()).strip()
                        if candidate:
                            name_line = candidate.title()
                            break
                    i += 1
                break
            if not _is_label_line(lines[i]):
                candidate = re.sub(r"[^A-ZÁÉÍÓÚÑÜ' -]", "", lines[i].upper()).strip()
                if candidate and len(candidate) >= 2:
                    surname_lines.append(candidate.title())
            i += 1

        if surname_lines and name_line:
            surname = _clean_spaces(" ".join(surname_lines))
            name = _clean_spaces(name_line)
            candidates.extend([surname, name])
            return surname, name, candidates

    return "", "", candidates


def _extract_birth_nationality(text: str) -> tuple[str, str]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    upper_lines = [ln.upper() for ln in lines]
    dob = ""
    nat = ""

    for i, line in enumerate(upper_lines):
        if any(k in line for k in ["FECHA NAC", "BIRTH DATE", "DOB"]):
            window = " ".join(upper_lines[i : i + 3])
            m = re.search(r"(\d{2}[/-]\d{2}[/-]\d{4}|\d{2}\s+\d{2}\s+\d{4}|\d{8})", window)
            if m:
                dob = normalize_date(re.sub(r"[^0-9]", "", m.group(1))) or ""
        if "NACIONALIDAD" in line or "NATIONALITY" in line:
            for j in range(i, min(i + 4, len(upper_lines))):
                for token in re.findall(r"\b[A-Z]{3}\b", upper_lines[j]):
                    if token not in {"NAT", "NAC", "SEX", "DOB"}:
                        nat = token
                        break
                if nat:
                    break

    if not dob:
        all_dates = []
        for raw in re.findall(r"\b\d{2}[/-]\d{2}[/-]\d{4}\b|\b\d{2}\s+\d{2}\s+\d{4}\b|\b\d{8}\b", text):
            nd = normalize_date(re.sub(r"[^0-9]", "", raw))
            if nd:
                all_dates.append(nd)
        if all_dates:
            all_dates.sort()
            dob = all_dates[0]
    return dob, nat


def _extract_from_form_pdf_tokens(text: str) -> dict[str, str]:
    tokens = _extract_keyed_tokens(text)
    if not any(k.startswith("DEX_") for k in tokens):
        return {}
    sexo = _extract_checkbox_token_value(text, "DEX_SEXO", {"H", "M", "X"})
    estado_civil = _extract_checkbox_token_value(text, "DEX_EC", {"S", "C", "V", "D", "SP", "UH"})
    return {
        "nie_or_nif": tokens.get("DEX_NIE_2", "") or tokens.get("DR_DNI", ""),
        "apellidos": tokens.get("DEX_APE1", ""),
        "nombre": tokens.get("DEX_NOMBRE", ""),
        "fecha_nacimiento": _to_spanish_date(
            f"{tokens.get('DEX_DIA_NAC','')}{tokens.get('DEX_MES_NAC','')}{tokens.get('DEX_ANYO_NAC','')}"
        ),
        "nacionalidad": tokens.get("DEX_NACION", ""),
        "lugar_nacimiento": tokens.get("DEX_LN", ""),
        "nombre_padre": tokens.get("DEX_NP", ""),
        "nombre_madre": tokens.get("DEX_NM", ""),
        "domicilio": tokens.get("DEX_DOMIC", ""),
        "numero": tokens.get("DEX_NUM", ""),
        "piso": tokens.get("DEX_PISO", ""),
        "municipio": tokens.get("DEX_LOCAL", ""),
        "provincia": tokens.get("DEX_PROV", ""),
        "codigo_postal": tokens.get("DEX_CP", ""),
        "telefono": tokens.get("DEX_TFNO", ""),
        "email": tokens.get("DEX_EMAIL", "") or tokens.get("DEX_MAIL", ""),
        "sexo": sexo,
        "estado_civil": estado_civil,
        "localidad_declaracion": tokens.get("FIR_PROV", ""),
        "fecha_declaracion": _compose_fecha_from_fir(tokens),
        "pasaporte": tokens.get("DEX_PASA", ""),
        "representante_apellidos": tokens.get("DR_APELLIDOS", ""),
        "representante_nombre": tokens.get("DR_NOMBRE", ""),
        "representante_dni": tokens.get("DR_DNI", ""),
        "representante_telefono": tokens.get("DR_TFNO", ""),
        "representante_email": tokens.get("DR_EMAIL", "") or tokens.get("DR_MAIL", ""),
    }


def _detect_form_kind(merged_upper: str, form_pdf: dict[str, str], tasa_code: str) -> str:
    if tasa_code and tasa_code.strip() and tasa_code.strip() != "790_012":
        return tasa_code.strip().lower()
    if form_pdf and (
        "MOVILIDAD INTERNACIONAL" in merged_upper
        or "MI-T" in merged_upper
        or "SOLICITUD DE AUTORIZACIÓN DE RESIDENCIA" in merged_upper
    ):
        return "mi_t"
    visual_markers = [
        "TASA",
        "MODELO",
        "IMPORTE",
        "FORMA DE PAGO",
        "APELLIDOS",
        "NIF/NIE",
    ]
    if sum(1 for marker in visual_markers if marker in merged_upper) >= 3 and not form_pdf:
        return "visual_generic"
    return "790_012"


def _address_candidate_lines(text: str) -> list[str]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    out: list[str] = []
    road_hint = re.compile(r"\b(C/|C\.|C\b|CALLE|AVDA|AVENIDA|PZ|PLAZA|PSO|PASEO|CTRA|CARRETERA|CMNO|TRV)\b", re.I)

    def _is_noise(line: str) -> bool:
        up = line.upper()
        clean = re.sub(r"[^A-Z]", "", up)
        if not clean:
            return True
        if clean in {"EUEU", "EUEUEU", "POBDE"}:
            return True
        if len(clean) <= 6 and len(set(clean)) <= 3:
            return True
        if _is_labelish_fragment(line):
            return True
        bad_address_lines = [
            "LUGAR DE NACIMIENTO",
            "CIUDAD DE NACIMIENTO",
            "NACIONALIDAD",
            "HIJO",
            "MADRE",
            "PADRE",
            "DATOS DEL DOCUMENTO",
            "PASAPORTE",
            "DOCUMENT NUMBER",
        ]
        if any(b in up for b in bad_address_lines):
            return True
        return False

    for idx, line in enumerate(lines):
        up = line.upper()
        if any(k in up for k in ["DOMICILIO", "DIRECCION", "DIRECCIÓN", "ADDRESS"]):
            continue
        if (road_hint.search(up) and re.search(r"\d", up)) or ("PBJ" in up and re.search(r"\d", up)):
            for off in range(0, 5):
                j = idx + off
                if j < len(lines) and not _is_noise(lines[j]):
                    out.append(lines[j])
    seen = set()
    uniq = []
    for line in out:
        if line not in seen:
            uniq.append(line)
            seen.add(line)
    return uniq


def _expand_abbrev(address: str) -> tuple[str, list[dict[str, str]]]:
    expanded = address
    used: list[dict[str, str]] = []
    for short, full in ADDRESS_ABBREVIATIONS.items():
        pattern = rf"\b{re.escape(short)}\b"
        if re.search(pattern, expanded, flags=re.I):
            expanded = re.sub(pattern, full, expanded, flags=re.I)
            used.append({"abbr": short, "expanded": full})
    return _clean_spaces(expanded), used


def _parse_address_parts(address: str, overrides: dict[str, Any]) -> dict[str, str]:
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

    up = address.upper()
    type_match = re.match(r"^\s*([A-ZÁÉÍÓÚÑÜ]+)\b", up)
    if type_match and not fields["tipo_via"]:
        token = type_match.group(1)
        if token in TIPO_VIA_CANONICAL:
            fields["tipo_via"] = token.title()

    if fields["tipo_via"] and not fields["nombre_via_publica"]:
        # Allow punctuation after tipo via (e.g. "CALLE. PORTUGAL")
        m = re.search(re.escape(fields["tipo_via"].upper()) + r"[.\s]+([^,\d]+)", up)
        if m:
            fields["nombre_via_publica"] = _clean_spaces(m.group(1)).title()

    if not fields["numero"]:
        m = re.search(r"\b(\d{1,5}[A-Z]?)\b", up)
        if m:
            fields["numero"] = m.group(1)

    if not fields["codigo_postal"]:
        m = re.search(r"\b(\d{5})\b", up)
        if m:
            fields["codigo_postal"] = m.group(1)

    if not fields["municipio"] or not fields["provincia"]:
        m = re.search(r"\b([A-ZÁÉÍÓÚÑÜ]{3,})\s+([A-ZÁÉÍÓÚÑÜ]{3,})\s*-\s*ESP\b", up)
        if m:
            if not fields["municipio"]:
                fields["municipio"] = m.group(1).title()
            if not fields["provincia"]:
                fields["provincia"] = m.group(2).title()

    if not fields["piso"]:
        m = re.search(r"\bPLANTA\s+([A-Z0-9]+)\b", up)
        if m:
            fields["piso"] = m.group(1)
    if not fields["puerta"]:
        m = re.search(r"\bPUERTA\s+([A-Z0-9]+)\b", up)
        if m:
            fields["puerta"] = m.group(1)

    # OCR DNI often carries "P01 0017" after street number.
    if not fields["piso"] or not fields["puerta"]:
        m = re.search(r"\bP\s*0?(\d{1,2})\s+(\d{2,5}[A-Z]?)\b", up)
        if m:
            if not fields["piso"]:
                fields["piso"] = m.group(1)
            if not fields["puerta"]:
                fields["puerta"] = m.group(2)

    fields["puerta"] = _normalize_puerta(fields["puerta"])

    return fields


def _extract_city_province_from_address_lines(address_lines: list[str]) -> tuple[str, str]:
    if not address_lines:
        return "", ""
    clean_lines: list[str] = []
    for raw in address_lines:
        up = raw.upper().strip()
        if not up:
            continue
        if "/" in up and any(k in up for k in ["LUGAR", "NACIMIENTO", "DOMICILIO", "ADDRESS"]):
            continue
        if _is_labelish_fragment(up):
            continue
        if up in {"CALLE", "AVENIDA", "PLAZA", "PASEO", "PASAJE", "CAMINO", "CARRETERA", "TRAVESIA", "DOMICILI"}:
            continue
        if re.search(r"\d", up) and len(clean_lines) == 0:
            # likely first line street
            continue
        # Keep only alphabetic tokens/spaces
        filtered = re.sub(r"[^A-ZÁÉÍÓÚÑÜ ]", " ", up)
        filtered = _clean_spaces(filtered).title()
        if filtered and len(filtered) >= 3:
            clean_lines.append(filtered)
    if not clean_lines:
        return "", ""
    if len(clean_lines) == 1:
        return clean_lines[0], ""
    return clean_lines[0], clean_lines[1]


def _extract_city_province_from_text(text: str) -> tuple[str, str]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for i, line in enumerate(lines):
        up = line.upper()
        m = re.search(r"\b([A-ZÁÉÍÓÚÑÜ]{3,})\s*-\s*ESP\b", up)
        if m:
            province = m.group(1).title()
            city = ""
            if i > 0:
                prev = re.sub(r"[^A-ZÁÉÍÓÚÑÜ ]", "", lines[i - 1].upper()).strip()
                if prev and len(prev) >= 3:
                    city = prev.title()
            return city, province
    return "", ""


def _token_set(v: str) -> set[str]:
    return {t for t in re.split(r"[^A-Z0-9]+", v.upper()) if t}


def _score_address_candidate(address: str, candidate: dict[str, Any]) -> int:
    target = _token_set(address)
    formatted = _token_set(_safe(candidate.get("formatted_address")))
    if not target or not formatted:
        return -1
    return len(target & formatted)


def _component(candidate: dict[str, Any], kind: str) -> str:
    for comp in candidate.get("address_components") or []:
        if kind in (comp.get("types") or []):
            return _safe(comp.get("long_name"))
    return ""


def _apply_geocode(
    address_freeform: str,
    geocode_candidates: list[dict[str, Any]] | None,
    parts: dict[str, str],
) -> tuple[str, str, list[dict[str, str]], dict[str, str]]:
    if not address_freeform or not geocode_candidates:
        return "", "", [], parts

    best = max(geocode_candidates, key=lambda c: _score_address_candidate(address_freeform, c))
    normalized = _safe(best.get("formatted_address"))
    place_id = _safe(best.get("place_id"))
    discrepancies: list[dict[str, str]] = []

    mapping = {
        "codigo_postal": _component(best, "postal_code"),
        "municipio": _component(best, "locality"),
        "provincia": _component(best, "administrative_area_level_2"),
        "nombre_via_publica": _component(best, "route"),
        "numero": _component(best, "street_number"),
    }
    for field, geo_val in mapping.items():
        cur = _safe(parts.get(field))
        if cur and geo_val and cur.upper() != geo_val.upper():
            discrepancies.append({"field": field, "ocr_or_override": cur, "geocode": geo_val})
        if not cur and geo_val:
            parts[field] = geo_val
    return normalized, place_id, discrepancies, parts


def _build_validation(fields: dict[str, Any], required_fields: list[str]) -> dict[str, Any]:
    missing = [f for f in required_fields if not _safe(fields.get(f))]
    needs = sorted(set(missing))

    forma_pago = _safe(fields.get("forma_pago")).lower()
    if "forma_pago" in required_fields and forma_pago in {"adeudo", "adeudo en cuenta"} and not fields.get("iban"):
        missing.append("iban")
        needs.append("iban")
    if "autoliquidacion_tipo" in fields and fields.get("autoliquidacion_tipo", "").lower() == "complementaria":
        if not fields.get("num_justificante"):
            missing.append("num_justificante")
            needs.append("num_justificante")
        if fields.get("importe_complementaria") is None:
            missing.append("importe_complementaria")
            needs.append("importe_complementaria")

    return {
        "ok": len(set(missing)) == 0,
        "errors": [] if len(set(missing)) == 0 else ["Missing required fields for tasa form completion."],
        "warnings": [],
        "missing_required_for_form": sorted(set(missing)),
        "needs_user_input": sorted(set(needs)),
    }


def build_tasa_document(
    *,
    ocr_front: str,
    ocr_back: str,
    user_overrides: dict[str, Any] | None = None,
    geocode_candidates: list[dict[str, Any]] | None = None,
    google_maps_api_key: str | None = None,
    tasa_code: str = "790_012",
    source_file: str = "",
) -> dict[str, Any]:
    overrides = user_overrides or {}
    merged = "\n".join(x for x in [ocr_front, ocr_back] if x).strip()
    merged_upper = merged.upper()
    form_pdf = _extract_from_form_pdf_tokens(merged)
    visual_fields = _extract_visual_fields(merged)
    mrz_candidates = _find_mrz_candidates(merged)
    mrz = parse_mrz_lines(mrz_candidates)
    mrz_surname, mrz_name, mrz_dob, mrz_nat = _extract_from_mrz_candidates(mrz_candidates)
    doc_candidates = _extract_doc_candidates(merged)
    if form_pdf.get("nie_or_nif"):
        doc_candidates = [form_pdf["nie_or_nif"], *doc_candidates]
    nie_or_nif = _safe(overrides.get("nie_or_nif")) or _pick_valid_doc(doc_candidates)
    if not nie_or_nif and mrz:
        nie_or_nif = mrz.document_number
    if not nie_or_nif and visual_fields.get("nif_nie"):
        nie_or_nif = visual_fields["nif_nie"]
    passport_number = _upper_compact(
        _safe(overrides.get("pasaporte")) or _safe(form_pdf.get("pasaporte")) or _safe(visual_fields.get("pasaporte"))
    )
    if not nie_or_nif and passport_number:
        nie_or_nif = passport_number
    nie_or_nif = _upper_compact(nie_or_nif)
    if nie_or_nif and not validate_spanish_document_number(nie_or_nif):
        # For non NIE/NIF flows (e.g. passport-based forms), keep the value as generic identity document.
        if not passport_number or _upper_compact(nie_or_nif) != passport_number:
            nie_or_nif = ""

    apellidos, nombre, name_candidates = _extract_names(merged)
    if mrz:
        apellidos = apellidos or mrz.surname
        nombre = nombre or mrz.name

    fecha_nacimiento, nacionalidad = _extract_birth_nationality(merged)
    if mrz:
        # MRZ is usually the most reliable source for canonical identity fields.
        apellidos = mrz.surname or apellidos
        nombre = mrz.name or nombre
        fecha_nacimiento = mrz.date_of_birth or fecha_nacimiento
        nacionalidad = mrz.nationality or nacionalidad
    else:
        apellidos = mrz_surname or apellidos
        nombre = mrz_name or nombre
        fecha_nacimiento = mrz_dob or fecha_nacimiento
        nacionalidad = mrz_nat or nacionalidad

    apellidos = _clean_spaces(_safe(overrides.get("apellidos")) or apellidos)
    nombre = _clean_spaces(_safe(overrides.get("nombre")) or nombre)
    if (not apellidos or _looks_like_name_noise(apellidos)) and _safe(visual_fields.get("apellidos")):
        apellidos = _clean_spaces(_safe(visual_fields.get("apellidos")))
    if (not nombre or _looks_like_name_noise(nombre)) and _safe(visual_fields.get("nombre")):
        nombre = _clean_spaces(_safe(visual_fields.get("nombre")))
    if form_pdf:
        apellidos = _clean_spaces(_safe(overrides.get("apellidos")) or form_pdf.get("apellidos") or apellidos)
        nombre = _clean_spaces(_safe(overrides.get("nombre")) or form_pdf.get("nombre") or nombre)
        fecha_nacimiento = _safe(overrides.get("fecha_nacimiento")) or form_pdf.get("fecha_nacimiento") or fecha_nacimiento
        nacionalidad = _safe(overrides.get("nacionalidad")) or form_pdf.get("nacionalidad") or nacionalidad

    if not fecha_nacimiento:
        fecha_nacimiento = _safe(visual_fields.get("fecha_nacimiento"))
    fecha_nacimiento = _to_spanish_date(fecha_nacimiento)
    if not nacionalidad:
        nacionalidad = _safe(visual_fields.get("nacionalidad"))
    lugar_nacimiento = (
        _safe(overrides.get("lugar_nacimiento"))
        or _safe(form_pdf.get("lugar_nacimiento"))
        or _safe(visual_fields.get("lugar_nacimiento"))
    )
    nombre_padre = _safe(overrides.get("nombre_padre")) or _safe(form_pdf.get("nombre_padre")) or _safe(visual_fields.get("nombre_padre"))
    nombre_madre = _safe(overrides.get("nombre_madre")) or _safe(form_pdf.get("nombre_madre")) or _safe(visual_fields.get("nombre_madre"))

    visual_full_name = _clean_spaces(_safe(visual_fields.get("full_name")))
    if _looks_like_name_noise(visual_full_name) or _is_labelish_fragment(visual_full_name):
        visual_full_name = ""
    full_name = _clean_spaces(
        _safe(overrides.get("full_name")) or f"{apellidos} {nombre}".strip() or visual_full_name
    )

    address_lines = _address_candidate_lines(merged)
    if visual_fields.get("domicilio"):
        if visual_fields["domicilio"] not in address_lines:
            address_lines = [visual_fields["domicilio"], *address_lines]
    if form_pdf.get("domicilio"):
        synthetic = [form_pdf.get("domicilio", "")]
        if form_pdf.get("numero"):
            synthetic[0] = f"{synthetic[0]} {form_pdf['numero']}".strip()
        if form_pdf.get("piso"):
            synthetic[0] = f"{synthetic[0]} {form_pdf['piso']}".strip()
        for k in ["municipio", "provincia"]:
            if form_pdf.get(k):
                synthetic.append(form_pdf[k])
        address_lines = [x for x in synthetic if x]
    # Deduplicate while preserving order to avoid noisy repeated lines.
    dedup_lines = list(dict.fromkeys(address_lines))
    address_freeform = _safe(overrides.get("address_freeform")) or " ".join(dedup_lines).strip()
    address_freeform = _clean_address_freeform(address_freeform)
    address_expanded, abbr_used = _expand_abbrev(address_freeform)
    address_parts = _parse_address_parts(address_expanded, overrides)
    if form_pdf:
        for field, key in [
            ("numero", "numero"),
            ("piso", "piso"),
            ("municipio", "municipio"),
            ("provincia", "provincia"),
            ("codigo_postal", "codigo_postal"),
        ]:
            if not address_parts.get(field) and form_pdf.get(key):
                address_parts[field] = _safe(form_pdf[key])
    if visual_fields:
        for field, key in [
            ("numero", "numero"),
            ("escalera", "escalera"),
            ("piso", "piso"),
            ("puerta", "puerta"),
            ("municipio", "municipio"),
            ("provincia", "provincia"),
            ("codigo_postal", "codigo_postal"),
        ]:
            if not address_parts.get(field) and visual_fields.get(key):
                address_parts[field] = _safe(visual_fields[key])

    effective_geocode = geocode_candidates
    if not effective_geocode and google_maps_api_key and address_expanded:
        effective_geocode = fetch_geocode_candidates(address_expanded, google_maps_api_key, region="es")

    city_from_text, province_from_text = _extract_city_province_from_text(merged)
    if not address_parts.get("municipio") and city_from_text:
        address_parts["municipio"] = city_from_text
    if not address_parts.get("provincia") and province_from_text:
        address_parts["provincia"] = province_from_text

    # Fallback from OCR address lines if not resolved by previous heuristics.
    city_line, province_line = _extract_city_province_from_address_lines(address_lines)
    if not address_parts.get("municipio") and city_line:
        address_parts["municipio"] = city_line
    if not address_parts.get("provincia") and province_line:
        address_parts["provincia"] = province_line

    if (
        (not effective_geocode)
        and google_maps_api_key
        and address_parts.get("nombre_via_publica")
        and address_parts.get("numero")
        and address_parts.get("municipio")
    ):
        query2 = (
            f"{address_parts.get('tipo_via', '')} {address_parts['nombre_via_publica']} {address_parts['numero']}, "
            f"{address_parts['municipio']}, {address_parts.get('provincia', '')}, España"
        )
        effective_geocode = fetch_geocode_candidates(query2, google_maps_api_key, region="es")

    normalized_address, place_id, discrepancies, address_parts = _apply_geocode(
        address_expanded, effective_geocode, address_parts
    )

    fields_790 = {
        "nif_nie": _safe(overrides.get("nif_nie")) or nie_or_nif,
        "apellidos_nombre_razon_social": _safe(overrides.get("apellidos_nombre_razon_social")) or full_name,
        "tipo_via": _safe(address_parts.get("tipo_via")),
        "nombre_via_publica": _safe(address_parts.get("nombre_via_publica")),
        "numero": _safe(address_parts.get("numero")),
        "escalera": _safe(address_parts.get("escalera")),
        "piso": _safe(address_parts.get("piso")),
        "puerta": _safe(address_parts.get("puerta")),
        "telefono": _safe(overrides.get("telefono")) or _safe(form_pdf.get("telefono")),
        "email": _normalize_email(overrides.get("email"))
        or _normalize_email(form_pdf.get("email"))
        or _normalize_email(visual_fields.get("email")),
        "fecha_nacimiento": _to_spanish_date(_safe(overrides.get("fecha_nacimiento"))) or fecha_nacimiento,
        "nacionalidad": _safe(overrides.get("nacionalidad")) or nacionalidad,
        "lugar_nacimiento": lugar_nacimiento,
        "nombre_padre": nombre_padre,
        "nombre_madre": nombre_madre,
        "municipio": _safe(address_parts.get("municipio")),
        "provincia": _safe(address_parts.get("provincia")),
        "codigo_postal": _safe(address_parts.get("codigo_postal")),
        "autoliquidacion_tipo": _safe(overrides.get("autoliquidacion_tipo")),
        "num_justificante": _safe(overrides.get("num_justificante")),
        "importe_complementaria": overrides.get("importe_complementaria"),
        "localidad_declaracion": _safe(overrides.get("localidad_declaracion"))
        or _safe(form_pdf.get("localidad_declaracion"))
        or _safe(visual_fields.get("localidad_declaracion")),
        "fecha": _safe(overrides.get("fecha")) or _safe(form_pdf.get("fecha_declaracion")) or _safe(visual_fields.get("fecha")),
        "importe_euros": overrides.get("importe_euros"),
        "forma_pago": _safe(overrides.get("forma_pago")) or _safe(visual_fields.get("forma_pago")),
        "iban": _safe(overrides.get("iban")) or _safe(visual_fields.get("iban")),
    }
    if not fields_790["telefono"] and visual_fields.get("telefono"):
        fields_790["telefono"] = _safe(visual_fields.get("telefono"))

    fields_mi_t = {
        "nif_nie": _safe(overrides.get("nif_nie")) or nie_or_nif,
        "pasaporte": _safe(overrides.get("pasaporte"))
        or _safe(form_pdf.get("pasaporte"))
        or _safe(visual_fields.get("pasaporte")),
        "apellidos": _safe(overrides.get("apellidos")) or apellidos,
        "nombre": _safe(overrides.get("nombre")) or nombre,
        "full_name": full_name,
        "fecha_nacimiento": _to_spanish_date(_safe(overrides.get("fecha_nacimiento"))) or fecha_nacimiento,
        "nacionalidad": _safe(overrides.get("nacionalidad")) or nacionalidad,
        "sexo": _safe(overrides.get("sexo")) or _safe(form_pdf.get("sexo")) or _safe(visual_fields.get("sexo")),
        "estado_civil": _safe(overrides.get("estado_civil"))
        or _safe(form_pdf.get("estado_civil"))
        or _safe(visual_fields.get("estado_civil")),
        "lugar_nacimiento": lugar_nacimiento,
        "nombre_padre": nombre_padre,
        "nombre_madre": nombre_madre,
        "telefono": _safe(overrides.get("telefono")) or _safe(form_pdf.get("telefono")),
        "email": _normalize_email(overrides.get("email"))
        or _normalize_email(form_pdf.get("email"))
        or _normalize_email(visual_fields.get("email")),
        "tipo_via": _safe(address_parts.get("tipo_via")),
        "nombre_via_publica": _safe(address_parts.get("nombre_via_publica")),
        "numero": _safe(address_parts.get("numero")),
        "escalera": _safe(address_parts.get("escalera")),
        "piso": _safe(address_parts.get("piso")),
        "puerta": _safe(address_parts.get("puerta")),
        "municipio": _safe(address_parts.get("municipio")),
        "provincia": _safe(address_parts.get("provincia")),
        "codigo_postal": _safe(address_parts.get("codigo_postal")),
        "localidad_declaracion": _safe(overrides.get("localidad_declaracion"))
        or _safe(form_pdf.get("localidad_declaracion")),
        "fecha": _safe(overrides.get("fecha")) or _safe(form_pdf.get("fecha_declaracion")),
        "representante_apellidos": _safe(overrides.get("representante_apellidos"))
        or _safe(form_pdf.get("representante_apellidos")),
        "representante_nombre": _safe(overrides.get("representante_nombre")) or _safe(form_pdf.get("representante_nombre")),
        "representante_dni": _safe(overrides.get("representante_dni")) or _safe(form_pdf.get("representante_dni")),
        "representante_telefono": _safe(overrides.get("representante_telefono"))
        or _safe(form_pdf.get("representante_telefono")),
        "representante_email": _normalize_email(overrides.get("representante_email"))
        or _normalize_email(form_pdf.get("representante_email")),
    }

    form_kind = _detect_form_kind(merged_upper, form_pdf, tasa_code)
    if form_kind == "790_012":
        required_for_form = REQUIRED_FIELDS_790_012
        base_fields = fields_790
    elif form_kind == "mi_t":
        required_for_form = REQUIRED_FIELDS_MI_T
        base_fields = fields_mi_t
    else:
        required_for_form = REQUIRED_FIELDS_VISUAL_GENERIC
        base_fields = {
            "nif_nie": _safe(overrides.get("nif_nie")) or _safe(visual_fields.get("nif_nie")) or nie_or_nif,
            "pasaporte": _safe(overrides.get("pasaporte")) or _safe(visual_fields.get("pasaporte")),
            "apellidos_nombre_razon_social": _safe(overrides.get("apellidos_nombre_razon_social")) or full_name,
            "tipo_via": _safe(address_parts.get("tipo_via")),
            "nombre_via_publica": _safe(address_parts.get("nombre_via_publica")),
            "numero": _safe(address_parts.get("numero")),
            "escalera": _safe(address_parts.get("escalera")),
            "piso": _safe(address_parts.get("piso")),
            "puerta": _safe(address_parts.get("puerta")),
            "telefono": _safe(overrides.get("telefono")) or _safe(visual_fields.get("telefono")),
            "email": _normalize_email(overrides.get("email")) or _normalize_email(visual_fields.get("email")),
            "fecha_nacimiento": _to_spanish_date(_safe(overrides.get("fecha_nacimiento"))) or fecha_nacimiento,
            "nacionalidad": _safe(overrides.get("nacionalidad")) or nacionalidad,
            "lugar_nacimiento": lugar_nacimiento,
            "nombre_padre": nombre_padre,
            "nombre_madre": nombre_madre,
            "municipio": _safe(address_parts.get("municipio")),
            "provincia": _safe(address_parts.get("provincia")),
            "codigo_postal": _safe(address_parts.get("codigo_postal")),
            "localidad_declaracion": _safe(overrides.get("localidad_declaracion"))
            or _safe(visual_fields.get("localidad_declaracion")),
            "fecha": _safe(overrides.get("fecha")) or _safe(visual_fields.get("fecha")),
            "forma_pago": _safe(overrides.get("forma_pago")) or _safe(visual_fields.get("forma_pago")),
            "iban": _safe(overrides.get("iban")) or _safe(visual_fields.get("iban")),
        }
    validation = _build_validation(base_fields, required_for_form)
    if not base_fields["nif_nie"]:
        validation["ok"] = False
        validation["errors"].append("NIE/NIF not confidently extracted.")
        validation["missing_required_for_form"] = sorted(
            set(validation["missing_required_for_form"] + ["nif_nie"])
        )
        validation["needs_user_input"] = sorted(set(validation["needs_user_input"] + ["nif_nie"]))

    if not address_expanded:
        validation["warnings"].append("Address not found in OCR or overrides.")
    if form_kind == "visual_generic":
        validation["warnings"].append("Visual OCR mode: low-confidence handwritten extraction.")

    card_extracted = {
        "nie_or_nif": nie_or_nif,
        "apellidos": apellidos,
        "nombre": nombre,
        "full_name": full_name,
        "fecha_nacimiento": _to_spanish_date(_safe(overrides.get("fecha_nacimiento"))) or fecha_nacimiento,
        "nacionalidad": _safe(overrides.get("nacionalidad")) or nacionalidad,
        "sexo": _safe(overrides.get("sexo")) or _safe(form_pdf.get("sexo")) or _safe(visual_fields.get("sexo")),
        "estado_civil": _safe(overrides.get("estado_civil"))
        or _safe(form_pdf.get("estado_civil"))
        or _safe(visual_fields.get("estado_civil")),
        "lugar_nacimiento": lugar_nacimiento,
        "nombre_padre": nombre_padre,
        "nombre_madre": nombre_madre,
        "id_esp_optional": _safe(overrides.get("id_esp_optional")) or _extract_idesp(merged_upper),
        "raw_candidates": {
            "nie_or_nif": doc_candidates,
            "full_name": [x for x in [full_name, *name_candidates] if x],
            "address_lines": address_lines,
        },
    }

    form_790_012 = {
        "fields": fields_790,
        "derived": {
            "address_freeform": address_freeform,
            "normalized_address": normalized_address,
            "place_id": place_id,
            "address_abbreviations_used": abbr_used,
        },
    }

    form_mi_t = {
        "fields": fields_mi_t,
        "derived": {
            "address_freeform": address_freeform,
            "normalized_address": normalized_address,
            "place_id": place_id,
            "address_abbreviations_used": abbr_used,
        },
    }

    form_visual_generic = {
        "fields": base_fields if form_kind == "visual_generic" else {},
        "derived": {
            "address_freeform": address_freeform,
            "normalized_address": normalized_address,
            "place_id": place_id,
            "address_abbreviations_used": abbr_used,
            "confidence": "low",
        },
    }

    forms: dict[str, Any] = {"790_012": form_790_012}
    if form_kind != "790_012":
        forms[form_kind] = form_mi_t if form_kind == "mi_t" else form_visual_generic

    return {
        "_id": {"$oid": uuid.uuid4().hex[:24]},
        "schema_version": "1.2.0",
        "document_type": "nie_tie_tasa_payload",
        "tasa_code": tasa_code,
        "source": {
            "source_file": source_file,
            "ocr_front": ocr_front,
            "ocr_back": ocr_back,
            "user_overrides": overrides,
        },
        "card_extracted": card_extracted,
        "forms": forms,
        "form_790_012": form_790_012,
        "form_mi_t": form_mi_t if form_kind != "790_012" else {},
        "form_visual_generic": form_visual_generic if form_kind == "visual_generic" else {},
        "validation": validation,
        "discrepancies": discrepancies,
        "reference": {
            "spanish_address_abbreviations": ADDRESS_ABBREVIATIONS,
        },
        "meta": {
            "created_at": {"$date": _now_iso()},
            "updated_at": {"$date": _now_iso()},
        },
    }


def to_json(document: dict[str, Any]) -> str:
    return json.dumps(document, ensure_ascii=False)
