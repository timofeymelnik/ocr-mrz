from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

_CONTROL_LETTERS = "TRWAGMYFPDXBNJZSQVHLCKE"


class ValidationError(ValueError):
    pass


def _normalize_sex_code(value: str) -> str:
    v = re.sub(r"[^A-Z]", "", (value or "").upper())
    if not v:
        return ""
    if v in {"H", "M", "X"}:
        return v
    if v in {"F", "FEMALE", "WOMAN", "MUJER"}:
        return "M"
    if v in {"MALE", "MAN", "HOMBRE"}:
        return "H"
    return ""


def validate_dni(document_number: str) -> bool:
    doc = re.sub(r"[^A-Z0-9]", "", (document_number or "").upper())
    if not re.fullmatch(r"\d{8}[A-Z]", doc):
        return False
    number = int(doc[:-1])
    return _CONTROL_LETTERS[number % 23] == doc[-1]


def validate_nie(document_number: str) -> bool:
    doc = re.sub(r"[^A-Z0-9]", "", (document_number or "").upper())
    if not re.fullmatch(r"[XYZ]\d{7}[A-Z]", doc):
        return False
    prefix = {"X": "0", "Y": "1", "Z": "2"}[doc[0]]
    number = int(prefix + doc[1:-1])
    return _CONTROL_LETTERS[number % 23] == doc[-1]


def validate_spanish_document_number(document_number: str) -> bool:
    return validate_dni(document_number) or validate_nie(document_number)


def normalize_date(date_str: str, allow_two_digit_year: bool = True) -> str | None:
    if not date_str:
        return None

    cleaned = re.sub(r"[^0-9]", "", date_str)
    formats = ["%Y%m%d", "%d%m%Y", "%Y%d%m"]

    if allow_two_digit_year and len(cleaned) == 6:
        yy = int(cleaned[:2])
        year = 1900 + yy if yy > 30 else 2000 + yy
        cleaned = f"{year}{cleaned[2:]}"

    if len(cleaned) != 8:
        return None

    for fmt in formats:
        try:
            dt = datetime.strptime(cleaned, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def normalize_mrz_date(yyMMdd: str, is_expiry: bool = False) -> str | None:
    if not re.fullmatch(r"\d{6}", yyMMdd):
        return None
    yy = int(yyMMdd[:2])
    if is_expiry:
        year = 2000 + yy if yy < 80 else 1900 + yy
    else:
        year = 1900 + yy if yy > 30 else 2000 + yy
    composed = f"{year}{yyMMdd[2:]}"
    return normalize_date(composed, allow_two_digit_year=False)


def to_spanish_date(date_str: str) -> str:
    v = (date_str or "").strip()
    if not v:
        return ""
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", v):
        return v
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
        return f"{v[8:10]}/{v[5:7]}/{v[0:4]}"
    iso = normalize_date(v)
    if iso and re.fullmatch(r"\d{4}-\d{2}-\d{2}", iso):
        return f"{iso[8:10]}/{iso[5:7]}/{iso[0:4]}"
    return ""


def _is_non_empty(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _today_ddmmyyyy() -> str:
    return datetime.now().strftime("%d/%m/%Y")


def _split_ddmmyyyy(value: str) -> tuple[str, str, str]:
    v = str(value or "").strip()
    m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", v)
    if not m:
        return "", "", ""
    return m.group(1), m.group(2), m.group(3)


def _compose_ddmmyyyy(day: str, month: str, year: str) -> str:
    d = re.sub(r"\D+", "", str(day or "")).zfill(2)[:2]
    m = re.sub(r"\D+", "", str(month or "")).zfill(2)[:2]
    y = re.sub(r"\D+", "", str(year or ""))[:4]
    if len(d) == 2 and len(m) == 2 and len(y) == 4:
        return f"{d}/{m}/{y}"
    return ""


def _apply_defaults(payload: dict[str, Any]) -> dict[str, Any]:
    payload.setdefault("identificacion", {})
    payload.setdefault("declarante", {})
    payload.setdefault("ingreso", {})
    payload.setdefault("extra", {})

    ident = payload["identificacion"] if isinstance(payload["identificacion"], dict) else {}
    if not isinstance(payload["identificacion"], dict):
        payload["identificacion"] = ident
    decl = payload["declarante"] if isinstance(payload["declarante"], dict) else {}
    extra = payload["extra"] if isinstance(payload["extra"], dict) else {}
    doc_type = str(ident.get("documento_tipo", "") or "").strip().lower()
    if doc_type not in {"pasaporte", "nif_tie_nie_dni"}:
        pasaporte = str(ident.get("pasaporte", "") or "").strip()
        nif_nie = str(ident.get("nif_nie", "") or "").strip()
        doc_type = "pasaporte" if pasaporte and (not nif_nie or nif_nie == pasaporte) else "nif_tie_nie_dni"
    ident["documento_tipo"] = doc_type

    composed_decl = _compose_ddmmyyyy(
        str(decl.get("fecha_dia", "") or ""),
        str(decl.get("fecha_mes", "") or ""),
        str(decl.get("fecha_anio", "") or ""),
    )
    if composed_decl:
        decl["fecha"] = composed_decl

    composed_birth = _compose_ddmmyyyy(
        str(extra.get("fecha_nacimiento_dia", "") or ""),
        str(extra.get("fecha_nacimiento_mes", "") or ""),
        str(extra.get("fecha_nacimiento_anio", "") or ""),
    )
    if composed_birth:
        extra["fecha_nacimiento"] = composed_birth

    if not str(decl.get("fecha", "") or "").strip():
        decl["fecha"] = _today_ddmmyyyy()

    d, m, y = _split_ddmmyyyy(str(decl.get("fecha", "") or ""))
    decl["fecha_dia"] = str(decl.get("fecha_dia", "") or d)
    decl["fecha_mes"] = str(decl.get("fecha_mes", "") or m)
    decl["fecha_anio"] = str(decl.get("fecha_anio", "") or y)

    d2, m2, y2 = _split_ddmmyyyy(str(extra.get("fecha_nacimiento", "") or ""))
    extra["fecha_nacimiento_dia"] = str(extra.get("fecha_nacimiento_dia", "") or d2)
    extra["fecha_nacimiento_mes"] = str(extra.get("fecha_nacimiento_mes", "") or m2)
    extra["fecha_nacimiento_anio"] = str(extra.get("fecha_nacimiento_anio", "") or y2)
    extra["sexo"] = _normalize_sex_code(str(extra.get("sexo", "") or ""))

    forma_pago = str(payload["ingreso"].get("forma_pago", "") or "").strip().lower()
    if forma_pago not in {"efectivo", "adeudo"}:
        payload["ingreso"]["forma_pago"] = "efectivo"

    return payload


def _split_identity_name(full_name: str, apellidos: str = "", nombre: str = "") -> tuple[str, str, str]:
    def _norm_token(v: str) -> str:
        return re.sub(r"[^a-z0-9]", "", (v or "").lower())

    a_raw = str(apellidos or "").strip()
    n_raw = str(nombre or "").strip()
    if a_raw or n_raw:
        a_tokens = [t for t in re.split(r"\s+", a_raw) if t]
        n_tokens = [t for t in re.split(r"\s+", n_raw) if t]
        if n_tokens and a_tokens:
            # OCR often duplicates the second surname at the beginning of "nombre":
            # apellidos="Garcia-Uceda", nombre="Uceda Raul" -> nombre="Raul".
            last_surname = a_tokens[-1]
            surname_tail = [t for t in re.split(r"[-']", last_surname) if t]
            candidates = [last_surname]
            if surname_tail:
                candidates.append(surname_tail[-1])
            if any(_norm_token(n_tokens[0]) == _norm_token(c) for c in candidates):
                n_tokens = n_tokens[1:]
        return (
            a_tokens[0] if a_tokens else "",
            " ".join(a_tokens[1:]) if len(a_tokens) > 1 else "",
            " ".join(n_tokens).strip(),
        )

    raw = str(full_name or "").strip()
    if not raw:
        return "", "", ""
    if "," in raw:
        left, right = [x.strip() for x in raw.split(",", 1)]
        l_tokens = [t for t in re.split(r"\s+", left) if t]
        return (
            l_tokens[0] if l_tokens else "",
            " ".join(l_tokens[1:]) if len(l_tokens) > 1 else "",
            right,
        )

    tokens = [t for t in re.split(r"\s+", raw) if t]
    if len(tokens) == 1:
        return tokens[0], "", ""
    if len(tokens) == 2:
        return tokens[0], "", tokens[1]
    return tokens[0], tokens[1], " ".join(tokens[2:])


def _require_path_or_json(raw: str) -> dict[str, Any]:
    possible_path = Path(raw)
    if possible_path.exists() and possible_path.is_file():
        if possible_path.suffix.lower() == ".jsonl":
            for line in possible_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    return json.loads(line)
            raise ValidationError(f"JSONL file is empty: {possible_path}")
        content = possible_path.read_text(encoding="utf-8")
        return json.loads(content)
    return json.loads(raw)


def load_input_payload(raw_json_or_path: str) -> dict[str, Any]:
    try:
        payload = _require_path_or_json(raw_json_or_path)
    except Exception as exc:
        raise ValidationError(f"Invalid JSON input: {exc}") from exc
    if isinstance(payload, list):
        if not payload:
            raise ValidationError("Input JSON array is empty.")
        payload = payload[0]
    if not isinstance(payload, dict):
        raise ValidationError("Input JSON must be an object.")
    return normalize_payload_for_form(payload)


def _pick_form_fields(payload: dict[str, Any]) -> dict[str, Any]:
    forms = payload.get("forms") if isinstance(payload.get("forms"), dict) else {}
    form_790 = payload.get("form_790_012") if isinstance(payload.get("form_790_012"), dict) else {}
    direct_790 = form_790.get("fields") if isinstance(form_790.get("fields"), dict) else {}

    tasa_code = str(payload.get("tasa_code", "") or "").strip().lower()

    def _has_identity(fields: dict[str, Any]) -> bool:
        return bool(str(fields.get("nif_nie", "") or "").strip() or str(fields.get("pasaporte", "") or "").strip())

    # 1) explicit tasa form
    if tasa_code and isinstance(forms.get(tasa_code), dict):
        fields = (forms.get(tasa_code) or {}).get("fields")
        if isinstance(fields, dict) and fields:
            return fields

    # 2) if legacy 790 has meaningful identity, prefer it
    if isinstance(direct_790, dict) and direct_790 and _has_identity(direct_790):
        return direct_790

    # 3) fallback by priority
    for key in ["790_012", "mi_t", "visual_generic"]:
        block = forms.get(key)
        fields = block.get("fields") if isinstance(block, dict) else {}
        if isinstance(fields, dict) and fields:
            return fields

    if isinstance(direct_790, dict):
        return direct_790
    return {}


def normalize_payload_for_form(payload: dict[str, Any]) -> dict[str, Any]:
    # Already in target shape
    if "identificacion" in payload and "domicilio" in payload:
        return _apply_defaults(payload)

    # OCR document shape -> form payload shape
    pipeline_payload = (((payload.get("pipeline") or {}).get("artifacts") or {}).get("form_payload_for_playwright"))
    if isinstance(pipeline_payload, dict) and "identificacion" in pipeline_payload and "domicilio" in pipeline_payload:
        return _apply_defaults(pipeline_payload)

    fields = _pick_form_fields(payload)
    if isinstance(fields, dict) and fields:
        municipio = str(fields.get("municipio", "") or "")
        localidad_declaracion = str(fields.get("localidad_declaracion", "") or "")
        card = payload.get("card_extracted") if isinstance(payload.get("card_extracted"), dict) else {}
        nombre_apellidos = (
            str(fields.get("apellidos_nombre_razon_social", "") or "")
            or str(fields.get("full_name", "") or "")
            or " ".join(
                x for x in [str(fields.get("apellidos", "") or "").strip(), str(fields.get("nombre", "") or "").strip()] if x
            )
        )
        primer_apellido, segundo_apellido, nombre = _split_identity_name(
            nombre_apellidos,
            str(fields.get("apellidos", "") or ""),
            str(fields.get("nombre", "") or ""),
        )
        doc_type = (
            str(fields.get("documento_tipo", "") or "").strip().lower()
            or str(card.get("documento_tipo", "") or "").strip().lower()
        )
        if doc_type not in {"pasaporte", "nif_tie_nie_dni"}:
            doc_type = "pasaporte" if str(fields.get("pasaporte", "") or "").strip() and not str(fields.get("nif_nie", "") or "").strip() else "nif_tie_nie_dni"
        nif_nie = str(fields.get("nif_nie", "") or "").strip()
        if not nif_nie and doc_type != "pasaporte":
            nif_nie = str(fields.get("pasaporte", "") or "").strip()
        normalized = {
            "identificacion": {
                "nif_nie": nif_nie,
                "pasaporte": str(fields.get("pasaporte", "") or ""),
                "documento_tipo": (
                    str(fields.get("documento_tipo", "") or "")
                    or str(card.get("documento_tipo", "") or "")
                ),
                "nombre_apellidos": nombre_apellidos,
                "primer_apellido": primer_apellido,
                "segundo_apellido": segundo_apellido,
                "nombre": nombre,
            },
            "domicilio": {
                "tipo_via": str(fields.get("tipo_via", "") or ""),
                "nombre_via": str(fields.get("nombre_via_publica", "") or ""),
                "numero": str(fields.get("numero", "") or ""),
                "escalera": str(fields.get("escalera", "") or ""),
                "piso": str(fields.get("piso", "") or ""),
                "puerta": str(fields.get("puerta", "") or ""),
                "telefono": str(fields.get("telefono", "") or ""),
                "municipio": str(fields.get("municipio", "") or ""),
                "provincia": str(fields.get("provincia", "") or ""),
                "cp": str(fields.get("codigo_postal", "") or ""),
            },
            "autoliquidacion": {
                "tipo": str(fields.get("autoliquidacion_tipo", "") or "principal").lower() or "principal",
                "num_justificante": str(fields.get("num_justificante", "") or ""),
                "importe_complementaria": fields.get("importe_complementaria"),
            },
            "tramite": payload.get("tramite") or {},
            "declarante": {
                "localidad": localidad_declaracion or municipio,
                "fecha": str(fields.get("fecha", "") or ""),
                "fecha_dia": str(fields.get("fecha_dia", "") or ""),
                "fecha_mes": str(fields.get("fecha_mes", "") or ""),
                "fecha_anio": str(fields.get("fecha_anio", "") or ""),
            },
            "ingreso": {
                "forma_pago": _map_forma_pago(str(fields.get("forma_pago", "") or "")),
                "iban": str(fields.get("iban", "") or ""),
            },
            "extra": {
                "email": str(fields.get("email", "") or ""),
                "fecha_nacimiento": to_spanish_date(
                    str(fields.get("fecha_nacimiento", "") or card.get("fecha_nacimiento", "") or "")
                ),
                "fecha_nacimiento_dia": str(fields.get("fecha_nacimiento_dia", "") or ""),
                "fecha_nacimiento_mes": str(fields.get("fecha_nacimiento_mes", "") or ""),
                "fecha_nacimiento_anio": str(fields.get("fecha_nacimiento_anio", "") or ""),
                "nacionalidad": str(fields.get("nacionalidad", "") or card.get("nacionalidad", "") or ""),
                "pais_nacimiento": str(fields.get("pais_nacimiento", "") or card.get("pais_nacimiento", "") or ""),
                "sexo": _normalize_sex_code(str(fields.get("sexo", "") or card.get("sexo", "") or "")),
                "estado_civil": str(fields.get("estado_civil", "") or card.get("estado_civil", "") or ""),
                "lugar_nacimiento": str(fields.get("lugar_nacimiento", "") or card.get("lugar_nacimiento", "") or ""),
                "nombre_padre": str(fields.get("nombre_padre", "") or card.get("nombre_padre", "") or ""),
                "nombre_madre": str(fields.get("nombre_madre", "") or card.get("nombre_madre", "") or ""),
                "representante_legal": str(fields.get("representante_legal", "") or ""),
                "representante_documento": str(fields.get("representante_documento", "") or ""),
                "titulo_representante": str(fields.get("titulo_representante", "") or ""),
                "hijos_escolarizacion_espana": str(fields.get("hijos_escolarizacion_espana", "") or ""),
            },
            "captcha": payload.get("captcha") or {"manual": True},
            "download": payload.get("download") or {"dir": "./downloads", "filename_prefix": "tasa790_012"},
        }
        return _apply_defaults(normalized)

    return _apply_defaults(payload)


def _map_forma_pago(value: str) -> str:
    v = value.strip().lower()
    if not v:
        return ""
    if "adeudo" in v:
        return "adeudo"
    if "efectivo" in v:
        return "efectivo"
    return v


def _validate_date_ddmmyyyy(value: str) -> bool:
    if not re.fullmatch(r"\d{2}/\d{2}/\d{4}", value):
        return False
    try:
        datetime.strptime(value, "%d/%m/%Y")
        return True
    except ValueError:
        return False


def _validate_iban(value: str) -> bool:
    iban = re.sub(r"\s+", "", value).upper()
    return bool(re.fullmatch(r"[A-Z]{2}\d{2}[A-Z0-9]{11,30}", iban))


def _pick(payload: dict[str, Any], *path: str) -> Any:
    node: Any = payload
    for key in path:
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    return node


def validate_payload(payload: dict[str, Any], *, require_tramite: bool = True) -> None:
    errors = collect_validation_errors(payload, require_tramite=require_tramite)
    if errors:
        raise ValidationError("\n".join(errors))


def collect_validation_issues(payload: dict[str, Any], *, require_tramite: bool = True) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []

    def add(code: str, field: str, message: str) -> None:
        issues.append({"code": code, "field": field, "message": message})

    required_paths = [
        ("identificacion", "nif_nie"),
        ("identificacion", "nombre_apellidos"),
        ("domicilio", "tipo_via"),
        ("domicilio", "nombre_via"),
        ("domicilio", "numero"),
        ("domicilio", "municipio"),
        ("domicilio", "provincia"),
        ("domicilio", "cp"),
        ("declarante", "localidad"),
        ("declarante", "fecha"),
        ("ingreso", "forma_pago"),
    ]
    if require_tramite:
        required_paths.extend([("tramite", "grupo"), ("tramite", "opcion")])
    for path in required_paths:
        value = _pick(payload, *path)
        if not _is_non_empty(value):
            field = ".".join(path)
            add("missing_required", field, f"Missing required field: {field}")

    nif_nie = str(_pick(payload, "identificacion", "nif_nie") or "").strip().upper()
    if nif_nie and not re.fullmatch(r"(?:[XYZ]\d{7}[A-Z]|\d{8}[A-Z]|[A-Z0-9\-]{5,20})", nif_nie):
        add("invalid_format", "identificacion.nif_nie", "identificacion.nif_nie has unexpected format.")

    cp = str(_pick(payload, "domicilio", "cp") or "").strip()
    if cp and not re.fullmatch(r"\d{5}", cp):
        add("invalid_format", "domicilio.cp", "domicilio.cp must have exactly 5 digits.")

    fecha = str(_pick(payload, "declarante", "fecha") or "").strip()
    if fecha and not _validate_date_ddmmyyyy(fecha):
        add("invalid_format", "declarante.fecha", "declarante.fecha must be in dd/mm/yyyy format.")

    forma_pago = str(_pick(payload, "ingreso", "forma_pago") or "").strip().lower()
    if forma_pago not in {"efectivo", "adeudo"}:
        add("invalid_value", "ingreso.forma_pago", "ingreso.forma_pago must be 'efectivo' or 'adeudo'.")

    if forma_pago == "adeudo":
        iban = str(_pick(payload, "ingreso", "iban") or "").strip()
        if not iban:
            add("missing_required", "ingreso.iban", "ingreso.iban is required when forma_pago='adeudo'.")
        elif not _validate_iban(iban):
            add("invalid_format", "ingreso.iban", "ingreso.iban format is invalid.")

    autoliquidacion = str(_pick(payload, "autoliquidacion", "tipo") or "principal").strip().lower()
    if autoliquidacion not in {"principal", "complementaria"}:
        add("invalid_value", "autoliquidacion.tipo", "autoliquidacion.tipo must be 'principal' or 'complementaria'.")
    if autoliquidacion == "complementaria":
        num_justificante = str(_pick(payload, "autoliquidacion", "num_justificante") or "").strip()
        importe = _pick(payload, "autoliquidacion", "importe_complementaria")
        if not num_justificante:
            add(
                "missing_required",
                "autoliquidacion.num_justificante",
                "autoliquidacion.num_justificante is required for complementaria.",
            )
        if importe in (None, ""):
            add(
                "missing_required",
                "autoliquidacion.importe_complementaria",
                "autoliquidacion.importe_complementaria is required for complementaria.",
            )

    tramite = _pick(payload, "tramite") or {}
    opcion_text = str(tramite.get("opcion", "")).lower()
    needs_count_keywords = [
        "incrementado por cada día",
        "certificados o informes",
        "por cada documento",
    ]
    if any(k in opcion_text for k in needs_count_keywords):
        if _pick(payload, "tramite", "cantidad") in (None, "") and _pick(payload, "tramite", "dias") in (None, ""):
            add(
                "missing_required",
                "tramite.cantidad",
                "tramite.cantidad or tramite.dias is required for selected trámite.",
            )

    return issues


def collect_validation_errors(payload: dict[str, Any], *, require_tramite: bool = True) -> list[str]:
    return [item["message"] for item in collect_validation_issues(payload, require_tramite=require_tramite)]
