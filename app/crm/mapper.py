from __future__ import annotations

from typing import Any


def _safe(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _pick_form_fields(document: dict[str, Any]) -> dict[str, Any]:
    forms = document.get("forms") or {}
    tasa_code = _safe(document.get("tasa_code")).lower()

    if tasa_code and isinstance(forms.get(tasa_code), dict):
        fields = (forms.get(tasa_code) or {}).get("fields") or {}
        if isinstance(fields, dict):
            return fields

    for key in ["790_012", "mi_t", "visual_generic"]:
        fields = (
            (forms.get(key) or {}).get("fields")
            if isinstance(forms.get(key), dict)
            else {}
        )
        if isinstance(fields, dict) and fields:
            return fields

    fields = (document.get("form_790_012") or {}).get("fields") or {}
    if isinstance(fields, dict):
        return fields
    return {}


def build_crm_profile(document: dict[str, Any]) -> dict[str, Any]:
    card = document.get("card_extracted") or {}
    fields = _pick_form_fields(document)
    pipeline_payload = ((document.get("pipeline") or {}).get("artifacts") or {}).get(
        "form_payload_for_playwright"
    ) or {}
    extra = pipeline_payload.get("extra") if isinstance(pipeline_payload, dict) else {}
    if not isinstance(extra, dict):
        extra = {}

    first_name = _safe(fields.get("nombre")) or _safe(card.get("nombre"))
    last_name = _safe(fields.get("apellidos")) or _safe(card.get("apellidos"))
    full_name = (
        _safe(fields.get("full_name"))
        or _safe(fields.get("apellidos_nombre_razon_social"))
        or _safe(card.get("full_name"))
        or " ".join(x for x in [last_name, first_name] if x).strip()
    )

    identity_number = (
        _safe(fields.get("nif_nie"))
        or _safe(fields.get("pasaporte"))
        or _safe(card.get("nie_or_nif"))
    )

    return {
        "entity_type": "person",
        "full_name": full_name,
        "first_name": first_name,
        "last_name": last_name,
        "identity": {
            "primary_number": identity_number,
            "nie_or_nif": _safe(card.get("nie_or_nif")),
            "passport": _safe(fields.get("pasaporte")),
            "nationality": _safe(extra.get("nacionalidad"))
            or _safe(fields.get("nacionalidad"))
            or _safe(card.get("nacionalidad")),
            "date_of_birth": _safe(extra.get("fecha_nacimiento"))
            or _safe(fields.get("fecha_nacimiento"))
            or _safe(card.get("fecha_nacimiento")),
            "place_of_birth": _safe(extra.get("lugar_nacimiento"))
            or _safe(fields.get("lugar_nacimiento"))
            or _safe(card.get("lugar_nacimiento")),
            "father_name": _safe(extra.get("nombre_padre"))
            or _safe(fields.get("nombre_padre"))
            or _safe(card.get("nombre_padre")),
            "mother_name": _safe(extra.get("nombre_madre"))
            or _safe(fields.get("nombre_madre"))
            or _safe(card.get("nombre_madre")),
        },
        "contacts": {
            "phone": _safe(fields.get("telefono")),
            "email": _safe(extra.get("email")) or _safe(fields.get("email")),
        },
        "address": {
            "street_type": _safe(fields.get("tipo_via")),
            "street_name": _safe(fields.get("nombre_via_publica")),
            "street_number": _safe(fields.get("numero")),
            "staircase": _safe(fields.get("escalera")),
            "floor": _safe(fields.get("piso")),
            "door": _safe(fields.get("puerta")),
            "municipio": _safe(fields.get("municipio")),
            "provincia": _safe(fields.get("provincia")),
            "postal_code": _safe(fields.get("codigo_postal")),
        },
        "declaration": {
            "localidad": _safe(fields.get("localidad_declaracion")),
            "fecha": _safe(fields.get("fecha")),
        },
    }
