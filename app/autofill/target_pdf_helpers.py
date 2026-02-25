"""PDF mapping helpers extracted from target_autofill."""

from __future__ import annotations

from typing import Any, Callable

import fitz


def pdf_value_for_field(
    field_name: str,
    value_map: dict[str, str],
    *,
    norm_text: Callable[[str], str],
    strip_extra_spaces: Callable[[str], str],
) -> str:
    """Map PDF field name to best-effort value from canonical map."""
    n = norm_text(field_name)
    if not n:
        return ""
    if "nombreyapellidosdeltitular" in n:
        return strip_extra_spaces(
            " ".join(
                x
                for x in [
                    value_map.get("nombre", ""),
                    value_map.get("primer_apellido", ""),
                    value_map.get("segundo_apellido", ""),
                ]
                if x
            )
        )
    if "piso" in n and "puert" in n:
        return (
            value_map.get("piso_puerta", "")
            or value_map.get("piso", "")
            or value_map.get("puerta", "")
        )
    if "pasaporte" in n or "passport" in n:
        return value_map.get("pasaporte", "") or value_map.get("nif_nie", "")
    if any(x in n for x in ["nif", "nie", "document"]):
        return value_map.get("nif_nie", "")
    if "primerapellido" in n or "apellido1" in n:
        return value_map.get("primer_apellido", "")
    if "segundoapellido" in n or "apellido2" in n:
        return value_map.get("segundo_apellido", "")
    if n == "nombre":
        return value_map.get("nombre", "")
    if "email" in n or "correo" in n:
        return value_map.get("email", "")
    if any(x in n for x in ["telefono", "phone", "movil"]):
        return value_map.get("telefono", "")
    if any(x in n for x in ["apellidosynombre", "nombreyapellidos", "fullname"]):
        return value_map.get("nombre_apellidos", "")
    if "apellidos" in n or "surname" in n:
        return value_map.get("nombre_apellidos", "")
    if n == "nombre" or "forename" in n:
        return value_map.get("nombre_apellidos", "")
    if "codigopostal" in n or n == "cp":
        return value_map.get("cp", "")
    if "municipio" in n or "city" in n:
        return value_map.get("municipio", "")
    if "provincia" in n or "province" in n:
        return value_map.get("provincia", "")
    if "tipovia" in n:
        return value_map.get("tipo_via", "")
    if "domicilioenespana" in n or n == "domicilio":
        return value_map.get("domicilio_en_espana", "")
    if "nombrevia" in n or "direccion" in n or "calle" in n:
        return value_map.get("nombre_via", "")
    if n in {"numero", "num"} or "numero" in n:
        return value_map.get("numero", "")
    if "fecha" in n and "nacimiento" not in n:
        return value_map.get("fecha", "")
    if "fechanacimiento" in n or "birth" in n:
        return value_map.get("fecha_nacimiento", "")
    if "importe" in n:
        return value_map.get("importe_euros", "")
    if "iban" in n:
        return value_map.get("iban", "")
    if "nacionalidad" in n or "nationality" in n:
        return value_map.get("nacionalidad", "")
    if "estadocivil" in n:
        return value_map.get("estado_civil", "")
    if "lugar" in n and "nac" in n:
        return value_map.get("lugar_nacimiento", "")
    if n == "pais" or "country" in n:
        return value_map.get("pais_nacimiento", "")
    if "padre" in n:
        return value_map.get("nombre_padre", "")
    if "madre" in n:
        return value_map.get("nombre_madre", "")
    if "representante" in n and "dni" not in n and "nie" not in n and "pas" not in n:
        return value_map.get("representante_legal", "")
    if "dniniepas" in n or (
        "representante" in n and any(x in n for x in ["dni", "nie", "pas"])
    ):
        return value_map.get("representante_documento", "")
    if "titulo" in n:
        return value_map.get("titulo_representante", "")
    return ""


def build_nif_split_field_map(
    doc: fitz.Document, explicit_by_field: dict[str, str], value_map: dict[str, str]
) -> dict[str, str]:
    """Infer split NIF field mapping by widget widths and x-order."""
    prefix = value_map.get("nif_nie_prefix", "")
    number = value_map.get("nif_nie_number", "")
    suffix = value_map.get("nif_nie_suffix", "")
    if not (prefix and number and suffix):
        return {}

    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    for page in doc:
        for w in page.widgets() or []:
            field_name = str((w.field_name or "")).strip()
            if not field_name or field_name in seen:
                continue
            if explicit_by_field.get(field_name) != "nif_nie":
                continue
            rect = w.rect
            candidates.append(
                {
                    "name": field_name,
                    "x0": float(rect.x0),
                    "y0": float(rect.y0),
                    "width": float(rect.x1 - rect.x0),
                }
            )
            seen.add(field_name)

    if len(candidates) < 3:
        return {}

    wide = sorted(
        [c for c in candidates if c["width"] > 40.0], key=lambda c: (c["y0"], c["x0"])
    )
    narrow = sorted(
        [c for c in candidates if c["width"] <= 40.0], key=lambda c: (c["y0"], c["x0"])
    )
    if not wide or len(narrow) < 2:
        return {}

    middle = wide[0]
    same_row_narrow = [c for c in narrow if abs(c["y0"] - middle["y0"]) <= 25.0]
    if len(same_row_narrow) >= 2:
        same_row_narrow.sort(key=lambda c: c["x0"])
        left = same_row_narrow[0]
        right = same_row_narrow[-1]
    else:
        left, right = narrow[0], narrow[1]
        if left["x0"] > right["x0"]:
            left, right = right, left

    return {
        str(left["name"]): "nif_nie_prefix",
        str(middle["name"]): "nif_nie_number",
        str(right["name"]): "nif_nie_suffix",
    }


def infer_pdf_checkbox_expected(
    field_name: str, mapped_key: str, value_map: dict[str, str], *, norm_text: Callable[[str], str]
) -> bool | None:
    """Infer expected checkbox state from field naming conventions and mapped key."""
    n = norm_text(field_name)
    sexo = (value_map.get("sexo", "") or "").strip().upper()
    estado = (value_map.get("estado_civil", "") or "").strip().upper()
    hijos = (value_map.get("hijos_escolarizacion_espana", "") or "").strip().upper()
    name_upper = (field_name or "").strip().upper()
    key = (mapped_key or "").strip().lower()

    if key == "sexo":
        if name_upper == "M":
            return sexo == "M"
        if name_upper == "CHKBOX":
            return sexo in {"H", "X"}
        return (
            ("x" in n and sexo == "X")
            or ("h" in n and sexo == "H")
            or ("m" in n and sexo == "M")
        )
    if key == "estado_civil":
        if name_upper in {"C", "V", "D", "SP", "CHKBOX-0"}:
            target = "S" if name_upper == "CHKBOX-0" else name_upper
            return estado == target
        return (
            ("sp" in n and estado == "SP")
            or ("s" in n and estado == "S")
            or ("c" in n and estado == "C")
            or ("v" in n and estado == "V")
            or ("d" in n and estado == "D")
        )
    if key == "hijos_escolarizacion_espana":
        if name_upper == "NO":
            return hijos == "NO"
        if "HIJAS" in name_upper or "HIJOS" in name_upper:
            return hijos == "SI"
        return (("si" in n or n.endswith("s")) and hijos == "SI") or (
            "no" in n and hijos == "NO"
        )

    if name_upper == "M":
        return sexo == "M"
    if name_upper == "CHKBOX":
        return sexo in {"H", "X"}
    if name_upper in {"C", "V", "D", "SP", "CHKBOX-0"}:
        target = "S" if name_upper == "CHKBOX-0" else name_upper
        return estado == target
    if name_upper == "NO":
        return hijos == "NO"
    if "HIJAS" in name_upper or "HIJOS" in name_upper:
        return hijos == "SI"
    if "sexo" in n:
        return (
            ("x" in n and sexo == "X")
            or ("h" in n and sexo == "H")
            or ("m" in n and sexo == "M")
        )
    if "estadocivil" in n:
        return (
            ("sp" in n and estado == "SP")
            or ("s" in n and estado == "S")
            or ("c" in n and estado == "C")
            or ("v" in n and estado == "V")
            or ("d" in n and estado == "D")
        )
    if "hijos" in n or "escolarizacion" in n:
        return (("si" in n or n.endswith("s")) and hijos == "SI") or (
            "no" in n and hijos == "NO"
        )
    return None


def should_ignore_pdf_mapping(
    field_name: str, mapped_key: str, source: str, widget_type: str
) -> bool:
    """Compatibility hook for mapping filters. Currently disabled by design."""
    _ = field_name
    _ = mapped_key
    _ = source
    _ = widget_type
    return False
