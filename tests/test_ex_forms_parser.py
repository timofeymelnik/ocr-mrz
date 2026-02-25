from __future__ import annotations

from app.data_builder.data_builder import build_tasa_document
from app.data_builder.ex_forms_parser import (
    detect_ex_form_code,
    normalize_ex_form_code,
    parse_ex_form_fields,
)


def test_normalize_ex_form_code_handles_variants() -> None:
    assert normalize_ex_form_code("EX-17") == "ex_17"
    assert normalize_ex_form_code("ex_03") == "ex_03"
    assert normalize_ex_form_code("  ex 9 ") == "ex_09"
    assert normalize_ex_form_code("790_012") == ""


def test_detect_ex_form_code_prefers_explicit_tasa_code() -> None:
    text = "SOLICITUD DE TARJETA EX - 17"
    assert detect_ex_form_code(merged_text=text, tasa_code="ex-10") == "ex_10"
    assert detect_ex_form_code(merged_text=text, tasa_code="790_012") == "ex_17"


def test_parse_ex17_structured_tail_extracts_identity_and_address() -> None:
    text = "\n".join(
        [
            "Solicitud de Tarjeta de Identidad de Extranjero",
            "EX-17",
            "Espacios para sellos",
            "de registro",
            "FG372666",
            "Y",
            "9840934",
            "F",
            "Melnyk",
            "Tymofii",
            "1",
            "1",
            "1992",
            "Odesa",
            "Ucrania",
            "Ucraniano",
            "Vasyl",
            "Olena",
            "C. Pablo Picasso",
            "2",
            "PB",
            "Torrevieja",
            "03184",
            "Alicante",
            "613592532",
            "timofeymelnik@gmail.com",
        ]
    )
    fields, strategy = parse_ex_form_fields(
        ex_form_code="ex_17",
        merged_text=text,
        fallback_fields={},
        overrides={},
    )

    assert strategy == "ex17_structured_tail"
    assert fields.get("nif_nie") == "Y9840934F"
    assert fields.get("pasaporte") == "FG372666"
    assert fields.get("apellidos") == "Melnyk"
    assert fields.get("nombre") == "Tymofii"
    assert fields.get("fecha_nacimiento") == "01/01/1992"
    assert fields.get("tipo_via") == "Calle"
    assert fields.get("nombre_via_publica") == "Pablo Picasso"
    assert fields.get("numero") == "2"
    assert fields.get("codigo_postal") == "03184"
    assert fields.get("email") == "timofeymelnik@gmail.com"


def test_build_tasa_document_generates_ex_form_payload() -> None:
    ocr_text = "\n".join(
        [
            "Solicitud de Tarjeta de Identidad de Extranjero",
            "EX-17",
            "Espacios para sellos",
            "de registro",
            "FG372666",
            "Y",
            "9840934",
            "F",
            "Melnyk",
            "Tymofii",
            "1",
            "1",
            "1992",
            "Odesa",
            "Ucrania",
            "Ucraniano",
            "Vasyl",
            "Olena",
            "C. Pablo Picasso",
            "2",
            "PB",
            "Torrevieja",
            "03184",
            "Alicante",
            "613592532",
            "timofeymelnik@gmail.com",
        ]
    )
    document = build_tasa_document(ocr_front=ocr_text, ocr_back="", user_overrides={})

    assert document.get("tasa_code") == "ex_17"
    forms = document.get("forms") or {}
    ex_form = (forms.get("ex_17") or {}).get("fields") or {}
    assert ex_form.get("nif_nie") == "Y9840934F"
    assert ex_form.get("pasaporte") == "FG372666"
    assert ex_form.get("nombre") == "Tymofii"
    assert ex_form.get("apellidos") == "Melnyk"
