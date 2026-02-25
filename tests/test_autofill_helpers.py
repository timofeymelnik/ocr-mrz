from __future__ import annotations

from pathlib import Path

import fitz

from app.autofill.form_filler import (
    _check_download_content,
    _download_filename,
    _extract_known_server_error,
    _is_blocked_page_html,
    _is_pdf_bytes,
    _slugify,
    _split_amount,
)
from app.autofill.target_autofill import (
    _canonical_from_placeholder,
    _compose_floor_door_token,
    _infer_spanish_province_from_cp,
    _infer_target_type,
    _normalize_ascii_upper,
    _normalize_door_token,
    _normalize_signal,
    _sanitize_floor_token,
    _split_address_details,
    _split_compact_floor_door,
    _split_date_parts,
    build_date_split_field_values,
    infer_pdf_checkbox_expected,
)


def test_target_autofill_basic_normalizers() -> None:
    assert _normalize_signal(" Sí ") == "s"
    assert _normalize_ascii_upper("áé x") == "AE X"
    assert _sanitize_floor_token(" C.P. ") == ""
    assert _normalize_door_token(" 21 ") == "21"


def test_target_autofill_address_splitters() -> None:
    floor, door = _split_compact_floor_door("5C", "")
    via, number, escalera, piso, puerta = _split_address_details(
        "Mayor Núm. 7 piso 2 puerta B"
    )

    assert floor == "5"
    assert door == "C"
    assert via == "Mayor"
    assert number == "7"
    assert piso == "2"
    assert puerta == "B"
    assert _compose_floor_door_token("2", "B") == "2 B"


def test_target_autofill_date_and_pdf_helpers() -> None:
    assert _split_date_parts("21/02/2026") == ("21", "02", "2026")

    doc = fitz.open()
    split = build_date_split_field_values(doc, {}, {"fecha_nacimiento": "01/03/1990"})
    assert split == {}

    assert infer_pdf_checkbox_expected("acepto", "", {"acepto": "SI"}) is None
    assert infer_pdf_checkbox_expected("acepto", "", {"acepto": "NO"}) is None


def test_target_autofill_target_and_placeholder_inference() -> None:
    assert _infer_target_type("https://example.com/form.pdf") == "pdf"
    assert _infer_target_type("https://example.com/form") == "html"
    assert _canonical_from_placeholder("{nombre}") == "nombre"
    assert _infer_spanish_province_from_cp("28001") == "MADRID"


def test_form_filler_helpers(tmp_path: Path) -> None:
    assert _slugify("a b/c") == "a_b_c"
    assert _split_amount("123.45") == ("123", "45")
    assert _is_pdf_bytes(b"%PDF-sample") is True
    assert _extract_known_server_error(b"Error 500 internal") == ""
    assert (
        _is_blocked_page_html("web esta bloqueada. contacte con el administrador")
        is True
    )

    payload = {"download": {"filename_prefix": "doc"}}
    assert _download_filename(payload, "default.pdf").endswith(".pdf")

    pdf_path = tmp_path / "ok.pdf"
    pdf_path.write_bytes(b"%PDF-1.7\nbody")
    valid, message = _check_download_content(pdf_path)
    assert valid is True
    assert message == ""
