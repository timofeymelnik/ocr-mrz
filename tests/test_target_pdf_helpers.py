from __future__ import annotations

from dataclasses import dataclass

from app.autofill.target_helpers import norm_text, strip_extra_spaces
from app.autofill.target_pdf_helpers import (
    build_nif_split_field_map,
    infer_pdf_checkbox_expected,
    pdf_value_for_field,
    should_ignore_pdf_mapping,
)


@dataclass
class _Rect:
    x0: float
    y0: float
    x1: float
    y1: float


@dataclass
class _Widget:
    field_name: str
    rect: _Rect


class _Page:
    def __init__(self, widgets: list[_Widget]) -> None:
        self._widgets = widgets

    def widgets(self) -> list[_Widget]:
        return self._widgets


def test_target_pdf_helpers_pdf_value_for_field_variants() -> None:
    value_map = {
        "nombre": "ANA",
        "primer_apellido": "LOPEZ",
        "segundo_apellido": "DIAZ",
        "pasaporte": "P123",
        "nif_nie": "X123",
        "piso_puerta": "2 B",
    }

    full_name = pdf_value_for_field(
        "Nombre y apellidos del titular",
        value_map,
        norm_text=norm_text,
        strip_extra_spaces=strip_extra_spaces,
    )
    passport = pdf_value_for_field(
        "Passport number",
        value_map,
        norm_text=norm_text,
        strip_extra_spaces=strip_extra_spaces,
    )
    piso_puerta = pdf_value_for_field(
        "Piso y puerta",
        value_map,
        norm_text=norm_text,
        strip_extra_spaces=strip_extra_spaces,
    )

    assert full_name == "ANA LOPEZ DIAZ"
    assert passport == "P123"
    assert piso_puerta == "2 B"


def test_target_pdf_helpers_build_nif_split_field_map() -> None:
    doc = [
        _Page(
            [
                _Widget("left", _Rect(10, 10, 30, 20)),
                _Widget("middle", _Rect(40, 10, 100, 20)),
                _Widget("right", _Rect(110, 10, 130, 20)),
            ]
        )
    ]
    explicit = {"left": "nif_nie", "middle": "nif_nie", "right": "nif_nie"}
    values = {
        "nif_nie_prefix": "X",
        "nif_nie_number": "1234567",
        "nif_nie_suffix": "Z",
    }

    mapping = build_nif_split_field_map(doc, explicit, values)

    assert mapping == {
        "left": "nif_nie_prefix",
        "middle": "nif_nie_number",
        "right": "nif_nie_suffix",
    }


def test_target_pdf_helpers_infer_checkbox_expected() -> None:
    value_map = {"sexo": "M", "estado_civil": "SP", "hijos_escolarizacion_espana": "SI"}

    sexo = infer_pdf_checkbox_expected("M", "sexo", value_map, norm_text=norm_text)
    estado = infer_pdf_checkbox_expected(
        "CHKBOX-0", "estado_civil", value_map, norm_text=norm_text
    )
    hijos = infer_pdf_checkbox_expected(
        "HIJOS", "hijos_escolarizacion_espana", value_map, norm_text=norm_text
    )
    none_case = infer_pdf_checkbox_expected("random", "", value_map, norm_text=norm_text)

    assert sexo is True
    assert estado is False
    assert hijos is True
    assert none_case is None


def test_target_pdf_helpers_should_ignore_mapping_is_disabled() -> None:
    assert should_ignore_pdf_mapping("name", "key", "source", "checkbox") is False
