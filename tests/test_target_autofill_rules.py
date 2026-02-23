from __future__ import annotations

from target_autofill import (
    build_autofill_value_map,
    _canonical_keys_from_placeholder_tokens,
    _eval_checked_when,
    _select_canonical_for_composite_placeholder,
)


def test_eval_checked_when_true_false_and_invalid() -> None:
    ctx = {"sexo": "M", "hijos_escolarizacion_espana": "SI"}
    assert _eval_checked_when('sexo == "M"', ctx) is True
    assert _eval_checked_when('sexo == "H"', ctx) is False
    assert _eval_checked_when("invalid expression", ctx) is None


def test_composite_placeholder_prefers_domicilio_key() -> None:
    known, unknown = _canonical_keys_from_placeholder_tokens("{domicilio_en_espana} {tipo_via} {nombre_via}")
    assert known == ["domicilio_en_espana", "tipo_via", "nombre_via"]
    assert unknown == []
    assert _select_canonical_for_composite_placeholder(known) == "domicilio_en_espana"


def test_build_value_map_extracts_house_token_from_nombre_via() -> None:
    payload = {
        "identificacion": {"nombre_apellidos": "EXAMPLE TESTER, ALFA", "nif_nie": "X1234567Z"},
        "domicilio": {
            "tipo_via": "Urbanizacion",
            "nombre_via": "Conjunto Demo casa 21",
            "numero": "8A",
            "escalera": "",
            "piso": "",
            "puerta": "",
            "municipio": "DemoCity",
            "provincia": "DemoProv",
            "cp": "12345",
        },
        "extra": {},
        "declarante": {},
    }
    values = build_autofill_value_map(payload)
    assert values["nombre_via"] == "Conjunto Demo"
    assert values["numero"] == "8A"
    assert values["puerta"] == "21"


def test_build_value_map_extracts_num_piso_puerta_when_embedded() -> None:
    payload = {
        "identificacion": {"nombre_apellidos": "EXAMPLE TESTER, ALFA", "nif_nie": "X1234567Z"},
        "domicilio": {
            "tipo_via": "Calle",
            "nombre_via": "Mayor Núm. 7 piso 2 puerta B",
            "numero": "",
            "escalera": "",
            "piso": "",
            "puerta": "",
            "municipio": "DemoTown",
            "provincia": "DemoProv",
            "cp": "10001",
        },
        "extra": {},
        "declarante": {},
    }
    values = build_autofill_value_map(payload)
    assert values["nombre_via"] == "Mayor"
    assert values["numero"] == "7"
    assert values["piso"] == "2"
    assert values["puerta"] == "B"


def test_build_value_map_ignores_noisy_floor_cp_value() -> None:
    payload = {
        "identificacion": {"nombre_apellidos": "EXAMPLE TESTER, ALFA", "nif_nie": "X1234567Z"},
        "domicilio": {
            "tipo_via": "Urbanizacion",
            "nombre_via": "Conjunto Demo casa 21",
            "numero": "8A",
            "escalera": "",
            "piso": "C.P.",
            "puerta": "",
            "municipio": "DemoCity",
            "provincia": "DemoProv",
            "cp": "12345",
        },
        "extra": {},
        "declarante": {},
    }
    values = build_autofill_value_map(payload)
    assert values["piso"] == ""
    assert values["domicilio_en_espana"] == "Urbanizacion Conjunto Demo 8A 21"
