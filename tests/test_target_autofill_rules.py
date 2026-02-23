from __future__ import annotations

from target_autofill import (
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
