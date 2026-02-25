from __future__ import annotations

import re

from app.autofill.placeholder_helpers import (
    canonical_from_placeholder,
    canonical_keys_from_placeholder_tokens,
    eval_checked_when,
    rule_context,
    select_canonical_for_composite_placeholder,
)
from app.autofill.target_helpers import (
    compose_floor_door_token,
    infer_spanish_province_from_cp,
    split_compact_floor_door,
    split_date_parts,
)


def test_placeholder_helpers_extract_and_select_canonical_keys() -> None:
    known, unknown = canonical_keys_from_placeholder_tokens(
        "{nombre} {foo} {nombre} {tipo_via} {foo}",
        placeholder_token_re=re.compile(r"\{([a-z_]+)\}", re.I),
        canonical_field_keys={"nombre", "tipo_via"},
    )
    selected = select_canonical_for_composite_placeholder(
        ["tipo_via", "nombre_via"]
    )
    single = canonical_from_placeholder(
        "{nombre}",
        placeholder_re=re.compile(r"^\{([a-z_]+)\}$", re.I),
        canonical_field_keys={"nombre"},
    )

    assert known == ["nombre", "tipo_via"]
    assert unknown == ["foo"]
    assert selected == "domicilio_en_espana"
    assert single == "nombre"


def test_placeholder_helpers_rule_context_and_eval_checked_when() -> None:
    context = rule_context({"sexo": " M ", "empty": ""})
    valid = eval_checked_when('sexo == "M"', context)
    invalid = eval_checked_when("invalid expression", context)
    empty = eval_checked_when("", context)

    assert context["sexo"] == "M"
    assert valid is True
    assert invalid is None
    assert empty is None


def test_target_helpers_floor_door_date_and_cp_branches() -> None:
    piso, puerta = split_compact_floor_door("5A", "A")
    compact_piso, compact_puerta = split_compact_floor_door("7B", "")
    duplicate = compose_floor_door_token("2B", "B")
    split_slash = split_date_parts("1/2/26")
    split_iso = split_date_parts("2026-12-05")
    split_digits = split_date_parts("01022026")
    split_invalid = split_date_parts("bad")
    cp_unknown = infer_spanish_province_from_cp("99000")

    assert (piso, puerta) == ("5", "A")
    assert (compact_piso, compact_puerta) == ("7", "B")
    assert duplicate == "2B"
    assert split_slash == ("01", "02", "2026")
    assert split_iso == ("05", "12", "2026")
    assert split_digits == ("01", "02", "2026")
    assert split_invalid == ("", "", "")
    assert cp_unknown == ""
