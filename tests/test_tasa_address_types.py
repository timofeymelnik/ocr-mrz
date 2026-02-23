from __future__ import annotations

import json
from pathlib import Path

from tasa_data_builder import _expand_abbrev, _parse_address_parts, _postal_tipo_via_aliases


def test_expand_abbrev_supports_urb() -> None:
    expanded, used = _expand_abbrev("URB Guadalmina 8A")
    assert expanded.startswith("Urbanizacion ")
    assert any(item.get("abbr") == "URB" for item in used)


def test_parse_address_parts_extracts_urbanizacion() -> None:
    parts = _parse_address_parts("Urbanizacion Pueblo de Guadalmina 8A", overrides={})
    assert parts["tipo_via"] == "Urbanizacion"
    assert parts["nombre_via_publica"] == "Pueblo De Guadalmina"
    assert parts["numero"] == "8A"


def test_parse_address_parts_uses_postal_dictionary_aliases(tmp_path: Path, monkeypatch) -> None:
    catalog = tmp_path / "postal_types.json"
    catalog.write_text(json.dumps({"aliases": {"RBLA": "RAMBLA"}}), encoding="utf-8")
    monkeypatch.setenv("POSTAL_STREET_TYPE_DICT_PATH", str(catalog))
    _postal_tipo_via_aliases.cache_clear()
    try:
        parts = _parse_address_parts("RBLA Catalunya 12", overrides={})
        assert parts["tipo_via"] == "Rambla"
        assert parts["nombre_via_publica"] == "Catalunya"
        assert parts["numero"] == "12"
    finally:
        _postal_tipo_via_aliases.cache_clear()

