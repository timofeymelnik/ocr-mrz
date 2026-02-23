from __future__ import annotations

import json
from pathlib import Path

from form_mapping_repository import FormMappingRepository


def test_single_latest_template_replaces_previous(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("MONGODB_URI", raising=False)
    repo = FormMappingRepository(tmp_path)
    url = "https://example.com/forms/abc"

    first = repo.save_template(
        target_url=url,
        fields=[{"selector": "#a"}],
        mappings=[{"selector": "#a", "canonical_key": "nombre", "field_kind": "text"}],
        source="learned",
    )
    second = repo.save_template(
        target_url=url,
        fields=[{"selector": "#b"}],
        mappings=[{"selector": "#b", "canonical_key": "cp", "field_kind": "text"}],
        source="uploaded_file",
    )

    loaded = repo.get_latest_for_url(url)
    assert loaded is not None
    assert loaded["source"] == "uploaded_file"
    assert loaded["fields_count"] == 1
    assert loaded["mappings_count"] == 1
    assert loaded["mappings"][0]["selector"] == "#b"
    assert loaded["mappings"][0]["canonical_key"] == "cp"
    assert loaded["created_at"] == first["created_at"]
    assert loaded["updated_at"] == second["updated_at"]

    files = list((tmp_path / "runtime" / "form_mappings").glob("*.json"))
    assert len(files) == 1
    parsed = json.loads(files[0].read_text(encoding="utf-8"))
    assert parsed["mappings"][0]["selector"] == "#b"

