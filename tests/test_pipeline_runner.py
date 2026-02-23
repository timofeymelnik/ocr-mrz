from __future__ import annotations

from pipeline_runner import attach_pipeline_metadata


def test_attach_pipeline_metadata_adds_handoff_and_crm() -> None:
    doc = {
        "schema_version": "1.2.0",
        "tasa_code": "visual_generic",
        "card_extracted": {"full_name": "TEST USER", "nie_or_nif": ""},
        "forms": {
            "visual_generic": {
                "fields": {
                    "nif_nie": "P1234567",
                    "full_name": "TEST USER",
                    "telefono": "600000000",
                    "email": "mock.user@example.test",
                },
                "derived": {},
            }
        },
    }
    out = attach_pipeline_metadata(
        document=doc,
        source_files=["sample.pdf"],
        ocr_details={"front_text_len": 100, "back_text_len": 0, "used_cached_ocr": False},
        parse_stage={"name": "parse_extract_map", "status": "success"},
        crm_stage={"name": "crm_mapping", "status": "success"},
        ocr_stage={"name": "ocr", "status": "success"},
    )
    assert "pipeline" in out
    assert "crm_profile" in out
    assert out["crm_profile"]["identity"]["primary_number"] == "P1234567"
    assert any(task["task"] == "verify_filled_fields" for task in out["human_tasks"])
