from __future__ import annotations

from app.pipeline.runner import attach_pipeline_metadata
from tests.mock_user import MOCK_USER


def test_attach_pipeline_metadata_adds_handoff_and_crm() -> None:
    ident = MOCK_USER["identificacion"]
    dom = MOCK_USER["domicilio"]
    extra = MOCK_USER["extra"]
    doc = {
        "schema_version": "1.2.0",
        "tasa_code": "visual_generic",
        "card_extracted": {"full_name": ident["nombre_apellidos"], "nie_or_nif": ident["nif_nie"]},
        "forms": {
            "visual_generic": {
                "fields": {
                    "nif_nie": ident["nif_nie"],
                    "full_name": ident["nombre_apellidos"],
                    "telefono": dom["telefono"],
                    "email": extra["email"],
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
    assert out["crm_profile"]["identity"]["primary_number"] == ident["nif_nie"]
    assert any(task["task"] == "verify_filled_fields" for task in out["human_tasks"])
