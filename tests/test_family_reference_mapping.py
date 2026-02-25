from __future__ import annotations

from app.core.validators import normalize_payload_for_form
from app.data_builder.data_builder import build_tasa_document
from tests.mock_user import MOCK_USER


def test_normalize_payload_includes_family_reference_for_mi_f() -> None:
    ident = MOCK_USER["identificacion"]
    nie = ident["nif_nie"]
    relative_passport = "TEST7654321"
    relative_last_name = "RELATIVE"
    relative_first_name = "PERSON"
    ocr_text = "\n".join(
        [
            "MOVILIDAD INTERNACIONAL",
            "MI-F",
            f"DS_NIE_1: {nie[0]}",
            f"DS_NIE_2: {nie[1:8]}",
            f"DS_NIE_3: {nie[8]}",
            f"DS_APE1: {ident['primer_apellido']}",
            f"DS_NOMBRE: {ident['nombre']}",
            f"DFD_PASAP: {relative_passport}",
            f"DFD_APE1: {relative_last_name}",
            f"DFD_NOMBRE: {relative_first_name}",
        ]
    )
    document = build_tasa_document(
        ocr_front=ocr_text, ocr_back="", user_overrides={}, source_kind="fmiliar"
    )
    payload = normalize_payload_for_form(document)

    familiar = (payload.get("referencias") or {}).get("familiar_que_da_derecho") or {}
    assert familiar.get("pasaporte") == relative_passport
    assert (
        familiar.get("nombre_apellidos")
        == f"{relative_last_name} {relative_first_name}"
    )
