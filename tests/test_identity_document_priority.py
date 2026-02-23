from __future__ import annotations

from tasa_data_builder import build_tasa_document
from validators import normalize_payload_for_form
from tests.mock_user import MOCK_USER


def test_build_tasa_document_prefers_mrz_identity_and_reconstructs_split_nie() -> None:
    ident = MOCK_USER["identificacion"]
    extra = MOCK_USER["extra"]
    nie = ident["nif_nie"]
    ocr_text = "\n".join(
        [
            f"DEX_NIE1: {nie[0]}",
            f"DEX_NIE_2: {nie[1:8]}",
            f"DEX_NIE_3: {nie[8]}",
            f"DEX_APE1: {ident['primer_apellido']}",
            f"DEX_NOMBRE: {ident['nombre']}",
            "DEX_DIA_NAC: 01",
            "DEX_MES_NAC: 01",
            "DEX_ANYO_NAC: 1980",
            f"DEX_NACION: {extra['nacionalidad']}",
            "P<UTOEXAMPLE<<ALFA<TESTER<<<<<<<<<<<<<<<<<<<<",
            "L898902C36UTO7408122M1204159ZE184226B<<<<<10",
        ]
    )

    document = build_tasa_document(ocr_front=ocr_text, ocr_back="", user_overrides={})
    card = document.get("card_extracted", {})

    assert card.get("nie_or_nif") == ident["nif_nie"]
    assert card.get("fecha_nacimiento") == "12/08/1974"
    assert card.get("apellidos") == "Example"
    assert card.get("nombre") == "Alfa Tester"


def test_normalize_payload_for_form_sets_document_type_defaults() -> None:
    ident = MOCK_USER["identificacion"]
    extra = MOCK_USER["extra"]
    passport_only = {
        "forms": {
            "790_012": {
                "fields": {
                    "nif_nie": "",
                    "pasaporte": ident["pasaporte"],
                    "apellidos_nombre_razon_social": ident["nombre_apellidos"],
                    "sexo": extra["sexo"],
                }
            }
        }
    }
    normalized_passport = normalize_payload_for_form(passport_only)
    assert normalized_passport["identificacion"]["documento_tipo"] == "pasaporte"
    assert normalized_passport["identificacion"]["nif_nie"] == ""
    assert normalized_passport["identificacion"]["pasaporte"] == ident["pasaporte"]
    assert normalized_passport["extra"]["sexo"] == "M"

    nie_doc = {
        "forms": {
            "790_012": {
                "fields": {
                    "nif_nie": ident["nif_nie"],
                    "pasaporte": ident["pasaporte"],
                    "apellidos_nombre_razon_social": ident["nombre_apellidos"],
                }
            }
        }
    }
    normalized_nie = normalize_payload_for_form(nie_doc)
    assert normalized_nie["identificacion"]["documento_tipo"] == "nif_tie_nie_dni"


def test_build_tasa_document_handles_truncated_passport_mrz_and_spaced_passport_number() -> None:
    extra = MOCK_USER["extra"]
    ocr_text = "\n".join(
        [
            "ПАСПОРТ /PASSPORT",
            "Номер паспорта/Passport No.",
            "12 3456789",
            "Дата рождения/Date of birth",
            "12.08.1974",
            "Место рождения/Place of birth",
            "X/F",
            "DEMO REGION / DEMOLAND",
            "P<RUSTESTER<<ALFA<<<<<",
            "1234567892UTO7408122M2806228<<<<<<<<<<<<<<",
        ]
    )
    document = build_tasa_document(ocr_front=ocr_text, ocr_back="", user_overrides={})
    card = document.get("card_extracted", {})
    fields_790 = document.get("form_790_012", {}).get("fields", {})

    assert card.get("nie_or_nif") == ""
    assert card.get("pasaporte") == "123456789"
    assert card.get("fecha_nacimiento") == extra["fecha_nacimiento"]
    assert card.get("nacionalidad") == extra["nacionalidad"]
    assert card.get("sexo") == "M"
    assert card.get("lugar_nacimiento") == "DEMO REGION / DEMOLAND"
    assert fields_790.get("numero") == ""
    assert fields_790.get("pasaporte") == "123456789"
