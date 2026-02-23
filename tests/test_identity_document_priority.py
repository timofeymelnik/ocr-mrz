from __future__ import annotations

from tasa_data_builder import build_tasa_document
from validators import normalize_payload_for_form


def test_build_tasa_document_prefers_mrz_identity_and_reconstructs_split_nie() -> None:
    ocr_text = "\n".join(
        [
            "DEX_NIE1: X",
            "DEX_NIE_2: 1234567",
            "DEX_NIE_3: Z",
            "DEX_APE1: TESTER",
            "DEX_NOMBRE: EXAMPLE",
            "DEX_DIA_NAC: 01",
            "DEX_MES_NAC: 01",
            "DEX_ANYO_NAC: 1980",
            "DEX_NACION: ESP",
            "P<UTOEXAMPLE<<ALFA<BETA<<<<<<<<<<<<<<<<<<<<<<",
            "L898902C36UTO7408122M1204159ZE184226B<<<<<10",
        ]
    )

    document = build_tasa_document(ocr_front=ocr_text, ocr_back="", user_overrides={})
    card = document.get("card_extracted", {})

    assert card.get("nie_or_nif") == "X1234567Z"
    assert card.get("fecha_nacimiento") == "12/08/1974"
    assert card.get("apellidos") == "Example"
    assert card.get("nombre") == "Alfa Beta"


def test_normalize_payload_for_form_sets_document_type_defaults() -> None:
    passport_only = {
        "forms": {
            "790_012": {
                "fields": {
                    "nif_nie": "",
                    "pasaporte": "P1234567",
                    "apellidos_nombre_razon_social": "TEST USER",
                    "sexo": "F",
                }
            }
        }
    }
    normalized_passport = normalize_payload_for_form(passport_only)
    assert normalized_passport["identificacion"]["documento_tipo"] == "pasaporte"
    assert normalized_passport["identificacion"]["nif_nie"] == ""
    assert normalized_passport["identificacion"]["pasaporte"] == "P1234567"
    assert normalized_passport["extra"]["sexo"] == "M"

    nie_doc = {
        "forms": {
            "790_012": {
                "fields": {
                    "nif_nie": "X1234567Z",
                    "pasaporte": "P1234567",
                    "apellidos_nombre_razon_social": "TEST USER",
                }
            }
        }
    }
    normalized_nie = normalize_payload_for_form(nie_doc)
    assert normalized_nie["identificacion"]["documento_tipo"] == "nif_tie_nie_dni"


def test_build_tasa_document_handles_truncated_passport_mrz_and_spaced_passport_number() -> None:
    ocr_text = "\n".join(
        [
            "ПАСПОРТ /PASSPORT",
            "Номер паспорта/Passport No.",
            "12 3456789",
            "Дата рождения/Date of birth",
            "12.08.1974",
            "Место рождения/Place of birth",
            "X/F",
            "КРАСНОДАРСКИЙ КРАЙ / RUSSIA",
            "P<RUSTESTER<<ALFA<<<<<",
            "1234567892RUS7408122F2806228<<<<<<<<<<<<<<",
        ]
    )
    document = build_tasa_document(ocr_front=ocr_text, ocr_back="", user_overrides={})
    card = document.get("card_extracted", {})
    fields_790 = document.get("form_790_012", {}).get("fields", {})

    assert card.get("nie_or_nif") == ""
    assert card.get("pasaporte") == "123456789"
    assert card.get("fecha_nacimiento") == "12/08/1974"
    assert card.get("nacionalidad") == "RUS"
    assert card.get("sexo") == "M"
    assert card.get("lugar_nacimiento") == "KRASNODARSKII KRAI / RUSSIA"
    assert fields_790.get("numero") == ""
    assert fields_790.get("pasaporte") == "123456789"
