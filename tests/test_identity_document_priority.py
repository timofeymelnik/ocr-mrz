from __future__ import annotations

from app.core.validators import normalize_payload_for_form
from app.data_builder.data_builder import build_tasa_document
from tests.mock_user import MOCK_USER


def test_build_tasa_document_prefers_mrz_identity_and_reconstructs_split_nie() -> None:
    ident = MOCK_USER["identificacion"]
    extra = MOCK_USER["extra"]
    nie = "X1234567Z"
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

    assert card.get("nie_or_nif") == nie
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


def test_build_tasa_document_handles_truncated_passport_mrz_and_spaced_passport_number() -> (
    None
):
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


def test_build_tasa_document_extracts_visa_mrz_identity() -> None:
    ocr_text = "\n".join(
        [
            "VISADO/VISA ESP",
            "N.I.E.Z3984420C",
            "VDESPFEDOTOV<<STEPAN<<<<<<<<<<<<<<<<",
            "0255851334RUS1308127M2612144<M<<1215",
        ]
    )
    document = build_tasa_document(
        ocr_front=ocr_text, ocr_back="", user_overrides={}, source_kind="visa"
    )
    card = document.get("card_extracted", {})
    fields_790 = document.get("form_790_012", {}).get("fields", {})

    assert card.get("apellidos") == "Fedotov"
    assert card.get("nombre") == "Stepan"
    assert card.get("pasaporte") == "025585133"
    assert card.get("nie_or_nif") == "Z3984420C"
    assert card.get("nacionalidad") == "RUS"
    assert card.get("sexo") == "H"
    assert card.get("fecha_nacimiento") == "12/08/2013"
    assert fields_790.get("numero") == ""
    assert fields_790.get("pasaporte") == "025585133"


def test_build_tasa_document_treats_fmiliar_as_form_source() -> None:
    ocr_text = "\n".join(
        [
            "SOLICITUD DE AUTORIZACION",
            "CALLE MAYOR 15 28013 MADRID MADRID - ESP",
        ]
    )
    document = build_tasa_document(
        ocr_front=ocr_text, ocr_back="", user_overrides={}, source_kind="fmiliar"
    )
    fields_790 = document.get("form_790_012", {}).get("fields", {})

    assert fields_790.get("tipo_via") == "Calle"
    assert fields_790.get("nombre_via_publica") == "Mayor"
    assert fields_790.get("numero") == "15"


def test_build_tasa_document_extracts_mi_f_ds_tokens() -> None:
    ident = MOCK_USER["identificacion"]
    home = MOCK_USER["domicilio"]
    extra = MOCK_USER["extra"]
    nie = "X0000000T"
    relative_passport = "TEST7654321"
    relative_last_name = "RELATIVE"
    relative_first_name = "PERSON"
    ocr_text = "\n".join(
        [
            "MOVILIDAD INTERNACIONAL",
            "MI-F",
            f"DS_PASAP: {ident['pasaporte']}",
            f"DS_NIE_1: {nie[0]}",
            f"DS_NIE_2: {nie[1:8]}",
            f"DS_NIE_3: {nie[8]}",
            f"DS_APE1: {ident['primer_apellido']}",
            f"DS_NOMBRE: {ident['nombre']}",
            f"DS_DIA_NAC: {extra['fecha_nacimiento_dia']}",
            f"DS_MES_NAC: {extra['fecha_nacimiento_mes']}",
            f"DS_ANYO_NAC: {extra['fecha_nacimiento_anio']}",
            f"DS_NACION: {extra['nacionalidad']}",
            "DS_SEXO: H",
            f"DS_DOMIC: {home['nombre_via']}",
            f"DS_NUM: {home['numero']}",
            f"DS_PISO: {home['piso']}",
            f"DS_LOCAL: {home['municipio']}",
            f"DS_CP: {home['cp']}",
            f"DS_PROV: {home['provincia']}",
            f"DS_TFNO_FIJO: {home['telefono']}",
            f"DS_EMAIL: {extra['email']}",
            f"DFD_PASAP: {relative_passport}",
            f"DFD_APE1: {relative_last_name}",
            f"DFD_NOMBRE: {relative_first_name}",
        ]
    )
    document = build_tasa_document(
        ocr_front=ocr_text, ocr_back="", user_overrides={}, source_kind="fmiliar"
    )
    card = document.get("card_extracted", {})
    fields = document.get("form_mi_t", {}).get("fields", {})

    assert card.get("nie_or_nif") == nie
    assert card.get("pasaporte") == ident["pasaporte"]
    assert card.get("apellidos") == ident["primer_apellido"]
    assert card.get("nombre") == ident["nombre"]
    assert card.get("fecha_nacimiento") == extra["fecha_nacimiento"]
    familiar = card.get("familiar_que_da_derecho", {})
    assert familiar.get("pasaporte") == relative_passport
    assert familiar.get("apellidos") == relative_last_name
    assert familiar.get("nombre") == relative_first_name
    assert familiar.get("full_name") == f"{relative_last_name} {relative_first_name}"
    assert fields.get("codigo_postal") == home["cp"]
    assert fields.get("telefono") == home["telefono"]
    assert fields.get("familiar_pasaporte") == relative_passport
