from __future__ import annotations

from validators import collect_validation_errors


def _payload_without_tramite() -> dict:
    return {
        "identificacion": {
            "nif_nie": "X1234567Z",
            "nombre_apellidos": "TEST USER",
        },
        "domicilio": {
            "tipo_via": "CALLE",
            "nombre_via": "Mayor",
            "numero": "10",
            "escalera": "",
            "piso": "",
            "puerta": "",
            "telefono": "600000000",
            "municipio": "DemoCity",
            "provincia": "DemoCity",
            "cp": "03001",
        },
        "autoliquidacion": {"tipo": "principal"},
        "tramite": {},
        "declarante": {"localidad": "DemoCity", "fecha": "21/02/2026"},
        "ingreso": {"forma_pago": "efectivo", "iban": ""},
    }


def test_tramite_not_required_in_manual_mode() -> None:
    errors = collect_validation_errors(_payload_without_tramite(), require_tramite=False)
    assert all("tramite." not in err for err in errors)


def test_tramite_required_in_auto_mode() -> None:
    errors = collect_validation_errors(_payload_without_tramite(), require_tramite=True)
    assert any("tramite.grupo" in err for err in errors)
    assert any("tramite.opcion" in err for err in errors)

