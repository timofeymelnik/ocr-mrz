from __future__ import annotations

from validators import collect_validation_issues


def test_collect_validation_issues_has_machine_readable_shape() -> None:
    payload = {
        "identificacion": {"nif_nie": "", "nombre_apellidos": ""},
        "domicilio": {"tipo_via": "", "nombre_via": "", "numero": "", "municipio": "", "provincia": "", "cp": ""},
        "autoliquidacion": {"tipo": "principal"},
        "tramite": {},
        "declarante": {"localidad": "", "fecha": ""},
        "ingreso": {"forma_pago": "efectivo", "iban": ""},
    }
    issues = collect_validation_issues(payload, require_tramite=False)
    assert issues
    first = issues[0]
    assert {"code", "field", "message"} <= set(first.keys())

