from __future__ import annotations

from app.core.validators import collect_validation_issues
from tests.mock_user import mock_payload


def test_collect_validation_issues_has_machine_readable_shape() -> None:
    payload = mock_payload()
    payload["identificacion"]["nif_nie"] = ""
    payload["identificacion"]["nombre_apellidos"] = ""
    payload["domicilio"]["tipo_via"] = ""
    payload["domicilio"]["nombre_via"] = ""
    payload["domicilio"]["numero"] = ""
    payload["domicilio"]["municipio"] = ""
    payload["domicilio"]["provincia"] = ""
    payload["domicilio"]["cp"] = ""
    payload["declarante"]["localidad"] = ""
    payload["declarante"]["fecha"] = ""
    payload["tramite"] = {}
    issues = collect_validation_issues(payload, require_tramite=False)
    assert issues
    first = issues[0]
    assert {"code", "field", "message"} <= set(first.keys())
