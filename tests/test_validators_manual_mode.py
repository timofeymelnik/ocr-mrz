from __future__ import annotations

from app.core.validators import collect_validation_errors
from tests.mock_user import mock_payload


def _payload_without_tramite() -> dict:
    payload = mock_payload()
    payload["tramite"] = {}
    return payload


def test_tramite_not_required_in_manual_mode() -> None:
    errors = collect_validation_errors(_payload_without_tramite(), require_tramite=False)
    assert all("tramite." not in err for err in errors)


def test_tramite_required_in_auto_mode() -> None:
    errors = collect_validation_errors(_payload_without_tramite(), require_tramite=True)
    assert any("tramite.grupo" in err for err in errors)
    assert any("tramite.opcion" in err for err in errors)
