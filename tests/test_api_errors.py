from __future__ import annotations

from app.api.errors import to_error_payload


def test_to_error_payload_preserves_structured_detail() -> None:
    payload = to_error_payload(
        {"error_code": "AUTH_TOKEN_INVALID", "message": "Invalid"},
        401,
    )

    assert payload == {"error_code": "AUTH_TOKEN_INVALID", "message": "Invalid"}


def test_to_error_payload_normalizes_plain_string() -> None:
    payload = to_error_payload("boom", 500)

    assert payload == {"error_code": "HTTP_500", "message": "boom"}
