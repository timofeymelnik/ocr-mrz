"""Security primitives for password hashing and token signing."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from typing import Any


def _b64url_encode(raw: bytes) -> str:
    """Return URL-safe base64 string without padding."""
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _b64url_decode(value: str) -> bytes:
    """Decode URL-safe base64 string with optional missing padding."""
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("utf-8"))


def hash_password(password: str) -> str:
    """Hash password using PBKDF2-HMAC-SHA256 with random salt."""
    salt = os.urandom(16)
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 120_000)
    return f"pbkdf2_sha256$120000${_b64url_encode(salt)}${_b64url_encode(derived)}"


def verify_password(password: str, stored_hash: str) -> bool:
    """Verify password against a stored PBKDF2 hash."""
    try:
        algo, rounds_raw, salt_b64, digest_b64 = stored_hash.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        rounds = int(rounds_raw)
        salt = _b64url_decode(salt_b64)
        expected = _b64url_decode(digest_b64)
    except Exception:
        return False

    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, rounds)
    return hmac.compare_digest(derived, expected)


def build_signed_token(payload: dict[str, Any], secret_key: str) -> str:
    """Create compact signed token using JWT-like 3-part structure."""
    header = {"alg": "HS256", "typ": "JWT"}
    header_part = _b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_part = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_part}.{payload_part}".encode("utf-8")
    signature = hmac.new(secret_key.encode("utf-8"), signing_input, hashlib.sha256).digest()
    signature_part = _b64url_encode(signature)
    return f"{header_part}.{payload_part}.{signature_part}"


def decode_signed_token(token: str, secret_key: str) -> dict[str, Any]:
    """Decode and verify compact signed token, raising ``ValueError`` on failure."""
    try:
        header_part, payload_part, signature_part = token.split(".", 2)
    except ValueError as exc:
        raise ValueError("Malformed token") from exc

    signing_input = f"{header_part}.{payload_part}".encode("utf-8")
    expected_sig = hmac.new(secret_key.encode("utf-8"), signing_input, hashlib.sha256).digest()
    got_sig = _b64url_decode(signature_part)
    if not hmac.compare_digest(expected_sig, got_sig):
        raise ValueError("Invalid token signature")

    payload_raw = _b64url_decode(payload_part)
    try:
        payload = json.loads(payload_raw.decode("utf-8"))
    except Exception as exc:
        raise ValueError("Invalid token payload") from exc

    exp = int(payload.get("exp") or 0)
    if exp and exp < int(time.time()):
        raise ValueError("Token expired")

    return payload
