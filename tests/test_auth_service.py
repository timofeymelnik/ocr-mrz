from __future__ import annotations

from dataclasses import dataclass

import pytest

from app.api.errors import ApiError
from app.auth.models import AuthUser, RefreshTokenRecord
from app.auth.service import AuthService
from app.core.config import AuthConfig


@dataclass
class _Repo:
    users: dict[str, AuthUser]
    refresh_tokens: dict[str, RefreshTokenRecord]

    def get_user_by_email(self, email: str) -> AuthUser | None:
        return self.users.get(email)

    def upsert_user(self, user: AuthUser) -> None:
        self.users[user.email] = user

    def save_refresh_token(self, record: RefreshTokenRecord) -> None:
        self.refresh_tokens[record.jti] = record

    def get_refresh_token(self, jti: str) -> RefreshTokenRecord | None:
        return self.refresh_tokens.get(jti)

    def revoke_refresh_token(self, jti: str) -> None:
        token = self.refresh_tokens.get(jti)
        if token is None:
            return
        token.revoked = True


def _build_service() -> AuthService:
    config = AuthConfig(
        enabled=True,
        secret_key="test-secret",
        access_token_ttl_seconds=300,
        refresh_token_ttl_seconds=1200,
        issuer="ocr-mrz-test",
        admin_email="admin@local",
        admin_password="admin123",
    )
    repo = _Repo(users={}, refresh_tokens={})
    service = AuthService(repo=repo, config=config)
    service.bootstrap_admin_user()
    return service


def test_auth_service_login_and_verify_access_token() -> None:
    service = _build_service()

    session = service.login("admin@local", "admin123")
    claims = service.verify_access_token(session.access_token)

    assert claims["email"] == "admin@local"
    assert session.token_type == "bearer"


def test_auth_service_login_invalid_credentials_raises_api_error() -> None:
    service = _build_service()

    with pytest.raises(ApiError) as exc:
        service.login("admin@local", "bad")

    assert exc.value.status_code == 401


def test_auth_service_refresh_rotates_token() -> None:
    service = _build_service()
    session = service.login("admin@local", "admin123")

    rotated = service.refresh(session.refresh_token)

    assert rotated.access_token
    assert rotated.refresh_token != session.refresh_token


def test_auth_service_logout_does_not_crash_on_invalid_token() -> None:
    service = _build_service()

    service.logout("bad-token")
