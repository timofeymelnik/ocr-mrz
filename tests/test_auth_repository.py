from __future__ import annotations

import json
from pathlib import Path

from app.auth.models import AuthUser, RefreshTokenRecord
from app.auth.repository import AuthRepository


def test_auth_repository_upsert_and_get_user_case_insensitive(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.delenv("MONGODB_URI", raising=False)
    repo = AuthRepository(tmp_path)
    user = AuthUser(
        user_id="u1",
        email="User@Test.Local",
        password_hash="hash",
        role="admin",
    )

    repo.upsert_user(user)
    found = repo.get_user_by_email("user@test.local")

    assert found is not None
    assert found.user_id == "u1"
    assert found.email == "User@Test.Local"


def test_auth_repository_save_get_and_revoke_refresh_token(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.delenv("MONGODB_URI", raising=False)
    repo = AuthRepository(tmp_path)
    token = RefreshTokenRecord(
        jti="j1",
        user_id="u1",
        token_hash="th",
        expires_at=2_000_000_000,
        revoked=False,
    )

    repo.save_refresh_token(token)
    saved = repo.get_refresh_token("j1")
    repo.revoke_refresh_token("j1")
    revoked = repo.get_refresh_token("j1")

    assert saved is not None
    assert saved.revoked is False
    assert revoked is not None
    assert revoked.revoked is True


def test_auth_repository_handles_corrupted_users_file(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.delenv("MONGODB_URI", raising=False)
    repo = AuthRepository(tmp_path)
    users_file = tmp_path / "runtime" / "auth_store" / "users.json"
    users_file.parent.mkdir(parents=True, exist_ok=True)
    users_file.write_text("{ invalid", encoding="utf-8")

    found = repo.get_user_by_email("broken@test.local")

    assert found is None


def test_auth_repository_replaces_existing_user_by_email(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.delenv("MONGODB_URI", raising=False)
    repo = AuthRepository(tmp_path)
    repo.upsert_user(
        AuthUser(user_id="u1", email="dupe@test.local", password_hash="h1", role="user")
    )
    repo.upsert_user(
        AuthUser(
            user_id="u2",
            email="DUPE@test.local",
            password_hash="h2",
            role="admin",
        )
    )

    users_file = tmp_path / "runtime" / "auth_store" / "users.json"
    rows = json.loads(users_file.read_text(encoding="utf-8"))
    found = repo.get_user_by_email("dupe@test.local")

    assert isinstance(rows, list)
    assert len(rows) == 1
    assert found is not None
    assert found.user_id == "u2"
