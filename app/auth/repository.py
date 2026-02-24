"""Repository for auth users and refresh token persistence."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from app.auth.models import AuthUser, RefreshTokenRecord

try:
    from pymongo import MongoClient
except Exception:  # pragma: no cover
    MongoClient = None  # type: ignore[assignment]


class AuthRepository:
    """Auth repository with MongoDB primary and file-store fallback."""

    def __init__(self, app_root: Path) -> None:
        """Initialize repository storage backends."""
        self._fallback_dir = app_root / "runtime" / "auth_store"
        self._fallback_dir.mkdir(parents=True, exist_ok=True)
        self._users_file = self._fallback_dir / "users.json"
        self._refresh_file = self._fallback_dir / "refresh_tokens.json"

        self._mongo_users = None
        self._mongo_refresh = None

        mongo_uri = os.getenv("MONGODB_URI", "").strip()
        mongo_db = os.getenv("MONGODB_DB", "ocr_mrz").strip() or "ocr_mrz"

        if mongo_uri and MongoClient is not None:
            try:
                client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
                client.admin.command("ping")
                db = client[mongo_db]
                self._mongo_users = db["auth_users"]
                self._mongo_refresh = db["auth_refresh_tokens"]
                self._mongo_users.create_index("email", unique=True)
                self._mongo_refresh.create_index("jti", unique=True)
            except Exception:
                self._mongo_users = None
                self._mongo_refresh = None

    def _read_json_file(self, path: Path) -> list[dict[str, Any]]:
        """Read list payload from JSON file with empty fallback."""
        if not path.exists():
            return []
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []
        return payload if isinstance(payload, list) else []

    def _write_json_file(self, path: Path, items: list[dict[str, Any]]) -> None:
        """Persist list payload to JSON file."""
        path.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")

    def get_user_by_email(self, email: str) -> AuthUser | None:
        """Get user by email from storage."""
        key = email.strip().lower()
        if self._mongo_users is not None:
            doc = self._mongo_users.find_one({"email": key}, {"_id": 0})
            return AuthUser.model_validate(doc) if doc else None

        for row in self._read_json_file(self._users_file):
            if str(row.get("email", "")).strip().lower() == key:
                return AuthUser.model_validate(row)
        return None

    def upsert_user(self, user: AuthUser) -> None:
        """Create or update auth user."""
        doc = user.model_dump()
        if self._mongo_users is not None:
            self._mongo_users.update_one({"email": user.email}, {"$set": doc}, upsert=True)
            return

        items = self._read_json_file(self._users_file)
        next_items = [row for row in items if str(row.get("email", "")).strip().lower() != user.email.lower()]
        next_items.append(doc)
        self._write_json_file(self._users_file, next_items)

    def save_refresh_token(self, record: RefreshTokenRecord) -> None:
        """Save refresh token record for rotation/revocation."""
        doc = record.model_dump()
        if self._mongo_refresh is not None:
            self._mongo_refresh.update_one({"jti": record.jti}, {"$set": doc}, upsert=True)
            return

        items = self._read_json_file(self._refresh_file)
        next_items = [row for row in items if str(row.get("jti", "")) != record.jti]
        next_items.append(doc)
        self._write_json_file(self._refresh_file, next_items)

    def get_refresh_token(self, jti: str) -> RefreshTokenRecord | None:
        """Get refresh token record by jti."""
        if self._mongo_refresh is not None:
            doc = self._mongo_refresh.find_one({"jti": jti}, {"_id": 0})
            return RefreshTokenRecord.model_validate(doc) if doc else None

        for row in self._read_json_file(self._refresh_file):
            if str(row.get("jti", "")) == jti:
                return RefreshTokenRecord.model_validate(row)
        return None

    def revoke_refresh_token(self, jti: str) -> None:
        """Mark refresh token record as revoked."""
        if self._mongo_refresh is not None:
            self._mongo_refresh.update_one({"jti": jti}, {"$set": {"revoked": True}})
            return

        items = self._read_json_file(self._refresh_file)
        for row in items:
            if str(row.get("jti", "")) == jti:
                row["revoked"] = True
        self._write_json_file(self._refresh_file, items)
