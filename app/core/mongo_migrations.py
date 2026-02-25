"""Versioned MongoDB schema migrations for runtime collections."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Callable

from app.core.logging import CORRELATION_ID_CTX

try:
    import pymongo
except Exception:  # pragma: no cover
    pymongo = None  # type: ignore[assignment]

MigrationFn = Callable[[Any], None]


def _migration_20260224_01_core_indexes(db: Any) -> None:
    db["crm_documents"].create_index("document_id", unique=True)
    db["crm_documents"].create_index("identifiers.document_number")
    db["crm_documents"].create_index("identifiers.nif_nie")
    db["crm_documents"].create_index("identifiers.passport")
    db["crm_documents"].create_index("updated_at")
    db["auth_users"].create_index("email", unique=True)
    db["auth_refresh_tokens"].create_index("jti", unique=True)


def _migration_20260224_02_refresh_token_ttl(db: Any) -> None:
    db["auth_refresh_tokens"].create_index(
        "expires_at_dt",
        expireAfterSeconds=0,
        name="idx_auth_refresh_tokens_expires_at_ttl",
    )


def apply_mongo_migrations() -> None:
    """Apply MongoDB migrations if MONGODB_URI is configured."""
    mongo_uri = os.getenv("MONGODB_URI", "").strip()
    mongo_db = os.getenv("MONGODB_DB", "ocr_mrz").strip() or "ocr_mrz"
    if not mongo_uri or pymongo is None:
        return

    migrations: list[tuple[str, MigrationFn]] = [
        ("20260224_01_core_indexes", _migration_20260224_01_core_indexes),
        ("20260224_02_refresh_token_ttl", _migration_20260224_02_refresh_token_ttl),
    ]

    client: Any = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
    try:
        try:
            client.admin.command("ping")
            db = client[mongo_db]
            migration_collection = db["schema_migrations"]
            migration_collection.create_index("migration_id", unique=True)

            for migration_id, migration_fn in migrations:
                if migration_collection.find_one({"migration_id": migration_id}):
                    continue
                migration_fn(db)
                migration_collection.insert_one(
                    {
                        "migration_id": migration_id,
                        "applied_at": datetime.now(timezone.utc),
                        "correlation_id": CORRELATION_ID_CTX.get(),
                    }
                )
        except Exception:
            return
    finally:
        client.close()
