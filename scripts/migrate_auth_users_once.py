#!/usr/bin/env python3
"""One-shot auth users migration from JSON fallback storage to MongoDB."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from pydantic import BaseModel

try:
    import pymongo
except Exception as exc:  # pragma: no cover
    raise RuntimeError("pymongo is required to run this script") from exc


DEFAULT_RUNTIME_DIR = Path("runtime")
DEFAULT_USERS_FILE = DEFAULT_RUNTIME_DIR / "auth_store" / "users.json"
DEFAULT_DB_NAME = "ocr_mrz"
DEFAULT_COLLECTION = "auth_users"
MAX_PREVIEW_ITEMS = 10


class AuthUser(BaseModel):
    """Auth user persistence model for migration validation."""

    user_id: str
    email: str
    password_hash: str
    role: str = "admin"
    is_active: bool = True
    email_verified: bool = True
    email_verification_token: str = ""


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Check and migrate auth users from JSON to MongoDB."
    )
    parser.add_argument(
        "--users-file",
        type=Path,
        default=DEFAULT_USERS_FILE,
        help="Path to fallback users.json file.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Only print diff/check report and do not write into MongoDB.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print migration plan without writing into MongoDB.",
    )
    return parser.parse_args()


def _load_source_rows(users_file: Path) -> list[dict[str, Any]]:
    """Load raw rows from source JSON file."""
    if not users_file.exists():
        return []
    payload = json.loads(users_file.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"Expected list in {users_file}, got {type(payload).__name__}")
    rows: list[dict[str, Any]] = []
    for item in payload:
        if isinstance(item, dict):
            rows.append(item)
    return rows


def _normalize_source_users(rows: list[dict[str, Any]]) -> tuple[list[AuthUser], int]:
    """Validate users and normalize emails to lowercase."""
    users: list[AuthUser] = []
    invalid_count = 0
    for row in rows:
        try:
            user = AuthUser.model_validate(row)
        except Exception:
            invalid_count += 1
            continue
        users.append(user.model_copy(update={"email": user.email.strip().lower()}))
    return users, invalid_count


def _collect_source_stats(
    users: list[AuthUser],
) -> tuple[dict[str, AuthUser], list[str]]:
    """Build source user map and collect duplicated emails."""
    unique_users: dict[str, AuthUser] = {}
    duplicates: list[str] = []
    for user in users:
        if user.email in unique_users:
            duplicates.append(user.email)
        unique_users[user.email] = user
    return unique_users, sorted(set(duplicates))


def _mongo_target_collection() -> Any:
    """Create Mongo collection object from environment variables."""
    mongo_uri = os.getenv("MONGODB_URI", "").strip()
    mongo_db = os.getenv("MONGODB_DB", DEFAULT_DB_NAME).strip() or DEFAULT_DB_NAME
    if not mongo_uri:
        raise RuntimeError("MONGODB_URI is empty. Set env var before running script.")
    client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    db = client[mongo_db]
    return client, db[DEFAULT_COLLECTION]


def _target_email_set(collection: Any) -> set[str]:
    """Return normalized email set from target collection."""
    return {
        str(row.get("email", "")).strip().lower()
        for row in collection.find({}, {"_id": 0, "email": 1})
    }


def _print_check_report(
    users_file: Path,
    source_total_rows: int,
    invalid_count: int,
    source_map: dict[str, AuthUser],
    duplicate_emails: list[str],
    target_emails: set[str],
) -> None:
    """Print source/target consistency report."""
    source_emails = set(source_map.keys())
    missing_in_target = sorted(source_emails - target_emails)
    extra_in_target = sorted(target_emails - source_emails)

    print(f"Source file: {users_file}")
    print(f"Source rows total: {source_total_rows}")
    print(f"Source valid users: {len(source_map)}")
    print(f"Source invalid rows skipped: {invalid_count}")
    print(f"Source duplicate emails: {len(duplicate_emails)}")
    if duplicate_emails:
        preview = ", ".join(duplicate_emails[:MAX_PREVIEW_ITEMS])
        print(f"Duplicate preview: {preview}")
    print(f"Target users total: {len(target_emails)}")
    print(f"Missing in target: {len(missing_in_target)}")
    if missing_in_target:
        preview = ", ".join(missing_in_target[:MAX_PREVIEW_ITEMS])
        print(f"Missing preview: {preview}")
    print(f"Extra in target: {len(extra_in_target)}")
    if extra_in_target:
        preview = ", ".join(extra_in_target[:MAX_PREVIEW_ITEMS])
        print(f"Extra preview: {preview}")


def _migrate_users(
    source_map: dict[str, AuthUser],
    collection: Any,
    dry_run: bool,
) -> tuple[int, int]:
    """Upsert source users into target collection by email."""
    if not source_map:
        return 0, 0

    target_emails_before = _target_email_set(collection)
    missing_before = set(source_map.keys()) - target_emails_before

    if dry_run:
        return len(source_map), len(missing_before)

    for user in source_map.values():
        collection.update_one(
            {"email": user.email},
            {"$set": user.model_dump()},
            upsert=True,
        )
    return len(source_map), len(missing_before)


def main() -> int:
    """Execute check or migration flow."""
    args = _parse_args()

    source_rows = _load_source_rows(args.users_file)
    source_users, invalid_count = _normalize_source_users(source_rows)
    source_map, duplicate_emails = _collect_source_stats(source_users)

    mongo_client = None
    try:
        mongo_client, collection = _mongo_target_collection()
        target_emails = _target_email_set(collection)

        if args.check:
            _print_check_report(
                users_file=args.users_file,
                source_total_rows=len(source_rows),
                invalid_count=invalid_count,
                source_map=source_map,
                duplicate_emails=duplicate_emails,
                target_emails=target_emails,
            )
            return 0

        processed, inserted_candidates = _migrate_users(
            source_map=source_map,
            collection=collection,
            dry_run=args.dry_run,
        )
        print(f"Source rows total: {len(source_rows)}")
        print(f"Source valid users: {len(source_map)}")
        print(f"Source invalid rows skipped: {invalid_count}")
        print(f"Processed users: {processed}")
        print(f"Potentially inserted users: {inserted_candidates}")
        print(f"Mode: {'dry-run' if args.dry_run else 'write'}")
        print(f"Target users total now: {collection.count_documents({})}")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        if mongo_client is not None:
            mongo_client.close()


if __name__ == "__main__":
    raise SystemExit(main())
