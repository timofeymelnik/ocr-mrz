#!/usr/bin/env python3
"""One-shot migration of runtime JSON fallback storage into MongoDB."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    import pymongo
except Exception as exc:  # pragma: no cover
    raise RuntimeError("pymongo is required to run this script") from exc


DEFAULT_RUNTIME_DIR = Path("runtime")
DEFAULT_DB_NAME = "ocr_mrz"
DEFAULT_CRM_COLLECTION = "crm_documents"
DEFAULT_MAPPING_COLLECTION = "form_mappings"
MAX_PREVIEW_ITEMS = 10


def _sanitize_for_mongo(value: Any) -> Any:
    """Sanitize nested payload to be safe for MongoDB document storage."""
    if isinstance(value, list):
        return [_sanitize_for_mongo(item) for item in value]
    if isinstance(value, dict):
        if set(value.keys()) == {"$oid"}:
            return str(value.get("$oid") or "")
        if set(value.keys()) == {"$date"}:
            return _sanitize_for_mongo(value.get("$date"))
        cleaned: dict[str, Any] = {}
        for raw_key, raw_val in value.items():
            key = str(raw_key)
            if key.startswith("$"):
                key = f"mongo_{key[1:]}"
            key = key.replace(".", "_")
            cleaned[key] = _sanitize_for_mongo(raw_val)
        return cleaned
    return value


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Check and migrate runtime fallback JSON storage to MongoDB.",
    )
    parser.add_argument(
        "--runtime-dir",
        type=Path,
        default=DEFAULT_RUNTIME_DIR,
        help="Path to runtime directory with fallback JSON storage.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Only print source/target report and do not write into MongoDB.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate data and print migration plan without writing.",
    )
    return parser.parse_args()


def _read_json(path: Path) -> Any:
    """Read JSON content from file."""
    return json.loads(path.read_text(encoding="utf-8"))


def _iter_json_files(directory: Path) -> Iterable[Path]:
    """Yield sorted JSON files from a directory."""
    if not directory.exists():
        return []
    return sorted(directory.glob("*.json"))


def _load_crm_documents(runtime_dir: Path) -> tuple[list[dict[str, Any]], int]:
    """Load CRM documents from fallback storage."""
    rows: list[dict[str, Any]] = []
    invalid_count = 0
    for path in _iter_json_files(runtime_dir / "crm_store"):
        try:
            payload = _read_json(path)
        except Exception:
            invalid_count += 1
            continue
        if not isinstance(payload, dict):
            invalid_count += 1
            continue
        document_id = str(payload.get("document_id") or "").strip()
        if not document_id:
            invalid_count += 1
            continue
        rows.append(payload)
    return rows, invalid_count


def _load_crm_clients(runtime_dir: Path) -> tuple[list[dict[str, Any]], int]:
    """Load CRM clients from fallback storage."""
    rows: list[dict[str, Any]] = []
    invalid_count = 0
    for path in _iter_json_files(runtime_dir / "crm_clients"):
        try:
            payload = _read_json(path)
        except Exception:
            invalid_count += 1
            continue
        if not isinstance(payload, dict):
            invalid_count += 1
            continue
        client_id = str(payload.get("client_id") or "").strip()
        if not client_id:
            invalid_count += 1
            continue
        rows.append(payload)
    return rows, invalid_count


def _load_form_mappings(runtime_dir: Path) -> tuple[list[dict[str, Any]], int]:
    """Load form mapping templates from fallback storage."""
    rows: list[dict[str, Any]] = []
    invalid_count = 0
    for path in _iter_json_files(runtime_dir / "form_mappings"):
        try:
            payload = _read_json(path)
        except Exception:
            invalid_count += 1
            continue
        if not isinstance(payload, dict):
            invalid_count += 1
            continue
        host = str(payload.get("host") or "").strip().lower()
        route_path = str(payload.get("path") or "").strip().lower()
        if not host or not route_path:
            invalid_count += 1
            continue
        rows.append(payload)
    return rows, invalid_count


def _load_auth_users(runtime_dir: Path) -> tuple[list[dict[str, Any]], int]:
    """Load auth users from fallback JSON file."""
    users_file = runtime_dir / "auth_store" / "users.json"
    if not users_file.exists():
        return [], 0
    payload = _read_json(users_file)
    if not isinstance(payload, list):
        return [], 1

    rows: list[dict[str, Any]] = []
    invalid_count = 0
    for item in payload:
        if not isinstance(item, dict):
            invalid_count += 1
            continue
        email = str(item.get("email") or "").strip().lower()
        if not email:
            invalid_count += 1
            continue
        row = dict(item)
        row["email"] = email
        rows.append(row)
    return rows, invalid_count


def _to_expires_at_dt(row: dict[str, Any]) -> dict[str, Any]:
    """Add Mongo TTL datetime field if expires_at is present."""
    next_row = dict(row)
    raw_expires_at = row.get("expires_at")
    try:
        expires_at = int(raw_expires_at)
    except (TypeError, ValueError):
        return next_row
    next_row["expires_at_dt"] = datetime.fromtimestamp(expires_at, tz=timezone.utc)
    return next_row


def _load_refresh_tokens(runtime_dir: Path) -> tuple[list[dict[str, Any]], int]:
    """Load refresh tokens from fallback JSON file."""
    refresh_file = runtime_dir / "auth_store" / "refresh_tokens.json"
    if not refresh_file.exists():
        return [], 0
    payload = _read_json(refresh_file)
    if not isinstance(payload, list):
        return [], 1

    rows: list[dict[str, Any]] = []
    invalid_count = 0
    for item in payload:
        if not isinstance(item, dict):
            invalid_count += 1
            continue
        jti = str(item.get("jti") or "").strip()
        if not jti:
            invalid_count += 1
            continue
        rows.append(_to_expires_at_dt(item))
    return rows, invalid_count


def _get_mongo_collections() -> tuple[Any, dict[str, Any]]:
    """Connect to Mongo and return target collections."""
    mongo_uri = os.getenv("MONGODB_URI", "").strip()
    mongo_db = os.getenv("MONGODB_DB", DEFAULT_DB_NAME).strip() or DEFAULT_DB_NAME
    crm_collection = (
        os.getenv("MONGODB_COLLECTION", DEFAULT_CRM_COLLECTION).strip()
        or DEFAULT_CRM_COLLECTION
    )
    mapping_collection = (
        os.getenv("MONGODB_MAPPING_COLLECTION", DEFAULT_MAPPING_COLLECTION).strip()
        or DEFAULT_MAPPING_COLLECTION
    )
    if not mongo_uri:
        raise RuntimeError("MONGODB_URI is empty. Set env var before running script.")

    client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    db = client[mongo_db]
    collections = {
        "crm_documents": db[crm_collection],
        "crm_clients": db["crm_clients"],
        "form_mappings": db[mapping_collection],
        "auth_users": db["auth_users"],
        "auth_refresh_tokens": db["auth_refresh_tokens"],
    }
    return client, collections


def _preview(values: list[str]) -> str:
    """Build compact preview string."""
    return ", ".join(values[:MAX_PREVIEW_ITEMS])


def _keyed_docs(rows: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    """Build dict indexed by key field with latest item preference."""
    keyed: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_key = str(row.get(key) or "").strip()
        if row_key:
            keyed[row_key] = row
    return keyed


def _mapping_key(row: dict[str, Any]) -> str:
    """Build unique key for form mapping row."""
    host = str(row.get("host") or "").strip().lower()
    route_path = str(row.get("path") or "").strip().lower()
    return f"{host}|{route_path}"


def _keyed_mappings(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Build dict indexed by host+path."""
    keyed: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = _mapping_key(row)
        if key != "|":
            keyed[key] = row
    return keyed


def _report_diff(title: str, source_keys: set[str], target_keys: set[str]) -> None:
    """Print diff between source and target key sets."""
    missing = sorted(source_keys - target_keys)
    extra = sorted(target_keys - source_keys)
    print(f"{title}:")
    print(f"  source: {len(source_keys)}")
    print(f"  target: {len(target_keys)}")
    print(f"  missing_in_target: {len(missing)}")
    if missing:
        print(f"  missing_preview: {_preview(missing)}")
    print(f"  extra_in_target: {len(extra)}")
    if extra:
        print(f"  extra_preview: {_preview(extra)}")


def _report_invalid(
    crm_docs_invalid: int,
    crm_clients_invalid: int,
    mappings_invalid: int,
    users_invalid: int,
    refresh_invalid: int,
) -> None:
    """Print invalid row counters."""
    print("Invalid rows skipped:")
    print(f"  crm_documents: {crm_docs_invalid}")
    print(f"  crm_clients: {crm_clients_invalid}")
    print(f"  form_mappings: {mappings_invalid}")
    print(f"  auth_users: {users_invalid}")
    print(f"  auth_refresh_tokens: {refresh_invalid}")


def _upsert_by_key(
    collection: Any,
    source_map: dict[str, dict[str, Any]],
    query_builder: Any,
    dry_run: bool,
) -> tuple[int, int]:
    """Upsert rows into collection and return processed/insert-candidate counters."""
    if not source_map:
        return 0, 0
    target_before = set(source_map.keys()) & {
        key for key in source_map if collection.find_one(query_builder(source_map[key]))
    }
    inserted_candidates = len(source_map) - len(target_before)
    if dry_run:
        return len(source_map), inserted_candidates
    for row in source_map.values():
        sanitized = _sanitize_for_mongo(row)
        collection.update_one(
            query_builder(row),
            {"$set": sanitized},
            upsert=True,
        )
    return len(source_map), inserted_candidates


def main() -> int:
    """Run check or migration workflow."""
    args = _parse_args()
    runtime_dir = args.runtime_dir
    if not runtime_dir.exists():
        print(f"ERROR: runtime dir not found: {runtime_dir}", file=sys.stderr)
        return 1

    crm_docs, crm_docs_invalid = _load_crm_documents(runtime_dir)
    crm_clients, crm_clients_invalid = _load_crm_clients(runtime_dir)
    mappings, mappings_invalid = _load_form_mappings(runtime_dir)
    auth_users, users_invalid = _load_auth_users(runtime_dir)
    refresh_tokens, refresh_invalid = _load_refresh_tokens(runtime_dir)

    crm_docs_map = _keyed_docs(crm_docs, "document_id")
    crm_clients_map = _keyed_docs(crm_clients, "client_id")
    mappings_map = _keyed_mappings(mappings)
    auth_users_map = _keyed_docs(auth_users, "email")
    refresh_map = _keyed_docs(refresh_tokens, "jti")

    mongo_client = None
    try:
        mongo_client, collections = _get_mongo_collections()

        target_doc_keys = {
            str(row.get("document_id") or "").strip()
            for row in collections["crm_documents"].find(
                {}, {"document_id": 1, "_id": 0}
            )
        }
        target_client_keys = {
            str(row.get("client_id") or "").strip()
            for row in collections["crm_clients"].find({}, {"client_id": 1, "_id": 0})
        }
        target_mapping_keys = {
            f"{str(row.get('host') or '').strip().lower()}|{str(row.get('path') or '').strip().lower()}"
            for row in collections["form_mappings"].find(
                {}, {"host": 1, "path": 1, "_id": 0}
            )
        }
        target_user_keys = {
            str(row.get("email") or "").strip().lower()
            for row in collections["auth_users"].find({}, {"email": 1, "_id": 0})
        }
        target_refresh_keys = {
            str(row.get("jti") or "").strip()
            for row in collections["auth_refresh_tokens"].find({}, {"jti": 1, "_id": 0})
        }

        if args.check:
            print(f"Runtime dir: {runtime_dir}")
            _report_invalid(
                crm_docs_invalid=crm_docs_invalid,
                crm_clients_invalid=crm_clients_invalid,
                mappings_invalid=mappings_invalid,
                users_invalid=users_invalid,
                refresh_invalid=refresh_invalid,
            )
            _report_diff(
                "crm_documents",
                set(crm_docs_map.keys()),
                target_doc_keys,
            )
            _report_diff(
                "crm_clients",
                set(crm_clients_map.keys()),
                target_client_keys,
            )
            _report_diff(
                "form_mappings",
                set(mappings_map.keys()),
                target_mapping_keys,
            )
            _report_diff(
                "auth_users",
                set(auth_users_map.keys()),
                target_user_keys,
            )
            _report_diff(
                "auth_refresh_tokens",
                set(refresh_map.keys()),
                target_refresh_keys,
            )
            return 0

        docs_processed, docs_inserted = _upsert_by_key(
            collections["crm_documents"],
            crm_docs_map,
            lambda row: {"document_id": str(row.get("document_id") or "").strip()},
            args.dry_run,
        )
        clients_processed, clients_inserted = _upsert_by_key(
            collections["crm_clients"],
            crm_clients_map,
            lambda row: {"client_id": str(row.get("client_id") or "").strip()},
            args.dry_run,
        )
        mappings_processed, mappings_inserted = _upsert_by_key(
            collections["form_mappings"],
            mappings_map,
            lambda row: {
                "host": str(row.get("host") or "").strip().lower(),
                "path": str(row.get("path") or "").strip().lower(),
            },
            args.dry_run,
        )
        users_processed, users_inserted = _upsert_by_key(
            collections["auth_users"],
            auth_users_map,
            lambda row: {"email": str(row.get("email") or "").strip().lower()},
            args.dry_run,
        )
        refresh_processed, refresh_inserted = _upsert_by_key(
            collections["auth_refresh_tokens"],
            refresh_map,
            lambda row: {"jti": str(row.get("jti") or "").strip()},
            args.dry_run,
        )

        print(f"Runtime dir: {runtime_dir}")
        _report_invalid(
            crm_docs_invalid=crm_docs_invalid,
            crm_clients_invalid=crm_clients_invalid,
            mappings_invalid=mappings_invalid,
            users_invalid=users_invalid,
            refresh_invalid=refresh_invalid,
        )
        print(f"Mode: {'dry-run' if args.dry_run else 'write'}")
        print("Processed:")
        print(f"  crm_documents: {docs_processed} (new: {docs_inserted})")
        print(f"  crm_clients: {clients_processed} (new: {clients_inserted})")
        print(f"  form_mappings: {mappings_processed} (new: {mappings_inserted})")
        print(f"  auth_users: {users_processed} (new: {users_inserted})")
        print(f"  auth_refresh_tokens: {refresh_processed} (new: {refresh_inserted})")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    finally:
        if mongo_client is not None:
            mongo_client.close()


if __name__ == "__main__":
    raise SystemExit(main())
