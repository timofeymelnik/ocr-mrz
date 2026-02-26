#!/usr/bin/env python3
"""One-shot backfill of missing CRM document client_id values."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Backfill missing client_id for CRM documents.",
    )
    parser.add_argument(
        "--runtime-dir",
        type=Path,
        default=Path("runtime"),
        help="Path to runtime directory.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Only report missing client_id documents.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Plan backfill actions without writing changes.",
    )
    return parser.parse_args()


def _read_json(path: Path) -> dict[str, Any] | None:
    """Read JSON object from file or return None on parse error."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _load_document_ids_from_fallback(runtime_dir: Path) -> list[str]:
    """Collect document ids from fallback CRM storage."""
    store_dir = runtime_dir / "crm_store"
    if not store_dir.exists():
        return []
    doc_ids: list[str] = []
    for path in sorted(store_dir.glob("*.json")):
        payload = _read_json(path)
        if not payload:
            continue
        doc_id = str(payload.get("document_id") or path.stem).strip()
        if doc_id:
            doc_ids.append(doc_id)
    return doc_ids


def _import_repo_class(project_root: Path) -> type[Any]:
    """Import CRMRepository class lazily from project source."""
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from app.crm.repository import CRMRepository  # type: ignore

    return CRMRepository


def _load_document_ids(repo: Any, runtime_dir: Path) -> list[str]:
    """Collect all CRM document ids from Mongo or fallback."""
    if getattr(repo, "_mongo_enabled", False) and getattr(repo, "_collection", None):
        cursor = repo._collection.find({}, {"_id": 0, "document_id": 1})  # type: ignore[attr-defined]
        doc_ids = [str(row.get("document_id") or "").strip() for row in cursor]
        return sorted([doc_id for doc_id in doc_ids if doc_id])
    return _load_document_ids_from_fallback(runtime_dir)


def main() -> int:
    """Run backfill workflow."""
    args = _parse_args()
    project_root = Path(__file__).resolve().parents[1]
    runtime_dir = args.runtime_dir

    repo_class = _import_repo_class(project_root)
    repo = repo_class(project_root)
    doc_ids = _load_document_ids(repo, runtime_dir)

    if not doc_ids:
        print("No CRM documents found.")
        return 0

    scanned = 0
    missing = 0
    updated = 0
    failed = 0

    for doc_id in doc_ids:
        scanned += 1
        doc = repo.get_document(doc_id) or {}
        client_id = str(doc.get("client_id") or "").strip()
        if client_id:
            continue
        missing += 1
        if args.check or args.dry_run:
            continue
        try:
            client = repo.ensure_client_entity(document_id=doc_id)
        except Exception:
            failed += 1
            continue
        next_client_id = str(client.get("client_id") or "").strip()
        if next_client_id:
            updated += 1
        else:
            failed += 1

    mode = "check" if args.check else ("dry-run" if args.dry_run else "write")
    print(f"Mode: {mode}")
    print(f"Scanned documents: {scanned}")
    print(f"Documents missing client_id: {missing}")
    print(f"Backfilled client_id: {updated}")
    print(f"Failed: {failed}")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
