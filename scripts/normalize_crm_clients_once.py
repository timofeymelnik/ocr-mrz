#!/usr/bin/env python3
"""One-shot normalization of CRM client entities in fallback runtime storage."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Normalize CRM client linkage using merged_into_document_id chains."
        )
    )
    parser.add_argument(
        "--app-root",
        type=Path,
        default=Path("."),
        help="Path containing runtime directory (runtime/crm_store, runtime/crm_clients).",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Only report normalization plan without writing.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute actions without writing.",
    )
    return parser.parse_args()


def _import_repo(project_root: Path) -> type[Any]:
    """Import CRMRepository from project source."""
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from app.crm.repository import CRMRepository  # type: ignore

    return CRMRepository


def _load_crm_doc_ids(runtime_dir: Path) -> list[str]:
    """Load CRM document ids from fallback files."""
    crm_store_dir = runtime_dir / "crm_store"
    if not crm_store_dir.exists():
        return []
    doc_ids: list[str] = []
    for path in sorted(crm_store_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        doc_id = str(payload.get("document_id") or path.stem).strip()
        if doc_id:
            doc_ids.append(doc_id)
    return doc_ids


def _load_doc_map(repo: Any, doc_ids: list[str]) -> dict[str, dict[str, Any]]:
    """Load full documents map via repository."""
    out: dict[str, dict[str, Any]] = {}
    for doc_id in doc_ids:
        doc = repo.get_document(doc_id)
        if isinstance(doc, dict):
            out[doc_id] = doc
    return out


def _resolve_root(
    doc_id: str, doc_map: dict[str, dict[str, Any]]
) -> tuple[str, bool, bool]:
    """Resolve merge root for document id.

    Returns tuple: (root_id, had_merge_link, had_broken_link).
    """
    current = doc_id
    visited: set[str] = set()
    had_merge_link = False
    had_broken_link = False

    while True:
        if current in visited:
            had_broken_link = True
            return doc_id, had_merge_link, had_broken_link
        visited.add(current)
        row = doc_map.get(current) or {}
        target = str(row.get("merged_into_document_id") or "").strip()
        if not target:
            return current, had_merge_link, had_broken_link
        had_merge_link = True
        if target not in doc_map:
            had_broken_link = True
            return current, had_merge_link, had_broken_link
        current = target


def main() -> int:
    """Run normalization workflow."""
    args = _parse_args()
    project_root = Path(__file__).resolve().parents[1]
    app_root = args.app_root.resolve()
    runtime_dir = app_root / "runtime"

    if not runtime_dir.exists():
        print(f"ERROR: runtime dir not found: {runtime_dir}", file=sys.stderr)
        return 1

    os.environ["MONGODB_URI"] = ""
    repo_class = _import_repo(project_root)
    repo = repo_class(app_root)

    doc_ids = _load_crm_doc_ids(runtime_dir)
    doc_map = _load_doc_map(repo, doc_ids)
    if not doc_map:
        print("No crm_store documents found.")
        return 0

    broken_links = 0
    merge_links = 0
    relink_actions: list[tuple[str, str]] = []

    for doc_id, row in doc_map.items():
        merged_into = str(row.get("merged_into_document_id") or "").strip()
        if merged_into:
            merge_links += 1
        root_id, had_merge_link, had_broken_link = _resolve_root(doc_id, doc_map)
        if had_broken_link:
            broken_links += 1
        if had_merge_link and doc_id != root_id:
            relink_actions.append((doc_id, root_id))

    mode = "check" if args.check else ("dry-run" if args.dry_run else "write")
    print(f"Mode: {mode}")
    print(f"App root: {app_root}")
    print(f"CRM documents scanned: {len(doc_map)}")
    print(f"Documents with merge links: {merge_links}")
    print(f"Broken merge links: {broken_links}")
    print(f"Client relink actions: {len(relink_actions)}")

    if args.check or args.dry_run:
        return 0

    fixed_broken = 0
    relinked = 0
    for doc_id, row in doc_map.items():
        merged_into = str(row.get("merged_into_document_id") or "").strip()
        if not merged_into:
            continue
        if merged_into not in doc_map:
            next_status = (
                "confirmed"
                if isinstance(row.get("edited_payload"), dict)
                and bool(row.get("edited_payload"))
                else "uploaded"
            )
            repo.update_document_fields(
                doc_id,
                {"merged_into_document_id": "", "status": next_status},
            )
            fixed_broken += 1

    for source_id, root_id in relink_actions:
        try:
            repo.ensure_client_entity(document_id=root_id, source_document_id=source_id)
        except Exception:
            continue
        relinked += 1

    final_doc_map = _load_doc_map(repo, doc_ids)
    backfilled = 0
    for doc_id, row in final_doc_map.items():
        if str(row.get("client_id") or "").strip():
            continue
        try:
            repo.ensure_client_entity(document_id=doc_id)
        except Exception:
            continue
        backfilled += 1

    client_ids_in_docs = {
        str(row.get("client_id") or "").strip()
        for row in _load_doc_map(repo, doc_ids).values()
        if str(row.get("client_id") or "").strip()
    }
    clients_dir = runtime_dir / "crm_clients"
    removed_orphans = 0
    for path in clients_dir.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        client_id = str(payload.get("client_id") or path.stem).strip()
        if client_id and client_id not in client_ids_in_docs:
            try:
                path.unlink()
                removed_orphans += 1
            except Exception:
                continue

    print(f"Broken merge links fixed: {fixed_broken}")
    print(f"Client relinks applied: {relinked}")
    print(f"Missing client_id backfilled: {backfilled}")
    print(f"Orphan crm_clients removed: {removed_orphans}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
