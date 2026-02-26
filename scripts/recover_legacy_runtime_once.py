#!/usr/bin/env python3
"""One-shot reconciliation of legacy runtime storage into current runtime layout."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

MAX_PREVIEW_ITEMS = 10
TARGET_SUBDIRS = {
    "crm_store": "crm_store",
    "crm_clients": "crm_clients",
    "form_mappings": "form_mappings",
    "auth_store": "auth_store",
    "documents": "documents",
    "uploads": "uploads",
    "queued_uploads": "queued_uploads",
}
DEFAULT_RUNTIME_DIR = Path("runtime")


def _parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Recover legacy runtime data into current runtime structure.",
    )
    parser.add_argument(
        "--runtime-dir",
        type=Path,
        default=DEFAULT_RUNTIME_DIR,
        help="Current target runtime directory.",
    )
    parser.add_argument(
        "--legacy-dir",
        action="append",
        default=[],
        help="Legacy runtime directory to import from. Can be passed multiple times.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Only print planned actions and do not write files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print actions without writing files.",
    )
    return parser.parse_args()


def _is_runtime_candidate(path: Path) -> bool:
    """Return whether path looks like runtime storage directory."""
    if not path.exists() or not path.is_dir():
        return False
    for subdir in TARGET_SUBDIRS.values():
        if (path / subdir).exists():
            return True
    if (path / "auth_store" / "users.json").exists():
        return True
    if list((path / "crm_store").glob("*.json")):
        return True
    return False


def _discover_legacy_candidates(runtime_dir: Path) -> list[Path]:
    """Find probable legacy runtime directories around project roots."""
    env_candidates = [
        (
            Path(os.getenv("LEGACY_RUNTIME_DIR", "")).expanduser()
            if os.getenv("LEGACY_RUNTIME_DIR")
            else None
        )
    ]
    cwd = Path.cwd()
    parent = cwd.parent
    candidates = [
        runtime_dir.parent / "app" / "runtime",
        runtime_dir.parent / "server" / "runtime",
        cwd / "app" / "runtime",
        cwd / "server" / "runtime",
        parent / "runtime",
        Path("/app/runtime"),
        Path("/app/app/runtime"),
    ]
    for item in env_candidates:
        if item is not None:
            candidates.append(item)

    normalized: list[Path] = []
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except Exception:
            continue
        if resolved == runtime_dir.resolve():
            continue
        if resolved not in normalized and _is_runtime_candidate(resolved):
            normalized.append(resolved)
    return normalized


def _safe_read_json(path: Path) -> dict[str, Any] | list[Any] | None:
    """Read JSON file and return payload or None on parse error."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if isinstance(payload, (dict, list)):
        return payload
    return None


def _safe_token(value: str) -> str:
    """Build safe token from host/path fragments."""
    return (value or "").strip().replace("/", "_").replace(":", "_")


def _normalize_url_parts(target_url: str) -> tuple[str, str]:
    """Normalize URL to repository host/path key format."""
    parsed = urlparse((target_url or "").strip())
    host = (parsed.netloc or "").lower()
    path = (parsed.path or "/").lower().strip()
    if not path:
        path = "/"
    if path != "/":
        path = path.rstrip("/") or "/"
    return host, path


def _target_mapping_file(payload: dict[str, Any], target_dir: Path) -> Path | None:
    """Compute current mapping file path from payload host/path or target_url."""
    host = str(payload.get("host") or "").strip().lower()
    route_path = str(payload.get("path") or "").strip().lower()
    if not host or not route_path:
        target_url = str(payload.get("target_url") or "").strip()
        if target_url:
            host, route_path = _normalize_url_parts(target_url)
    if not host or not route_path:
        return None
    safe_host = _safe_token(host)
    safe_path = _safe_token(route_path).strip("_") or "root"
    return target_dir / f"{safe_host}__{safe_path}.json"


def _updated_at(payload: dict[str, Any], file_path: Path) -> str:
    """Return comparable update marker from payload or file mtime."""
    marker = str(payload.get("updated_at") or "").strip()
    if marker:
        return marker
    return str(file_path.stat().st_mtime)


def _should_replace(
    existing: dict[str, Any], incoming: dict[str, Any], source_file: Path
) -> bool:
    """Decide whether incoming JSON should replace existing file content."""
    return _updated_at(incoming, source_file) >= _updated_at(existing, source_file)


def _ensure_dirs(runtime_dir: Path) -> None:
    """Ensure all target runtime subdirectories exist."""
    runtime_dir.mkdir(parents=True, exist_ok=True)
    for subdir in TARGET_SUBDIRS.values():
        (runtime_dir / subdir).mkdir(parents=True, exist_ok=True)


def _merge_keyed_json_dir(
    source_dir: Path,
    target_dir: Path,
    *,
    key_field: str,
    check: bool,
    dry_run: bool,
) -> tuple[int, int, int]:
    """Merge JSON files by stable key field into target directory."""
    scanned = 0
    copied = 0
    replaced = 0
    if not source_dir.exists():
        return scanned, copied, replaced

    for source_file in sorted(source_dir.glob("*.json")):
        scanned += 1
        source_payload = _safe_read_json(source_file)
        if not isinstance(source_payload, dict):
            continue
        key = str(source_payload.get(key_field) or "").strip()
        if not key:
            key = source_file.stem.strip()
        if not key:
            continue
        target_file = target_dir / f"{key}.json"
        if not target_file.exists():
            copied += 1
            if not (check or dry_run):
                target_file.write_text(
                    json.dumps(source_payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            continue
        target_payload = _safe_read_json(target_file)
        if not isinstance(target_payload, dict):
            replaced += 1
            if not (check or dry_run):
                target_file.write_text(
                    json.dumps(source_payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            continue
        if _should_replace(target_payload, source_payload, source_file):
            replaced += 1
            if not (check or dry_run):
                target_file.write_text(
                    json.dumps(source_payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
    return scanned, copied, replaced


def _merge_form_mappings(
    source_dir: Path,
    target_dir: Path,
    *,
    check: bool,
    dry_run: bool,
) -> tuple[int, int, int]:
    """Merge form mapping templates by normalized host/path key."""
    scanned = 0
    copied = 0
    replaced = 0
    if not source_dir.exists():
        return scanned, copied, replaced

    for source_file in sorted(source_dir.glob("*.json")):
        scanned += 1
        source_payload = _safe_read_json(source_file)
        if not isinstance(source_payload, dict):
            continue
        target_file = _target_mapping_file(source_payload, target_dir)
        if target_file is None:
            continue
        if not target_file.exists():
            copied += 1
            if not (check or dry_run):
                target_file.write_text(
                    json.dumps(source_payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            continue
        target_payload = _safe_read_json(target_file)
        if not isinstance(target_payload, dict):
            replaced += 1
            if not (check or dry_run):
                target_file.write_text(
                    json.dumps(source_payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            continue
        if _should_replace(target_payload, source_payload, source_file):
            replaced += 1
            if not (check or dry_run):
                target_file.write_text(
                    json.dumps(source_payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
    return scanned, copied, replaced


def _merge_auth_list_file(
    source_file: Path,
    target_file: Path,
    *,
    key_field: str,
    check: bool,
    dry_run: bool,
) -> tuple[int, int]:
    """Merge JSON list records by key field and persist into target list file."""
    if not source_file.exists():
        return 0, 0

    source_payload = _safe_read_json(source_file)
    if not isinstance(source_payload, list):
        return 0, 0
    source_rows = [row for row in source_payload if isinstance(row, dict)]

    target_payload = _safe_read_json(target_file)
    target_rows: list[dict[str, Any]]
    if isinstance(target_payload, list):
        target_rows = [row for row in target_payload if isinstance(row, dict)]
    else:
        target_rows = []

    target_map: dict[str, dict[str, Any]] = {}
    for row in target_rows:
        key = str(row.get(key_field) or "").strip()
        if key:
            target_map[key] = row

    merged = 0
    for row in source_rows:
        key = str(row.get(key_field) or "").strip()
        if not key:
            continue
        if key not in target_map:
            merged += 1
            target_map[key] = row
            continue
        source_marker = str(row.get("updated_at") or row.get("expires_at") or "")
        target_marker = str(
            target_map[key].get("updated_at") or target_map[key].get("expires_at") or ""
        )
        if source_marker >= target_marker:
            target_map[key] = row
            merged += 1

    if merged and not (check or dry_run):
        target_file.parent.mkdir(parents=True, exist_ok=True)
        ordered = sorted(
            target_map.values(),
            key=lambda item: str(item.get(key_field) or ""),
        )
        target_file.write_text(
            json.dumps(ordered, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return len(source_rows), merged


def _copy_if_missing(
    source_file: Path, target_file: Path, check: bool, dry_run: bool
) -> bool:
    """Copy file only when target file does not exist."""
    if not source_file.exists() or target_file.exists():
        return False
    if not (check or dry_run):
        target_file.parent.mkdir(parents=True, exist_ok=True)
        target_file.write_bytes(source_file.read_bytes())
    return True


def _print_preview(title: str, items: list[str]) -> None:
    """Print compact preview list."""
    preview = ", ".join(items[:MAX_PREVIEW_ITEMS])
    print(f"{title}: {preview}")


def main() -> int:
    """Run legacy runtime recovery workflow."""
    args = _parse_args()
    runtime_dir = args.runtime_dir.resolve()
    _ensure_dirs(runtime_dir)

    user_legacy_dirs: list[Path] = []
    for raw in args.legacy_dir:
        path = Path(raw).expanduser()
        if not path.exists():
            continue
        user_legacy_dirs.append(path.resolve())

    auto_legacy_dirs = _discover_legacy_candidates(runtime_dir)
    legacy_dirs: list[Path] = []
    for directory in user_legacy_dirs + auto_legacy_dirs:
        if directory != runtime_dir and directory not in legacy_dirs:
            legacy_dirs.append(directory)

    if not legacy_dirs:
        print("No legacy runtime directories detected. Nothing to recover.")
        return 0

    print(f"Target runtime: {runtime_dir}")
    print(f"Legacy dirs detected: {len(legacy_dirs)}")
    _print_preview("Legacy preview", [str(path) for path in legacy_dirs])
    print(
        f"Mode: {'check' if args.check else ('dry-run' if args.dry_run else 'write')}"
    )

    crm_scanned_total = 0
    crm_copied_total = 0
    crm_replaced_total = 0
    clients_scanned_total = 0
    clients_copied_total = 0
    clients_replaced_total = 0
    mappings_scanned_total = 0
    mappings_copied_total = 0
    mappings_replaced_total = 0
    auth_users_scanned_total = 0
    auth_users_merged_total = 0
    refresh_scanned_total = 0
    refresh_merged_total = 0
    sqlite_copied = False

    for legacy_dir in legacy_dirs:
        crm_scanned, crm_copied, crm_replaced = _merge_keyed_json_dir(
            legacy_dir / "crm_store",
            runtime_dir / "crm_store",
            key_field="document_id",
            check=args.check,
            dry_run=args.dry_run,
        )
        crm_scanned_total += crm_scanned
        crm_copied_total += crm_copied
        crm_replaced_total += crm_replaced

        clients_scanned, clients_copied, clients_replaced = _merge_keyed_json_dir(
            legacy_dir / "crm_clients",
            runtime_dir / "crm_clients",
            key_field="client_id",
            check=args.check,
            dry_run=args.dry_run,
        )
        clients_scanned_total += clients_scanned
        clients_copied_total += clients_copied
        clients_replaced_total += clients_replaced

        mappings_scanned, mappings_copied, mappings_replaced = _merge_form_mappings(
            legacy_dir / "form_mappings",
            runtime_dir / "form_mappings",
            check=args.check,
            dry_run=args.dry_run,
        )
        mappings_scanned_total += mappings_scanned
        mappings_copied_total += mappings_copied
        mappings_replaced_total += mappings_replaced

        users_scanned, users_merged = _merge_auth_list_file(
            legacy_dir / "auth_store" / "users.json",
            runtime_dir / "auth_store" / "users.json",
            key_field="email",
            check=args.check,
            dry_run=args.dry_run,
        )
        auth_users_scanned_total += users_scanned
        auth_users_merged_total += users_merged

        refresh_scanned, refresh_merged = _merge_auth_list_file(
            legacy_dir / "auth_store" / "refresh_tokens.json",
            runtime_dir / "auth_store" / "refresh_tokens.json",
            key_field="jti",
            check=args.check,
            dry_run=args.dry_run,
        )
        refresh_scanned_total += refresh_scanned
        refresh_merged_total += refresh_merged

        if _copy_if_missing(
            legacy_dir / "app_state.db",
            runtime_dir / "app_state.db",
            check=args.check,
            dry_run=args.dry_run,
        ):
            sqlite_copied = True

    print("Merged summary:")
    print(
        f"  crm_store scanned={crm_scanned_total} copied={crm_copied_total} replaced={crm_replaced_total}"
    )
    print(
        f"  crm_clients scanned={clients_scanned_total} copied={clients_copied_total} replaced={clients_replaced_total}"
    )
    print(
        f"  form_mappings scanned={mappings_scanned_total} copied={mappings_copied_total} replaced={mappings_replaced_total}"
    )
    print(
        f"  auth_users scanned={auth_users_scanned_total} merged={auth_users_merged_total}"
    )
    print(
        f"  auth_refresh_tokens scanned={refresh_scanned_total} merged={refresh_merged_total}"
    )
    print(f"  sqlite app_state.db copied_if_missing={sqlite_copied}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
