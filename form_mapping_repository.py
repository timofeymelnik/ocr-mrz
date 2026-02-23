from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

LOGGER = logging.getLogger(__name__)

try:
    from pymongo import MongoClient
    from pymongo.collection import Collection
except Exception:  # pragma: no cover
    MongoClient = None  # type: ignore[assignment]
    Collection = Any  # type: ignore[assignment]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_url_parts(target_url: str) -> tuple[str, str]:
    parsed = urlparse((target_url or "").strip())
    return (parsed.netloc or "").lower(), (parsed.path or "/").lower()


def _safe_token(value: str) -> str:
    return (value or "").strip().replace("/", "_").replace(":", "_")


def _target_key(host: str, path: str) -> str:
    safe_host = _safe_token(host)
    safe_path = _safe_token(path).strip("_") or "root"
    return f"{safe_host}__{safe_path}"


class FormMappingRepository:
    """
    Single-latest template store per target (host + path).
    No revisions/history/artifact snapshots are persisted.
    """

    def __init__(self, app_root: Path) -> None:
        self.app_root = app_root
        self._fallback_dir = app_root / "runtime" / "form_mappings"
        self._fallback_dir.mkdir(parents=True, exist_ok=True)

        self._mongo_enabled = False
        self._collection: Collection | None = None

        mongo_uri = os.getenv("MONGODB_URI", "").strip()
        mongo_db = os.getenv("MONGODB_DB", "ocr_mrz").strip() or "ocr_mrz"
        mongo_collection = os.getenv("MONGODB_MAPPING_COLLECTION", "form_mappings").strip() or "form_mappings"
        if mongo_uri and MongoClient is not None:
            try:
                client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
                client.admin.command("ping")
                self._collection = client[mongo_db][mongo_collection]
                self._collection.create_index([("host", 1), ("path", 1)], unique=True)
                self._collection.create_index([("updated_at", -1)])
                self._mongo_enabled = True
                LOGGER.info("FormMappingRepository using MongoDB: db=%s collection=%s", mongo_db, mongo_collection)
            except Exception:
                LOGGER.exception("MongoDB form mapping connection failed. Falling back to local store.")
                self._mongo_enabled = False
                self._collection = None
        else:
            LOGGER.warning("MONGODB_URI missing or pymongo unavailable. Using local form mapping store fallback.")

    def _fallback_file(self, host: str, path: str) -> Path:
        return self._fallback_dir / f"{_target_key(host, path)}.json"

    @staticmethod
    def _normalize_mappings(mappings: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for item in mappings or []:
            selector = str(item.get("selector") or "").strip()
            canonical_key = str(item.get("canonical_key") or "").strip()
            field_kind = str(item.get("field_kind") or "text").strip().lower()
            match_value = str(item.get("match_value") or "").strip()
            checked_when = str(item.get("checked_when") or "").strip()
            if not selector:
                continue
            if field_kind not in {"text", "select", "checkbox", "radio"}:
                field_kind = "text"
            if field_kind in {"checkbox", "radio"} and not (match_value and checked_when):
                # Rule-based controls must define both properties.
                continue
            if field_kind in {"text", "select"} and not canonical_key:
                continue
            normalized.append(
                {
                    "selector": selector,
                    "canonical_key": canonical_key,
                    "field_kind": field_kind,
                    "match_value": match_value,
                    "checked_when": checked_when,
                    "confidence": float(item.get("confidence") or 1.0),
                    "source": str(item.get("source") or "user"),
                }
            )
        return normalized

    def get_latest_for_url(self, target_url: str) -> dict[str, Any] | None:
        host, path = _normalize_url_parts(target_url)
        if not host:
            return None
        if self._mongo_enabled and self._collection is not None:
            doc = self._collection.find_one({"host": host, "path": path}, {"_id": 0})
            return dict(doc) if doc else None

        file_path = self._fallback_file(host, path)
        if not file_path.exists():
            return None
        try:
            return json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:
            LOGGER.exception("Failed reading local mapping template: %s", file_path)
            return None

    def get_template_for_revision(self, *, target_url: str, revision: str) -> dict[str, Any] | None:
        # Revisions are removed. Keep method for compatibility with callers.
        _ = revision
        return self.get_latest_for_url(target_url)

    def save_template(
        self,
        *,
        target_url: str,
        fields: list[dict[str, Any]],
        mappings: list[dict[str, Any]],
        template_pdf_bytes: bytes | None = None,
        source: str = "user",
    ) -> dict[str, Any]:
        host, path = _normalize_url_parts(target_url)
        if not host:
            raise ValueError("target_url is required for mapping template save.")

        _ = template_pdf_bytes
        now = _now_iso()
        normalized_mappings = self._normalize_mappings(mappings)
        template = {
            "host": host,
            "path": path,
            "updated_at": now,
            "source": source,
            "valid": True,
            "fields_snapshot": fields or [],
            "fields_count": len(fields or []),
            "mappings": normalized_mappings,
            "mappings_count": len(normalized_mappings),
        }

        if self._mongo_enabled and self._collection is not None:
            existing = self._collection.find_one({"host": host, "path": path}, {"_id": 0})
            template["created_at"] = str((existing or {}).get("created_at") or now)
            self._collection.update_one({"host": host, "path": path}, {"$set": template}, upsert=True)
            return template

        file_path = self._fallback_file(host, path)
        if file_path.exists():
            try:
                existing = json.loads(file_path.read_text(encoding="utf-8"))
                template["created_at"] = str((existing or {}).get("created_at") or now)
            except Exception:
                template["created_at"] = now
        else:
            template["created_at"] = now
        file_path.write_text(json.dumps(template, ensure_ascii=False, indent=2), encoding="utf-8")
        return template
