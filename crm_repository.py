from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)

try:
    from pymongo import MongoClient
    from pymongo.collection import Collection
except Exception:  # pragma: no cover
    MongoClient = None  # type: ignore[assignment]
    Collection = Any  # type: ignore[assignment]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe(payload: dict[str, Any], *path: str) -> str:
    node: Any = payload
    for key in path:
        if not isinstance(node, dict):
            return ""
        node = node.get(key)
    if node is None:
        return ""
    return str(node).strip()


def _identifiers_from_payload(payload: dict[str, Any]) -> dict[str, str]:
    return {
        "document_number": _safe(payload, "identificacion", "nif_nie"),
        "name": _safe(payload, "identificacion", "nombre_apellidos"),
    }


def _summary_from_record(record: dict[str, Any]) -> dict[str, Any]:
    identifiers = record.get("identifiers") or {}
    return {
        "document_id": str(record.get("document_id") or record.get("_id") or ""),
        "document_number": str(identifiers.get("document_number") or ""),
        "name": str(identifiers.get("name") or ""),
        "updated_at": str(record.get("updated_at") or ""),
        "status": str(record.get("status") or "unknown"),
        "has_edited": bool(record.get("edited_payload")),
    }


def _normalized_doc_number(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", (value or "").upper())


def _normalized_name(value: str) -> str:
    return re.sub(r"[^A-Z0-9]+", " ", (value or "").upper()).strip()


def _dedupe_summaries(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # Keep latest record per identity key (document number preferred, then normalized name).
    dedup: dict[str, dict[str, Any]] = {}
    for item in sorted(items, key=lambda d: str(d.get("updated_at") or ""), reverse=True):
        doc_no = _normalized_doc_number(str(item.get("document_number") or ""))
        name = _normalized_name(str(item.get("name") or ""))
        key = f"doc:{doc_no}" if doc_no else (f"name:{name}" if name else f"id:{item.get('document_id','')}")
        if key not in dedup:
            dedup[key] = item
    return list(dedup.values())


class CRMRepository:
    def __init__(self, app_root: Path) -> None:
        self.app_root = app_root
        self._fallback_dir = app_root / "runtime" / "crm_store"
        self._fallback_dir.mkdir(parents=True, exist_ok=True)

        self._mongo_enabled = False
        self._collection: Collection | None = None

        mongo_uri = os.getenv("MONGODB_URI", "").strip()
        mongo_db = os.getenv("MONGODB_DB", "ocr_mrz").strip() or "ocr_mrz"
        mongo_collection = os.getenv("MONGODB_COLLECTION", "crm_documents").strip() or "crm_documents"
        if mongo_uri and MongoClient is not None:
            try:
                client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
                client.admin.command("ping")
                self._collection = client[mongo_db][mongo_collection]
                self._collection.create_index("document_id", unique=True)
                self._collection.create_index("identifiers.document_number")
                self._collection.create_index("identifiers.name")
                self._collection.create_index("updated_at")
                self._mongo_enabled = True
                LOGGER.info("CRMRepository using MongoDB: db=%s collection=%s", mongo_db, mongo_collection)
            except Exception:
                LOGGER.exception("MongoDB connection failed. Falling back to local CRM store.")
                self._mongo_enabled = False
                self._collection = None
        else:
            LOGGER.warning("MONGODB_URI is not set or pymongo unavailable. Using local CRM store fallback.")

    def _fallback_path(self, document_id: str) -> Path:
        return self._fallback_dir / f"{document_id}.json"

    def _read_fallback(self, document_id: str) -> dict[str, Any] | None:
        path = self._fallback_path(document_id)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            LOGGER.exception("Failed reading fallback CRM record: %s", path)
            return None

    def _write_fallback(self, document_id: str, record: dict[str, Any]) -> None:
        path = self._fallback_path(document_id)
        path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")

    def _get(self, document_id: str) -> dict[str, Any] | None:
        if self._mongo_enabled and self._collection is not None:
            doc = self._collection.find_one({"document_id": document_id}, {"_id": 0})
            return dict(doc) if doc else None
        return self._read_fallback(document_id)

    def _save(self, record: dict[str, Any]) -> None:
        document_id = str(record.get("document_id") or "")
        if not document_id:
            raise ValueError("document_id is required for CRM save.")
        if self._mongo_enabled and self._collection is not None:
            self._collection.update_one({"document_id": document_id}, {"$set": record}, upsert=True)
            return
        self._write_fallback(document_id, record)

    def upsert_from_upload(
        self,
        *,
        document_id: str,
        payload: dict[str, Any],
        ocr_document: dict[str, Any],
        source: dict[str, Any],
        missing_fields: list[str],
        manual_steps_required: list[str],
        form_url: str,
        target_url: str,
        identity_match_found: bool = False,
        identity_source_document_id: str = "",
        enrichment_preview: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        now = _now_iso()
        existing = self._get(document_id) or {}
        edited_payload = existing.get("edited_payload") if isinstance(existing.get("edited_payload"), dict) else None
        effective_payload = edited_payload or payload
        record = {
            "document_id": document_id,
            "status": "uploaded",
            "created_at": str(existing.get("created_at") or now),
            "updated_at": now,
            "identifiers": _identifiers_from_payload(effective_payload),
            "ocr_payload": payload,
            "edited_payload": edited_payload,
            "effective_payload": effective_payload,
            "ocr_document": ocr_document,
            "source": source,
            "missing_fields": missing_fields,
            "manual_steps_required": manual_steps_required,
            "form_url": form_url,
            "target_url": target_url,
            "browser_session_id": str(existing.get("browser_session_id") or ""),
            "identity_match_found": bool(identity_match_found or existing.get("identity_match_found")),
            "identity_source_document_id": str(identity_source_document_id or existing.get("identity_source_document_id") or ""),
            "enrichment_preview": enrichment_preview if enrichment_preview is not None else (existing.get("enrichment_preview") or []),
            "enrichment_log": existing.get("enrichment_log") or {},
        }
        self._save(record)
        return record

    def save_edited_payload(
        self,
        *,
        document_id: str,
        payload: dict[str, Any],
        missing_fields: list[str] | None = None,
    ) -> dict[str, Any]:
        now = _now_iso()
        existing = self._get(document_id) or {}
        record = {
            "document_id": document_id,
            "status": "confirmed",
            "created_at": str(existing.get("created_at") or now),
            "updated_at": now,
            "identifiers": _identifiers_from_payload(payload),
            "ocr_payload": existing.get("ocr_payload") or payload,
            "edited_payload": payload,
            "effective_payload": payload,
            "ocr_document": existing.get("ocr_document") or {},
            "source": existing.get("source") or {},
            "missing_fields": missing_fields if missing_fields is not None else (existing.get("missing_fields") or []),
            "manual_steps_required": existing.get("manual_steps_required") or [],
            "form_url": str(existing.get("form_url") or ""),
            "target_url": str(existing.get("target_url") or ""),
            "browser_session_id": str(existing.get("browser_session_id") or ""),
            "identity_match_found": bool(existing.get("identity_match_found")),
            "identity_source_document_id": str(existing.get("identity_source_document_id") or ""),
            "enrichment_preview": existing.get("enrichment_preview") or [],
            "enrichment_log": existing.get("enrichment_log") or {},
        }
        self._save(record)
        return record

    def set_browser_session(self, document_id: str, session_id: str) -> None:
        existing = self._get(document_id) or {}
        existing["document_id"] = document_id
        existing["browser_session_id"] = session_id
        existing["updated_at"] = _now_iso()
        if "created_at" not in existing:
            existing["created_at"] = existing["updated_at"]
        self._save(existing)

    def get_document(self, document_id: str) -> dict[str, Any] | None:
        return self._get(document_id)

    def update_document_fields(self, document_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        existing = self._get(document_id) or {"document_id": document_id, "created_at": _now_iso()}
        existing.update(updates or {})
        existing["document_id"] = document_id
        existing["updated_at"] = _now_iso()
        self._save(existing)
        return existing

    def search_documents(self, query: str = "", limit: int = 30) -> list[dict[str, Any]]:
        q = (query or "").strip()
        limit = max(1, min(int(limit or 30), 200))
        if self._mongo_enabled and self._collection is not None:
            filter_doc: dict[str, Any] = {}
            if q:
                regex = {"$regex": re.escape(q), "$options": "i"}
                filter_doc = {
                    "$or": [
                        {"identifiers.name": regex},
                        {"identifiers.document_number": regex},
                    ]
                }
            docs = (
                self._collection.find(
                    filter_doc,
                    {
                        "_id": 0,
                        "document_id": 1,
                        "identifiers": 1,
                        "updated_at": 1,
                        "status": 1,
                        "edited_payload": 1,
                    },
                )
                .sort("updated_at", -1)
                .limit(max(limit * 4, 100))
            )
            summaries = [_summary_from_record(dict(doc)) for doc in docs]
            return _dedupe_summaries(summaries)[:limit]

        results: list[dict[str, Any]] = []
        for path in self._fallback_dir.glob("*.json"):
            try:
                doc = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            summary = _summary_from_record(doc)
            if q:
                hay = f"{summary.get('name', '')} {summary.get('document_number', '')}".lower()
                if q.lower() not in hay:
                    continue
            results.append(summary)
        results.sort(key=lambda d: str(d.get("updated_at") or ""), reverse=True)
        return _dedupe_summaries(results)[:limit]

    def find_latest_by_identity(self, document_number: str, exclude_document_id: str = "") -> dict[str, Any] | None:
        identity = _normalized_doc_number(document_number)
        if not identity:
            return None

        exclude = str(exclude_document_id or "").strip()
        if self._mongo_enabled and self._collection is not None:
            docs = self._collection.find(
                {"identifiers.document_number": {"$exists": True, "$ne": ""}},
                {"_id": 0},
            ).sort("updated_at", -1)
            for doc in docs:
                item = dict(doc)
                if exclude and str(item.get("document_id") or "") == exclude:
                    continue
                current = _normalized_doc_number(str(((item.get("identifiers") or {}).get("document_number") or "")))
                if current == identity:
                    return item
            return None

        records: list[dict[str, Any]] = []
        for path in self._fallback_dir.glob("*.json"):
            try:
                doc = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if exclude and str(doc.get("document_id") or "") == exclude:
                continue
            current = _normalized_doc_number(str(((doc.get("identifiers") or {}).get("document_number") or "")))
            if current == identity:
                records.append(doc)
        if not records:
            return None
        records.sort(key=lambda d: str(d.get("updated_at") or ""), reverse=True)
        return records[0]
