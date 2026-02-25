from __future__ import annotations

import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)

try:
    import pymongo
except Exception:  # pragma: no cover
    pymongo = None  # type: ignore[assignment]


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
    nif_nie = _safe(payload, "identificacion", "nif_nie")
    passport = _safe(payload, "identificacion", "pasaporte")
    primary_number = nif_nie or passport
    return {
        "document_number": primary_number,
        "nif_nie": nif_nie,
        "passport": passport,
        "name": _safe(payload, "identificacion", "nombre_apellidos"),
    }


def _is_empty_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, list):
        return len(value) == 0
    if isinstance(value, dict):
        return len(value) == 0
    return False


def _deep_merge_first_non_empty(base: Any, incoming: Any) -> Any:
    """Merge two values preserving first non-empty leaf from base."""
    if isinstance(base, dict) and isinstance(incoming, dict):
        merged: dict[str, Any] = {}
        keys = set(base.keys()) | set(incoming.keys())
        for key in keys:
            if key in base and key in incoming:
                merged[key] = _deep_merge_first_non_empty(base[key], incoming[key])
            elif key in base:
                merged[key] = base[key]
            else:
                merged[key] = incoming[key]
        return merged
    if _is_empty_value(base) and not _is_empty_value(incoming):
        return incoming
    return base


def _summary_from_record(record: dict[str, Any]) -> dict[str, Any]:
    identifiers = record.get("identifiers") or {}
    return {
        "document_id": str(record.get("document_id") or record.get("_id") or ""),
        "client_id": str(record.get("client_id") or ""),
        "merged_into_document_id": str(record.get("merged_into_document_id") or ""),
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
    for item in sorted(
        items, key=lambda d: str(d.get("updated_at") or ""), reverse=True
    ):
        doc_no = _normalized_doc_number(str(item.get("document_number") or ""))
        name = _normalized_name(str(item.get("name") or ""))
        key = (
            f"doc:{doc_no}"
            if doc_no
            else (f"name:{name}" if name else f"id:{item.get('document_id','')}")
        )
        if key not in dedup:
            dedup[key] = item
    return list(dedup.values())


def _client_group_key(summary: dict[str, Any]) -> str:
    """Build grouping key for client-centric listing."""
    client_id = str(summary.get("client_id") or "").strip()
    if client_id:
        return f"client:{client_id}"
    doc_no = _normalized_doc_number(str(summary.get("document_number") or ""))
    if doc_no:
        return f"doc:{doc_no}"
    name = _normalized_name(str(summary.get("name") or ""))
    if name:
        return f"name:{name}"
    return f"id:{str(summary.get('document_id') or '').strip()}"


class CRMRepository:
    def __init__(self, app_root: Path) -> None:
        self.app_root = app_root
        self._fallback_dir = app_root / "runtime" / "crm_store"
        self._fallback_dir.mkdir(parents=True, exist_ok=True)
        self._clients_fallback_dir = app_root / "runtime" / "crm_clients"
        self._clients_fallback_dir.mkdir(parents=True, exist_ok=True)

        self._mongo_enabled = False
        self._collection: Any | None = None
        self._clients_collection: Any | None = None

        mongo_uri = os.getenv("MONGODB_URI", "").strip()
        mongo_db = os.getenv("MONGODB_DB", "ocr_mrz").strip() or "ocr_mrz"
        mongo_collection = (
            os.getenv("MONGODB_COLLECTION", "crm_documents").strip() or "crm_documents"
        )
        if mongo_uri and pymongo is not None:
            try:
                client: Any = pymongo.MongoClient(
                    mongo_uri, serverSelectionTimeoutMS=3000
                )
                client.admin.command("ping")
                self._collection = client[mongo_db][mongo_collection]
                self._clients_collection = client[mongo_db]["crm_clients"]
                self._collection.create_index("document_id", unique=True)
                self._collection.create_index("identifiers.document_number")
                self._collection.create_index("identifiers.nif_nie")
                self._collection.create_index("identifiers.passport")
                self._collection.create_index("identifiers.name")
                self._collection.create_index("updated_at")
                self._clients_collection.create_index("client_id", unique=True)
                self._clients_collection.create_index("updated_at")
                self._mongo_enabled = True
                LOGGER.info(
                    "CRMRepository using MongoDB: db=%s collection=%s",
                    mongo_db,
                    mongo_collection,
                )
            except Exception:
                LOGGER.exception(
                    "MongoDB connection failed. Falling back to local CRM store."
                )
                self._mongo_enabled = False
                self._collection = None
                self._clients_collection = None
        else:
            LOGGER.warning(
                "MONGODB_URI is not set or pymongo unavailable. Using local CRM store fallback."
            )

    def _fallback_path(self, document_id: str) -> Path:
        return self._fallback_dir / f"{document_id}.json"

    def _client_fallback_path(self, client_id: str) -> Path:
        return self._clients_fallback_dir / f"{client_id}.json"

    def _read_fallback(self, document_id: str) -> dict[str, Any] | None:
        path = self._fallback_path(document_id)
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            LOGGER.exception("Failed reading fallback CRM record: %s", path)
            return None
        return payload if isinstance(payload, dict) else None

    def _write_fallback(self, document_id: str, record: dict[str, Any]) -> None:
        path = self._fallback_path(document_id)
        path.write_text(
            json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def _read_client_fallback(self, client_id: str) -> dict[str, Any] | None:
        path = self._client_fallback_path(client_id)
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            LOGGER.exception("Failed reading fallback CRM client: %s", path)
            return None
        return payload if isinstance(payload, dict) else None

    def _write_client_fallback(self, client_id: str, record: dict[str, Any]) -> None:
        path = self._client_fallback_path(client_id)
        path.write_text(
            json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
        )

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
            self._collection.update_one(
                {"document_id": document_id}, {"$set": record}, upsert=True
            )
            return
        self._write_fallback(document_id, record)

    def _get_client(self, client_id: str) -> dict[str, Any] | None:
        if self._mongo_enabled and self._clients_collection is not None:
            doc = self._clients_collection.find_one(
                {"client_id": client_id}, {"_id": 0}
            )
            return dict(doc) if doc else None
        return self._read_client_fallback(client_id)

    def _save_client(self, record: dict[str, Any]) -> None:
        client_id = str(record.get("client_id") or "").strip()
        if not client_id:
            raise ValueError("client_id is required for CRM client save.")
        if self._mongo_enabled and self._clients_collection is not None:
            self._clients_collection.update_one(
                {"client_id": client_id},
                {"$set": record},
                upsert=True,
            )
            return
        self._write_client_fallback(client_id, record)

    def _client_identity_from_doc(self, record: dict[str, Any]) -> dict[str, str]:
        identifiers = record.get("identifiers") or {}
        return {
            "document_number": str(identifiers.get("document_number") or "").strip(),
            "nif_nie": str(identifiers.get("nif_nie") or "").strip(),
            "passport": str(identifiers.get("passport") or "").strip(),
            "name": str(identifiers.get("name") or "").strip(),
        }

    @staticmethod
    def _profile_sort_key(record: dict[str, Any]) -> tuple[int, str]:
        """Sort docs by profile quality then recency."""
        has_edited = bool(record.get("edited_payload"))
        status = str(record.get("status") or "").strip().lower()
        quality = 1 if has_edited or status == "confirmed" else 0
        return quality, str(record.get("updated_at") or "")

    def _build_profile_from_documents(self, docs: list[dict[str, Any]]) -> dict[str, Any]:
        """Aggregate a client profile by picking first non-empty value by priority."""
        profile: dict[str, Any] = {}
        for doc in sorted(docs, key=self._profile_sort_key, reverse=True):
            payload = doc.get("effective_payload") or doc.get("edited_payload") or {}
            if not isinstance(payload, dict):
                continue
            profile = _deep_merge_first_non_empty(profile, payload)
        return profile

    def _build_client_identities(
        self, docs: list[dict[str, Any]], profile_payload: dict[str, Any]
    ) -> dict[str, list[str]]:
        """Build unified client identities from linked documents and profile."""
        values: dict[str, set[str]] = {
            "document_number": set(),
            "nif_nie": set(),
            "passport": set(),
            "name": set(),
        }
        for doc in docs:
            ident = self._client_identity_from_doc(doc)
            for key, value in ident.items():
                if value:
                    values[key].add(value)
        profile_ident = _identifiers_from_payload(profile_payload)
        for key in values:
            profile_value = str(profile_ident.get(key) or "").strip()
            if profile_value:
                values[key].add(profile_value)
        return {key: sorted(val) for key, val in values.items() if val}

    def get_client(self, client_id: str) -> dict[str, Any] | None:
        """Return client entity by id."""
        key = str(client_id or "").strip()
        if not key:
            return None
        return self._get_client(key)

    def list_full_documents_by_client(self, client_id: str) -> list[dict[str, Any]]:
        """Return full CRM document records linked to a client."""
        key = str(client_id or "").strip()
        if not key:
            return []
        if self._mongo_enabled and self._collection is not None:
            docs = self._collection.find({"client_id": key}, {"_id": 0}).sort(
                "updated_at", -1
            )
            return [dict(doc) for doc in docs]

        result: list[dict[str, Any]] = []
        for path in self._fallback_dir.glob("*.json"):
            try:
                doc = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if str(doc.get("client_id") or "").strip() != key:
                continue
            if isinstance(doc, dict):
                result.append(doc)
        result.sort(key=lambda row: str(row.get("updated_at") or ""), reverse=True)
        return result

    def update_client_profile(
        self,
        client_id: str,
        profile_payload: dict[str, Any],
        *,
        profile_source_document_id: str = "",
        profile_merge_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Persist profile payload and derived fields for client."""
        key = str(client_id or "").strip()
        if not key:
            raise ValueError("client_id is required for update_client_profile.")
        existing = self._get_client(key)
        if not existing:
            raise ValueError(f"CRM client not found: {key}")
        docs = self.list_full_documents_by_client(key)
        now = _now_iso()
        identities = self._build_client_identities(docs, profile_payload)
        updated = {
            **existing,
            "profile_payload": profile_payload,
            "profile_source_document_id": str(profile_source_document_id or ""),
            "profile_updated_at": now,
            "documents_count": len(docs),
            "identities": identities,
            "updated_at": now,
        }
        if profile_merge_meta is not None:
            updated["profile_merge_meta"] = profile_merge_meta
        self._save_client(updated)
        return updated

    def delete_client(self, client_id: str) -> bool:
        """Delete only client entity record (documents handled separately)."""
        key = str(client_id or "").strip()
        if not key:
            return False
        if self._mongo_enabled and self._clients_collection is not None:
            result = self._clients_collection.delete_one({"client_id": key})
            return bool(result.deleted_count)
        path = self._client_fallback_path(key)
        if not path.exists():
            return False
        try:
            path.unlink()
            return True
        except Exception:
            LOGGER.exception("Failed deleting fallback CRM client entity: %s", path)
            return False

    def delete_documents_by_client(self, client_id: str) -> list[str]:
        """Delete all documents linked to client and return removed ids."""
        docs = self.list_full_documents_by_client(client_id)
        deleted_ids: list[str] = []
        for doc in docs:
            doc_id = str(doc.get("document_id") or "").strip()
            if not doc_id:
                continue
            if self._mongo_enabled and self._collection is not None:
                result = self._collection.delete_one({"document_id": doc_id})
                deleted = bool(result.deleted_count)
            else:
                path = self._fallback_path(doc_id)
                if not path.exists():
                    deleted = False
                else:
                    try:
                        path.unlink()
                        deleted = True
                    except Exception:
                        LOGGER.exception("Failed deleting fallback CRM record: %s", path)
                        deleted = False
            if deleted:
                deleted_ids.append(doc_id)
        return deleted_ids

    def ensure_client_entity(
        self,
        *,
        document_id: str,
        source_document_id: str = "",
    ) -> dict[str, Any]:
        """Create or update client entity and link requested documents to it."""
        primary_id = str(document_id or "").strip()
        source_id = str(source_document_id or "").strip()
        if not primary_id:
            raise ValueError("document_id is required for ensure_client_entity.")

        primary_doc = self._get(primary_id)
        if not primary_doc:
            raise ValueError(f"CRM document not found: {primary_id}")
        source_doc = self._get(source_id) if source_id else None

        primary_client_id = str(primary_doc.get("client_id") or "").strip()
        source_client_id = (
            str(source_doc.get("client_id") or "").strip() if source_doc else ""
        )
        client_id = primary_client_id or source_client_id or uuid.uuid4().hex

        aggregated_ids: set[str] = {primary_id}
        if source_doc:
            aggregated_ids.add(source_id)

        source_client: dict[str, Any] | None = None
        if source_client_id and source_client_id != client_id:
            source_client = self._get_client(source_client_id)
            if source_client:
                for doc_id in source_client.get("document_ids") or []:
                    doc_id_str = str(doc_id or "").strip()
                    if doc_id_str:
                        aggregated_ids.add(doc_id_str)

        target_client = self._get_client(client_id) or {}
        for doc_id in target_client.get("document_ids") or []:
            doc_id_str = str(doc_id or "").strip()
            if doc_id_str:
                aggregated_ids.add(doc_id_str)

        linked_docs: list[dict[str, Any]] = []
        for doc_id in sorted(aggregated_ids):
            doc = self._get(doc_id)
            if not doc:
                continue
            doc["client_id"] = client_id
            self._save(doc)
            linked_docs.append(doc)

        now = _now_iso()
        display_name = ""
        identities: dict[str, set[str]] = {
            "document_number": set(),
            "nif_nie": set(),
            "passport": set(),
            "name": set(),
        }
        for doc in linked_docs:
            ident = self._client_identity_from_doc(doc)
            if not display_name and ident.get("name"):
                display_name = ident["name"]
            for key, value in ident.items():
                if value:
                    identities[key].add(value)

        client_record = {
            "client_id": client_id,
            "created_at": str(target_client.get("created_at") or now),
            "updated_at": now,
            "primary_document_id": primary_id,
            "display_name": display_name,
            "document_ids": sorted(
                str(doc.get("document_id") or "") for doc in linked_docs
            ),
            "documents_count": len(linked_docs),
            "profile_payload": target_client.get("profile_payload")
            if isinstance(target_client.get("profile_payload"), dict)
            else self._build_profile_from_documents(linked_docs),
            "profile_source_document_id": str(
                target_client.get("profile_source_document_id") or primary_id
            ),
            "profile_updated_at": str(target_client.get("profile_updated_at") or now),
            "profile_merge_meta": target_client.get("profile_merge_meta") or {},
            "identities": {
                key: sorted(values) for key, values in identities.items() if values
            },
        }
        client_record["identities"] = self._build_client_identities(
            linked_docs,
            client_record.get("profile_payload")
            if isinstance(client_record.get("profile_payload"), dict)
            else {},
        )
        self._save_client(client_record)

        if source_client_id and source_client_id != client_id:
            if self._mongo_enabled and self._clients_collection is not None:
                self._clients_collection.delete_one({"client_id": source_client_id})
            else:
                source_client_path = self._client_fallback_path(source_client_id)
                if source_client_path.exists():
                    try:
                        source_client_path.unlink()
                    except Exception:
                        LOGGER.exception(
                            "Failed deleting merged CRM client entity: %s",
                            source_client_path,
                        )

        return client_record

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
        merge_candidates: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        now = _now_iso()
        existing = self._get(document_id) or {}
        edited_payload = (
            existing.get("edited_payload")
            if isinstance(existing.get("edited_payload"), dict)
            else None
        )
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
            "identity_match_found": bool(
                identity_match_found or existing.get("identity_match_found")
            ),
            "identity_source_document_id": str(
                identity_source_document_id
                or existing.get("identity_source_document_id")
                or ""
            ),
            "client_match": existing.get("client_match") or {},
            "client_match_decision": str(
                existing.get("client_match_decision") or "none"
            ),
            "workflow_stage": str(existing.get("workflow_stage") or "review"),
            "enrichment_preview": (
                enrichment_preview
                if enrichment_preview is not None
                else (existing.get("enrichment_preview") or [])
            ),
            "enrichment_log": existing.get("enrichment_log") or {},
            "merge_candidates": (
                merge_candidates
                if merge_candidates is not None
                else (existing.get("merge_candidates") or [])
            ),
            "family_links": existing.get("family_links") or [],
            "family_reference": existing.get("family_reference") or {},
            "client_id": str(existing.get("client_id") or ""),
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
            "missing_fields": (
                missing_fields
                if missing_fields is not None
                else (existing.get("missing_fields") or [])
            ),
            "manual_steps_required": existing.get("manual_steps_required") or [],
            "form_url": str(existing.get("form_url") or ""),
            "target_url": str(existing.get("target_url") or ""),
            "browser_session_id": str(existing.get("browser_session_id") or ""),
            "identity_match_found": bool(existing.get("identity_match_found")),
            "identity_source_document_id": str(
                existing.get("identity_source_document_id") or ""
            ),
            "client_match": existing.get("client_match") or {},
            "client_match_decision": str(
                existing.get("client_match_decision") or "none"
            ),
            "workflow_stage": str(existing.get("workflow_stage") or "prepare"),
            "enrichment_preview": existing.get("enrichment_preview") or [],
            "enrichment_log": existing.get("enrichment_log") or {},
            "merge_candidates": existing.get("merge_candidates") or [],
            "family_links": existing.get("family_links") or [],
            "family_reference": existing.get("family_reference") or {},
            "client_id": str(existing.get("client_id") or ""),
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

    def update_document_fields(
        self, document_id: str, updates: dict[str, Any]
    ) -> dict[str, Any]:
        existing = self._get(document_id) or {
            "document_id": document_id,
            "created_at": _now_iso(),
        }
        existing.update(updates or {})
        existing["document_id"] = document_id
        existing["updated_at"] = _now_iso()
        self._save(existing)
        return existing

    def search_documents(
        self, query: str = "", limit: int = 30, dedupe: bool = True
    ) -> list[dict[str, Any]]:
        q = (query or "").strip()
        limit = max(1, min(int(limit or 30), 200))
        if self._mongo_enabled and self._collection is not None:
            filter_doc: dict[str, Any] = {
                "$or": [
                    {"merged_into_document_id": {"$exists": False}},
                    {"merged_into_document_id": ""},
                ]
            }
            if q:
                regex = {"$regex": re.escape(q), "$options": "i"}
                filter_doc = {
                    "$and": [
                        filter_doc,
                        {
                            "$or": [
                                {"identifiers.name": regex},
                                {"identifiers.document_number": regex},
                                {"identifiers.nif_nie": regex},
                                {"identifiers.passport": regex},
                            ]
                        },
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
            if dedupe:
                return _dedupe_summaries(summaries)[:limit]
            return summaries[:limit]

        results: list[dict[str, Any]] = []
        for path in self._fallback_dir.glob("*.json"):
            try:
                doc = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if str(doc.get("merged_into_document_id") or "").strip():
                continue
            summary = _summary_from_record(doc)
            if q:
                hay = f"{summary.get('name', '')} {summary.get('document_number', '')}".lower()
                if q.lower() not in hay:
                    continue
            results.append(summary)
        results.sort(key=lambda d: str(d.get("updated_at") or ""), reverse=True)
        if dedupe:
            return _dedupe_summaries(results)[:limit]
        return results[:limit]

    def list_documents_by_client(
        self,
        client_id: str,
        *,
        limit: int = 200,
        include_merged: bool = True,
    ) -> list[dict[str, Any]]:
        """Return documents linked to a client entity."""
        key = str(client_id or "").strip()
        if not key:
            return []
        limit = max(1, min(int(limit or 200), 500))

        if self._mongo_enabled and self._collection is not None:
            filter_doc: dict[str, Any] = {"client_id": key}
            if not include_merged:
                filter_doc["$or"] = [
                    {"merged_into_document_id": {"$exists": False}},
                    {"merged_into_document_id": ""},
                ]
            docs = (
                self._collection.find(
                    filter_doc,
                    {
                        "_id": 0,
                        "document_id": 1,
                        "client_id": 1,
                        "merged_into_document_id": 1,
                        "identifiers": 1,
                        "updated_at": 1,
                        "status": 1,
                        "edited_payload": 1,
                    },
                )
                .sort("updated_at", -1)
                .limit(limit)
            )
            return [_summary_from_record(dict(doc)) for doc in docs]

        results: list[dict[str, Any]] = []
        for path in self._fallback_dir.glob("*.json"):
            try:
                doc = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if str(doc.get("client_id") or "").strip() != key:
                continue
            if (not include_merged) and str(
                doc.get("merged_into_document_id") or ""
            ).strip():
                continue
            results.append(_summary_from_record(doc))
        results.sort(key=lambda d: str(d.get("updated_at") or ""), reverse=True)
        return results[:limit]

    def list_clients(self, query: str = "", limit: int = 100) -> list[dict[str, Any]]:
        """Return client-centric summaries (one row per client/group)."""
        client_items: list[dict[str, Any]] = []
        if self._mongo_enabled and self._clients_collection is not None:
            cursor = self._clients_collection.find({}, {"_id": 0}).sort("updated_at", -1)
            for row in cursor:
                client = dict(row)
                identities = client.get("identities") or {}
                document_number = str(
                    (identities.get("document_number") or [""])[0] or ""
                ).strip()
                name = str(
                    client.get("display_name")
                    or (identities.get("name") or [""])[0]
                    or ""
                ).strip()
                if query.strip():
                    hay = f"{name} {document_number}".lower()
                    if query.strip().lower() not in hay:
                        continue
                client_items.append(
                    {
                        "document_id": str(client.get("primary_document_id") or ""),
                        "primary_document_id": str(
                            client.get("primary_document_id") or ""
                        ),
                        "client_id": str(client.get("client_id") or ""),
                        "document_number": document_number,
                        "name": name,
                        "updated_at": str(client.get("updated_at") or ""),
                        "status": "review",
                        "has_edited": bool(client.get("profile_updated_at")),
                        "documents_count": int(client.get("documents_count") or 0),
                    }
                )
        else:
            for path in self._clients_fallback_dir.glob("*.json"):
                try:
                    client = json.loads(path.read_text(encoding="utf-8"))
                except Exception:
                    continue
                if not isinstance(client, dict):
                    continue
                identities = client.get("identities") or {}
                document_number = str(
                    (identities.get("document_number") or [""])[0] or ""
                ).strip()
                name = str(
                    client.get("display_name")
                    or (identities.get("name") or [""])[0]
                    or ""
                ).strip()
                if query.strip():
                    hay = f"{name} {document_number}".lower()
                    if query.strip().lower() not in hay:
                        continue
                client_items.append(
                    {
                        "document_id": str(client.get("primary_document_id") or ""),
                        "primary_document_id": str(
                            client.get("primary_document_id") or ""
                        ),
                        "client_id": str(client.get("client_id") or ""),
                        "document_number": document_number,
                        "name": name,
                        "updated_at": str(client.get("updated_at") or ""),
                        "status": "review",
                        "has_edited": bool(client.get("profile_updated_at")),
                        "documents_count": int(client.get("documents_count") or 0),
                    }
                )
        if client_items:
            client_items.sort(
                key=lambda row: str(row.get("updated_at") or ""), reverse=True
            )
            return client_items[: max(1, min(int(limit or 100), 500))]

        summaries = self.search_documents(
            query=query, limit=max(limit * 4, 1000), dedupe=False
        )
        groups: dict[str, list[dict[str, Any]]] = {}
        for summary in summaries:
            key = _client_group_key(summary)
            groups.setdefault(key, []).append(summary)

        items: list[dict[str, Any]] = []
        for _, docs in groups.items():
            docs_sorted = sorted(
                docs,
                key=lambda row: str(row.get("updated_at") or ""),
                reverse=True,
            )
            primary = docs_sorted[0]
            items.append(
                {
                    "document_id": str(primary.get("document_id") or ""),
                    "primary_document_id": str(primary.get("document_id") or ""),
                    "client_id": str(primary.get("client_id") or ""),
                    "document_number": str(primary.get("document_number") or ""),
                    "name": str(primary.get("name") or ""),
                    "updated_at": str(primary.get("updated_at") or ""),
                    "status": str(primary.get("status") or "unknown"),
                    "has_edited": bool(primary.get("has_edited")),
                    "documents_count": len(docs),
                }
            )
        items.sort(key=lambda row: str(row.get("updated_at") or ""), reverse=True)
        return items[: max(1, min(int(limit or 100), 500))]

    def find_latest_by_identity(
        self, document_number: str, exclude_document_id: str = ""
    ) -> dict[str, Any] | None:
        return self.find_latest_by_identities(
            [document_number], exclude_document_id=exclude_document_id
        )

    def find_latest_by_identities(
        self, candidates: list[str], exclude_document_id: str = ""
    ) -> dict[str, Any] | None:
        normalized = [_normalized_doc_number(v) for v in (candidates or [])]
        keys = [v for v in normalized if v]
        if not keys:
            return None

        exclude = str(exclude_document_id or "").strip()
        if self._mongo_enabled and self._collection is not None:
            docs = self._collection.find(
                {
                    "$or": [
                        {"identifiers.document_number": {"$exists": True, "$ne": ""}},
                        {"identifiers.nif_nie": {"$exists": True, "$ne": ""}},
                        {"identifiers.passport": {"$exists": True, "$ne": ""}},
                    ]
                },
                {"_id": 0},
            ).sort("updated_at", -1)
            for doc in docs:
                item = dict(doc)
                if exclude and str(item.get("document_id") or "") == exclude:
                    continue
                identifiers = item.get("identifiers") or {}
                current_values = [
                    _normalized_doc_number(
                        str(identifiers.get("document_number") or "")
                    ),
                    _normalized_doc_number(str(identifiers.get("nif_nie") or "")),
                    _normalized_doc_number(str(identifiers.get("passport") or "")),
                ]
                if any(v and v in keys for v in current_values):
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
            identifiers = doc.get("identifiers") or {}
            current_values = [
                _normalized_doc_number(str(identifiers.get("document_number") or "")),
                _normalized_doc_number(str(identifiers.get("nif_nie") or "")),
                _normalized_doc_number(str(identifiers.get("passport") or "")),
            ]
            if any(v and v in keys for v in current_values):
                records.append(doc)
        if not records:
            return None
        records.sort(key=lambda d: str(d.get("updated_at") or ""), reverse=True)
        return records[0]

    def delete_document(self, document_id: str) -> bool:
        doc_id = str(document_id or "").strip()
        if not doc_id:
            return False
        existing = self._get(doc_id) or {}
        client_id = str(existing.get("client_id") or "").strip()
        if self._mongo_enabled and self._collection is not None:
            result = self._collection.delete_one({"document_id": doc_id})
            deleted = bool(result.deleted_count)
        else:
            path = self._fallback_path(doc_id)
            if not path.exists():
                deleted = False
            else:
                try:
                    path.unlink()
                    deleted = True
                except Exception:
                    LOGGER.exception("Failed deleting fallback CRM record: %s", path)
                    deleted = False

        if deleted and client_id:
            client = self._get_client(client_id)
            if client:
                doc_ids = [
                    str(value).strip()
                    for value in (client.get("document_ids") or [])
                    if str(value).strip() and str(value).strip() != doc_id
                ]
                if doc_ids:
                    client["document_ids"] = sorted(doc_ids)
                    client["documents_count"] = len(doc_ids)
                    if str(client.get("primary_document_id") or "").strip() == doc_id:
                        client["primary_document_id"] = doc_ids[0]
                    docs = self.list_full_documents_by_client(client_id)
                    profile_payload = client.get("profile_payload")
                    if not isinstance(profile_payload, dict) or not profile_payload:
                        profile_payload = self._build_profile_from_documents(docs)
                    client["identities"] = self._build_client_identities(
                        docs,
                        profile_payload if isinstance(profile_payload, dict) else {},
                    )
                    client["updated_at"] = _now_iso()
                    self._save_client(client)
                else:
                    self.delete_client(client_id)
        return deleted
