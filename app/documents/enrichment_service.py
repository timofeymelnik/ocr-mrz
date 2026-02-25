"""Document enrichment and family-linking domain logic."""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from typing import Any, Callable

ENRICHMENT_PATHS: list[str] = [
    "identificacion.nif_nie",
    "identificacion.pasaporte",
    "identificacion.documento_tipo",
    "identificacion.nombre_apellidos",
    "identificacion.primer_apellido",
    "identificacion.segundo_apellido",
    "identificacion.nombre",
    "domicilio.tipo_via",
    "domicilio.nombre_via",
    "domicilio.numero",
    "domicilio.escalera",
    "domicilio.piso",
    "domicilio.puerta",
    "domicilio.telefono",
    "domicilio.municipio",
    "domicilio.provincia",
    "domicilio.cp",
    "declarante.localidad",
    "declarante.fecha",
    "declarante.fecha_dia",
    "declarante.fecha_mes",
    "declarante.fecha_anio",
    "ingreso.forma_pago",
    "ingreso.iban",
    "extra.email",
    "extra.fecha_nacimiento",
    "extra.fecha_nacimiento_dia",
    "extra.fecha_nacimiento_mes",
    "extra.fecha_nacimiento_anio",
    "extra.nacionalidad",
    "extra.pais_nacimiento",
    "extra.sexo",
    "extra.estado_civil",
    "extra.lugar_nacimiento",
    "extra.nombre_padre",
    "extra.nombre_madre",
    "extra.representante_legal",
    "extra.representante_documento",
    "extra.titulo_representante",
    "extra.hijos_escolarizacion_espana",
]


@dataclass(frozen=True)
class DocumentEnrichmentService:
    """Encapsulates merge/enrichment/family reference logic for documents."""

    repo: Any
    default_target_url: str
    safe_value: Callable[[Any], str]
    normalize_payload_for_form: Callable[[dict[str, Any]], dict[str, Any]]
    collect_validation_errors: Callable[[dict[str, Any], bool], list[str]]
    read_or_bootstrap_record: Callable[[str], dict[str, Any]]
    write_record: Callable[[str, dict[str, Any]], None]

    @staticmethod
    def normalize_identity(value: str) -> str:
        """Normalize identity value to uppercase alphanumeric form."""
        return re.sub(r"[^A-Z0-9]", "", (value or "").upper())

    @staticmethod
    def safe_payload_get(payload: dict[str, Any], path: str) -> str:
        """Read a nested payload value by dotted path as a trimmed string."""
        node: Any = payload
        for part in path.split("."):
            if not isinstance(node, dict):
                return ""
            node = node.get(part)
        if node is None:
            return ""
        return str(node).strip()

    @staticmethod
    def safe_payload_set(payload: dict[str, Any], path: str, value: str) -> None:
        """Write a nested payload value by dotted path, creating branches."""
        parts = path.split(".")
        node: Any = payload
        for part in parts[:-1]:
            if not isinstance(node.get(part), dict):
                node[part] = {}
            node = node[part]
        node[parts[-1]] = value

    def identity_candidates(self, payload: dict[str, Any]) -> list[str]:
        """Extract unique identity candidates from payload."""
        out: list[str] = []
        for path in ["identificacion.nif_nie", "identificacion.pasaporte"]:
            value = self.normalize_identity(self.safe_payload_get(payload, path))
            if value and value not in out:
                out.append(value)
        return out

    def split_full_name_simple(self, value: str) -> tuple[str, str, str]:
        """Split a full name into first surname, second surname, first name."""
        raw = self.safe_value(value)
        if not raw:
            return "", "", ""
        if "," in raw:
            left, right = [item.strip() for item in raw.split(",", 1)]
            parts = [part for part in re.split(r"\s+", left) if part]
            return (
                parts[0] if parts else "",
                " ".join(parts[1:]) if len(parts) > 1 else "",
                right,
            )
        parts = [part for part in re.split(r"\s+", raw) if part]
        if len(parts) == 1:
            return parts[0], "", ""
        if len(parts) == 2:
            return parts[0], "", parts[1]
        return parts[0], parts[1], " ".join(parts[2:])

    def family_reference_from_payload(self, payload: dict[str, Any]) -> dict[str, str]:
        """Extract family reference metadata from payload if available."""
        refs_raw = payload.get("referencias")
        refs: dict[str, Any] = refs_raw if isinstance(refs_raw, dict) else {}
        fam_raw = refs.get("familiar_que_da_derecho")
        fam: dict[str, Any] = fam_raw if isinstance(fam_raw, dict) else {}
        if not fam:
            return {}
        nif_nie = self.normalize_identity(self.safe_value(fam.get("nif_nie")))
        pasaporte = self.normalize_identity(self.safe_value(fam.get("pasaporte")))
        nombre_apellidos = self.safe_value(fam.get("nombre_apellidos"))
        primer_apellido = self.safe_value(fam.get("primer_apellido"))
        nombre = self.safe_value(fam.get("nombre"))
        if not nombre_apellidos:
            nombre_apellidos = " ".join(
                part for part in [primer_apellido, nombre] if part
            ).strip()
        document_number = nif_nie or pasaporte
        if not document_number:
            return {}
        return {
            "document_number": document_number,
            "nif_nie": nif_nie,
            "pasaporte": pasaporte,
            "nombre_apellidos": nombre_apellidos,
            "primer_apellido": primer_apellido,
            "nombre": nombre,
        }

    def build_family_payload(self, family_ref: dict[str, str]) -> dict[str, Any]:
        """Build normalized payload for auto-created family-related record."""
        first_last, second_last, first_name = self.split_full_name_simple(
            self.safe_value(family_ref.get("nombre_apellidos"))
        )
        primer_apellido = (
            self.safe_value(family_ref.get("primer_apellido")) or first_last
        )
        nombre = self.safe_value(family_ref.get("nombre")) or first_name
        payload = {
            "identificacion": {
                "nif_nie": self.safe_value(family_ref.get("nif_nie")),
                "pasaporte": self.safe_value(family_ref.get("pasaporte")),
                "documento_tipo": (
                    "pasaporte"
                    if self.safe_value(family_ref.get("pasaporte"))
                    and not self.safe_value(family_ref.get("nif_nie"))
                    else "nif_tie_nie_dni"
                ),
                "nombre_apellidos": self.safe_value(family_ref.get("nombre_apellidos")),
                "primer_apellido": primer_apellido,
                "segundo_apellido": second_last,
                "nombre": nombre,
            },
            "domicilio": {},
            "autoliquidacion": {
                "tipo": "principal",
                "num_justificante": "",
                "importe_complementaria": None,
            },
            "tramite": {},
            "declarante": {},
            "ingreso": {"forma_pago": "efectivo", "iban": ""},
            "extra": {},
            "captcha": {"manual": True},
            "download": {"dir": "./downloads", "filename_prefix": "family_related"},
        }
        return self.normalize_payload_for_form(payload)

    def merge_family_links(
        self, existing: list[dict[str, Any]], new_link: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Merge family links list without duplicates."""
        links: list[dict[str, Any]] = [row for row in existing if isinstance(row, dict)]
        key = (
            self.safe_value(new_link.get("related_document_id")),
            self.safe_value(new_link.get("relation")),
            self.safe_value(new_link.get("document_number")),
        )
        for row in links:
            row_key = (
                self.safe_value(row.get("related_document_id")),
                self.safe_value(row.get("relation")),
                self.safe_value(row.get("document_number")),
            )
            if row_key == key:
                return links
        links.append(new_link)
        return links

    def name_tokens(self, payload: dict[str, Any]) -> set[str]:
        """Build uppercase name token set used for merge candidate scoring."""
        parts = [
            self.safe_payload_get(payload, "identificacion.primer_apellido"),
            self.safe_payload_get(payload, "identificacion.segundo_apellido"),
            self.safe_payload_get(payload, "identificacion.nombre"),
            self.safe_payload_get(payload, "identificacion.nombre_apellidos"),
        ]
        joined = " ".join(parts).upper()
        return {token for token in re.split(r"[^A-Z0-9]+", joined) if len(token) >= 2}

    def enrich_payload_fill_empty(
        self,
        *,
        payload: dict[str, Any],
        source_payload: dict[str, Any],
        source_document_id: str,
        selected_fields: set[str] | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
        """Fill empty fields from source payload and collect applied/skipped rows."""
        applied: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        out = json.loads(json.dumps(payload, ensure_ascii=False))
        for path in ENRICHMENT_PATHS:
            if selected_fields is not None and path not in selected_fields:
                continue
            current = self.safe_payload_get(out, path)
            suggested = self.safe_payload_get(source_payload, path)
            if not suggested:
                continue
            if current:
                reason = (
                    "equal"
                    if self.safe_value(current).upper()
                    == self.safe_value(suggested).upper()
                    else "conflict"
                )
                skipped.append(
                    {
                        "field": path,
                        "current_value": current,
                        "suggested_value": suggested,
                        "source": source_document_id,
                        "reason": reason,
                    }
                )
                continue
            self.safe_payload_set(out, path, suggested)
            applied.append(
                {
                    "field": path,
                    "current_value": current,
                    "suggested_value": suggested,
                    "source": source_document_id,
                }
            )
        return out, applied, skipped

    def merge_candidates_for_payload(
        self, document_id: str, payload: dict[str, Any], *, limit: int = 10
    ) -> list[dict[str, Any]]:
        """Suggest merge candidates from CRM based on identity and name overlap."""
        target_ids = set(self.identity_candidates(payload))
        target_name_tokens = self.name_tokens(payload)
        out: list[dict[str, Any]] = []
        summaries = self.repo.search_documents(query="", limit=200)
        for item in summaries:
            candidate_id = self.safe_value(item.get("document_id"))
            if not candidate_id or candidate_id == document_id:
                continue
            crm_doc = self.repo.get_document(candidate_id) or {}
            source_payload = (
                crm_doc.get("effective_payload")
                or crm_doc.get("edited_payload")
                or crm_doc.get("ocr_payload")
                or {}
            )
            if not isinstance(source_payload, dict):
                continue
            candidate_ids = set(self.identity_candidates(source_payload))
            candidate_name_tokens = self.name_tokens(source_payload)
            identity_overlap = sorted(target_ids & candidate_ids)
            name_overlap = sorted(target_name_tokens & candidate_name_tokens)
            score = 0
            reasons: list[str] = []
            if identity_overlap:
                score += 100
                reasons.append("document_match")
            if len(name_overlap) >= 2:
                score += 40
                reasons.append("name_overlap")
            elif len(name_overlap) == 1:
                score += 15
                reasons.append("partial_name_overlap")
            if score <= 0:
                continue
            out.append(
                {
                    "document_id": candidate_id,
                    "name": self.safe_value(item.get("name")),
                    "document_number": self.safe_value(item.get("document_number")),
                    "updated_at": self.safe_value(item.get("updated_at")),
                    "score": score,
                    "reasons": reasons,
                    "identity_overlap": identity_overlap,
                    "name_overlap": name_overlap,
                }
            )
        out.sort(
            key=lambda row: (int(row.get("score") or 0), row.get("updated_at") or ""),
            reverse=True,
        )
        return out[:limit]

    def sync_family_reference(
        self, document_id: str, payload: dict[str, Any], source: dict[str, Any]
    ) -> dict[str, Any]:
        """Sync and/or create related family document and bidirectional links."""
        family_ref = self.family_reference_from_payload(payload)
        if not family_ref:
            return {"linked": False, "family_links": []}

        family_payload = self.build_family_payload(family_ref)
        identity_keys = [
            value
            for value in [
                self.safe_value(family_ref.get("nif_nie")),
                self.safe_value(family_ref.get("pasaporte")),
            ]
            if value
        ]
        linked_doc = self.repo.find_latest_by_identities(
            identity_keys, exclude_document_id=document_id
        )
        related_document_id = self.safe_value((linked_doc or {}).get("document_id"))
        created = False

        if related_document_id:
            linked_doc_payload = linked_doc or {}
            existing_payload = (
                linked_doc_payload.get("effective_payload")
                or linked_doc_payload.get("edited_payload")
                or linked_doc_payload.get("ocr_payload")
                or {}
            )
            if isinstance(existing_payload, dict):
                merged_payload, applied, _ = self.enrich_payload_fill_empty(
                    payload=existing_payload,
                    source_payload=family_payload,
                    source_document_id=document_id,
                )
                if applied:
                    self.repo.save_edited_payload(
                        document_id=related_document_id,
                        payload=merged_payload,
                        missing_fields=self.collect_validation_errors(
                            merged_payload, False
                        ),
                    )
        else:
            related_document_id = uuid.uuid4().hex
            created = True
            self.repo.upsert_from_upload(
                document_id=related_document_id,
                payload=family_payload,
                ocr_document={},
                source={
                    "source_kind": "family_reference_auto",
                    "origin_document_id": document_id,
                    "original_filename": self.safe_value(
                        source.get("original_filename")
                    ),
                    "stored_path": self.safe_value(source.get("stored_path")),
                    "preview_url": self.safe_value(source.get("preview_url")),
                },
                missing_fields=self.collect_validation_errors(family_payload, False),
                manual_steps_required=[
                    "verify_filled_fields",
                    "submit_or_download_manually",
                ],
                form_url=self.default_target_url,
                target_url=self.default_target_url,
            )

        forward_link = {
            "relation": "familiar_que_da_derecho",
            "related_document_id": related_document_id,
            "document_number": self.safe_value(family_ref.get("document_number")),
            "created_from_reference": created,
        }
        identities = self.identity_candidates(payload)
        backward_link = {
            "relation": "titular_familiar_dependiente",
            "related_document_id": document_id,
            "document_number": self.safe_value(identities[0] if identities else ""),
            "created_from_reference": False,
        }

        primary_doc = self.repo.get_document(document_id) or {}
        primary_links = self.merge_family_links(
            primary_doc.get("family_links") or [], forward_link
        )
        self.repo.update_document_fields(document_id, {"family_links": primary_links})

        if related_document_id:
            related_doc = self.repo.get_document(related_document_id) or {}
            related_links = self.merge_family_links(
                related_doc.get("family_links") or [], backward_link
            )
            self.repo.update_document_fields(
                related_document_id, {"family_links": related_links}
            )

        return {
            "linked": True,
            "related_document_id": related_document_id,
            "created": created,
            "family_links": primary_links,
            "family_reference": family_ref,
        }

    def enrich_record_payload_by_identity(
        self,
        document_id: str,
        payload: dict[str, Any],
        *,
        persist: bool = True,
        source_document_id: str = "",
        selected_fields: list[str] | None = None,
    ) -> dict[str, Any]:
        """Enrich payload from latest matching identity record."""
        identity_candidates = self.identity_candidates(payload)
        source_doc_id = self.safe_value(source_document_id)
        if not identity_candidates and not source_doc_id:
            return {
                "identity_match_found": False,
                "identity_source_document_id": "",
                "identity_key": "",
                "enrichment_preview": [],
                "applied_fields": [],
                "skipped_fields": [],
                "payload": payload,
            }

        source_record: dict[str, Any] | None = None
        if source_doc_id:
            source_record = self.repo.get_document(source_doc_id)
            if (
                not source_record
                or self.safe_value(source_record.get("document_id")) == document_id
            ):
                return {
                    "identity_match_found": False,
                    "identity_source_document_id": "",
                    "identity_key": (
                        identity_candidates[0] if identity_candidates else ""
                    ),
                    "enrichment_preview": [],
                    "applied_fields": [],
                    "skipped_fields": [],
                    "payload": payload,
                }
        elif identity_candidates:
            source_record = self.repo.find_latest_by_identities(
                identity_candidates, exclude_document_id=document_id
            )
        if not source_record:
            return {
                "identity_match_found": False,
                "identity_source_document_id": "",
                "identity_key": identity_candidates[0] if identity_candidates else "",
                "enrichment_preview": [],
                "applied_fields": [],
                "skipped_fields": [],
                "payload": payload,
            }

        source_payload = (
            source_record.get("effective_payload")
            or source_record.get("edited_payload")
            or source_record.get("ocr_payload")
            or {}
        )
        resolved_source_document_id = str(source_record.get("document_id") or "")
        source_candidates = self.identity_candidates(
            source_payload if isinstance(source_payload, dict) else {}
        )
        identity_key = next(
            (
                candidate
                for candidate in identity_candidates
                if candidate in source_candidates
            ),
            identity_candidates[0] if identity_candidates else "",
        )
        allowed_fields = {
            self.safe_value(field)
            for field in (selected_fields or [])
            if self.safe_value(field)
        }
        selected = allowed_fields if allowed_fields else None
        enriched, applied, skipped = self.enrich_payload_fill_empty(
            payload=payload,
            source_payload=source_payload if isinstance(source_payload, dict) else {},
            source_document_id=resolved_source_document_id,
            selected_fields=selected,
        )
        if persist:
            record = self.read_or_bootstrap_record(document_id)
            record["payload"] = enriched
            record["identity_key"] = identity_key
            record["identity_match_found"] = True
            record["identity_source_document_id"] = resolved_source_document_id
            record["enrichment_preview"] = applied
            record["enrichment_log"] = {
                "applied_fields": applied,
                "skipped_fields": skipped,
            }
            record["missing_fields"] = self.collect_validation_errors(enriched, False)
            self.write_record(document_id, record)
            self.repo.save_edited_payload(
                document_id=document_id,
                payload=enriched,
                missing_fields=record["missing_fields"],
            )
            self.repo.update_document_fields(
                document_id,
                {
                    "identity_key": identity_key,
                    "identity_match_found": True,
                    "identity_source_document_id": resolved_source_document_id,
                    "enrichment_preview": applied,
                    "enrichment_log": {
                        "applied_fields": applied,
                        "skipped_fields": skipped,
                    },
                },
            )
            if source_doc_id and source_doc_id != document_id:
                self.repo.update_document_fields(
                    source_doc_id,
                    {
                        "status": "merged",
                        "merged_into_document_id": document_id,
                    },
                )
        return {
            "identity_match_found": True,
            "identity_source_document_id": resolved_source_document_id,
            "identity_key": identity_key,
            "enrichment_preview": applied,
            "enrichment_skipped": skipped,
            "applied_fields": [row["field"] for row in applied],
            "skipped_fields": [row["field"] for row in skipped],
            "payload": enriched,
        }
