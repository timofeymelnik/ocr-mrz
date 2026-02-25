"""Template resolution service for browser autofill endpoints."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol


class FormMappingRepositoryProtocol(Protocol):
    """Protocol for mapping repository used by template resolver."""

    def get_latest_for_url(self, target_url: str) -> dict[str, Any] | None:
        """Return latest mapping template for URL when present."""


@dataclass
class TemplateResolution:
    """Resolved mapping template payload for a target URL."""

    is_valid: bool
    current_url: str
    template_source: str
    effective_mappings: list[dict[str, Any]]
    error_code: str = ""
    message: str = ""


class TemplateMappingService:
    """Service that resolves and validates latest template mappings for URL."""

    def __init__(
        self,
        *,
        form_mapping_repo: FormMappingRepositoryProtocol,
        safe_value: Callable[[Any], str],
        collect_validation_errors: Callable[[dict[str, Any], bool], list[str]],
        collect_validation_issues: Callable[
            [dict[str, Any], bool], list[dict[str, Any]]
        ],
    ) -> None:
        """Initialize service with explicit dependencies."""
        self._form_mapping_repo = form_mapping_repo
        self._safe_value = safe_value
        self._collect_validation_errors = collect_validation_errors
        self._collect_validation_issues = collect_validation_issues

    def resolve_for_url(self, current_url: str) -> TemplateResolution:
        """Resolve latest template and convert mappings to canonical shape."""
        template = self._form_mapping_repo.get_latest_for_url(current_url)
        if not template:
            return TemplateResolution(
                is_valid=False,
                current_url=current_url,
                template_source="",
                effective_mappings=[],
                error_code="TEMPLATE_NOT_FOUND",
                message="Template mapping not found for current URL.",
            )

        learned = list(template.get("mappings") or [])
        if not learned:
            return TemplateResolution(
                is_valid=False,
                current_url=current_url,
                template_source=self._safe_value(template.get("source")),
                effective_mappings=[],
                error_code="TEMPLATE_INVALID",
                message="Template has no mappings.",
            )

        merged_map: dict[str, dict[str, Any]] = {}
        for item in learned:
            selector = self._safe_value(item.get("selector"))
            if not selector:
                continue
            merged_map[selector] = {
                "selector": selector,
                "canonical_key": self._safe_value(item.get("canonical_key")),
                "field_kind": self._safe_value(item.get("field_kind")) or "text",
                "match_value": self._safe_value(item.get("match_value")),
                "checked_when": self._safe_value(item.get("checked_when")),
                "source": self._safe_value(item.get("source")) or "template",
                "confidence": float(item.get("confidence") or 0.99),
            }

        return TemplateResolution(
            is_valid=True,
            current_url=current_url,
            template_source=self._safe_value(template.get("source")),
            effective_mappings=list(merged_map.values()),
        )

    def build_template_response(
        self,
        *,
        document_id: str,
        current_url: str,
        payload: dict[str, Any],
        fill_strategy: str,
    ) -> tuple[int, dict[str, Any]]:
        """Build HTTP payload for template preview endpoint."""
        resolution = self.resolve_for_url(current_url)
        if not resolution.is_valid:
            return (
                422,
                {
                    "status": "error",
                    "error_code": resolution.error_code,
                    "document_id": document_id,
                    "message": resolution.message,
                    "form_url": current_url,
                },
            )

        missing_fields = self._collect_validation_errors(payload, False)
        validation_issues = self._collect_validation_issues(payload, False)
        return (
            200,
            {
                "document_id": document_id,
                "form_url": current_url,
                "fill_strategy": fill_strategy,
                "template_source": resolution.template_source,
                "effective_mappings": resolution.effective_mappings,
                "missing_fields": missing_fields,
                "validation_issues": validation_issues,
            },
        )
