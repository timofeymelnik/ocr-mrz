"""Workflow helpers for operator document pipeline."""

from __future__ import annotations

from typing import Any

WORKFLOW_UPLOAD = "upload"
WORKFLOW_CLIENT_MATCH = "client_match"
WORKFLOW_REVIEW = "review"
WORKFLOW_PREPARE = "prepare"
WORKFLOW_AUTOFILL = "autofill"

WORKFLOW_NEXT_STEP_BY_STAGE: dict[str, str] = {
    WORKFLOW_UPLOAD: WORKFLOW_CLIENT_MATCH,
    WORKFLOW_CLIENT_MATCH: WORKFLOW_REVIEW,
    WORKFLOW_REVIEW: WORKFLOW_PREPARE,
    WORKFLOW_PREPARE: WORKFLOW_AUTOFILL,
    WORKFLOW_AUTOFILL: WORKFLOW_AUTOFILL,
}


def stage_to_next_step(stage: str) -> str:
    """Return next workflow step for current stage."""
    normalized_stage = (stage or "").strip().lower()
    return WORKFLOW_NEXT_STEP_BY_STAGE.get(normalized_stage, WORKFLOW_REVIEW)


def resolve_workflow_stage(record: dict[str, Any]) -> str:
    """Resolve workflow stage from persisted record fields."""
    explicit_stage = str(record.get("workflow_stage") or "").strip().lower()
    if explicit_stage in WORKFLOW_NEXT_STEP_BY_STAGE:
        return explicit_stage

    if (
        bool(record.get("identity_match_found"))
        and str(record.get("client_match_decision") or "pending").strip().lower()
        == "pending"
    ):
        return WORKFLOW_CLIENT_MATCH

    status = str(record.get("status") or "").strip().lower()
    if status == "confirmed":
        return WORKFLOW_PREPARE
    if status == "autofill_done":
        return WORKFLOW_AUTOFILL
    return WORKFLOW_REVIEW
