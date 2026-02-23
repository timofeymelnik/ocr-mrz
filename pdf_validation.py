from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import fitz  # PyMuPDF

from target_autofill import (
    CANONICAL_FIELD_KEYS,
    build_autofill_value_map,
    build_date_split_field_values,
    infer_pdf_checkbox_expected,
)


def _resolve_field_name(selector: str) -> str:
    raw = str(selector or "").strip()
    if raw.startswith("pdf:"):
        return raw[4:]
    return raw


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _is_checked(widget: fitz.Widget) -> bool:
    value = _normalize_text(getattr(widget, "field_value", ""))
    if isinstance(getattr(widget, "field_value", None), bool):
        return bool(getattr(widget, "field_value"))
    if not value:
        return False
    lowered = value.lower()
    return lowered not in {"off", "0", "false", "no", "none"}


def validate_filled_pdf_against_mapping(
    *,
    payload: dict[str, Any],
    filled_pdf_path: Path,
    mappings: list[dict[str, Any]],
) -> dict[str, Any]:
    if not filled_pdf_path.exists():
        raise FileNotFoundError(f"Filled PDF not found: {filled_pdf_path}")

    value_map = build_autofill_value_map(payload)
    field_report: list[dict[str, Any]] = []
    missing: list[str] = []
    unexpected: list[str] = []

    with fitz.open(str(filled_pdf_path)) as doc:
        widgets_by_name: dict[str, fitz.Widget] = {}
        for page in doc:
            for widget in page.widgets() or []:
                name = _normalize_text(widget.field_name)
                if name and name not in widgets_by_name:
                    widgets_by_name[name] = widget

        explicit_by_field: dict[str, str] = {}
        for item in mappings or []:
            selector = _normalize_text(item.get("selector"))
            key = _normalize_text(item.get("canonical_key"))
            if not selector or key not in CANONICAL_FIELD_KEYS:
                continue
            explicit_by_field[_resolve_field_name(selector)] = key

        date_split_values = build_date_split_field_values(doc, explicit_by_field, value_map)

        for item in mappings or []:
            selector = _normalize_text(item.get("selector"))
            key = _normalize_text(item.get("canonical_key"))
            if not selector or key not in CANONICAL_FIELD_KEYS:
                continue

            field_name = _resolve_field_name(selector)
            widget = widgets_by_name.get(field_name)
            if widget is None:
                field_report.append(
                    {
                        "selector": selector,
                        "canonical_key": key,
                        "expected": "",
                        "actual": "",
                        "ok": False,
                        "reason": "field_not_found_in_filled_pdf",
                    }
                )
                missing.append(selector)
                continue

            widget_type = _normalize_text(getattr(widget, "field_type_string", "")).lower()
            expected = date_split_values.get(field_name, value_map.get(key, ""))

            if "check" in widget_type:
                expected_bool = infer_pdf_checkbox_expected(field_name, key, value_map)
                if expected_bool is None:
                    expected_bool = bool(str(expected).strip())
                actual_bool = _is_checked(widget)
                ok = actual_bool == bool(expected_bool)
                reason = "" if ok else "checkbox_mismatch"
                field_report.append(
                    {
                        "selector": selector,
                        "canonical_key": key,
                        "expected": bool(expected_bool),
                        "actual": actual_bool,
                        "ok": ok,
                        "reason": reason,
                    }
                )
                if not ok:
                    missing.append(selector)
                continue

            actual_text = _normalize_text(getattr(widget, "field_value", ""))
            expected_text = _normalize_text(expected)
            ok = actual_text == expected_text
            reason = "" if ok else "value_mismatch"
            field_report.append(
                {
                    "selector": selector,
                    "canonical_key": key,
                    "expected": expected_text,
                    "actual": actual_text,
                    "ok": ok,
                    "reason": reason,
                }
            )
            if expected_text and not actual_text:
                missing.append(selector)
            if (not expected_text) and actual_text:
                unexpected.append(selector)

    total_checked = len(field_report)
    matched = sum(1 for row in field_report if row.get("ok"))
    mismatched = total_checked - matched
    unfilled_required = len(set(missing))
    matches = mismatched == 0 and unfilled_required == 0

    return {
        "status": "ok",
        "matches": matches,
        "field_report": field_report,
        "missing": sorted(set(missing)),
        "unexpected": sorted(set(unexpected)),
        "summary": {
            "total_checked": total_checked,
            "matched": matched,
            "mismatched": mismatched,
            "unfilled_required": unfilled_required,
        },
    }
