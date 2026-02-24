from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from app.io.json_store import append_documents_jsonl
from app.ocr_extract.ocr import VisionOCRClient
from app.pipeline.runner import attach_pipeline_metadata, stage_failed, stage_start, stage_success
from app.data_builder.data_builder import build_tasa_document

INPUT_DIR = Path("./input")
OUTPUT_JSONL = Path("./output.jsonl")
DEBUG_OCR_DIR = Path("./debug_ocr")
OVERRIDES_DIR = Path("./overrides")
GEOCODE_DIR = Path("./geocode")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def list_input_files(input_dir: Path) -> list[Path]:
    allowed = {".jpg", ".jpeg", ".png", ".pdf"}
    return sorted([p for p in input_dir.iterdir() if p.is_file() and p.suffix.lower() in allowed])


def _load_json(path: Path) -> dict[str, Any] | list[dict[str, Any]] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logging.getLogger("ocr_main").exception("Failed to parse JSON: %s", path)
        return None


def _sanitize_text(text: str) -> str:
    return (text or "").encode("utf-8", errors="replace").decode("utf-8", errors="replace")


def _safe_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    content = json.dumps(payload, ensure_ascii=False, indent=2)
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)


def _doc_key_from_name(name: str) -> str:
    stem = Path(name).stem
    stem = re.sub(r"(?i)([_\- ]?(front|frente|anverso|face1|obverse))$", "", stem)
    stem = re.sub(r"(?i)([_\- ]?(back|reverso|dorso|face2|reverse))$", "", stem)
    return stem.strip() or Path(name).stem


def _side_from_name(name: str) -> str:
    lower = name.lower()
    if any(tok in lower for tok in ["back", "reverso", "dorso", "face2", "reverse"]):
        return "back"
    return "front"


def group_files_by_document(files: list[Path]) -> dict[str, dict[str, Path]]:
    groups: dict[str, dict[str, Path]] = {}
    for file_path in files:
        key = _doc_key_from_name(file_path.name)
        side = _side_from_name(file_path.name)
        groups.setdefault(key, {})
        groups[key][side] = file_path
    return groups


def main() -> None:
    load_dotenv()
    setup_logging()
    logger = logging.getLogger("ocr_main")

    if not INPUT_DIR.exists():
        raise FileNotFoundError(f"Input directory not found: {INPUT_DIR.resolve()}")

    save_ocr_debug = os.getenv("SAVE_OCR_DEBUG", "0") == "1"
    tasa_code = os.getenv("TASA_CODE", "790_012")
    google_maps_api_key = os.getenv("GOOGLE_MAPS_API_KEY", "").strip()
    if not google_maps_api_key:
        google_maps_api_key = os.getenv("GOOGLE_CLOUD_VISION_API_KEY", "").strip()

    ocr_client = VisionOCRClient()
    files = list_input_files(INPUT_DIR)
    groups = group_files_by_document(files)

    total = len(groups)
    success = 0
    failed = 0
    docs: list[dict[str, Any]] = []

    def _build_doc_with_pipeline(
        *,
        ocr_front: str,
        ocr_back: str,
        source_file: str,
        source_files: list[str],
        user_overrides: dict[str, Any],
        geocode_candidates: list[dict[str, Any]] | None,
        ocr_stage_record: dict[str, Any],
    ) -> dict[str, Any]:
        parse_started = stage_start()
        try:
            doc = build_tasa_document(
                ocr_front=ocr_front,
                ocr_back=ocr_back,
                user_overrides=user_overrides,
                geocode_candidates=geocode_candidates,
                google_maps_api_key=google_maps_api_key,
                tasa_code=tasa_code,
                source_file=source_file,
            )
            parse_stage_record = stage_success(
                "parse_extract_map",
                parse_started,
                details={
                    "schema_version": doc.get("schema_version"),
                    "forms_available": sorted((doc.get("forms") or {}).keys()),
                },
            )
        except Exception as exc:
            parse_stage_record = stage_failed("parse_extract_map", parse_started, str(exc))
            raise

        crm_started = stage_start()
        try:
            crm_stage_record = stage_success("crm_mapping", crm_started)
            return attach_pipeline_metadata(
                document=doc,
                source_files=source_files,
                ocr_details={
                    "front_text_len": len(ocr_front or ""),
                    "back_text_len": len(ocr_back or ""),
                    "used_cached_ocr": bool(ocr_stage_record.get("details", {}).get("used_cached_ocr")),
                },
                parse_stage=parse_stage_record,
                crm_stage=crm_stage_record,
                ocr_stage=ocr_stage_record,
            )
        except Exception as exc:
            crm_stage_record = stage_failed("crm_mapping", crm_started, str(exc))
            raise RuntimeError(f"CRM mapping stage failed: {crm_stage_record}") from exc

    for doc_key, sides in groups.items():
        logger.info("Processing document group: %s", doc_key)
        ocr_started = stage_start()
        try:
            ocr_front = ""
            ocr_back = ""
            source_file = ""
            source_files = [p.name for p in sides.values()]
            if "front" in sides:
                front_result = ocr_client.extract_text(sides["front"])
                ocr_front = front_result.full_text
                source_file = sides["front"].name
                ocr_source = getattr(front_result, "ocr_source", "live")
            else:
                ocr_source = "live"
            if "back" in sides:
                back_result = ocr_client.extract_text(sides["back"])
                ocr_back = back_result.full_text
                if ocr_source == "live":
                    ocr_source = getattr(back_result, "ocr_source", "live")
                if not source_file:
                    source_file = sides["back"].name

            if save_ocr_debug:
                debug_path = DEBUG_OCR_DIR / f"{doc_key}.json"
                _safe_write_json(
                    debug_path,
                    {
                        "ocr_front": _sanitize_text(ocr_front),
                        "ocr_back": _sanitize_text(ocr_back),
                    },
                )

            overrides_raw = _load_json(OVERRIDES_DIR / f"{doc_key}.json")
            geocode_raw = _load_json(GEOCODE_DIR / f"{doc_key}.json")
            user_overrides = overrides_raw if isinstance(overrides_raw, dict) else {}
            geocode_candidates = geocode_raw if isinstance(geocode_raw, list) else None

            ocr_stage_record = stage_success(
                "ocr",
                ocr_started,
                details={"source": ocr_source, "used_cached_ocr": False},
            )
            doc = _build_doc_with_pipeline(
                ocr_front=ocr_front,
                ocr_back=ocr_back,
                source_file=source_file,
                source_files=source_files,
                user_overrides=user_overrides,
                geocode_candidates=geocode_candidates,
                ocr_stage_record=ocr_stage_record,
            )
            docs.append(doc)
            success += 1
        except Exception:
            logger.exception("Live OCR failed for document group: %s", doc_key)
            # Fallback to cached OCR text if present.
            try:
                cached_path = DEBUG_OCR_DIR / f"{doc_key}.json"
                if not cached_path.exists():
                    raise FileNotFoundError(f"Cached OCR not found: {cached_path}")
                cached = json.loads(cached_path.read_text(encoding="utf-8"))
                ocr_front = _sanitize_text(str(cached.get("ocr_front", "") or ""))
                ocr_back = _sanitize_text(str(cached.get("ocr_back", "") or ""))
                if not ocr_front and not ocr_back:
                    raise ValueError("Cached OCR file is empty.")

                overrides_raw = _load_json(OVERRIDES_DIR / f"{doc_key}.json")
                geocode_raw = _load_json(GEOCODE_DIR / f"{doc_key}.json")
                user_overrides = overrides_raw if isinstance(overrides_raw, dict) else {}
                geocode_candidates = geocode_raw if isinstance(geocode_raw, list) else None

                ocr_stage_record = stage_success(
                    "ocr",
                    ocr_started,
                    details={"source": "cache", "used_cached_ocr": True},
                )
                doc = _build_doc_with_pipeline(
                    ocr_front=ocr_front,
                    ocr_back=ocr_back,
                    source_file=source_file,
                    source_files=[p.name for p in sides.values()],
                    user_overrides=user_overrides,
                    geocode_candidates=geocode_candidates,
                    ocr_stage_record=ocr_stage_record,
                )
                docs.append(doc)
                success += 1
                logger.warning("Used cached OCR fallback for document group: %s", doc_key)
            except Exception:
                failed += 1
                logger.exception("Failed processing document group even with cached OCR: %s", doc_key)

    if docs:
        appended = append_documents_jsonl(OUTPUT_JSONL, docs)
        logger.info("Appended %s document(s) to %s", appended, OUTPUT_JSONL.resolve())
    else:
        logger.warning("No documents extracted. JSONL was not updated.")

    print("\nSummary statistics")
    print(f"total processed: {total}")
    print(f"success: {success}")
    print(f"failed: {failed}")


if __name__ == "__main__":
    main()
