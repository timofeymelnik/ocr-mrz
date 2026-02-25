from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List

import fitz  # PyMuPDF
from google.cloud import vision

LOGGER = logging.getLogger(__name__)


@dataclass
class OCRResult:
    full_text: str
    pages: List[str]
    mrz_candidates: List[str]
    ocr_source: str


class VisionOCRClient:
    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or os.getenv("GOOGLE_CLOUD_VISION_API_KEY")
        self.client: vision.ImageAnnotatorClient | None = None
        if self.api_key:
            self.client = vision.ImageAnnotatorClient(
                client_options={"api_key": self.api_key}
            )
        else:
            LOGGER.warning(
                "GOOGLE_CLOUD_VISION_API_KEY is not set. Vision OCR will be unavailable for image-only documents."
            )

    def extract_text(self, path: Path) -> OCRResult:
        suffix = path.suffix.lower()
        if suffix == ".pdf":
            text_layer_pages = self._extract_pdf_text_layer(path)
            text_layer_full = self._merge_pages_dedup(text_layer_pages)
            text_layer_informative = self._is_informative_pdf_text(text_layer_full)
            if text_layer_informative:
                LOGGER.info("Using PDF text-layer extractor for: %s", path.name)
                full_text = text_layer_full
                mrz_candidates = self._find_mrz_candidates(full_text)
                return OCRResult(
                    full_text=full_text,
                    pages=text_layer_pages,
                    mrz_candidates=mrz_candidates,
                    ocr_source="pdf_text_layer",
                )

            # OCR type #2 fallback for scanned PDFs with deterministic hybrid merge.
            scan_pages = self._extract_pdf_text(path)
            scan_full = self._merge_pages_dedup(scan_pages)
            if text_layer_full:
                pages = self._merge_page_lists(text_layer_pages, scan_pages)
                full_text = self._merge_pages_dedup(pages)
                source = "pdf_hybrid"
            else:
                pages = scan_pages
                full_text = scan_full
                source = "pdf_ocr_scan"
            mrz_candidates = self._find_mrz_candidates(full_text)
            return OCRResult(
                full_text=full_text,
                pages=pages,
                mrz_candidates=mrz_candidates,
                ocr_source=source,
            )
        elif suffix in {".jpg", ".jpeg", ".png"}:
            pages = [self._extract_image_text(path.read_bytes())]
            source = "image_ocr"
        else:
            raise ValueError(f"Unsupported file type: {path.suffix}")

        full_text = self._sanitize_text(self._merge_pages_dedup(pages))
        mrz_candidates = self._find_mrz_candidates(full_text)
        return OCRResult(
            full_text=full_text,
            pages=pages,
            mrz_candidates=mrz_candidates,
            ocr_source=source,
        )

    def _extract_pdf_text(self, path: Path) -> List[str]:
        LOGGER.info("Running OCR for PDF: %s", path.name)
        pages_text: List[str] = []
        pdf = fitz.open(path)
        try:
            for page in pdf:
                pix = page.get_pixmap(dpi=300, alpha=False)
                img_bytes = pix.tobytes("png")
                text = self._extract_image_text(img_bytes)
                pages_text.append(self._sanitize_text(text))
        finally:
            pdf.close()
        return pages_text

    def _extract_pdf_text_layer(self, path: Path) -> List[str]:
        LOGGER.info("Extracting text-layer for PDF: %s", path.name)
        pages_text: List[str] = []
        pdf = fitz.open(path)
        try:
            for page in pdf:
                text_layer = page.get_text("text") or ""
                widgets_text: List[str] = []
                widgets = page.widgets() or []
                for w in widgets:
                    field_name = (w.field_name or "").strip()
                    field_value = (
                        (w.field_value or "").strip()
                        if hasattr(w, "field_value")
                        else ""
                    )
                    if field_name or field_value:
                        widgets_text.append(f"{field_name}: {field_value}".strip(": "))
                merged_page_text = "\n".join(
                    [
                        text_layer.strip(),
                        "\n".join(widgets_text).strip(),
                    ]
                ).strip()
                pages_text.append(self._sanitize_text(merged_page_text))
        finally:
            pdf.close()
        return pages_text

    def _extract_image_text(self, image_bytes: bytes) -> str:
        if not self.client:
            raise RuntimeError(
                "Vision OCR client is unavailable. Set GOOGLE_CLOUD_VISION_API_KEY for image/scanned document OCR."
            )
        image = vision.Image(content=image_bytes)
        response = self.client.document_text_detection(image=image)
        if response.error.message:
            raise RuntimeError(f"Vision API error: {response.error.message}")
        text = (
            response.full_text_annotation.text if response.full_text_annotation else ""
        )
        return self._sanitize_text(text)

    @staticmethod
    def _sanitize_text(text: str) -> str:
        # Drops invalid surrogate code points that break JSON/UTF-8 writes.
        return (
            (text or "")
            .encode("utf-8", errors="replace")
            .decode("utf-8", errors="replace")
        )

    @staticmethod
    def _looks_like_form_pdf(text: str) -> bool:
        up = (text or "").upper()
        markers = [
            "TASA MODELO 790",
            "N.I.F./N.I.E",
            "APELLIDOS Y NOMBRE O RAZÓN SOCIAL",
            "FORMA DE PAGO",
            "DESCARGAR IMPRESO RELLENADO",
        ]
        return sum(1 for marker in markers if marker in up) >= 2

    @staticmethod
    def _is_informative_pdf_text(text: str) -> bool:
        if not text:
            return False
        if len(text) >= 180:
            return True
        return VisionOCRClient._looks_like_form_pdf(text)

    @staticmethod
    def _dedup_lines(text: str) -> str:
        seen: set[str] = set()
        out: list[str] = []
        for raw in (text or "").splitlines():
            line = (raw or "").strip()
            if not line:
                continue
            key = line.upper()
            if key in seen:
                continue
            seen.add(key)
            out.append(line)
        return "\n".join(out)

    @classmethod
    def _merge_pages_dedup(cls, pages: List[str]) -> str:
        merged = "\n".join((p or "").strip() for p in pages if (p or "").strip())
        return cls._sanitize_text(cls._dedup_lines(merged))

    @classmethod
    def _merge_page_lists(cls, first: List[str], second: List[str]) -> List[str]:
        total = max(len(first), len(second))
        out: List[str] = []
        for idx in range(total):
            p1 = first[idx] if idx < len(first) else ""
            p2 = second[idx] if idx < len(second) else ""
            out.append(cls._dedup_lines(f"{p1}\n{p2}"))
        return out

    @staticmethod
    def _find_mrz_candidates(text: str) -> List[str]:
        candidates: List[str] = []
        for raw_line in text.splitlines():
            line = "".join(ch for ch in raw_line.upper() if ch.isalnum() or ch == "<")
            if len(line) >= 20 and line.count("<") >= 2:
                candidates.append(line)
        return candidates
