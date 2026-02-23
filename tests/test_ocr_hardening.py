from __future__ import annotations

from ocr import VisionOCRClient


def test_dedup_lines_and_merge_pages() -> None:
    text = "A\nB\nA\n\nB\nC"
    assert VisionOCRClient._dedup_lines(text) == "A\nB\nC"

    merged = VisionOCRClient._merge_page_lists(["A\nB", "X"], ["B\nC", "X\nY"])
    assert merged[0] == "A\nB\nC"
    assert merged[1] == "X\nY"


def test_informative_pdf_text_detection() -> None:
    assert VisionOCRClient._is_informative_pdf_text("TASA MODELO 790\nFORMA DE PAGO") is True
    assert VisionOCRClient._is_informative_pdf_text("short") is False

