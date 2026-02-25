from __future__ import annotations

from app.documents.source_kind import detect_source_kind


def test_detect_source_kind_identifies_ex17_as_nie_tie() -> None:
    text = "\n".join(
        [
            "Solicitud de Tarjeta de Identidad",
            "de Extranjero (TIE)",
            "EX-17",
            "N.I.E.",
        ]
    )
    detection = detect_source_kind(text=text, filename="scan.pdf")

    assert detection.source_kind == "nie_tie"
    assert detection.requires_review is False
    assert detection.confidence >= 0.88


def test_detect_source_kind_fallback_still_returns_anketa() -> None:
    detection = detect_source_kind(text="random low-signal OCR", filename="scan.pdf")
    assert detection.source_kind == "anketa"
    assert detection.requires_review is True
