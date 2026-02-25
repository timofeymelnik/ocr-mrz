from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import app.browser.session_manager as session_manager


class _FakePage:
    def __init__(self, outcomes: list[str], url: str = "about:blank") -> None:
        self._outcomes = outcomes
        self.url = url
        self.calls: list[str] = []

    def goto(self, target_url: str, wait_until: str, timeout: int) -> None:
        _ = target_url
        _ = timeout
        self.calls.append(wait_until)
        outcome = self._outcomes.pop(0)
        if outcome == "ok":
            self.url = "https://example.test/ok"
            return
        raise RuntimeError(outcome)


def test_session_manager_debug_safe_normalizes_strings() -> None:
    assert session_manager._debug_safe("A b/c") == "a_b_c"


def test_session_manager_navigate_with_fallback_succeeds_on_second_attempt() -> None:
    page = _FakePage(["timeout", "ok"])

    session_manager._navigate_with_fallback(
        cast(Any, page), "https://example.test", 1000
    )

    assert page.calls == ["domcontentloaded", "load"]


def test_session_manager_navigate_with_fallback_accepts_err_aborted_for_pdf() -> None:
    page = _FakePage(["ERR_ABORTED"], url="about:blank")

    session_manager._navigate_with_fallback(
        cast(Any, page), "https://example.test/doc.pdf", 1000
    )

    assert page.calls == ["domcontentloaded"]


def test_session_manager_navigate_with_fallback_raises_after_all_failures() -> None:
    page = _FakePage(["first", "second", "third"])

    try:
        session_manager._navigate_with_fallback(
            cast(Any, page), "https://example.test", 1000
        )
    except RuntimeError as exc:
        assert "Load failed for URL" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError after fallback exhaustion")


def test_session_manager_looks_like_pdf_url_short_circuit() -> None:
    assert session_manager._looks_like_pdf_url("https://example.test/file.pdf") is True


def test_session_manager_looks_like_pdf_url_detects_head_content_type(monkeypatch) -> None:
    def _fake_head(*args, **kwargs):
        _ = args
        _ = kwargs
        return SimpleNamespace(
            headers={"content-type": "application/pdf"},
            url="https://example.test/doc",
        )

    monkeypatch.setattr(session_manager.requests, "head", _fake_head)
    monkeypatch.setattr(
        session_manager.requests,
        "get",
        lambda *a, **k: SimpleNamespace(headers={}, url="https://example.test/doc"),
    )

    assert session_manager._looks_like_pdf_url("https://example.test/doc") is True


def test_session_manager_stop_playwright_if_idle_stops_instance() -> None:
    fake = SimpleNamespace(stopped=False)

    def _stop() -> None:
        fake.stopped = True

    fake.stop = _stop
    session_manager._PLAYWRIGHT = cast(Any, fake)
    session_manager._SESSIONS.clear()

    session_manager._stop_playwright_if_idle()

    assert fake.stopped is True
    assert session_manager._PLAYWRIGHT is None
