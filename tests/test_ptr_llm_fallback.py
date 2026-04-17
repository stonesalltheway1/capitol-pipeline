"""Tests for the Haiku 4.5 PTR LLM fallback. Never calls the real API."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from capitol_pipeline.parsers import ptr_llm_fallback
from capitol_pipeline.parsers.ptr_llm_fallback import (
    TRANSACTIONS_SCHEMA,
    _estimate_confidence,
    extract_via_haiku,
)


def test_schema_exports_have_expected_shape() -> None:
    assert TRANSACTIONS_SCHEMA["type"] == "object"
    assert "transactions" in TRANSACTIONS_SCHEMA["properties"]
    item_props = TRANSACTIONS_SCHEMA["properties"]["transactions"]["items"]["properties"]
    for field in (
        "ticker",
        "asset_description",
        "transaction_type",
        "transaction_date",
        "notification_date",
        "amount_min",
        "amount_max",
        "owner",
        "asset_type",
    ):
        assert field in item_props, f"missing schema field: {field}"


def test_kill_switch_disabled_returns_empty(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PTR_LLM_FALLBACK_ENABLED", "false")
    pdf = tmp_path / "any.pdf"
    pdf.write_bytes(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")

    result = extract_via_haiku(pdf)

    assert result["transactions"] == []
    assert result["confidence"] == 0.0
    assert "disabled" in result["parser_notes"].lower()
    assert result["usage"] == {"input": 0, "cached_input": 0, "output": 0}


def test_missing_api_key_returns_empty(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PTR_LLM_FALLBACK_ENABLED", "true")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    pdf = tmp_path / "any.pdf"
    pdf.write_bytes(b"%PDF-1.4\n")

    result = extract_via_haiku(pdf)

    assert result["transactions"] == []
    assert result["confidence"] == 0.0


def test_missing_pdf_returns_empty(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PTR_LLM_FALLBACK_ENABLED", "true")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")

    result = extract_via_haiku(tmp_path / "does-not-exist.pdf")

    assert result["transactions"] == []
    assert "pdf not found" in result["parser_notes"]


def test_pdf_too_large_returns_empty(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PTR_LLM_FALLBACK_ENABLED", "true")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setattr(ptr_llm_fallback, "MAX_PDF_BYTES", 100)
    pdf = tmp_path / "big.pdf"
    pdf.write_bytes(b"x" * 200)

    result = extract_via_haiku(pdf)

    assert result["transactions"] == []
    assert "too large" in result["parser_notes"]


def test_estimate_confidence_tiers() -> None:
    assert _estimate_confidence([], "") == 0.0
    assert _estimate_confidence([{"asset_description": "x"}], "") == 1.0
    assert _estimate_confidence([{"asset_description": "x"}], "row 3 notification date unclear") == 0.7
    assert _estimate_confidence([{"asset_description": "x"}], "Could not read most of the form") == 0.4


def test_extract_uses_mocked_client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PTR_LLM_FALLBACK_ENABLED", "true")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setattr(ptr_llm_fallback, "_client", None)

    pdf = tmp_path / "mini.pdf"
    pdf.write_bytes(b"%PDF-1.4\n%EOF\n")

    captured_kwargs: dict = {}

    class _Block:
        def __init__(self, type_: str, payload: dict) -> None:
            self.type = type_
            self.input = payload

    class _Usage:
        input_tokens = 1200
        cache_read_input_tokens = 1000
        output_tokens = 350

    class _Response:
        content = [
            _Block(
                "tool_use",
                {
                    "transactions": [
                        {
                            "asset_description": "Apple Inc.",
                            "ticker": "AAPL",
                            "transaction_type": "purchase",
                            "transaction_date": "2026-03-01",
                            "notification_date": "2026-03-02",
                            "amount_min": 1001,
                            "amount_max": 15000,
                            "owner": "self",
                            "asset_type": "Stock",
                        }
                    ],
                    "parser_notes": "",
                },
            )
        ]
        usage = _Usage()

    class _Messages:
        def create(self, **kwargs):  # type: ignore[no-untyped-def]
            captured_kwargs.update(kwargs)
            return _Response()

    class _FakeClient:
        messages = _Messages()

    monkeypatch.setattr(ptr_llm_fallback, "_client_once", lambda: _FakeClient())

    result = extract_via_haiku(pdf)

    assert len(result["transactions"]) == 1
    assert result["transactions"][0]["ticker"] == "AAPL"
    assert result["confidence"] == 1.0
    assert result["usage"] == {"input": 1200, "cached_input": 1000, "output": 350}
    assert captured_kwargs["model"] == ptr_llm_fallback.MODEL_ID
    assert captured_kwargs["tool_choice"]["name"] == "record_transactions"
    assert captured_kwargs["system"][0]["cache_control"] == {"type": "ephemeral"}


def test_api_exception_returns_empty(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PTR_LLM_FALLBACK_ENABLED", "true")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")

    class _BoomClient:
        class messages:  # noqa: N801
            @staticmethod
            def create(**_kwargs):  # type: ignore[no-untyped-def]
                raise RuntimeError("boom")

    monkeypatch.setattr(ptr_llm_fallback, "_client_once", lambda: _BoomClient())

    pdf = tmp_path / "mini.pdf"
    pdf.write_bytes(b"%PDF-1.4\n")

    result = extract_via_haiku(pdf)

    assert result["transactions"] == []
    assert result["confidence"] == 0.0
    assert "api error" in result["parser_notes"]
