"""Tests for the Claude vision PTR path. Never calls the real API.

Every test patches ``ptr_vision._client_once`` (the module-level client
factory) so the request kwargs are inspectable and no network call is made.
The database exporter is never touched; only the pure status helper is.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from capitol_pipeline.bridges.capitol_exposed import build_trade_id
from capitol_pipeline.cli import resolve_house_stub_status
from capitol_pipeline.models.congress import FilingStub, HousePtrParseResult, MemberMatch
from capitol_pipeline.parsers import house_ptr, ptr_vision
from capitol_pipeline.parsers.house_ptr import parse_house_ptr_text
from capitol_pipeline.parsers.ptr_vision import (
    MODEL_ID,
    PTR_VISION_SCHEMA,
    VISION_PARSER_VERSION,
    build_vision_metadata,
    estimate_cost_usd,
    extract_via_vision,
    legibility_confidence,
    majority_illegible,
    summarize_legibility,
)

FORBIDDEN_SCHEMA_KEYWORDS = {"maxItems", "minItems", "minLength", "maxLength", "pattern"}

CHEVRON_TEXT = (
    "P T R Clerk of the House of Representatives\n"
    "F I Name: Hon. Roger Williams Status: Member State/District: TX25 "
    "T ID Owner Asset Transaction Type Date Notification Date Amount Cap. Gains > $200?\n"
    "Chevron Corporation Common Stock (CVX) [ST] S (partial) 12/22/2025 12/22/2025 "
    "$15,001 - $50,000\n"
    "Filing ID #20033783"
)

CHEVRON_VISION_ROW: dict[str, Any] = {
    "owner": None,
    "asset_description": "Chevron Corporation Common Stock",
    "ticker": "CVX",
    "asset_type_code": "ST",
    "transaction_type": "sale_partial",
    "transaction_date": "2025-12-22",
    "notification_date": "2025-12-22",
    "amount_min": 15001,
    "amount_max": 50000,
    "cap_gains_over_200": True,
    "comment": "Subholding Of: Charles Schwab 4067",
    "legibility": "clear",
}


# ---------------------------------------------------------------------------
# Fake Anthropic client
# ---------------------------------------------------------------------------


class _Block:
    def __init__(self, type_: str, *, text: str | None = None, payload: dict | None = None) -> None:
        self.type = type_
        if text is not None:
            self.text = text
        if payload is not None:
            self.input = payload


class _Usage:
    def __init__(self) -> None:
        self.input_tokens = 4_000
        self.cache_read_input_tokens = 2_000
        self.cache_creation_input_tokens = 1_000
        self.output_tokens = 800


class _Message:
    def __init__(self, payload: dict, *, stop_reason: str = "end_turn") -> None:
        self.content = [
            _Block("thinking"),
            _Block("text", text=json.dumps(payload)),
        ]
        self.stop_reason = stop_reason
        self.stop_details = None
        self.usage = _Usage()


class _StreamManager:
    def __init__(self, message: _Message) -> None:
        self._message = message

    def __enter__(self) -> "_StreamManager":
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False

    def get_final_message(self) -> _Message:
        return self._message


class _Messages:
    def __init__(self, message: _Message, captured: dict, calls: list[int]) -> None:
        self._message = message
        self._captured = captured
        self._calls = calls

    def stream(self, **kwargs: Any) -> _StreamManager:
        self._calls.append(1)
        self._captured.update(kwargs)
        return _StreamManager(self._message)


class _FakeClient:
    def __init__(self, message: _Message, captured: dict, calls: list[int]) -> None:
        self.messages = _Messages(message, captured, calls)


def _install_fake_client(
    monkeypatch: pytest.MonkeyPatch,
    payload: dict,
    *,
    stop_reason: str = "end_turn",
) -> tuple[dict, list[int]]:
    captured: dict = {}
    calls: list[int] = []
    client = _FakeClient(_Message(payload, stop_reason=stop_reason), captured, calls)
    monkeypatch.setattr(ptr_vision, "_client", None)
    monkeypatch.setattr(ptr_vision, "_client_once", lambda: client)
    return captured, calls


def _enable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CAPITOL_PTR_VISION_DISABLED", raising=False)
    monkeypatch.delenv("CAPITOL_PTR_VISION_MODEL", raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-not-a-real-key")


def _write_pdf(tmp_path: Path, name: str = "scan.pdf") -> Path:
    pdf = tmp_path / name
    pdf.write_bytes(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\ntrailer\n%%EOF\n")
    return pdf


def _stub() -> FilingStub:
    return FilingStub(
        doc_id="20033783",
        filing_year=2026,
        filing_date="2026-01-05",
        member=MemberMatch(
            id="m-roger-williams",
            name="Roger Williams",
            slug="roger-williams",
            party="R",
            state="TX",
            district="25",
        ),
        source="house-clerk",
        source_url="https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20033783.pdf",
    )


def _walk_schema_keys(node: Any) -> list[str]:
    keys: list[str] = []
    if isinstance(node, dict):
        for key, value in node.items():
            keys.append(key)
            keys.extend(_walk_schema_keys(value))
    elif isinstance(node, list):
        for item in node:
            keys.extend(_walk_schema_keys(item))
    return keys


# ---------------------------------------------------------------------------
# Schema shape
# ---------------------------------------------------------------------------


def test_schema_declares_every_required_field() -> None:
    assert PTR_VISION_SCHEMA["type"] == "object"
    assert PTR_VISION_SCHEMA["additionalProperties"] is False
    for field in ("filer_name", "filing_date", "page_count", "notes", "transactions"):
        assert field in PTR_VISION_SCHEMA["properties"]
        assert field in PTR_VISION_SCHEMA["required"]

    item = PTR_VISION_SCHEMA["properties"]["transactions"]["items"]
    for field in (
        "owner",
        "asset_description",
        "ticker",
        "asset_type_code",
        "transaction_type",
        "transaction_date",
        "notification_date",
        "amount_min",
        "amount_max",
        "cap_gains_over_200",
        "comment",
        "legibility",
    ):
        assert field in item["properties"], f"missing schema field: {field}"
        assert field in item["required"], f"schema field not required: {field}"

    assert item["properties"]["transaction_type"]["enum"] == [
        "purchase",
        "sale",
        "sale_partial",
        "exchange",
    ]
    assert item["properties"]["legibility"]["enum"] == ["clear", "partial", "illegible"]
    owner = item["properties"]["owner"]
    assert "enum" not in owner  # a nullable enum must be anyOf, not type+enum with null
    assert owner["anyOf"] == [
        {"type": "string", "enum": ["self", "spouse", "dependent", "joint"]},
        {"type": "null"},
    ]


def test_schema_avoids_keywords_structured_outputs_reject() -> None:
    used = set(_walk_schema_keys(PTR_VISION_SCHEMA))
    assert not (used & FORBIDDEN_SCHEMA_KEYWORDS)


def test_system_prompt_clears_the_minimum_cacheable_prefix() -> None:
    # Sonnet 5 caches nothing below 1,024 tokens; ~4 chars/token is the floor
    # this prompt has to clear for cache_control to do anything at all.
    assert len(ptr_vision.SYSTEM_PROMPT) > 4_500


# ---------------------------------------------------------------------------
# Request shape + good response
# ---------------------------------------------------------------------------


def test_good_response_parses_and_request_is_well_formed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    payload = {
        "filer_name": "Roger Williams",
        "filing_date": "2026-01-05",
        "page_count": 2,
        "notes": "Second page is a faxed continuation.",
        "transactions": [
            CHEVRON_VISION_ROW,
            {**CHEVRON_VISION_ROW, "ticker": None, "legibility": "partial"},
        ],
    }
    captured, calls = _install_fake_client(monkeypatch, payload)

    result = extract_via_vision(_write_pdf(tmp_path))

    assert calls == [1]
    assert result["ok"] is True
    assert result["skipped"] is False
    assert len(result["transactions"]) == 2
    assert result["filer_name"] == "Roger Williams"
    assert result["page_count"] == 2
    assert result["legibility"] == {"clear": 1, "partial": 1, "illegible": 0, "total": 2}
    assert result["confidence"] == 0.8
    assert result["needs_review"] is False
    assert result["parser_version"] == VISION_PARSER_VERSION

    # Request shape
    assert captured["model"] == MODEL_ID
    assert captured["max_tokens"] == 16000
    assert captured["thinking"] == {"type": "adaptive"}
    assert captured["output_config"]["effort"] == "medium"
    assert captured["output_config"]["format"]["type"] == "json_schema"
    assert captured["output_config"]["format"]["schema"] is PTR_VISION_SCHEMA
    assert "temperature" not in captured
    assert "top_p" not in captured
    assert "budget_tokens" not in json.dumps(captured["thinking"])

    # Cached, substantive system block
    assert captured["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert captured["system"][0]["type"] == "text"

    # The PDF document block precedes the text block
    content = captured["messages"][0]["content"]
    assert content[0]["type"] == "document"
    assert content[0]["source"]["type"] == "base64"
    assert content[0]["source"]["media_type"] == "application/pdf"
    assert "\n" not in content[0]["source"]["data"]
    assert content[1]["type"] == "text"


def test_model_id_env_override(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _enable(monkeypatch)
    monkeypatch.setenv("CAPITOL_PTR_VISION_MODEL", "claude-opus-5")
    captured, _ = _install_fake_client(
        monkeypatch,
        {"filer_name": None, "filing_date": None, "page_count": 1, "notes": None,
         "transactions": [CHEVRON_VISION_ROW]},
    )

    result = extract_via_vision(_write_pdf(tmp_path))

    assert captured["model"] == "claude-opus-5"
    assert result["model"] == "claude-opus-5"


# ---------------------------------------------------------------------------
# Kill switch and guardrails
# ---------------------------------------------------------------------------


def test_kill_switch_blocks_the_call(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _enable(monkeypatch)
    monkeypatch.setenv("CAPITOL_PTR_VISION_DISABLED", "1")
    _, calls = _install_fake_client(monkeypatch, {"transactions": []})

    result = extract_via_vision(_write_pdf(tmp_path))

    assert calls == []
    assert result["skipped"] is True
    assert result["transactions"] == []
    assert result["confidence"] == 0.0
    assert result["cost_usd"] == 0.0
    assert "CAPITOL_PTR_VISION_DISABLED" in str(result["reason"])


def test_missing_credentials_blocks_the_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("CAPITOL_PTR_VISION_DISABLED", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_AUTH_TOKEN", raising=False)
    _, calls = _install_fake_client(monkeypatch, {"transactions": []})

    result = extract_via_vision(_write_pdf(tmp_path))

    assert calls == []
    assert result["skipped"] is True
    assert "credentials" in str(result["reason"])


def test_page_limit_skips_without_calling_the_model(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    monkeypatch.setattr(ptr_vision, "count_pdf_pages", lambda _path: 41)
    _, calls = _install_fake_client(monkeypatch, {"transactions": [CHEVRON_VISION_ROW]})

    result = extract_via_vision(_write_pdf(tmp_path))

    assert calls == []
    assert result["skipped"] is True
    assert result["page_count"] == 41
    assert "41 pages" in str(result["reason"])
    assert result["needs_review"] is True


def test_byte_limit_skips_without_calling_the_model(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    monkeypatch.setattr(ptr_vision, "MAX_VISION_PDF_BYTES", 32)
    _, calls = _install_fake_client(monkeypatch, {"transactions": [CHEVRON_VISION_ROW]})
    pdf = tmp_path / "big.pdf"
    pdf.write_bytes(b"%PDF-1.4\n" + b"x" * 500)

    result = extract_via_vision(pdf)

    assert calls == []
    assert "too large" in str(result["reason"])


def test_refusal_and_truncation_are_reported_not_parsed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    payload = {"filer_name": None, "filing_date": None, "page_count": 1, "notes": None,
               "transactions": [CHEVRON_VISION_ROW]}

    _install_fake_client(monkeypatch, payload, stop_reason="refusal")
    refused = extract_via_vision(_write_pdf(tmp_path))
    assert refused["skipped"] is True
    assert refused["transactions"] == []
    assert "refused" in str(refused["reason"])
    # usage is still recorded so the run accounts for what it spent
    assert refused["usage"]["output"] == 800

    _install_fake_client(monkeypatch, payload, stop_reason="max_tokens")
    truncated = extract_via_vision(_write_pdf(tmp_path))
    assert truncated["skipped"] is True
    assert "max_tokens" in str(truncated["reason"])


def test_retries_once_on_a_429_then_succeeds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    monkeypatch.setattr(ptr_vision, "RETRY_SLEEP_SECONDS", 0)

    payload = {"filer_name": None, "filing_date": None, "page_count": 1, "notes": None,
               "transactions": [CHEVRON_VISION_ROW]}
    message = _Message(payload)
    attempts: list[int] = []

    class _RateLimited(Exception):
        status_code = 429

    class _FlakyMessages:
        def stream(self, **_kwargs: Any) -> _StreamManager:
            attempts.append(1)
            if len(attempts) == 1:
                raise _RateLimited("429 too many requests")
            return _StreamManager(message)

    class _FlakyClient:
        messages = _FlakyMessages()

    monkeypatch.setattr(ptr_vision, "_client", None)
    monkeypatch.setattr(ptr_vision, "_client_once", lambda: _FlakyClient())

    result = extract_via_vision(_write_pdf(tmp_path))

    assert len(attempts) == 2
    assert result["ok"] is True
    assert result["attempts"] == 2


def test_sdk_without_output_config_falls_back_to_strict_tool_use(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    payload = {"filer_name": None, "filing_date": None, "page_count": 1, "notes": None,
               "transactions": [CHEVRON_VISION_ROW]}

    class _ToolMessage:
        content = [_Block("tool_use", payload=payload)]
        stop_reason = "tool_use"
        stop_details = None
        usage = _Usage()

    seen: list[dict] = []

    class _OldSdkMessages:
        def stream(self, **kwargs: Any):  # type: ignore[no-untyped-def]
            seen.append(kwargs)
            if "output_config" in kwargs:
                raise TypeError("stream() got an unexpected keyword argument 'output_config'")
            return _StreamManager(_ToolMessage())  # type: ignore[arg-type]

    class _OldSdkClient:
        messages = _OldSdkMessages()

    monkeypatch.setattr(ptr_vision, "_client", None)
    monkeypatch.setattr(ptr_vision, "_client_once", lambda: _OldSdkClient())

    result = extract_via_vision(_write_pdf(tmp_path))

    assert len(seen) == 2
    assert "output_config" not in seen[1]
    tool = seen[1]["tools"][0]
    assert tool["name"] == ptr_vision.VISION_TOOL_NAME
    assert tool["strict"] is True
    assert tool["input_schema"] is PTR_VISION_SCHEMA
    assert result["ok"] is True
    assert result["structuredOutput"] is False
    assert len(result["transactions"]) == 1


def test_api_rejecting_json_schema_falls_back_to_strict_tool_use(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    payload = {"filer_name": None, "filing_date": None, "page_count": 1, "notes": None,
               "transactions": [CHEVRON_VISION_ROW]}

    class _ToolMessage:
        content = [_Block("tool_use", payload=payload)]
        stop_reason = "tool_use"
        stop_details = None
        usage = _Usage()

    class _Rejected(Exception):
        status_code = 400

    seen: list[dict] = []

    class _PickyMessages:
        def stream(self, **kwargs: Any):  # type: ignore[no-untyped-def]
            seen.append(kwargs)
            if "format" in kwargs.get("output_config", {}):
                raise _Rejected("output_config.format.schema: unsupported keyword")
            return _StreamManager(_ToolMessage())  # type: ignore[arg-type]

    class _PickyClient:
        messages = _PickyMessages()

    monkeypatch.setattr(ptr_vision, "_client", None)
    monkeypatch.setattr(ptr_vision, "_client_once", lambda: _PickyClient())

    result = extract_via_vision(_write_pdf(tmp_path))

    assert len(seen) == 2
    assert seen[1]["output_config"] == {"effort": "medium"}
    assert result["ok"] is True
    assert result["structuredOutput"] is False


def test_non_retryable_error_gives_up_immediately(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    attempts: list[int] = []

    class _BadRequest(Exception):
        status_code = 400

    class _BadMessages:
        def stream(self, **_kwargs: Any):  # type: ignore[no-untyped-def]
            attempts.append(1)
            raise _BadRequest("schema rejected")

    class _BadClient:
        messages = _BadMessages()

    monkeypatch.setattr(ptr_vision, "_client", None)
    monkeypatch.setattr(ptr_vision, "_client_once", lambda: _BadClient())

    result = extract_via_vision(_write_pdf(tmp_path))

    assert len(attempts) == 1
    assert result["skipped"] is True
    assert "api error" in str(result["reason"])


# ---------------------------------------------------------------------------
# Legibility, confidence, cost
# ---------------------------------------------------------------------------


def test_legibility_summary_and_confidence() -> None:
    counts = summarize_legibility(
        [
            {"legibility": "clear"},
            {"legibility": "partial"},
            {"legibility": "illegible"},
            {"legibility": "nonsense"},  # coerced to partial
        ]
    )
    assert counts == {"clear": 1, "partial": 2, "illegible": 1, "total": 4}
    assert legibility_confidence(counts) == 0.55
    assert majority_illegible(counts) is False
    assert legibility_confidence({"total": 0}) == 0.0
    assert majority_illegible({"total": 0}) is True


def test_cost_uses_sonnet_5_rates_with_cache_multipliers() -> None:
    # 1M plain input ($2) + 1M cache reads ($0.20) + 1M cache writes ($2.50)
    # + 1M output ($10) = $14.70
    cost = estimate_cost_usd(
        {
            "input": 1_000_000,
            "cache_read": 1_000_000,
            "cache_write": 1_000_000,
            "output": 1_000_000,
        }
    )
    assert cost == pytest.approx(14.70)


def test_vision_metadata_carries_usage_and_cost(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    _install_fake_client(
        monkeypatch,
        {"filer_name": "Roger Williams", "filing_date": None, "page_count": 1,
         "notes": None, "transactions": [CHEVRON_VISION_ROW]},
    )

    metadata = build_vision_metadata(extract_via_vision(_write_pdf(tmp_path)))

    assert metadata["model"] == MODEL_ID
    assert metadata["parserVersion"] == VISION_PARSER_VERSION
    assert metadata["ok"] is True
    assert metadata["needsReview"] is False
    assert metadata["rowCount"] == 1
    assert metadata["usage"] == {
        "inputTokens": 4_000,
        "cacheReadTokens": 2_000,
        "cacheWriteTokens": 1_000,
        "outputTokens": 800,
    }
    # 4000*$2 + 2000*$0.20 + 1000*$2.50 + 800*$10, per MTok
    assert metadata["costUsd"] == pytest.approx(0.0189)
    assert metadata["pricing"]["inputPerMTok"] == 2.0
    assert metadata["pricing"]["outputPerMTok"] == 10.0
    assert "transactions" not in metadata


# ---------------------------------------------------------------------------
# Illegible majority keeps the stub in the review queue
# ---------------------------------------------------------------------------


def test_illegible_majority_keeps_the_stub_in_needs_review(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    rows = [
        {**CHEVRON_VISION_ROW, "legibility": "illegible"},
        {**CHEVRON_VISION_ROW, "asset_description": "Apple Inc.", "ticker": "AAPL",
         "legibility": "illegible"},
        {**CHEVRON_VISION_ROW, "asset_description": "RTX Corporation", "ticker": "RTX",
         "legibility": "clear"},
    ]
    _install_fake_client(
        monkeypatch,
        {"filer_name": None, "filing_date": None, "page_count": 1, "notes": None,
         "transactions": rows},
    )

    stub = _stub()
    result, metadata = house_ptr._run_vision_parse(_write_pdf(tmp_path), stub, "")

    assert result is not None
    parsed, trades = result
    assert len(parsed.transactions) == 3
    assert metadata["needsReview"] is True
    assert parsed.parser_confidence == pytest.approx(0.33)
    assert resolve_house_stub_status(stub, parsed, trades) == "needs_review"


def test_legible_majority_leaves_the_review_queue(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    rows = [
        {**CHEVRON_VISION_ROW, "legibility": "illegible"},
        {**CHEVRON_VISION_ROW, "asset_description": "Apple Inc.", "ticker": "AAPL",
         "legibility": "partial"},
        {**CHEVRON_VISION_ROW, "asset_description": "RTX Corporation", "ticker": "RTX",
         "legibility": "clear"},
    ]
    _install_fake_client(
        monkeypatch,
        {"filer_name": None, "filing_date": None, "page_count": 1, "notes": None,
         "transactions": rows},
    )

    stub = _stub()
    result, metadata = house_ptr._run_vision_parse(_write_pdf(tmp_path), stub, "")

    assert result is not None
    parsed, trades = result
    assert metadata["needsReview"] is False
    # Confidence is honest (0.53) but the legibility verdict is what decides.
    assert parsed.parser_confidence < 0.6
    assert resolve_house_stub_status(stub, parsed, trades) == "parsed"


def test_unresolved_member_can_never_be_marked_parsed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    _install_fake_client(
        monkeypatch,
        {"filer_name": None, "filing_date": None, "page_count": 1, "notes": None,
         "transactions": [CHEVRON_VISION_ROW]},
    )
    stub = _stub()
    stub.member.id = None

    result, _metadata = house_ptr._run_vision_parse(_write_pdf(tmp_path), stub, "")

    assert result is not None
    parsed, trades = result
    assert resolve_house_stub_status(stub, parsed, trades) == "needs_review"


def test_status_helper_falls_back_to_the_confidence_threshold() -> None:
    stub = _stub()
    parsed = HousePtrParseResult(doc_id=stub.doc_id, parser_confidence=0.7)
    trades = [object()]  # only truthiness matters here
    assert resolve_house_stub_status(stub, parsed, trades) == "parsed"  # type: ignore[arg-type]

    parsed_low = HousePtrParseResult(doc_id=stub.doc_id, parser_confidence=0.5)
    assert resolve_house_stub_status(stub, parsed_low, trades) == "needs_review"  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Parity with a text-parsed row
# ---------------------------------------------------------------------------


def test_vision_rows_are_indistinguishable_from_text_parsed_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    stub = _stub()
    text_parsed, text_rows = parse_house_ptr_text(CHEVRON_TEXT, stub)
    assert len(text_rows) == 1, "fixture should yield exactly one text-parsed row"
    text_row = text_rows[0]

    _enable(monkeypatch)
    _install_fake_client(
        monkeypatch,
        {"filer_name": "Roger Williams", "filing_date": "2026-01-05", "page_count": 1,
         "notes": None, "transactions": [CHEVRON_VISION_ROW]},
    )

    result, _metadata = house_ptr._run_vision_parse(_write_pdf(tmp_path), stub, CHEVRON_TEXT)
    assert result is not None
    vision_parsed, vision_rows = result
    assert len(vision_rows) == 1
    vision_row = vision_rows[0]

    # Same canonical CapitolExposed trade id: tr-house-{doc_id}-{line}
    assert build_trade_id(text_row) == "tr-house-20033783-1"
    assert build_trade_id(vision_row) == build_trade_id(text_row)
    assert vision_row.source_id == text_row.source_id

    for field in (
        "ticker",
        "asset_description",
        "asset_type",
        "transaction_type",
        "transaction_date",
        "disclosure_date",
        "amount_min",
        "amount_max",
        "owner",
        "source",
        "disclosure_kind",
        "source_url",
    ):
        assert getattr(vision_row, field) == getattr(text_row, field), field

    # Member resolution comes from the stub on both paths.
    assert vision_row.member.id == stub.member.id == text_row.member.id

    # Crypto classifier ran on both paths.
    assert vision_row.normalized_asset is not None
    assert vision_row.normalized_asset.kind == text_row.normalized_asset.kind

    # Only provenance differs.
    assert text_parsed.parser_version == "regex-v1"
    assert vision_parsed.parser_version == VISION_PARSER_VERSION
    assert vision_row.parser_version == VISION_PARSER_VERSION


def test_vision_crypto_rows_go_through_the_classifier(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    _install_fake_client(
        monkeypatch,
        {
            "filer_name": None,
            "filing_date": None,
            "page_count": 1,
            "notes": None,
            "transactions": [
                {
                    **CHEVRON_VISION_ROW,
                    "asset_description": "Bitcoin",
                    "ticker": "BTC",
                    "transaction_type": "purchase",
                }
            ],
        },
    )

    result, _metadata = house_ptr._run_vision_parse(_write_pdf(tmp_path), _stub(), "")

    assert result is not None
    _parsed, rows = result
    assert rows[0].asset_type == "Cryptocurrency"
    assert rows[0].normalized_asset is not None
    assert rows[0].normalized_asset.kind == "direct_crypto"


def test_vision_owner_and_partial_sale_mapping(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    _install_fake_client(
        monkeypatch,
        {
            "filer_name": None,
            "filing_date": None,
            "page_count": 1,
            "notes": None,
            "transactions": [
                {**CHEVRON_VISION_ROW, "owner": "dependent"},
                {**CHEVRON_VISION_ROW, "owner": "joint", "asset_description": "Apple Inc.",
                 "ticker": "AAPL", "transaction_type": "exchange"},
                {**CHEVRON_VISION_ROW, "owner": "spouse", "asset_description": "RTX Corporation",
                 "ticker": "RTX", "transaction_type": "purchase"},
            ],
        },
    )

    result, _metadata = house_ptr._run_vision_parse(_write_pdf(tmp_path), _stub(), "")

    assert result is not None
    parsed, _rows = result
    assert [t.owner for t in parsed.transactions] == ["child", "joint", "spouse"]
    # sale_partial collapses to the site's "sale", matching the text parser.
    assert [t.transaction_type for t in parsed.transactions] == ["sale", "exchange", "purchase"]


# ---------------------------------------------------------------------------
# Review-chain wiring
# ---------------------------------------------------------------------------


def test_ocr_text_is_decent_rejects_scanner_junk() -> None:
    junk = "| 9 984 F 1 | Sale | 1 | " * 40
    assert house_ptr.ocr_text_is_decent(junk) is False
    assert house_ptr.ocr_text_is_decent("") is False
    assert house_ptr.ocr_text_is_decent(None) is False
    assert house_ptr.ocr_text_is_decent(CHEVRON_TEXT * 6) is True


def test_parse_house_ptr_pdf_calls_vision_when_the_text_path_is_weak(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """auto: junk OCR text -> vision, and never the Haiku text fallback."""

    _enable(monkeypatch)
    pdf = _write_pdf(tmp_path)
    stub = _stub()

    class _JunkProcessor:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def process_file(self, _path: Path) -> Any:
            class _Doc:
                ocrText = "| 9 984 F 1 | Sale | 1 | " * 30

            class _Result:
                document = _Doc()

            return _Result()

    monkeypatch.setattr(house_ptr, "OcrProcessor", _JunkProcessor)
    monkeypatch.setattr(
        house_ptr,
        "extract_via_haiku",
        lambda *_a, **_k: pytest.fail("Haiku text fallback must not run on junk OCR text"),
    )
    _, calls = _install_fake_client(
        monkeypatch,
        {"filer_name": None, "filing_date": None, "page_count": 1, "notes": None,
         "transactions": [CHEVRON_VISION_ROW]},
    )

    parsed, rows = house_ptr.parse_house_ptr_pdf(
        pdf, stub=stub, backend="pymupdf", vision_backend="auto"
    )

    assert calls == [1]
    assert parsed.parser_version == VISION_PARSER_VERSION
    assert len(rows) == 1
    assert isinstance(parsed.vision_report, dict)
    assert parsed.vision_report["ok"] is True


def test_parse_house_ptr_pdf_skips_vision_when_backend_is_off(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    pdf = _write_pdf(tmp_path)

    class _GoodProcessor:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def process_file(self, _path: Path) -> Any:
            class _Doc:
                ocrText = CHEVRON_TEXT

            class _Result:
                document = _Doc()

            return _Result()

    monkeypatch.setattr(house_ptr, "OcrProcessor", _GoodProcessor)
    _, calls = _install_fake_client(monkeypatch, {"transactions": [CHEVRON_VISION_ROW]})

    parsed, rows = house_ptr.parse_house_ptr_pdf(
        pdf, stub=_stub(), backend="pymupdf", vision_backend="off"
    )

    assert calls == []
    assert parsed.parser_version == "regex-v1"
    assert len(rows) == 1
    assert parsed.vision_report is None


def test_parse_house_ptr_pdf_records_a_skipped_vision_attempt(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A skipped or failed vision call still lands in the stub metadata."""

    _enable(monkeypatch)
    monkeypatch.setenv("CAPITOL_PTR_VISION_DISABLED", "1")
    pdf = _write_pdf(tmp_path)

    class _EmptyProcessor:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def process_file(self, _path: Path) -> Any:
            class _Doc:
                ocrText = ""

            class _Result:
                document = _Doc()

            return _Result()

    monkeypatch.setattr(house_ptr, "OcrProcessor", _EmptyProcessor)
    monkeypatch.setattr(
        house_ptr,
        "extract_via_haiku",
        lambda *_a, **_k: {"transactions": [], "parser_notes": "", "usage": {}, "confidence": 0.0},
    )
    _, calls = _install_fake_client(monkeypatch, {"transactions": [CHEVRON_VISION_ROW]})

    parsed, rows = house_ptr.parse_house_ptr_pdf(
        pdf, stub=_stub(), backend="pymupdf", vision_backend="auto"
    )

    assert calls == []
    assert rows == []
    assert isinstance(parsed.vision_report, dict)
    assert parsed.vision_report["ok"] is False
    assert "CAPITOL_PTR_VISION_DISABLED" in str(parsed.vision_report["reason"])
