"""The free Gemini path. Never calls the real API.

Every test replaces ``GeminiProvider._post`` (the single HTTP call) with a
recorder, so the request body and the response mapping are inspectable and no
network call is made. The Anthropic client factory is left unpatched on
purpose: if anything on this path reached for it, these tests would fail.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import pytest

from capitol_pipeline.parsers import ptr_vision, ptr_vision_provider
from capitol_pipeline.parsers.ptr_vision import PTR_VISION_SCHEMA, extract_via_vision
from capitol_pipeline.parsers.ptr_vision_provider import (
    GEMINI_MODEL_B_ID,
    GEMINI_MODEL_ID,
    GeminiError,
    GeminiProvider,
    _RateLimiter,
    gemini_response_schema,
)

CHEVRON_ROW: dict[str, Any] = {
    "page_number": 1,
    "owner": None,
    "asset_description": "Chevron Corporation Common Stock",
    "ticker": "CVX",
    "asset_type_code": "ST",
    "transaction_type": "sale_partial",
    "transaction_date": "2025-12-22",
    "notification_date": "2025-12-22",
    "amount_min": 15001,
    "amount_max": 50000,
    "amount_column_letter": "B",
    "cap_gains_over_200": False,
    "comment": None,
    "legibility": "clear",
}


def _payload(*rows: dict[str, Any]) -> dict[str, Any]:
    return {
        "filer_name": "Roger Williams",
        "filing_date": "2025-12-30",
        "page_count": 1,
        "notes": None,
        "no_transactions_stated": not rows,
        "transactions": [dict(row) for row in rows],
    }


def _body(payload: dict[str, Any], *, finish: str = "STOP") -> dict[str, Any]:
    return {
        "candidates": [
            {
                "finishReason": finish,
                "content": {
                    "parts": [
                        {"text": "thinking about the ladder", "thought": True},
                        {"text": json.dumps(payload)},
                    ]
                },
            }
        ],
        "usageMetadata": {
            "promptTokenCount": 4_000,
            "candidatesTokenCount": 800,
            "thoughtsTokenCount": 200,
        },
    }


def _enable(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "CAPITOL_PTR_VISION_DISABLED",
        "CAPITOL_PTR_VISION_MODEL",
        "CAPITOL_PTR_VISION_MODEL_B",
        "CAPITOL_PTR_VISION_EFFORT",
        "CAPITOL_PTR_VISION_CHUNK_PAGES",
        "CAPITOL_PTR_VISION_ORIENTATION",
        "CAPITOL_PTR_VISION_PROVIDER",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("GEMINI_API_KEY", "test-key-not-a-real-one")
    monkeypatch.setenv("CAPITOL_PTR_VISION_GEMINI_RPM", "0")  # no pacing in tests


def _write_real_pdf(tmp_path: Path, *, pages: int = 1, portrait: bool = True) -> Path:
    fitz = pytest.importorskip("fitz")
    document = fitz.open()
    width, height = (612, 792) if portrait else (792, 612)
    for index in range(pages):
        page = document.new_page(width=width, height=height)
        page.insert_text((72, 100), f"PERIODIC TRANSACTION REPORT page {index + 1}", fontsize=18)
    pdf = tmp_path / "scan.pdf"
    document.save(str(pdf))
    document.close()
    return pdf


def _install(monkeypatch: pytest.MonkeyPatch, bodies: list[dict[str, Any]]) -> list[tuple[str, dict]]:
    """Record every POST and answer with ``bodies`` in order (the last repeats)."""

    calls: list[tuple[str, dict]] = []

    def _post(self: GeminiProvider, model: str, body: dict[str, Any], **_kwargs: Any) -> dict[str, Any]:
        calls.append((model, body))
        return bodies[min(len(calls) - 1, len(bodies) - 1)]

    monkeypatch.setattr(GeminiProvider, "_post", _post)
    return calls


# ---------------------------------------------------------------------------
# The response schema Gemini actually accepts
# ---------------------------------------------------------------------------


def test_response_schema_drops_what_gemini_rejects() -> None:
    translated = gemini_response_schema(PTR_VISION_SCHEMA)
    text = json.dumps(translated)

    # additionalProperties is rejected outright, and a ["string", "null"] type
    # union is not a type Gemini understands.
    assert "additionalProperties" not in text
    row = translated["properties"]["transactions"]["items"]
    assert row["properties"]["ticker"] == {
        "type": "string",
        "description": PTR_VISION_SCHEMA["properties"]["transactions"]["items"]["properties"][
            "ticker"
        ]["description"],
        "nullable": True,
    }
    assert row["properties"]["amount_min"]["type"] == "integer"
    assert "nullable" not in row["properties"]["amount_min"]

    # The anyOf around the nullable enums exists only because Anthropic
    # rejected a nullable enum; Gemini takes the enum directly.
    owner = row["properties"]["owner"]
    assert owner["type"] == "string"
    assert owner["enum"] == ["self", "spouse", "dependent", "joint"]
    assert owner["nullable"] is True
    assert "anyOf" not in owner
    letter = row["properties"]["amount_column_letter"]
    assert letter["enum"][-1] == "K" and letter["nullable"] is True

    # Required stays, and the row is filled in the order the form prints it.
    assert row["required"] == PTR_VISION_SCHEMA["properties"]["transactions"]["items"]["required"]
    assert row["propertyOrdering"][:2] == ["owner", "asset_description"]
    assert translated["properties"]["transactions"]["type"] == "array"


def test_response_schema_leaves_a_plain_schema_alone() -> None:
    assert gemini_response_schema({"type": "string"}) == {"type": "string"}
    assert gemini_response_schema({"type": ["null"]}) == {"nullable": True}


# ---------------------------------------------------------------------------
# The request
# ---------------------------------------------------------------------------


def test_request_body_carries_the_pages_the_schema_and_the_thinking_level(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _enable(monkeypatch)
    provider = GeminiProvider()
    body = provider.request_body(
        [
            {"kind": "text", "text": "Page 1 of 1:"},
            {"kind": "image", "png": b"\x89PNG-not-really"},
            {"kind": "text", "text": "Transcribe every transaction row"},
        ],
        system="SYSTEM",
        schema=PTR_VISION_SCHEMA,
        effort="medium",
        max_tokens=64_000,
    )

    assert body["systemInstruction"] == {"parts": [{"text": "SYSTEM"}]}
    parts = body["contents"][0]["parts"]
    assert parts[0] == {"text": "Page 1 of 1:"}
    assert parts[1]["inlineData"]["mimeType"] == "image/png"
    assert base64.standard_b64decode(parts[1]["inlineData"]["data"]) == b"\x89PNG-not-really"
    generation = body["generationConfig"]
    assert generation["responseMimeType"] == "application/json"
    assert generation["maxOutputTokens"] == 64_000
    assert generation["thinkingConfig"] == {"thinkingLevel": "medium"}
    assert "additionalProperties" not in json.dumps(generation["responseSchema"])


def test_effort_maps_onto_thinking_levels(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable(monkeypatch)
    provider = GeminiProvider()
    for effort, level in (("low", "low"), ("high", "high"), ("xhigh", "high"), ("max", "high")):
        body = provider.request_body(
            [{"kind": "text", "text": "x"}],
            system="S",
            schema=None,
            effort=effort,
            max_tokens=8,
        )
        assert body["generationConfig"]["thinkingConfig"] == {"thinkingLevel": level}


def test_a_whole_pdf_is_refused_rather_than_misread(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable(monkeypatch)
    provider = GeminiProvider()
    with pytest.raises(ValueError, match="render the pages first"):
        provider.content([{"kind": "document", "pdf": b"%PDF-1.4"}])


# ---------------------------------------------------------------------------
# The response
# ---------------------------------------------------------------------------


def test_response_maps_usage_stop_reason_and_payload() -> None:
    response = GeminiProvider._response(_body(_payload(CHEVRON_ROW)), GEMINI_MODEL_ID)

    assert response["stop_reason"] == "end_turn"
    assert response["payload"]["transactions"][0]["ticker"] == "CVX"
    # Thinking tokens are output tokens, and the visible text excludes them.
    assert response["usage"] == {
        "input": 4_000,
        "cache_read": 0,
        "cache_write": 0,
        "output": 1_000,
    }
    assert "thinking about the ladder" not in response["text"]


def test_truncation_and_refusal_are_reported_in_the_readers_vocabulary() -> None:
    truncated = GeminiProvider._response(_body(_payload(), finish="MAX_TOKENS"), GEMINI_MODEL_ID)
    assert truncated["stop_reason"] == "max_tokens"

    refused = GeminiProvider._response(_body(_payload(), finish="SAFETY"), GEMINI_MODEL_ID)
    assert refused["stop_reason"] == "refusal"
    assert refused["detail"] == "SAFETY"

    blocked = GeminiProvider._response(
        {"promptFeedback": {"blockReason": "PROHIBITED_CONTENT"}}, GEMINI_MODEL_ID
    )
    assert blocked["stop_reason"] == "refusal"
    assert blocked["detail"] == "PROHIBITED_CONTENT"


def test_a_fenced_json_answer_is_still_read() -> None:
    body = _body(_payload(CHEVRON_ROW))
    body["candidates"][0]["content"]["parts"] = [
        {"text": "```json\n" + json.dumps(_payload(CHEVRON_ROW)) + "\n```"}
    ]
    response = GeminiProvider._response(body, GEMINI_MODEL_ID)
    assert response["payload"]["transactions"][0]["asset_description"].startswith("Chevron")


# ---------------------------------------------------------------------------
# Rate limits
# ---------------------------------------------------------------------------


def test_the_pacer_spaces_calls_per_model() -> None:
    slept: list[float] = []
    limiter = _RateLimiter(rpm=60.0)  # one a second
    assert limiter.wait("m", sleep=slept.append) == 0.0
    second = limiter.wait("m", sleep=slept.append)
    assert 0.9 <= second <= 1.0
    # A different model has its own quota.
    assert limiter.wait("other", sleep=slept.append) == 0.0
    # Only the call that had to wait slept.
    assert len(slept) == 1


def test_a_429_backs_off_and_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable(monkeypatch)
    provider = GeminiProvider()
    slept: list[float] = []
    attempts: list[int] = []

    class _Response:
        def __init__(self, status_code: int, body: dict[str, Any]) -> None:
            self.status_code = status_code
            self._body = body
            self.headers: dict[str, str] = {}
            self.text = json.dumps(body)

        def json(self) -> dict[str, Any]:
            return self._body

    def _fake_post(_url: str, **_kwargs: Any) -> _Response:
        attempts.append(1)
        if len(attempts) == 1:
            return _Response(
                429,
                {
                    "error": {
                        "message": "Quota exceeded",
                        "details": [{"retryDelay": "7s"}],
                    }
                },
            )
        return _Response(200, _body(_payload(CHEVRON_ROW)))

    import httpx

    monkeypatch.setattr(httpx, "post", _fake_post)
    body = provider._post(GEMINI_MODEL_ID, {"contents": []}, sleep=slept.append)

    assert len(attempts) == 2
    assert body["candidates"][0]["finishReason"] == "STOP"
    # The API's own retryDelay is honoured (plus a little jitter).
    assert 7.0 <= slept[0] <= 8.0


def test_a_400_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable(monkeypatch)
    provider = GeminiProvider()
    attempts: list[int] = []

    class _Response:
        status_code = 400
        headers: dict[str, str] = {}
        text = "bad schema"

        def json(self) -> dict[str, Any]:
            return {"error": {"message": "Invalid JSON payload: responseSchema"}}

    def _fake_post(_url: str, **_kwargs: Any) -> _Response:
        attempts.append(1)
        return _Response()

    import httpx

    monkeypatch.setattr(httpx, "post", _fake_post)
    with pytest.raises(GeminiError) as caught:
        provider._post(GEMINI_MODEL_ID, {"contents": []}, sleep=lambda _s: None)

    assert len(attempts) == 1
    assert caught.value.status_code == 400
    # ... and it is recognised as the schema being refused, so the reader
    # downgrades to JSON without a schema rather than failing the filing.
    assert provider.rejected_structured_output(caught.value) is True
    assert provider.is_retryable(caught.value) is False


# ---------------------------------------------------------------------------
# End to end
# ---------------------------------------------------------------------------


def test_gemini_is_the_default_provider_and_reads_twice_with_two_versions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    calls = _install(monkeypatch, [_body(_payload(CHEVRON_ROW))])

    def _no_anthropic() -> Any:  # pragma: no cover - must never run
        raise AssertionError("the free path must not build an Anthropic client")

    monkeypatch.setattr(ptr_vision, "_client_once", _no_anthropic)

    result = extract_via_vision(_write_real_pdf(tmp_path), filing_year=2025)

    assert result["ok"] is True
    assert result["provider"] == "gemini"
    assert result["parser_version"] == "gemini-vision-v2"
    # Read A and read B are two different model versions: two samples of one
    # model agree with themselves, two versions do not.
    assert [model for model, _body in calls] == [GEMINI_MODEL_ID, GEMINI_MODEL_B_ID]
    assert GEMINI_MODEL_ID != GEMINI_MODEL_B_ID
    assert result["transactions"][0]["ticker"] == "CVX"
    assert result["needs_review"] is False
    # Free tier: the cost record is a true zero, not an estimate nobody paid.
    assert result["cost_usd"] == 0.0
    assert result["cost_estimate_usd"] == 0.0
    assert all(call["costUsd"] == 0.0 for call in result["calls"])
    # No orientation call was made at all: the rotation came from the detector.
    assert [call["label"] for call in result["calls"]] == ["read A", "read B"]
    assert result["orientation"][0]["method"] in {"grid", "grid-consensus", "heuristic"}


def test_the_two_versions_disagreeing_withholds_the_row(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Measured on Wied 9115665: 3.8-flash read Purchase / $1,001-$15,000 and
    # 3.5-flash read Sale / $15,001-$50,000. The second was right, and neither
    # read on its own could have told us that.
    _enable(monkeypatch)
    read_a = dict(CHEVRON_ROW, transaction_type="purchase", amount_min=1001, amount_max=15000)
    read_b = dict(CHEVRON_ROW, transaction_type="sale", amount_min=15001, amount_max=50000)
    _install(monkeypatch, [_body(_payload(read_a)), _body(_payload(read_b))])

    result = extract_via_vision(_write_real_pdf(tmp_path), filing_year=2025)

    assert result["needs_review"] is True
    assert any("transaction_type" in reason for reason in result["needs_review_reasons"])
    row = result["transactions"][0]
    assert row["transaction_type"] is None
    assert row["amount_min"] is None
    assert row["legibility"] == "illegible"


def test_missing_credentials_skip_rather_than_fall_back_to_the_paid_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-not-a-real-key")

    result = extract_via_vision(_write_real_pdf(tmp_path))

    assert result["ok"] is False
    assert result["skipped"] is True
    assert "GEMINI_API_KEY" in str(result["reason"])
    assert result["provider"] == "gemini"


def test_the_provider_switch_still_selects_anthropic(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable(monkeypatch)
    monkeypatch.setenv("CAPITOL_PTR_VISION_PROVIDER", "anthropic")
    provider = ptr_vision_provider.resolve_provider(anthropic_client_factory=lambda: None)
    assert provider.name == "anthropic"
    assert provider.read_model == ptr_vision_provider.ANTHROPIC_MODEL_ID
    assert provider.price(provider.read_model) == (5.0, 25.0)

    monkeypatch.setenv("CAPITOL_PTR_VISION_PROVIDER", "nonsense")
    assert (
        ptr_vision_provider.resolve_provider(anthropic_client_factory=lambda: None).name == "gemini"
    )
