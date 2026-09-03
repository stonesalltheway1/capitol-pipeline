"""Tests for the Anthropic vision PTR path. Never calls the real API.

The default provider is Gemini; this file pins ``CAPITOL_PTR_VISION_PROVIDER``
to ``anthropic`` (and the orientation pick to ``model``) so the paid path keeps
its coverage. The free path is covered in ``test_ptr_vision_gemini.py``.

Every test patches ``ptr_vision._client_once`` (the module-level client
factory) so the request kwargs are inspectable and no network call is made.
The fake client answers ``messages.stream`` (the transcription reads, cycled
through a list of payloads so the two reads can differ) and
``messages.create`` (the Haiku orientation question). The database exporter is
never touched; only the pure status helper is.
"""

from __future__ import annotations

import base64
import json
import struct
from pathlib import Path
from typing import Any

import pytest

from capitol_pipeline.bridges.capitol_exposed import build_trade_id
from capitol_pipeline.cli import resolve_house_stub_status
from capitol_pipeline.models.congress import FilingStub, HousePtrParseResult, MemberMatch
from capitol_pipeline.parsers import house_ptr, ptr_vision, ptr_vision_provider
from capitol_pipeline.parsers.house_ptr import parse_house_ptr_text
from capitol_pipeline.parsers.ptr_vision import (
    MODEL_ID,
    ORIENTATION_MODEL_ID,
    PTR_VISION_SCHEMA,
    VISION_PARSER_VERSION,
    build_vision_metadata,
    detect_orientation,
    estimate_cost_usd,
    extract_via_vision,
    is_vision_parser_version,
    legibility_confidence,
    majority_illegible,
    match_rows,
    orientation_heuristic,
    pricing_for_model,
    reconcile_reads,
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

ATT_ROW: dict[str, Any] = {
    "owner": "self",
    "asset_description": "AT&T",
    "ticker": None,
    "asset_type_code": None,
    "transaction_type": "purchase",
    "transaction_date": "2022-01-06",
    "notification_date": "2023-07-22",
    "amount_min": 1001,
    "amount_max": 15000,
    "cap_gains_over_200": None,
    "comment": None,
    "legibility": "partial",
}

GE_ROW: dict[str, Any] = {
    **ATT_ROW,
    "asset_description": "General Electric",
    "transaction_type": "sale_partial",
}


def _payload(*rows: dict[str, Any], **header: Any) -> dict[str, Any]:
    base = {
        "filer_name": None,
        "filing_date": None,
        "page_count": 1,
        "notes": None,
        "no_transactions_stated": False,
    }
    base.update(header)
    base["transactions"] = list(rows)
    return base


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
    def __init__(self, *, input_tokens: int = 4_000, output_tokens: int = 800) -> None:
        self.input_tokens = input_tokens
        self.cache_read_input_tokens = 2_000
        self.cache_creation_input_tokens = 1_000
        self.output_tokens = output_tokens


class _Message:
    def __init__(self, payload: dict, *, stop_reason: str = "end_turn") -> None:
        self.content = [
            _Block("thinking"),
            _Block("text", text=json.dumps(payload)),
        ]
        self.stop_reason = stop_reason
        self.stop_details = None
        self.usage = _Usage()


class _OrientationMessage:
    """What ``messages.create`` returns for the Haiku orientation question."""

    def __init__(self, answer: str) -> None:
        self.content = [_Block("text", text=answer)]
        self.stop_reason = "end_turn"
        self.stop_details = None
        self.usage = _Usage(input_tokens=1_500, output_tokens=2)
        self.usage.cache_read_input_tokens = 0
        self.usage.cache_creation_input_tokens = 0


class _StreamManager:
    def __init__(self, message: Any) -> None:
        self._message = message

    def __enter__(self) -> "_StreamManager":
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False

    def get_final_message(self) -> Any:
        return self._message


class _Messages:
    def __init__(
        self,
        messages: list[Any],
        captured: list[dict],
        calls: list[int],
        *,
        orientation_answers: list[str],
        orientation_error: Exception | None,
        orientation_requests: list[dict],
    ) -> None:
        self._messages = messages
        self._captured = captured
        self._calls = calls
        self._orientation_answers = orientation_answers
        self._orientation_error = orientation_error
        self._orientation_requests = orientation_requests

    def stream(self, **kwargs: Any) -> _StreamManager:
        index = len(self._calls)
        self._calls.append(1)
        self._captured.append(kwargs)
        return _StreamManager(self._messages[min(index, len(self._messages) - 1)])

    def create(self, **kwargs: Any) -> _OrientationMessage:
        index = len(self._orientation_requests)
        self._orientation_requests.append(kwargs)
        if self._orientation_error is not None:
            raise self._orientation_error
        answers = self._orientation_answers
        return _OrientationMessage(answers[min(index, len(answers) - 1)])


class _FakeClient:
    def __init__(self, messages: _Messages) -> None:
        self.messages = messages


class _Captured(list):
    """A list of read kwargs that also carries the orientation requests."""

    orientation_requests: list[dict]


def _install_fake_client(
    monkeypatch: pytest.MonkeyPatch,
    payloads: dict | list[dict],
    *,
    stop_reason: str = "end_turn",
    orientation: str | list[str] = "0",
    orientation_error: Exception | None = None,
) -> tuple[list[dict], list[int]]:
    """Install a fake client. Returns ``(captured_read_kwargs, read_calls)``.

    ``payloads`` is one payload (both reads identical) or a list, one per read.
    ``orientation`` is the text Haiku answers with (a list cycles per call).
    The orientation requests are attached to the returned list as
    ``captured.orientation_requests`` for tests that need them.
    """

    payload_list = payloads if isinstance(payloads, list) else [payloads]
    captured = _Captured()
    calls: list[int] = []
    orientation_requests: list[dict] = []
    messages = _Messages(
        [_Message(payload, stop_reason=stop_reason) for payload in payload_list],
        captured,
        calls,
        orientation_answers=orientation if isinstance(orientation, list) else [orientation],
        orientation_error=orientation_error,
        orientation_requests=orientation_requests,
    )
    client = _FakeClient(messages)
    monkeypatch.setattr(ptr_vision, "_client", None)
    monkeypatch.setattr(ptr_vision, "_client_once", lambda: client)
    captured.orientation_requests = orientation_requests  # type: ignore[attr-defined]
    return captured, calls


def _enable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CAPITOL_PTR_VISION_DISABLED", raising=False)
    monkeypatch.delenv("CAPITOL_PTR_VISION_MODEL", raising=False)
    monkeypatch.delenv("CAPITOL_PTR_VISION_MODEL_B", raising=False)
    # This file is the Anthropic path's test: it asserts on the Messages
    # request dict and on Claude's prices. The default provider is Gemini and
    # the default orientation pick is the free detector, so both are pinned.
    monkeypatch.setenv("CAPITOL_PTR_VISION_PROVIDER", "anthropic")
    monkeypatch.setenv("CAPITOL_PTR_VISION_ORIENTATION", "model")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-not-a-real-key")


def _write_pdf(tmp_path: Path, name: str = "scan.pdf") -> Path:
    """A PDF stub pymupdf cannot render: exercises the document-block fallback."""

    pdf = tmp_path / name
    pdf.write_bytes(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\ntrailer\n%%EOF\n")
    return pdf


def _write_real_pdf(tmp_path: Path, *, portrait: bool = True, pages: int = 1) -> Path:
    """A real one-or-more page PDF rendered by pymupdf: exercises the image path."""

    fitz = pytest.importorskip("fitz")
    document = fitz.open()
    width, height = (612, 792) if portrait else (792, 612)
    for index in range(pages):
        page = document.new_page(width=width, height=height)
        page.insert_text((72, 100), f"PERIODIC TRANSACTION REPORT page {index + 1}", fontsize=18)
    pdf = tmp_path / "real.pdf"
    document.save(str(pdf))
    document.close()
    return pdf


def _png_size(png_b64: str) -> tuple[int, int]:
    raw = base64.b64decode(png_b64)
    assert raw[:8] == b"\x89PNG\r\n\x1a\n"
    width, height = struct.unpack(">II", raw[16:24])
    return width, height


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
    for field in (
        "filer_name",
        "filing_date",
        "page_count",
        "notes",
        "no_transactions_stated",
        "transactions",
    ):
        assert field in PTR_VISION_SCHEMA["properties"]
        assert field in PTR_VISION_SCHEMA["required"]
    assert PTR_VISION_SCHEMA["properties"]["no_transactions_stated"]["type"] == "boolean"

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
        "amount_column_letter",
        "cap_gains_over_200",
        "comment",
        "legibility",
    ):
        assert field in item["properties"], f"missing schema field: {field}"
        assert field in item["required"], f"schema field not required: {field}"
    letter = item["properties"]["amount_column_letter"]
    assert letter["anyOf"] == [
        {"type": "string", "enum": list("ABCDEFGHIJK")},
        {"type": "null"},
    ]

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
    # Opus 5 caches nothing below 512 tokens and Sonnet 5 below 1,024;
    # ~4 chars/token is the floor this prompt has to clear for cache_control
    # to do anything at all.
    assert len(ptr_vision.SYSTEM_PROMPT) > 4_500


# ---------------------------------------------------------------------------
# Versioning
# ---------------------------------------------------------------------------


def test_parser_version_is_generic_and_matched_by_prefix() -> None:
    assert VISION_PARSER_VERSION == "claude-vision-v2"
    assert is_vision_parser_version(VISION_PARSER_VERSION)
    assert is_vision_parser_version("claude-sonnet-5-vision-v1")
    assert is_vision_parser_version("Claude-Opus-5-Vision-v3")
    assert not is_vision_parser_version("regex-v1")
    assert not is_vision_parser_version("haiku-4.5-fallback-v1")
    assert not is_vision_parser_version(None)
    assert not is_vision_parser_version("")


def test_status_override_applies_to_old_and_new_vision_versions() -> None:
    stub = _stub()
    trades = [object()]
    for version in ("claude-sonnet-5-vision-v1", VISION_PARSER_VERSION):
        parsed = HousePtrParseResult(
            doc_id=stub.doc_id,
            parser_confidence=0.3,
            parser_version=version,
            vision_report={"ok": True, "needsReview": False},
        )
        assert resolve_house_stub_status(stub, parsed, trades) == "parsed", version  # type: ignore[arg-type]
        parsed_review = parsed.model_copy(update={"vision_report": {"ok": True, "needsReview": True}})
        assert resolve_house_stub_status(stub, parsed_review, trades) == "needs_review", version  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Request shape + good response (document-block fallback)
# ---------------------------------------------------------------------------


def test_good_response_parses_and_request_is_well_formed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    payload = _payload(
        CHEVRON_VISION_ROW,
        {**CHEVRON_VISION_ROW, "ticker": None, "legibility": "partial"},
        filer_name="Roger Williams",
        filing_date="2026-01-05",
        page_count=2,
        notes="Second page is a faxed continuation.",
    )
    captured, calls = _install_fake_client(monkeypatch, payload)

    result = extract_via_vision(_write_pdf(tmp_path))

    # Two independent reads of the same content.
    assert calls == [1, 1]
    assert captured[0]["messages"] == captured[1]["messages"]
    assert result["ok"] is True
    assert result["skipped"] is False
    assert len(result["transactions"]) == 2
    assert result["filer_name"] == "Roger Williams"
    assert result["page_count"] == 2
    assert result["legibility"] == {"clear": 1, "partial": 1, "illegible": 0, "total": 2}
    assert result["confidence"] == 0.8
    assert result["needs_review"] is False
    assert result["parser_version"] == VISION_PARSER_VERSION
    assert result["model"] == "claude-opus-5"

    # Request shape
    request = captured[0]
    assert request["model"] == MODEL_ID == "claude-opus-5"
    assert request["thinking"] == {"type": "adaptive"}
    assert request["output_config"]["effort"] == ptr_vision.resolve_effort() == "medium"
    assert request["max_tokens"] == ptr_vision.MAX_OUTPUT_TOKENS == 64000
    assert request["output_config"]["format"]["type"] == "json_schema"
    assert request["output_config"]["format"]["schema"] is PTR_VISION_SCHEMA
    assert "temperature" not in request
    assert "top_p" not in request
    assert "budget_tokens" not in json.dumps(request["thinking"])

    # Cached, substantive system block
    assert request["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert request["system"][0]["type"] == "text"

    # No renderable pages -> the PDF document block precedes the text block
    content = request["messages"][0]["content"]
    assert content[0]["type"] == "document"
    assert content[0]["source"]["type"] == "base64"
    assert content[0]["source"]["media_type"] == "application/pdf"
    assert "\n" not in content[0]["source"]["data"]
    assert content[1]["type"] == "text"
    assert result["orientation"] is None
    assert captured.orientation_requests == []  # type: ignore[attr-defined]


def test_model_id_env_override(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _enable(monkeypatch)
    monkeypatch.setenv("CAPITOL_PTR_VISION_MODEL", "claude-sonnet-5")
    captured, _ = _install_fake_client(monkeypatch, _payload(CHEVRON_VISION_ROW))

    result = extract_via_vision(_write_pdf(tmp_path))

    assert captured[0]["model"] == "claude-sonnet-5"
    assert captured[1]["model"] == "claude-sonnet-5"
    assert result["model"] == "claude-sonnet-5"
    # Cost follows the override: two reads at Sonnet 5 rates.
    per_read = (4_000 * 2.0 + 2_000 * 0.2 + 1_000 * 2.5 + 800 * 10.0) / 1_000_000
    assert result["cost_usd"] == pytest.approx(2 * per_read)


def test_filing_context_is_appended_to_the_user_turn(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    captured, _ = _install_fake_client(monkeypatch, _payload(CHEVRON_VISION_ROW))

    extract_via_vision(_write_pdf(tmp_path), filing_year=2023, filing_date="2023-08-11")

    text = captured[0]["messages"][0]["content"][-1]["text"]
    assert "2023-08-11" in text
    assert "never to fill in a date you cannot read" in text
    # The cached system prompt is untouched by per-filing context.
    assert captured[0]["system"][0]["text"] == ptr_vision.SYSTEM_PROMPT


# ---------------------------------------------------------------------------
# Page images + orientation
# ---------------------------------------------------------------------------


def test_orientation_heuristic_rotates_portrait_pages() -> None:
    # The paper form is landscape: a portrait page is a sideways scan.
    assert orientation_heuristic(1275, 1650) == 90
    assert orientation_heuristic(1650, 1275) == 0
    assert orientation_heuristic(1000, 1000) == 0


def _orientation_client(
    monkeypatch: pytest.MonkeyPatch,
    answers: list[str],
    *,
    error: Exception | None = None,
) -> tuple[Any, list[dict]]:
    captured, _ = _install_fake_client(
        monkeypatch, _payload(), orientation=answers, orientation_error=error
    )
    provider = ptr_vision_provider.AnthropicProvider(ptr_vision._client_once)
    return provider, captured.orientation_requests  # type: ignore[attr-defined]


def _fake_render(calls: list[tuple[int, int]]) -> Any:
    def render(rotation: int, max_long_edge: int = ptr_vision.MAX_IMAGE_LONG_EDGE) -> bytes:
        calls.append((rotation, max_long_edge))
        return f"png-{rotation}".encode()

    return render


def test_detect_orientation_compares_four_candidates_and_confirms(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, requests = _orientation_client(monkeypatch, ["270", "YES"])
    rendered: list[tuple[int, int]] = []

    rotation, method, usage = detect_orientation(
        client, _fake_render(rendered), width=612, height=792
    )

    assert (rotation, method) == (270, "model-confirmed")
    # All four candidates rendered small, in rotation order.
    small = ptr_vision.ORIENTATION_CANDIDATE_LONG_EDGE
    assert rendered == [(0, small), (90, small), (180, small), (270, small)]
    assert len(requests) == 2

    first = requests[0]
    assert first["model"] == ORIENTATION_MODEL_ID == "claude-haiku-4-5"
    assert first["max_tokens"] <= 16
    assert "thinking" not in first
    content = first["messages"][0]["content"]
    assert [block["type"] for block in content] == [
        "text", "image", "text", "image", "text", "image", "text", "image", "text",
    ]
    assert [block["text"] for block in content if block["type"] == "text"][:4] == [
        "Label 0:", "Label 90:", "Label 180:", "Label 270:",
    ]
    assert all(
        block["source"]["media_type"] == "image/png" for block in content if block["type"] == "image"
    )
    assert content[3]["source"]["data"] == base64.b64encode(b"png-90").decode()
    assert "0, 90, 180, or 270" in content[-1]["text"]

    # The confirmation shows the chosen candidate alone.
    confirm = requests[1]["messages"][0]["content"]
    assert confirm[0]["source"]["data"] == base64.b64encode(b"png-270").decode()
    assert "YES or NO" in confirm[1]["text"]
    assert usage["input"] == 3_000  # both Haiku calls counted


def test_detect_orientation_falls_back_when_confirmation_says_no(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Picked 0 for a portrait page, then retracted: the heuristic (90) wins.
    client, _ = _orientation_client(monkeypatch, ["0", "NO"])
    assert detect_orientation(client, _fake_render([]), width=612, height=792)[:2] == (
        90,
        "model-corrected",
    )
    # Picked 90 and retracted, but 90 is also the heuristic: take the opposite turn.
    client, _ = _orientation_client(monkeypatch, ["90", "NO"])
    assert detect_orientation(client, _fake_render([]), width=612, height=792)[:2] == (
        270,
        "model-corrected",
    )


def test_detect_orientation_falls_back_to_the_heuristic_when_the_call_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _ = _orientation_client(monkeypatch, ["0"], error=RuntimeError("haiku down"))
    assert detect_orientation(client, _fake_render([]), width=612, height=792)[:2] == (
        90,
        "heuristic",
    )
    assert detect_orientation(client, _fake_render([]), width=792, height=612)[:2] == (
        0,
        "heuristic",
    )


def test_detect_orientation_falls_back_on_an_unparseable_answer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, _ = _orientation_client(monkeypatch, ["it is sideways"])
    assert detect_orientation(client, _fake_render([]), width=612, height=792)[:2] == (
        90,
        "heuristic",
    )


def test_detect_orientation_falls_back_when_rendering_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, requests = _orientation_client(monkeypatch, ["0"])

    def broken(_rotation: int, _max_long_edge: int = 0) -> bytes:
        raise RuntimeError("render exploded")

    assert detect_orientation(client, broken, width=612, height=792)[:2] == (90, "heuristic")
    assert requests == []


def test_image_request_shape_with_a_real_pdf(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _enable(monkeypatch)
    pdf = _write_real_pdf(tmp_path, portrait=True, pages=2)
    captured, calls = _install_fake_client(monkeypatch, _payload(CHEVRON_VISION_ROW), orientation="0")

    result = extract_via_vision(pdf)

    assert calls == [1, 1]
    content = captured[0]["messages"][0]["content"]
    # label, image, label, image, instruction
    assert [block["type"] for block in content] == ["text", "image", "text", "image", "text"]
    assert content[0]["text"] == "Page 1 of 2:"
    assert content[2]["text"] == "Page 2 of 2:"
    sizes: list[tuple[int, int]] = []
    for block in (content[1], content[3]):
        assert block["source"] == {
            "type": "base64",
            "media_type": "image/png",
            "data": block["source"]["data"],
        }
        assert "\n" not in block["source"]["data"]
        width, height = _png_size(block["source"]["data"])
        assert max(width, height) <= ptr_vision.MAX_IMAGE_LONG_EDGE
        assert height > width  # answered 0 -> left portrait
        sizes.append((width, height))
    assert content[-1]["type"] == "text"
    assert "Transcribe every transaction row" in content[-1]["text"]
    assert not any(block["type"] == "document" for block in content)
    # Both reads saw byte-identical images.
    assert captured[1]["messages"] == captured[0]["messages"]

    # Portrait pages are the typed form: no close-up strips.
    assert result["orientation"] == [
        {"page": 1, "rotation": 0, "method": "model-confirmed", "width": sizes[0][0], "height": sizes[0][1], "strips": 0, "gridColumns": 0},
        {"page": 2, "rotation": 0, "method": "model-confirmed", "width": sizes[1][0], "height": sizes[1][1], "strips": 0, "gridColumns": 0},
    ]
    assert result["chunks"] == [
        {
            "chunk": 1,
            "pages": "1-2",
            "rowsA": 1,
            "rowsB": 1,
            "matched": 1,
            "fieldDisagreements": {},
            "halvedA": False,
            "halvedB": False,
            "readBFailed": None,
            "letterConflicts": 0,
            "usage": result["chunks"][0]["usage"],
            "costUsd": result["chunks"][0]["costUsd"],
        }
    ]
    assert "This filing has 2 page(s); all of them are shown." in content[-1]["text"]
    orientation_requests = captured.orientation_requests  # type: ignore[attr-defined]
    assert len(orientation_requests) == 4  # per page: four candidates, then a confirmation
    assert all(request["model"] == ORIENTATION_MODEL_ID for request in orientation_requests)
    # Candidates are rendered smaller than the read images.
    candidate = orientation_requests[0]["messages"][0]["content"][1]
    assert max(_png_size(candidate["source"]["data"])) <= ptr_vision.ORIENTATION_CANDIDATE_LONG_EDGE

    # Usage and cost are summed across the orientation calls and both reads.
    assert result["usage"] == {
        "input": 2 * 4_000 + 4 * 1_500,
        "cache_read": 2 * 2_000,
        "cache_write": 2 * 1_000,
        "output": 2 * 800 + 4 * 2,
    }
    read_cost = (4_000 * 5.0 + 2_000 * 0.5 + 1_000 * 6.25 + 800 * 25.0) / 1_000_000
    orient_cost = (1_500 * 1.0 + 2 * 5.0) / 1_000_000
    assert result["cost_usd"] == pytest.approx(2 * read_cost + 4 * orient_cost)
    assert [call["label"] for call in result["calls"]] == ["orientation", "read A", "read B"]
    assert result["calls"][0]["model"] == ORIENTATION_MODEL_ID
    assert result["calls"][1]["model"] == MODEL_ID
    # Each read records what it saw, one compact line per row, for reviewers.
    assert result["calls"][1]["rows"] == [
        "Chevron Corporation Common Stock | sale_partial | 2025-12-22 | 2025-12-22 | 15001-50000 | - | clear"
    ]
    assert result["calls"][2]["rows"] == result["calls"][1]["rows"]


def test_portrait_page_is_rotated_upright_before_the_read(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    pdf = _write_real_pdf(tmp_path, portrait=True)
    captured, _ = _install_fake_client(
        monkeypatch, _payload(CHEVRON_VISION_ROW), orientation=["90", "YES"]
    )

    result = extract_via_vision(pdf)

    content = captured[0]["messages"][0]["content"]
    image = content[1]
    width, height = _png_size(image["source"]["data"])
    assert width > height  # rotated 90 -> landscape
    assert result["orientation"][0]["rotation"] == 90
    assert result["orientation"][0]["method"] == "model-confirmed"
    assert result["orientation"][0]["width"] == width
    assert result["orientation"][0]["height"] == height
    # Landscape after rotation is the paper form: two close-up strips follow
    # the full page, each captioned, before the instruction.
    assert [block["type"] for block in content] == [
        "text", "image", "text", "image", "text", "image", "text",
    ]
    assert "close-up strip 1 of 2" in content[2]["text"] and "top part" in content[2]["text"]
    assert "close-up strip 2 of 2" in content[4]["text"] and "bottom part" in content[4]["text"]
    for strip in (content[3], content[5]):
        strip_width, strip_height = _png_size(strip["source"]["data"])
        assert max(strip_width, strip_height) <= ptr_vision.MAX_IMAGE_LONG_EDGE
        # The strip covers 58% of the page width at a higher zoom than the page.
        assert strip_width > 0.58 * width
    assert result["orientation"][0]["strips"] == 2


def test_portrait_page_uses_the_heuristic_when_haiku_is_down(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    pdf = _write_real_pdf(tmp_path, portrait=True)
    captured, _ = _install_fake_client(
        monkeypatch, _payload(CHEVRON_VISION_ROW), orientation_error=RuntimeError("503")
    )

    result = extract_via_vision(pdf)

    image = captured[0]["messages"][0]["content"][1]
    width, height = _png_size(image["source"]["data"])
    assert width > height
    assert result["orientation"] == [
        {"page": 1, "rotation": 90, "method": "heuristic", "width": width, "height": height, "strips": 2, "gridColumns": 0}
    ]
    assert result["ok"] is True


def test_document_block_fallback_when_pymupdf_is_unavailable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    pdf = _write_real_pdf(tmp_path)
    monkeypatch.setattr(ptr_vision, "_pdf_module", lambda: None)
    captured, calls = _install_fake_client(monkeypatch, _payload(CHEVRON_VISION_ROW))

    result = extract_via_vision(pdf)

    assert calls == [1, 1]
    content = captured[0]["messages"][0]["content"]
    assert content[0]["type"] == "document"
    assert content[1]["type"] == "text"
    assert result["orientation"] is None
    assert captured.orientation_requests == []  # type: ignore[attr-defined]
    assert result["ok"] is True


# ---------------------------------------------------------------------------
# Two-read agreement
# ---------------------------------------------------------------------------


def test_reconcile_reads_keeps_fields_both_reads_agree_on() -> None:
    rows, agreement = reconcile_reads([ATT_ROW, GE_ROW], [dict(ATT_ROW), dict(GE_ROW)])

    assert agreement == {
        "rowsA": 2,
        "rowsB": 2,
        "matched": 2,
        "unmatchedA": 0,
        "unmatchedB": 0,
        "rowCountsAgree": True,
        "fieldDisagreements": {},
    }
    assert [row["asset_description"] for row in rows] == ["AT&T", "General Electric"]
    assert rows[0]["transaction_date"] == "2022-01-06"
    assert rows[0]["amount_min"] == 1001 and rows[0]["amount_max"] == 15000
    assert rows[0]["transaction_type"] == "purchase"
    assert rows[1]["transaction_type"] == "sale_partial"
    assert [row["legibility"] for row in rows] == ["partial", "partial"]


def test_reconcile_reads_matches_on_normalized_description() -> None:
    # Case, punctuation and an "Inc." suffix should not stop a match.
    read_b = {**ATT_ROW, "asset_description": "at & t inc"}
    rows, agreement = reconcile_reads([ATT_ROW], [read_b])

    assert agreement["matched"] == 1
    # Same legibility on both: the longer of two agreeing-ish descriptions wins.
    assert rows[0]["asset_description"] == "at & t inc"
    assert rows[0]["transaction_date"] == "2022-01-06"


def test_reconcile_reads_prefers_the_more_legible_description() -> None:
    # A handwriting misread of the same row ("Art+T" for "AT&T") still pairs
    # up, and the reading the model rated clear wins over the longer one.
    read_a = {**ATT_ROW, "legibility": "clear"}
    read_b = {**ATT_ROW, "asset_description": "Art+T", "legibility": "partial"}
    rows, agreement = reconcile_reads([read_a], [read_b])

    assert agreement["matched"] == 1
    assert rows[0]["asset_description"] == "AT&T"
    assert rows[0]["legibility"] == "partial"  # worst of the two ratings


def test_reconcile_reads_nulls_a_disputed_transaction_date_and_marks_illegible() -> None:
    read_b = {**ATT_ROW, "transaction_date": "2023-07-12"}
    rows, agreement = reconcile_reads([ATT_ROW], [read_b])

    assert rows[0]["transaction_date"] is None
    assert rows[0]["legibility"] == "illegible"
    assert rows[0]["amount_min"] == 1001  # untouched fields survive
    assert "transaction_date" in rows[0]["comment"]
    assert agreement["fieldDisagreements"] == {"transaction_date": 1}
    assert agreement["rowCountsAgree"] is True


def test_reconcile_reads_nulls_a_disputed_amount_and_marks_illegible() -> None:
    read_b = {**ATT_ROW, "amount_min": 15001, "amount_max": 50000}
    rows, agreement = reconcile_reads([ATT_ROW], [read_b])

    assert rows[0]["amount_min"] is None and rows[0]["amount_max"] is None
    assert rows[0]["legibility"] == "illegible"
    assert rows[0]["transaction_date"] == "2022-01-06"
    assert agreement["fieldDisagreements"] == {"amount": 1}


def test_reconcile_reads_nulls_a_disputed_type_and_marks_illegible() -> None:
    read_b = {**ATT_ROW, "transaction_type": "sale"}
    rows, agreement = reconcile_reads([ATT_ROW], [read_b])

    assert rows[0]["transaction_type"] is None
    assert rows[0]["legibility"] == "illegible"
    assert agreement["fieldDisagreements"] == {"transaction_type": 1}


def test_reconcile_reads_soft_fields_null_but_only_downgrade_to_partial() -> None:
    read_a = {**CHEVRON_VISION_ROW, "owner": "self"}
    read_b = {**CHEVRON_VISION_ROW, "owner": "spouse", "ticker": "cvx", "notification_date": None}
    rows, agreement = reconcile_reads([read_a], [read_b])

    assert rows[0]["owner"] is None
    assert rows[0]["ticker"] == "CVX"  # case-insensitive agreement
    assert rows[0]["notification_date"] is None
    assert rows[0]["transaction_date"] == "2025-12-22"
    assert rows[0]["legibility"] == "partial"
    assert agreement["fieldDisagreements"] == {"notification_date": 1, "owner": 1}


def test_reconcile_reads_keeps_an_unmatched_row_as_illegible() -> None:
    read_b = {**ATT_ROW, "asset_description": "Art+T", "transaction_date": "2023-07-12"}
    rows, agreement = reconcile_reads([ATT_ROW, GE_ROW], [dict(ATT_ROW), read_b])

    assert agreement["rowsA"] == 2 and agreement["rowsB"] == 2
    assert agreement["matched"] == 1
    assert agreement["unmatchedA"] == 1 and agreement["unmatchedB"] == 1
    assert agreement["rowCountsAgree"] is True
    assert [row["asset_description"] for row in rows] == ["AT&T", "General Electric", "Art+T"]
    assert rows[0]["legibility"] == "partial"
    assert rows[1]["legibility"] == "illegible"
    assert "read A" in rows[1]["comment"]
    assert rows[2]["legibility"] == "illegible"
    assert "read B" in rows[2]["comment"]
    assert rows[2]["transaction_date"] == "2023-07-12"  # values kept for the reviewer


def test_reconcile_reads_records_a_row_count_mismatch() -> None:
    rows, agreement = reconcile_reads([ATT_ROW, GE_ROW], [dict(ATT_ROW)])

    assert agreement["rowsA"] == 2 and agreement["rowsB"] == 1 and agreement["matched"] == 1
    assert agreement["rowCountsAgree"] is False
    assert [row["legibility"] for row in rows] == ["partial", "illegible"]


def test_match_rows_uses_transaction_type_to_pair_same_asset_rows() -> None:
    buy = {**ATT_ROW, "transaction_type": "purchase"}
    sell = {**ATT_ROW, "transaction_type": "sale"}
    # Read B lists them in the opposite order.
    pairs = match_rows([buy, sell], [dict(sell), dict(buy)])
    assert [(i, j) for i, j, _ratio in pairs] == [(0, 1), (1, 0)]


def test_match_rows_ignores_dissimilar_descriptions() -> None:
    assert match_rows([ATT_ROW], [GE_ROW]) == []
    assert match_rows([ATT_ROW], [{**ATT_ROW, "asset_description": "Apple Inc"}]) == []
    assert match_rows([GE_ROW], [{**GE_ROW, "asset_description": "General Motors"}]) == []
    # A bare generic suffix never matches on containment alone.
    assert match_rows([{**ATT_ROW, "asset_description": "Inc"}], [{**ATT_ROW, "asset_description": "Apple Inc"}]) == []


def test_two_reads_end_to_end_agree(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _enable(monkeypatch)
    _install_fake_client(
        monkeypatch, [_payload(ATT_ROW, GE_ROW), _payload(dict(ATT_ROW), dict(GE_ROW))]
    )

    result = extract_via_vision(_write_pdf(tmp_path), filing_year=2023, filing_date="2023-08-11")

    assert result["ok"] is True
    assert result["needs_review"] is False
    assert result["needs_review_reasons"] == []
    assert result["read_agreement"]["matched"] == 2
    assert [row["transaction_date"] for row in result["transactions"]] == ["2022-01-06"] * 2
    assert result["attempts"] == 2
    assert result["usage"]["input"] == 8_000


def test_two_reads_end_to_end_disagree_on_a_date(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    _install_fake_client(
        monkeypatch,
        [
            _payload(ATT_ROW, GE_ROW),
            _payload({**ATT_ROW, "transaction_date": "2023-07-12"}, dict(GE_ROW)),
        ],
    )

    result = extract_via_vision(_write_pdf(tmp_path), filing_year=2023)

    assert result["needs_review"] is True
    assert result["needs_review_reasons"] == ["reads disagree on transaction_date"]
    assert result["legibility"] == {"clear": 0, "partial": 1, "illegible": 1, "total": 2}
    assert result["transactions"][0]["transaction_date"] is None
    assert result["transactions"][1]["transaction_date"] == "2022-01-06"


def test_two_reads_end_to_end_with_different_row_counts_need_review(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    _install_fake_client(monkeypatch, [_payload(ATT_ROW, GE_ROW), _payload(dict(ATT_ROW))])

    result = extract_via_vision(_write_pdf(tmp_path), filing_year=2023)

    assert result["read_agreement"]["rowsA"] == 2
    assert result["read_agreement"]["rowsB"] == 1
    assert result["read_agreement"]["rowCountsAgree"] is False
    assert result["needs_review"] is True
    assert "reads disagree on row count" in result["needs_review_reasons"]
    metadata = build_vision_metadata(result)
    assert metadata["readAgreement"] == result["read_agreement"]
    assert metadata["needsReview"] is True


def test_example_row_is_scrubbed_from_both_reads_before_matching(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    example = {**ATT_ROW, "asset_description": "Example: Mega Corp. Common Stock",
               "transaction_date": "2020-02-05"}
    leaked = {**GE_ROW, "transaction_date": "2020-02-05"}
    _install_fake_client(
        monkeypatch,
        [_payload(example, ATT_ROW, leaked), _payload(dict(ATT_ROW), dict(example), dict(leaked))],
    )

    result = extract_via_vision(_write_pdf(tmp_path), filing_year=2023)

    assert result["example_row_scrubs"] == 4  # example row + leaked date, in each read
    assert [row["asset_description"] for row in result["transactions"]] == ["AT&T", "General Electric"]
    assert result["transactions"][1]["transaction_date"] is None
    assert result["transactions"][1]["legibility"] == "illegible"
    assert result["read_agreement"]["rowsA"] == 2 and result["read_agreement"]["rowsB"] == 2
    assert build_vision_metadata(result)["exampleRowScrubs"] == 4


def test_second_read_failure_keeps_rows_but_sends_them_all_to_review(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    message = _Message(_payload(ATT_ROW, GE_ROW))
    seen: list[int] = []

    class _BadRequest(Exception):
        status_code = 400

    class _SecondReadFails:
        def stream(self, **_kwargs: Any) -> _StreamManager:
            seen.append(1)
            if len(seen) == 2:
                raise _BadRequest("boom")
            return _StreamManager(message)

    class _Client:
        messages = _SecondReadFails()

    monkeypatch.setattr(ptr_vision, "_client", None)
    monkeypatch.setattr(ptr_vision, "_client_once", lambda: _Client())

    result = extract_via_vision(_write_pdf(tmp_path), filing_year=2023)

    assert len(seen) == 2
    assert result["ok"] is True
    assert result["needs_review"] is True
    assert result["read_agreement"]["rowsB"] is None
    assert "boom" in result["read_agreement"]["readBFailed"]
    assert [row["legibility"] for row in result["transactions"]] == ["illegible", "illegible"]
    assert result["calls"][1]["ok"] is False


def test_run_vision_parse_keeps_bond_names_the_text_cleaner_would_gut(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # The text-layer cleaner strips "...<3+ digits> " as an account number,
    # which turns this handwritten muni bond into "5%". Pin that behaviour so
    # the guard is exercised whatever the live cleaner does.
    import re as _re

    _enable(monkeypatch)
    monkeypatch.setattr(
        house_ptr,
        "clean_asset_description",
        lambda raw, _ticker: _re.sub(r"^.*?\b\d{3,}\s+", "", raw),
    )
    bond = {**ATT_ROW, "asset_description": "MINNESOTA ST BD GRP 160 5%", "owner": "spouse"}
    _install_fake_client(monkeypatch, _payload(bond, dict(CHEVRON_VISION_ROW)))
    stub = _stub()

    result, metadata = house_ptr._run_vision_parse(_write_pdf(tmp_path), stub, "")

    assert result is not None
    parsed, trades = result
    assert [t.asset_description for t in parsed.transactions] == [
        "MINNESOTA ST BD GRP 160 5%",
        "Chevron Corporation Common Stock",  # untouched when cleaning is benign
    ]
    assert trades[0].asset_description == "MINNESOTA ST BD GRP 160 5%"
    assert metadata["descriptionsRestored"] == 1


def test_run_vision_parse_drops_rows_with_a_disputed_type(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    _install_fake_client(
        monkeypatch,
        [
            _payload(ATT_ROW, GE_ROW),
            _payload({**ATT_ROW, "transaction_type": "sale"}, dict(GE_ROW)),
        ],
    )
    stub = _stub()
    stub.filing_year = 2023
    stub.filing_date = "2023-08-11"

    result, metadata = house_ptr._run_vision_parse(_write_pdf(tmp_path), stub, "")

    assert result is not None
    parsed, trades = result
    # AT&T (disputed type) is dropped rather than defaulting to "purchase".
    assert [t.asset_description for t in parsed.transactions] == ["General Electric"]
    assert len(trades) == 1
    assert metadata["rowsDroppedForType"] == 1
    assert metadata["needsReview"] is True
    assert resolve_house_stub_status(stub, parsed, trades) == "needs_review"


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
    monkeypatch.setattr(ptr_vision, "count_pdf_pages", lambda _path: 61)
    _, calls = _install_fake_client(monkeypatch, _payload(CHEVRON_VISION_ROW))

    result = extract_via_vision(_write_pdf(tmp_path))

    assert calls == []
    assert result["skipped"] is True
    assert result["page_count"] == 61
    assert "61 pages" in str(result["reason"])
    assert ptr_vision.MAX_VISION_PDF_PAGES == 60
    assert result["needs_review"] is True


def test_byte_limit_skips_without_calling_the_model(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    monkeypatch.setattr(ptr_vision, "MAX_VISION_PDF_BYTES", 32)
    _, calls = _install_fake_client(monkeypatch, _payload(CHEVRON_VISION_ROW))
    pdf = tmp_path / "big.pdf"
    pdf.write_bytes(b"%PDF-1.4\n" + b"x" * 500)

    result = extract_via_vision(pdf)

    assert calls == []
    assert "too large" in str(result["reason"])


def test_refusal_and_truncation_are_reported_not_parsed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    payload = _payload(CHEVRON_VISION_ROW)

    _, calls = _install_fake_client(monkeypatch, payload, stop_reason="refusal")
    refused = extract_via_vision(_write_pdf(tmp_path))
    assert calls == [1]  # a refused first read is not repeated
    assert refused["skipped"] is True
    assert refused["transactions"] == []
    assert "refused" in str(refused["reason"])
    # usage is still recorded so the run accounts for what it spent
    assert refused["usage"]["output"] == 800

    _, calls = _install_fake_client(monkeypatch, payload, stop_reason="max_tokens")
    truncated = extract_via_vision(_write_pdf(tmp_path))
    assert calls == [1]
    assert truncated["skipped"] is True
    assert "max_tokens" in str(truncated["reason"])


def test_retries_once_on_a_429_then_succeeds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    monkeypatch.setattr(ptr_vision, "RETRY_SLEEP_SECONDS", 0)

    message = _Message(_payload(CHEVRON_VISION_ROW))
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

    # read A: 429 then success; read B: success
    assert len(attempts) == 3
    assert result["ok"] is True
    assert result["attempts"] == 3
    assert [call["attempts"] for call in result["calls"]] == [2, 1]


def test_sdk_without_output_config_falls_back_to_strict_tool_use(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    payload = _payload(CHEVRON_VISION_ROW)

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

    # read A: rejected then tool form; read B goes straight to the tool form.
    assert len(seen) == 3
    assert "output_config" not in seen[1]
    assert "output_config" not in seen[2]
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
    payload = _payload(CHEVRON_VISION_ROW)

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

    assert len(seen) == 3
    assert seen[1]["output_config"] == {"effort": ptr_vision.EFFORT}
    assert seen[2]["output_config"] == {"effort": ptr_vision.EFFORT}
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

    assert len(attempts) == 1  # the second read is not attempted
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


def test_pricing_table_covers_the_read_and_orientation_models() -> None:
    assert pricing_for_model("claude-opus-5") == (5.0, 25.0)
    assert pricing_for_model("claude-haiku-4-5") == (1.0, 5.0)
    assert pricing_for_model("claude-sonnet-5") == (2.0, 10.0)
    # Family fallback for dated / future ids, Opus tier for anything unknown.
    assert pricing_for_model("claude-opus-5-20270101") == (5.0, 25.0)
    assert pricing_for_model("claude-haiku-9") == (1.0, 5.0)
    assert pricing_for_model("something-else") == ptr_vision.DEFAULT_PRICING
    assert (ptr_vision.PRICE_INPUT_PER_MTOK, ptr_vision.PRICE_OUTPUT_PER_MTOK) == (5.0, 25.0)


def test_cost_uses_opus_5_rates_with_cache_multipliers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CAPITOL_PTR_VISION_MODEL", raising=False)
    usage = {
        "input": 1_000_000,
        "cache_read": 1_000_000,
        "cache_write": 1_000_000,
        "output": 1_000_000,
    }
    # 1M plain input ($5) + 1M cache reads ($0.50) + 1M cache writes ($6.25)
    # + 1M output ($25) = $36.75
    assert estimate_cost_usd(usage) == pytest.approx(36.75)
    assert estimate_cost_usd(usage, "claude-opus-5") == pytest.approx(36.75)


def test_cost_uses_haiku_4_5_rates_for_orientation_calls() -> None:
    # 1M plain input ($1) + 1M cache reads ($0.10) + 1M cache writes ($1.25)
    # + 1M output ($5) = $7.35
    cost = estimate_cost_usd(
        {
            "input": 1_000_000,
            "cache_read": 1_000_000,
            "cache_write": 1_000_000,
            "output": 1_000_000,
        },
        ORIENTATION_MODEL_ID,
    )
    assert cost == pytest.approx(7.35)


def test_vision_metadata_carries_usage_and_cost(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    _install_fake_client(monkeypatch, _payload(CHEVRON_VISION_ROW, filer_name="Roger Williams"))

    metadata = build_vision_metadata(extract_via_vision(_write_pdf(tmp_path)))

    assert metadata["model"] == MODEL_ID
    assert metadata["orientationModel"] == ORIENTATION_MODEL_ID
    assert metadata["parserVersion"] == VISION_PARSER_VERSION
    assert metadata["ok"] is True
    assert metadata["needsReview"] is False
    assert metadata["rowCount"] == 1
    # Summed over two reads (document fallback: no orientation call).
    assert metadata["usage"] == {
        "inputTokens": 8_000,
        "cacheReadTokens": 4_000,
        "cacheWriteTokens": 2_000,
        "outputTokens": 1_600,
    }
    # per read: 4000*$5 + 2000*$0.50 + 1000*$6.25 + 800*$25, per MTok = $0.04725
    assert metadata["costUsd"] == pytest.approx(0.0945)
    assert metadata["pricing"]["inputPerMTok"] == 5.0
    assert metadata["pricing"]["outputPerMTok"] == 25.0
    assert metadata["pricing"]["orientation"] == {"inputPerMTok": 1.0, "outputPerMTok": 5.0}
    assert metadata["orientation"] is None
    assert metadata["readAgreement"]["matched"] == 1
    assert [call["label"] for call in metadata["calls"]] == ["read A", "read B"]
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
    _install_fake_client(monkeypatch, _payload(*rows))

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
    _install_fake_client(monkeypatch, _payload(*rows))

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
    _install_fake_client(monkeypatch, _payload(CHEVRON_VISION_ROW))
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
        _payload(CHEVRON_VISION_ROW, filer_name="Roger Williams", filing_date="2026-01-05"),
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
        _payload(
            {
                **CHEVRON_VISION_ROW,
                "asset_description": "Bitcoin",
                "ticker": "BTC",
                "transaction_type": "purchase",
            }
        ),
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
        _payload(
            {**CHEVRON_VISION_ROW, "owner": "dependent"},
            {**CHEVRON_VISION_ROW, "owner": "joint", "asset_description": "Apple Inc.",
             "ticker": "AAPL", "transaction_type": "exchange"},
            {**CHEVRON_VISION_ROW, "owner": "spouse", "asset_description": "RTX Corporation",
             "ticker": "RTX", "transaction_type": "purchase"},
        ),
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
    _, calls = _install_fake_client(monkeypatch, _payload(CHEVRON_VISION_ROW))

    parsed, rows = house_ptr.parse_house_ptr_pdf(
        pdf, stub=stub, backend="pymupdf", vision_backend="auto"
    )

    assert calls == [1, 1]
    assert parsed.parser_version == VISION_PARSER_VERSION
    assert len(rows) == 1
    assert isinstance(parsed.vision_report, dict)
    assert parsed.vision_report["ok"] is True
    assert parsed.vision_report["readAgreement"]["matched"] == 1


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
    _, calls = _install_fake_client(monkeypatch, _payload(CHEVRON_VISION_ROW))

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
    _, calls = _install_fake_client(monkeypatch, _payload(CHEVRON_VISION_ROW))

    parsed, rows = house_ptr.parse_house_ptr_pdf(
        pdf, stub=_stub(), backend="pymupdf", vision_backend="auto"
    )

    assert calls == []
    assert rows == []
    assert isinstance(parsed.vision_report, dict)
    assert parsed.vision_report["ok"] is False
    assert "CAPITOL_PTR_VISION_DISABLED" in str(parsed.vision_report["reason"])


def test_scrub_example_row_values_drops_the_form_example_and_nulls_its_dates() -> None:
    from capitol_pipeline.parsers.ptr_vision import scrub_example_row_values

    rows = [
        {"asset_description": "Example: Mega Corp. Common Stock", "transaction_date": "2020-02-05"},
        {
            "asset_description": "General Electric",
            "transaction_date": "2020-02-05",
            "notification_date": "2023-07-22",
            "legibility": "partial",
        },
        {
            "asset_description": "AT&T",
            "transaction_date": "2020-03-07",
            "notification_date": None,
            "legibility": "clear",
        },
        {"asset_description": "Apple Inc", "transaction_date": "2022-01-06", "legibility": "clear"},
    ]
    kept, scrubbed = scrub_example_row_values(rows, filing_year=2023)

    assert scrubbed == 3
    assert [r["asset_description"] for r in kept] == ["General Electric", "AT&T", "Apple Inc"]
    assert kept[0]["transaction_date"] is None
    assert kept[0]["notification_date"] == "2023-07-22"
    assert kept[0]["legibility"] == "illegible"
    assert kept[1]["transaction_date"] is None
    assert kept[1]["legibility"] == "illegible"
    assert kept[2]["transaction_date"] == "2022-01-06"
    assert kept[2]["legibility"] == "clear"


def test_scrub_example_row_values_keeps_early_2020_dates_on_2020_filings() -> None:
    from capitol_pipeline.parsers.ptr_vision import scrub_example_row_values

    rows = [{"asset_description": "Boeing", "transaction_date": "2020-02-05", "legibility": "clear"}]
    kept, scrubbed = scrub_example_row_values(rows, filing_year=2020)
    assert scrubbed == 0
    assert kept[0]["transaction_date"] == "2020-02-05"
    assert kept[0]["legibility"] == "clear"


def test_system_prompt_warns_about_the_preprinted_example_row() -> None:
    from capitol_pipeline.parsers.ptr_vision import SYSTEM_PROMPT

    assert "Mega Corp" in SYSTEM_PROMPT
    assert "02/05/20" in SYSTEM_PROMPT and "03/07/20" in SYSTEM_PROMPT
