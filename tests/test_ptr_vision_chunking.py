"""Round-two tests for the vision path: chunked long filings, the cost guard,
"nothing to report" as a terminal state, reuse of a stub's previous result,
close-up strips of the amount grid, and the amount column letter.

Never calls the real API: ``ptr_vision._client_once`` is patched with a fake
whose ``messages.stream`` answers each read from a responder callable and
whose ``messages.create`` answers the orientation questions.
"""

from __future__ import annotations

import json
import struct
import base64
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import pytest

from capitol_pipeline.cli import resolve_house_stub_status
from capitol_pipeline.models.congress import FilingStub, HousePtrTransaction, MemberMatch
from capitol_pipeline.parsers import house_ptr, ptr_vision
from capitol_pipeline.parsers.ptr_vision import (
    AMOUNT_LETTER_BANDS,
    DEFAULT_MAX_FILING_COST_USD,
    VISION_PARSER_VERSION,
    apply_amount_letter_check,
    build_vision_metadata,
    chunk_page_list,
    estimate_filing_cost_usd,
    extract_via_vision,
    grid_strip_rects,
    reconcile_reads,
    resolve_effort,
)

ROW: dict[str, Any] = {
    "owner": "self",
    "asset_description": "AT&T",
    "ticker": None,
    "asset_type_code": None,
    "transaction_type": "purchase",
    "transaction_date": "2022-01-06",
    "notification_date": "2023-07-22",
    "amount_min": 1001,
    "amount_max": 15000,
    "amount_column_letter": "A",
    "cap_gains_over_200": None,
    "comment": None,
    "legibility": "clear",
}


def _row(name: str, **overrides: Any) -> dict[str, Any]:
    return {**ROW, "asset_description": name, **overrides}


def _payload(*rows: dict[str, Any], no_transactions_stated: bool = False, **header: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "filer_name": None,
        "filing_date": None,
        "page_count": None,
        "notes": None,
        "no_transactions_stated": no_transactions_stated,
        "transactions": list(rows),
    }
    base.update(header)
    return base


# ---------------------------------------------------------------------------
# Fake client
# ---------------------------------------------------------------------------


class _Block:
    def __init__(self, type_: str, text: str | None = None) -> None:
        self.type = type_
        if text is not None:
            self.text = text


class _Usage:
    def __init__(self, input_tokens: int = 4_000, output_tokens: int = 800) -> None:
        self.input_tokens = input_tokens
        self.cache_read_input_tokens = 2_000
        self.cache_creation_input_tokens = 0
        self.output_tokens = output_tokens


class _Message:
    def __init__(self, payload: dict, *, stop_reason: str = "end_turn") -> None:
        self.content = [_Block("thinking"), _Block("text", json.dumps(payload))]
        self.stop_reason = stop_reason
        self.stop_details = None
        self.usage = _Usage()


class _OrientationMessage:
    def __init__(self, answer: str) -> None:
        self.content = [_Block("text", answer)]
        self.stop_reason = "end_turn"
        self.stop_details = None
        self.usage = _Usage(1_500, 2)
        self.usage.cache_read_input_tokens = 0


class _Stream:
    def __init__(self, message: Any) -> None:
        self._message = message

    def __enter__(self) -> "_Stream":
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False

    def get_final_message(self) -> Any:
        return self._message


Responder = Callable[[dict[str, Any], int], Any]


class _Messages:
    def __init__(self, responder: Responder, orientation: list[str]) -> None:
        self.responder = responder
        self.orientation = orientation
        self.reads: list[dict[str, Any]] = []
        self.orientation_requests: list[dict[str, Any]] = []

    def stream(self, **kwargs: Any) -> _Stream:
        index = len(self.reads)
        self.reads.append(kwargs)
        return _Stream(self.responder(kwargs, index))

    def create(self, **kwargs: Any) -> _OrientationMessage:
        index = len(self.orientation_requests)
        self.orientation_requests.append(kwargs)
        return _OrientationMessage(self.orientation[min(index, len(self.orientation) - 1)])


class _Client:
    def __init__(self, messages: _Messages) -> None:
        self.messages = messages


def _install(
    monkeypatch: pytest.MonkeyPatch,
    responder: Responder | list[dict[str, Any]] | dict[str, Any],
    *,
    orientation: list[str] | None = None,
) -> _Messages:
    """Install a fake client.

    ``responder`` is a callable ``(request_kwargs, read_index) -> message``,
    or a payload / list of payloads answered in call order (the last one
    repeats).
    """

    if not callable(responder):
        payloads = responder if isinstance(responder, list) else [responder]

        def responder(_kwargs: dict[str, Any], index: int) -> _Message:  # type: ignore[misc]
            return _Message(payloads[min(index, len(payloads) - 1)])

    messages = _Messages(responder, orientation or ["0"])  # type: ignore[arg-type]
    monkeypatch.setattr(ptr_vision, "_client", None)
    monkeypatch.setattr(ptr_vision, "_client_once", lambda: _Client(messages))
    return messages


def _enable(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "CAPITOL_PTR_VISION_DISABLED",
        "CAPITOL_PTR_VISION_MODEL",
        "CAPITOL_PTR_VISION_EFFORT",
        "CAPITOL_PTR_VISION_CHUNK_PAGES",
        "CAPITOL_PTR_VISION_MAX_COST_USD",
        "CAPITOL_PTR_VISION_GRID_ZOOM",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-not-a-real-key")
    monkeypatch.setattr(ptr_vision, "RETRY_SLEEP_SECONDS", 0)


def _write_stub_pdf(tmp_path: Path) -> Path:
    """Unrenderable stub -> document-block fallback, no orientation calls."""

    pdf = tmp_path / "stub.pdf"
    pdf.write_bytes(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\ntrailer\n%%EOF\n")
    return pdf


def _write_real_pdf(tmp_path: Path, *, pages: int, portrait: bool = True) -> Path:
    fitz = pytest.importorskip("fitz")
    document = fitz.open()
    width, height = (612, 792) if portrait else (792, 612)
    for index in range(pages):
        page = document.new_page(width=width, height=height)
        page.insert_text((72, 100), f"PERIODIC TRANSACTION REPORT page {index + 1}", fontsize=18)
    pdf = tmp_path / f"real{pages}.pdf"
    document.save(str(pdf))
    document.close()
    return pdf


def _png_size(png_b64: str) -> tuple[int, int]:
    raw = base64.b64decode(png_b64)
    assert raw[:8] == b"\x89PNG\r\n\x1a\n"
    return struct.unpack(">II", raw[16:24])


def _images(request: dict[str, Any]) -> int:
    return sum(1 for block in request["messages"][0]["content"] if block["type"] == "image")


def _user_text(request: dict[str, Any]) -> str:
    return request["messages"][0]["content"][-1]["text"]


def _stub(**overrides: Any) -> FilingStub:
    fields: dict[str, Any] = dict(
        doc_id="8219444",
        filing_year=2023,
        filing_date="2023-04-07",
        member=MemberMatch(
            id="m-K000389", name="Ro Khanna", slug="ro-khanna", party="D", state="CA", district="17"
        ),
        source="house-clerk",
        source_url="https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2023/8219444.pdf",
    )
    fields.update(overrides)
    return FilingStub(**fields)


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------


def test_chunk_page_list_groups_consecutive_pages() -> None:
    pages = [{"index": index} for index in range(1, 10)]
    groups = chunk_page_list(pages, 4)
    assert [[page["index"] for page in group] for group in groups] == [[1, 2, 3, 4], [5, 6, 7, 8], [9]]
    assert chunk_page_list([], 4) == []
    assert len(chunk_page_list(pages, 0)) == 9  # floor of one page per chunk


def test_long_filing_is_read_in_chunks_with_continuous_line_numbers(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    monkeypatch.setenv("CAPITOL_PTR_VISION_CHUNK_PAGES", "4")
    pdf = _write_real_pdf(tmp_path, pages=6)
    chunk_one = _payload(_row("Lear Corporation"), _row("Pepsico"), filer_name="Ro Khanna")
    chunk_two = _payload(_row("Microsoft"))
    # Call order: chunk 1 read A, chunk 1 read B, chunk 2 read A, chunk 2 read B.
    fake = _install(monkeypatch, [chunk_one, chunk_one, chunk_two, chunk_two])

    result = extract_via_vision(pdf, filing_year=2023, filing_date="2023-04-07")

    assert len(fake.reads) == 4
    assert [_images(request) for request in fake.reads] == [4, 4, 2, 2]
    assert "pages 1 to 4" in _user_text(fake.reads[0])
    assert "pages 5 to 6" in _user_text(fake.reads[2])
    assert "Earlier pages are not shown" in _user_text(fake.reads[2])
    assert "2023-04-07" in _user_text(fake.reads[2])
    # Page labels carry the filing-wide numbering, not the chunk's.
    assert fake.reads[2]["messages"][0]["content"][0]["text"] == "Page 5 of 6:"

    assert result["ok"] is True
    assert [row["asset_description"] for row in result["transactions"]] == [
        "Lear Corporation",
        "Pepsico",
        "Microsoft",
    ]
    assert result["filer_name"] == "Ro Khanna"  # header from the first chunk
    assert result["page_count"] == 6
    assert result["chunk_pages"] == 4
    assert [(chunk["chunk"], chunk["pages"], chunk["rowsA"], chunk["matched"]) for chunk in result["chunks"]] == [
        (1, "1-4", 2, 2),
        (2, "5-6", 1, 1),
    ]
    assert result["read_agreement"] == {
        "rowsA": 3,
        "rowsB": 3,
        "matched": 3,
        "unmatchedA": 0,
        "unmatchedB": 0,
        "rowCountsAgree": True,
        "fieldDisagreements": {},
        "chunks": 2,
    }
    assert [(call["label"], call["pages"]) for call in result["calls"]] == [
        ("orientation", "1-6"),
        ("read A", "1-4"),
        ("read B", "1-4"),
        ("read A", "5-6"),
        ("read B", "5-6"),
    ]
    # Usage is summed across every call, per-chunk usage is recorded too.
    assert result["usage"]["input"] == 4 * 4_000 + len(fake.orientation_requests) * 1_500
    assert result["chunks"][0]["usage"]["input"] == 8_000
    assert result["needs_review"] is False

    # Through the parser: line numbers keep counting across chunks.
    fake.reads.clear()
    parsed_result, metadata = house_ptr._run_vision_parse(pdf, _stub(), "")
    assert parsed_result is not None
    parsed, trades = parsed_result
    assert [t.line_number for t in parsed.transactions] == [1, 2, 3]
    assert [t.source_id for t in trades] == ["8219444:1", "8219444:2", "8219444:3"]
    assert metadata["rowsRecovered"] == 3
    assert metadata["rowsTranscribed"] == 3
    assert metadata["chunkPages"] == 4
    assert len(metadata["chunks"]) == 2


def test_truncated_read_is_retried_with_the_page_group_halved(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    monkeypatch.setenv("CAPITOL_PTR_VISION_CHUNK_PAGES", "4")
    pdf = _write_real_pdf(tmp_path, pages=4)

    def responder(request: dict[str, Any], _index: int) -> _Message:
        if _images(request) > 2:
            return _Message(_payload(), stop_reason="max_tokens")
        first_page = request["messages"][0]["content"][0]["text"]
        return _Message(_payload(_row(f"Row from {first_page}")))

    fake = _install(monkeypatch, responder)

    result = extract_via_vision(pdf)

    # A: 1-4 truncates -> 1-2, 3-4. B: the same.
    assert [_images(request) for request in fake.reads] == [4, 2, 2, 4, 2, 2]
    assert [(call["label"], call["pages"], call["stopReason"]) for call in result["calls"][1:]] == [
        ("read A", "1-4", "max_tokens"),
        ("read A", "1-2", "end_turn"),
        ("read A", "3-4", "end_turn"),
        ("read B", "1-4", "max_tokens"),
        ("read B", "1-2", "end_turn"),
        ("read B", "3-4", "end_turn"),
    ]
    assert result["ok"] is True
    assert [row["asset_description"] for row in result["transactions"]] == [
        "Row from Page 1 of 4:",
        "Row from Page 3 of 4:",
    ]
    assert result["chunks"][0]["halvedA"] is True
    assert result["chunks"][0]["halvedB"] is True
    assert result["read_agreement"]["matched"] == 2
    assert result["needs_review"] is False


def test_truncation_after_halving_gives_up_on_the_filing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    monkeypatch.setenv("CAPITOL_PTR_VISION_CHUNK_PAGES", "4")
    pdf = _write_real_pdf(tmp_path, pages=4)
    fake = _install(monkeypatch, lambda _request, _index: _Message(_payload(), stop_reason="max_tokens"))

    result = extract_via_vision(pdf)

    # 1-4 truncates, 1-2 truncates again: stop, and never pay for read B.
    assert [_images(request) for request in fake.reads] == [4, 2]
    assert result["skipped"] is True
    assert "max_tokens" in str(result["reason"])
    assert "after halving" in str(result["reason"])
    assert "pages 1-2" in str(result["reason"])
    assert result["cost_usd"] > 0  # what was spent is still accounted for
    assert result["needs_review"] is True


def test_single_page_truncation_is_not_retried(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    fake = _install(monkeypatch, lambda _r, _i: _Message(_payload(), stop_reason="max_tokens"))

    result = extract_via_vision(_write_stub_pdf(tmp_path))

    assert len(fake.reads) == 1
    assert result["skipped"] is True
    assert "max_tokens" in str(result["reason"])


# ---------------------------------------------------------------------------
# Cost guard
# ---------------------------------------------------------------------------


def test_cost_estimate_admits_sixty_pages_under_the_default_ceiling() -> None:
    sixty = estimate_filing_cost_usd(60, model="claude-opus-5", strips_per_page=2, chunk_pages=4)
    assert 20.0 < sixty <= DEFAULT_MAX_FILING_COST_USD
    one = estimate_filing_cost_usd(1, model="claude-opus-5", strips_per_page=2, chunk_pages=4)
    assert 0.2 < one < 0.6
    assert estimate_filing_cost_usd(0) == 0.0
    # Two reads: doubling the pages roughly doubles the estimate.
    assert estimate_filing_cost_usd(20, model="claude-opus-5") == pytest.approx(
        2 * estimate_filing_cost_usd(10, model="claude-opus-5"), rel=0.05
    )


def test_cost_ceiling_refuses_a_filing_before_any_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    monkeypatch.setenv("CAPITOL_PTR_VISION_MAX_COST_USD", "1.00")
    monkeypatch.setattr(ptr_vision, "count_pdf_pages", lambda _path: 40)
    fake = _install(monkeypatch, _payload(_row("x")))

    result = extract_via_vision(_write_stub_pdf(tmp_path))

    assert fake.reads == [] and fake.orientation_requests == []
    assert result["skipped"] is True
    assert "estimated cost $" in str(result["reason"])
    assert "$1.00 ceiling" in str(result["reason"])
    assert "CAPITOL_PTR_VISION_MAX_COST_USD" in str(result["reason"])
    assert result["cost_estimate_usd"] > 1.0
    assert result["cost_ceiling_usd"] == 1.0
    assert result["page_count"] == 40
    metadata = build_vision_metadata(result)
    assert metadata["costEstimateUsd"] == result["cost_estimate_usd"]
    assert metadata["costCeilingUsd"] == 1.0

    # Raising the ceiling lets the same filing through.
    monkeypatch.setenv("CAPITOL_PTR_VISION_MAX_COST_USD", "50")
    result = extract_via_vision(_write_stub_pdf(tmp_path))
    assert result["skipped"] is False
    assert len(fake.reads) == 2


def test_cost_overrun_abandons_a_filing_mid_way(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    monkeypatch.setenv("CAPITOL_PTR_VISION_CHUNK_PAGES", "1")
    # The ceiling is tiny but the pre-flight estimate is patched to pass.
    monkeypatch.setenv("CAPITOL_PTR_VISION_MAX_COST_USD", "0.05")
    monkeypatch.setattr(ptr_vision, "estimate_filing_cost_usd", lambda *_a, **_k: 0.01)
    pdf = _write_real_pdf(tmp_path, pages=3)
    fake = _install(monkeypatch, _payload(_row("x")))

    result = extract_via_vision(pdf)

    # Each read costs ~$0.04 at Opus rates; after chunk 1 (two reads) we are
    # past 1.5 x $0.05, so chunks 2 and 3 are never sent.
    assert len(fake.reads) == 2
    assert result["skipped"] is True
    assert "cost ceiling exceeded mid-filing" in str(result["reason"])
    assert len(result["chunks"]) == 1


def test_effort_is_configurable(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _enable(monkeypatch)
    assert resolve_effort() == "medium"
    monkeypatch.setenv("CAPITOL_PTR_VISION_EFFORT", "high")
    assert resolve_effort() == "high"
    fake = _install(monkeypatch, _payload(_row("x")))
    extract_via_vision(_write_stub_pdf(tmp_path))
    assert fake.reads[0]["output_config"]["effort"] == "high"
    monkeypatch.setenv("CAPITOL_PTR_VISION_EFFORT", "turbo")
    assert resolve_effort() == "medium"


# ---------------------------------------------------------------------------
# Nothing to report
# ---------------------------------------------------------------------------


def test_no_transactions_is_terminal_when_both_reads_state_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    _install(
        monkeypatch,
        _payload(no_transactions_stated=True, notes="Grid reads NOTHING TO REPORT FOR JANUARY 2023"),
    )
    pdf = _write_stub_pdf(tmp_path)

    result = extract_via_vision(pdf)

    assert result["ok"] is True
    assert result["skipped"] is False
    assert result["no_transactions"] is True
    assert result["transactions"] == []
    assert result["reason"] == "form states no transactions"
    assert result["needs_review"] is False
    assert result["needs_review_reasons"] == []
    assert result["confidence"] == 0.9
    metadata = build_vision_metadata(result)
    assert metadata["noTransactions"] is True
    assert metadata["ok"] is True
    assert metadata["rowCount"] == 0

    stub = _stub(doc_id="8219362")
    parsed_result, metadata = house_ptr._run_vision_parse(pdf, stub, "")
    assert parsed_result is not None
    parsed, trades = parsed_result
    assert parsed.transactions == [] and trades == []
    assert parsed.parser_version == VISION_PARSER_VERSION
    assert metadata["rowsRecovered"] == 0
    assert resolve_house_stub_status(stub, parsed, trades) == "parsed"
    # An unresolved member still cannot be parsed.
    stub.member.id = None
    assert resolve_house_stub_status(stub, parsed, trades) == "needs_review"


def test_no_transactions_result_skips_the_haiku_text_fallback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    _install(monkeypatch, _payload(no_transactions_stated=True))
    pdf = _write_stub_pdf(tmp_path)

    class _JunkProcessor:
        def __init__(self, *_a: Any, **_k: Any) -> None:
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
        lambda *_a, **_k: pytest.fail("Haiku text fallback must not run after a no-transactions read"),
    )

    parsed, rows = house_ptr.parse_house_ptr_pdf(pdf, stub=_stub(), backend="pymupdf", vision_backend="auto")

    assert rows == []
    assert parsed.parser_version == VISION_PARSER_VERSION
    assert parsed.vision_report["noTransactions"] is True  # type: ignore[index]


def test_zero_rows_without_the_statement_stay_in_review(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    # Read A sees the statement, read B does not: no agreement, no terminal state.
    _install(monkeypatch, [_payload(no_transactions_stated=True), _payload(no_transactions_stated=False)])
    pdf = _write_stub_pdf(tmp_path)

    result = extract_via_vision(pdf)

    assert result["ok"] is False
    assert result["no_transactions"] is False
    assert result["reason"] == "model returned no transaction rows"
    assert result["needs_review"] is True

    stub = _stub()
    parsed_result, metadata = house_ptr._run_vision_parse(pdf, stub, "")
    assert parsed_result is None
    assert metadata["noTransactions"] is False
    assert metadata["needsReview"] is True
    assert metadata["rowsRecovered"] == 0


def test_status_helper_never_parses_zero_rows_without_the_flag() -> None:
    stub = _stub()
    from capitol_pipeline.models.congress import HousePtrParseResult

    parsed = HousePtrParseResult(
        doc_id=stub.doc_id,
        parser_confidence=0.9,
        parser_version=VISION_PARSER_VERSION,
        vision_report={"ok": True, "needsReview": False, "noTransactions": False},
    )
    assert resolve_house_stub_status(stub, parsed, []) == "needs_review"
    flagged = parsed.model_copy(update={"vision_report": {"ok": True, "needsReview": False, "noTransactions": True}})
    assert resolve_house_stub_status(stub, flagged, []) == "parsed"
    # The flag only applies to zero-row results; rows without trades stay in review.
    with_rows = flagged.model_copy(
        update={
            "transactions": [
                HousePtrTransaction(
                    line_number=1, asset_description="x", asset_type="Asset", transaction_type="purchase"
                )
            ]
        }
    )
    assert resolve_house_stub_status(stub, with_rows, []) == "needs_review"


# ---------------------------------------------------------------------------
# Reuse of a stub's previous result
# ---------------------------------------------------------------------------


def _prior(pdf: Path, *, age_days: int = 1, version: str = VISION_PARSER_VERSION, sha: str | None = None) -> dict[str, Any]:
    import hashlib

    rows = [
        HousePtrTransaction(
            line_number=1,
            asset_description="Lear Corporation",
            asset_type="Asset",
            transaction_type="purchase",
            transaction_date="2023-03-10",
            notification_date="2023-04-03",
            amount_min=1001,
            amount_max=15000,
            owner="spouse",
        ).model_dump(),
        HousePtrTransaction(
            line_number=2,
            asset_description="Pepsico",
            asset_type="Asset",
            transaction_type="sale",
            transaction_date="2023-03-15",
            amount_min=1001,
            amount_max=15000,
            owner="spouse",
        ).model_dump(),
    ]
    at = (datetime.now(timezone.utc) - timedelta(days=age_days)).isoformat(timespec="seconds")
    return {
        "visionParse": {
            "ok": True,
            "parserVersion": version,
            "pdfSha256": sha or hashlib.sha256(pdf.read_bytes()).hexdigest(),
            "at": at,
            "confidence": 0.8,
            "needsReview": False,
            "costUsd": 0.42,
            "model": "claude-opus-5",
            "readAgreement": {"rowsA": 2, "rowsB": 2, "matched": 2},
        },
        "parsedTransactions": rows,
    }


def test_unchanged_pdf_reuses_the_previous_vision_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    pdf = _write_stub_pdf(tmp_path)
    fake = _install(monkeypatch, _payload(_row("should not be read")))
    stub = _stub(prior_vision=_prior(pdf))

    parsed_result, metadata = house_ptr._run_vision_parse(pdf, stub, "")

    assert fake.reads == []  # no spend
    assert parsed_result is not None
    parsed, trades = parsed_result
    assert [t.asset_description for t in parsed.transactions] == ["Lear Corporation", "Pepsico"]
    assert [t.source_id for t in trades] == ["8219444:1", "8219444:2"]
    assert trades[0].member.id == "m-K000389"  # the now-resolved member
    assert parsed.parser_confidence == 0.8
    assert metadata["reused"] is True
    assert metadata["costUsd"] == 0.0
    assert metadata["originalCostUsd"] == 0.42
    assert metadata["rowsRecovered"] == 2
    assert metadata["usage"]["inputTokens"] == 0
    assert resolve_house_stub_status(stub, parsed, trades) == "parsed"


@pytest.mark.parametrize(
    "mutate",
    [
        lambda prior, pdf: prior["visionParse"].update({"pdfSha256": "0" * 64}),
        lambda prior, pdf: prior["visionParse"].update(
            {"at": (datetime.now(timezone.utc) - timedelta(days=40)).isoformat()}
        ),
        lambda prior, pdf: prior["visionParse"].update({"parserVersion": "claude-sonnet-5-vision-v1"}),
        lambda prior, pdf: prior["visionParse"].update({"ok": False}),
        lambda prior, pdf: prior.update({"parsedTransactions": []}),
    ],
    ids=["hash-changed", "stale", "old-version", "not-ok", "no-rows"],
)
def test_prior_result_is_not_reused_when_it_no_longer_applies(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, mutate: Any
) -> None:
    _enable(monkeypatch)
    pdf = _write_stub_pdf(tmp_path)
    fake = _install(monkeypatch, _payload(_row("fresh read")))
    prior = _prior(pdf)
    mutate(prior, pdf)
    stub = _stub(prior_vision=prior)

    parsed_result, metadata = house_ptr._run_vision_parse(pdf, stub, "")

    assert len(fake.reads) == 2
    assert parsed_result is not None
    assert [t.asset_description for t in parsed_result[0].transactions] == ["fresh read"]
    assert "reused" not in metadata
    assert metadata["pdfSha256"] is not None and metadata["at"]


def test_no_transactions_result_is_reused_too(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _enable(monkeypatch)
    pdf = _write_stub_pdf(tmp_path)
    fake = _install(monkeypatch, _payload(_row("should not be read")))
    prior = _prior(pdf)
    prior["visionParse"]["noTransactions"] = True
    prior["parsedTransactions"] = []
    stub = _stub(prior_vision=prior)

    parsed_result, metadata = house_ptr._run_vision_parse(pdf, stub, "")

    assert fake.reads == []
    assert parsed_result is not None
    assert parsed_result[0].transactions == []
    assert metadata["reused"] is True and metadata["noTransactions"] is True
    assert resolve_house_stub_status(stub, parsed_result[0], []) == "parsed"


def test_queue_row_hydrates_prior_vision() -> None:
    from capitol_pipeline.cli import build_stub_from_queue_row

    row = {
        "doc_id": "8220068",
        "filing_year": 2023,
        "source": "house-clerk",
        "source_url": "https://example.test/8220068.pdf",
        "metadata": {
            "memberName": "Doug Lamborn",
            "visionParse": {"ok": True, "pdfSha256": "abc"},
            "parsedTransactions": [{"line_number": 1}],
        },
    }
    stub = build_stub_from_queue_row(row)
    assert stub.prior_vision == {
        "visionParse": {"ok": True, "pdfSha256": "abc"},
        "parsedTransactions": [{"line_number": 1}],
    }
    assert "prior_vision" not in stub.model_dump()
    bare = build_stub_from_queue_row({**row, "metadata": {"memberName": "Doug Lamborn"}})
    assert bare.prior_vision is None


# ---------------------------------------------------------------------------
# Close-up strips
# ---------------------------------------------------------------------------


def test_grid_strip_rects_cover_the_right_hand_part_with_overlap() -> None:
    rects = grid_strip_rects(1000.0, 600.0)
    assert [label for _rect, label in rects] == ["top part", "bottom part"]
    (x0, y0, x1, y1), _ = rects[0]
    assert x0 == pytest.approx(420.0) and x1 == 1000.0 and y0 == 0.0 and y1 == pytest.approx(360.0)
    (x0, y0, x1, y1), _ = rects[1]
    assert y0 == pytest.approx(240.0) and y1 == 600.0


def test_landscape_pages_get_two_close_up_strips_and_portrait_pages_none(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    landscape = _write_real_pdf(tmp_path, pages=1, portrait=False)
    fake = _install(monkeypatch, _payload(_row("x")))

    result = extract_via_vision(landscape)

    content = fake.reads[0]["messages"][0]["content"]
    assert [block["type"] for block in content] == ["text", "image", "text", "image", "text", "image", "text"]
    page_width, page_height = _png_size(content[1]["source"]["data"])
    assert page_width > page_height
    for caption, strip in ((content[2], content[3]), (content[4], content[5])):
        assert "close-up strip" in caption["text"] and "higher zoom" in caption["text"]
        width, height = _png_size(strip["source"]["data"])
        assert max(width, height) <= ptr_vision.MAX_IMAGE_LONG_EDGE
        assert width > 0.58 * page_width  # 58% of the page at > 1x zoom
    assert result["orientation"][0]["strips"] == 2
    assert "close-up strips" in ptr_vision.SYSTEM_PROMPT

    # Same read on a portrait (typed) page: no strips.
    fake.reads.clear()
    extract_via_vision(_write_real_pdf(tmp_path, pages=1, portrait=True))
    assert [block["type"] for block in fake.reads[0]["messages"][0]["content"]] == ["text", "image", "text"]

    # And the strips can be switched off.
    monkeypatch.setenv("CAPITOL_PTR_VISION_GRID_ZOOM", "0")
    fake.reads.clear()
    result = extract_via_vision(landscape)
    assert [block["type"] for block in fake.reads[0]["messages"][0]["content"]] == ["text", "image", "text"]
    assert result["orientation"][0]["strips"] == 0


# ---------------------------------------------------------------------------
# Amount column letter
# ---------------------------------------------------------------------------


def test_letter_bands_match_the_printed_ladder() -> None:
    assert AMOUNT_LETTER_BANDS["A"] == (1001, 15000)
    assert AMOUNT_LETTER_BANDS["B"] == (15001, 50000)
    assert AMOUNT_LETTER_BANDS["J"] == (50000001, 100000000)
    assert "K" not in AMOUNT_LETTER_BANDS
    assert "count boxes leftward to column A" in ptr_vision.SYSTEM_PROMPT


def test_letter_band_conflict_inside_a_read_nulls_the_amount() -> None:
    rows = [
        _row("Amazon Com Inc", amount_column_letter="B", amount_min=1001, amount_max=15000),
        _row("Apple", amount_column_letter="A", amount_min=1001, amount_max=15000),
        _row("Flag only", amount_column_letter="K", amount_min=1000001, amount_max=5000000),
        _row("Typed form", amount_column_letter=None, amount_min=15001, amount_max=50000),
    ]
    checked, conflicts = apply_amount_letter_check(rows)
    assert conflicts == 1
    assert checked[0]["amount_min"] is None and checked[0]["amount_max"] is None
    assert checked[0]["legibility"] == "partial"
    assert "letter B does not match" in checked[0]["comment"]
    assert checked[1]["amount_min"] == 1001 and checked[1]["legibility"] == "clear"
    assert checked[2]["amount_min"] == 1000001  # K is a flag, not a band
    assert checked[3]["amount_min"] == 15001


def test_letter_disagreement_between_reads_nulls_the_amount_and_marks_partial() -> None:
    read_a = _row("Amazon Com Inc", amount_column_letter="A", amount_min=1001, amount_max=15000)
    read_b = _row("Amazon Com Inc", amount_column_letter="B", amount_min=15001, amount_max=50000)
    rows, agreement = reconcile_reads([read_a], [read_b])
    assert rows[0]["amount_min"] is None and rows[0]["amount_max"] is None
    assert rows[0]["amount_column_letter"] is None
    assert rows[0]["legibility"] == "partial"
    assert rows[0]["transaction_date"] == "2022-01-06"
    assert agreement["fieldDisagreements"] == {"amount_column_letter": 1}

    # A conflict flagged inside one read is a disagreement for the pair too.
    flagged, _ = apply_amount_letter_check([_row("Amazon Com Inc", amount_column_letter="B")])
    rows, agreement = reconcile_reads(flagged, [dict(read_a)])
    assert rows[0]["amount_min"] is None and rows[0]["legibility"] == "partial"
    assert agreement["fieldDisagreements"] == {"amount_column_letter": 1}
    assert "_amount_letter_conflict" not in rows[0]

    # Agreement keeps the band and the letter.
    rows, agreement = reconcile_reads([dict(read_a)], [dict(read_a)])
    assert rows[0]["amount_min"] == 1001 and rows[0]["amount_column_letter"] == "A"
    assert agreement["fieldDisagreements"] == {}


def test_letter_conflict_sends_the_filing_to_review_end_to_end(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    read_a = _payload(_row("Amazon Com Inc", amount_column_letter="A"))
    read_b = _payload(_row("Amazon Com Inc", amount_column_letter="B", amount_min=15001, amount_max=50000))
    _install(monkeypatch, [read_a, read_b])

    result = extract_via_vision(_write_stub_pdf(tmp_path))

    row = result["transactions"][0]
    assert row["amount_min"] is None and row["amount_max"] is None
    assert row["legibility"] == "partial"
    assert result["needs_review"] is True
    assert result["needs_review_reasons"] == ["amount nulled after column-letter conflict"]
    assert result["amount_letter_conflicts"] == 0  # no within-read conflict, one between reads
    assert build_vision_metadata(result)["needsReviewReasons"] == result["needs_review_reasons"]
    # The row summary shows the letter each read reported.
    assert result["calls"][0]["rows"] == ["Amazon Com Inc | purchase | 2022-01-06 | 2023-07-22 | 1001-15000 (A) | self | clear"]
    assert "(B)" in result["calls"][1]["rows"][0]


def test_rows_recovered_counts_rows_that_survive_date_validation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    _install(monkeypatch, _payload(_row("Good"), _row("Future", transaction_date="2099-01-01")))
    stub = _stub()

    parsed_result, metadata = house_ptr._run_vision_parse(_write_stub_pdf(tmp_path), stub, "")

    assert parsed_result is not None
    assert [t.asset_description for t in parsed_result[0].transactions] == ["Good"]
    assert metadata["rowsTranscribed"] == 2
    assert metadata["rowsRecovered"] == 1
    assert metadata["pdfSha256"] and metadata["at"]


def test_sale_and_partial_sale_readings_agree_and_keep_the_partial() -> None:
    # The attachment form ticks "Sale" and "Partial Sale" together; one read
    # says sale, the other sale_partial. The site collapses both to "sale", so
    # that is agreement, and the more specific reading is kept.
    read_a = _row("Booking Holdings", transaction_type="sale")
    read_b = _row("Booking Holdings", transaction_type="sale_partial")
    rows, agreement = reconcile_reads([read_a], [read_b])
    assert agreement["fieldDisagreements"] == {}
    assert rows[0]["transaction_type"] == "sale_partial"
    assert rows[0]["legibility"] == "clear"
    # A purchase against a sale is still a disagreement.
    rows, agreement = reconcile_reads([_row("x", transaction_type="purchase")], [_row("x", transaction_type="sale")])
    assert agreement["fieldDisagreements"] == {"transaction_type": 1}
    assert rows[0]["transaction_type"] is None


# ---------------------------------------------------------------------------
# Checkbox detector hook
# ---------------------------------------------------------------------------


def _grid_with(marks: dict[int, int], rows: int = 6):
    from capitol_pipeline.parsers.ptr_grid import analyze_amount_grid, draw_synthetic_grid

    analysis = analyze_amount_grid(draw_synthetic_grid(marks=marks, rows=rows))
    assert analysis is not None
    return analysis


def test_detector_confirms_and_contradicts_the_model_letter() -> None:
    from capitol_pipeline.parsers.ptr_vision import apply_checkbox_detector

    # Page 1: ticks in C, B, B (indices 2, 1, 1). The model agrees on the first
    # two and says A for the third.
    pages = [{"index": 1, "grid": _grid_with({0: 2, 1: 1, 2: 1})}]
    rows = [
        _row("Johnson", page_number=1, amount_column_letter="C", amount_min=50001, amount_max=100000),
        _row("Amgen", page_number=1, amount_column_letter="B", amount_min=15001, amount_max=50000),
        _row("Apple", page_number=1, amount_column_letter="A", amount_min=1001, amount_max=15000),
    ]
    rows, summaries = apply_checkbox_detector(rows, pages)

    assert [row["detectorLetter"] for row in rows] == ["C", "B", "B"]
    assert [row["detectorStatus"] for row in rows] == ["agree", "agree", "disagree"]
    assert rows[0]["amount_min"] == 50001 and rows[0]["legibility"] == "clear"
    assert rows[2]["amount_min"] is None and rows[2]["amount_max"] is None
    assert rows[2]["amount_column_letter"] is None
    assert rows[2]["legibility"] == "partial"
    assert "checkbox detector reads column B" in rows[2]["comment"]
    assert summaries == [
        {
            "page": 1,
            "status": "ok",
            "columns": 11,
            "bands": summaries[0]["bands"],
            "candidates": 3,
            "rows": 3,
            "rowsAligned": 3,
            "agreed": 2,
            "disagreed": 1,
            "ambiguous": 0,
        }
    ]


def test_detector_uses_the_band_when_the_model_gave_no_letter() -> None:
    from capitol_pipeline.parsers.ptr_vision import apply_checkbox_detector

    pages = [{"index": 3, "grid": _grid_with({0: 0})}]
    rows = [_row("Ford", page_number=3, amount_column_letter=None, amount_min=15001, amount_max=50000)]
    rows, summaries = apply_checkbox_detector(rows, pages)
    assert rows[0]["detectorLetter"] == "A"
    assert rows[0]["detectorStatus"] == "disagree"  # band says B, tick is in A
    assert rows[0]["amount_min"] is None
    assert summaries[0]["disagreed"] == 1


def test_detector_ambiguity_nulls_and_misalignment_is_recorded() -> None:
    from capitol_pipeline.parsers.ptr_vision import apply_checkbox_detector

    # An ambiguous tick (two adjacent boxes) on page 1; a row count mismatch on page 2.
    from capitol_pipeline.parsers.ptr_grid import analyze_amount_grid, draw_synthetic_grid

    ambiguous = analyze_amount_grid(draw_synthetic_grid(marks={}, rows=5, ambiguous_rows=(0,)))
    pages = [{"index": 1, "grid": ambiguous}, {"index": 2, "grid": _grid_with({0: 1, 1: 1})}, {"index": 3, "grid": None}]
    rows = [
        _row("Amazon", page_number=1, amount_column_letter="D", amount_min=100001, amount_max=250000),
        _row("Lear", page_number=2, amount_column_letter="B", amount_min=15001, amount_max=50000),
        _row("Pepsi", page_number=2, amount_column_letter="B", amount_min=15001, amount_max=50000),
        _row("Extra", page_number=2, amount_column_letter="B", amount_min=15001, amount_max=50000),
        _row("Typed", page_number=3, amount_column_letter=None, amount_min=1001, amount_max=15000),
        _row("Lost", page_number=None),
    ]
    rows, summaries = apply_checkbox_detector(rows, pages)

    assert rows[0]["detectorStatus"] == "ambiguous" and rows[0]["amount_min"] is None
    assert rows[0]["legibility"] == "partial"
    assert [row["detectorStatus"] for row in rows[1:4]] == ["unaligned"] * 3
    assert rows[1]["amount_min"] == 15001  # untouched when unaligned
    assert rows[4]["detectorStatus"] == "no-grid" and rows[4]["amount_min"] == 1001
    assert rows[5]["detectorStatus"] == "unchecked"
    by_page = {summary["page"]: summary for summary in summaries}
    assert by_page[1]["ambiguous"] == 1
    assert by_page[2]["status"] == "unaligned" and by_page[2]["rows"] == 3
    assert by_page[3]["status"] == "no-grid"


def test_detector_verdicts_reach_the_result_and_review_reasons(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _enable(monkeypatch)
    pdf = _write_real_pdf(tmp_path, pages=1, portrait=False)
    monkeypatch.setattr(ptr_vision, "_analyze_page_grid", lambda _page, _module: _grid_with({0: 1, 1: 0}))
    read = _payload(
        _row("Ford", page_number=1, amount_column_letter="B", amount_min=15001, amount_max=50000),
        _row("Amazon", page_number=1, amount_column_letter="B", amount_min=15001, amount_max=50000),
    )
    _install(monkeypatch, read)

    result = extract_via_vision(pdf)

    rows = result["transactions"]
    assert rows[0]["detectorStatus"] == "agree" and rows[0]["amount_min"] == 15001
    assert rows[1]["detectorStatus"] == "disagree" and rows[1]["amount_min"] is None
    assert rows[1]["detectorLetter"] == "A"
    assert result["detector"]["agreed"] == 1 and result["detector"]["disagreed"] == 1
    assert result["detector"]["pages"][0]["page"] == 1
    assert "checkbox detector disagreed on 1 row(s)" in result["needs_review_reasons"]
    assert result["needs_review"] is True
    assert result["orientation"][0]["gridColumns"] == 11
    metadata = build_vision_metadata(result)
    assert metadata["detector"]["disagreed"] == 1
    assert any("det:A/disagree" in line for line in metadata["rows"])
    assert "_unmatched" not in rows[0] and "_amount_letter_conflict" not in rows[0]


def test_page_number_is_in_the_schema_and_prompt() -> None:
    from capitol_pipeline.parsers.ptr_vision import PTR_VISION_SCHEMA, SYSTEM_PROMPT

    item = PTR_VISION_SCHEMA["properties"]["transactions"]["items"]
    assert "page_number" in item["properties"] and "page_number" in item["required"]
    assert "page_number" in SYSTEM_PROMPT


def test_page_range_knob_reads_only_those_pages(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _enable(monkeypatch)
    monkeypatch.setenv("CAPITOL_PTR_VISION_PAGE_RANGE", "3-4")
    pdf = _write_real_pdf(tmp_path, pages=6)
    fake = _install(monkeypatch, _payload(_row("x", page_number=3)))

    result = extract_via_vision(pdf)

    content = fake.reads[0]["messages"][0]["content"]
    assert content[0]["text"] == "Page 3 of 6:"
    assert [block["type"] for block in content].count("image") == 2
    assert [entry["page"] for entry in result["orientation"]] == [3, 4]
    assert "pages 3 to 4" in _user_text(fake.reads[0])
    monkeypatch.setenv("CAPITOL_PTR_VISION_PAGE_RANGE", "nonsense")
    assert ptr_vision.resolve_page_range() is None
