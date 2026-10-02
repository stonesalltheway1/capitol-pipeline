"""Making the scanned-PTR vision path safe to run on a timer (2026-10-02).

Seven defects, each with the fixture that exposed it. The real reads below are
the two-model reads the supervised audit of 2026-10-02 captured verbatim
(read A ``claude-sonnet-5-5``, read B ``claude-opus-5-5``, one page per call)
for filings it then checked against 2x renders of the page; the page images
are ``tests/fixtures/ptr_pages/<doc>_p<page>.png`` at the production zoom.

1. The checkbox detector took the form's printed example row for a first row.
2. The publish gate worked per row, so a disputed filing still published.
3. The vision path merged two identical printed rows into one trade.
4. A vision re-read minted new trade ids beside the live ones.
5. A saved read replayed whatever version made it, and withdrawn ids came back.
6. A failed read overwrote the saved one; no budget, no clean stop on quota.
7. The six-hourly review had no way to keep to the current filing cycle.

No test touches the network or the database.
"""

from __future__ import annotations

import copy
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from capitol_pipeline import cli
from capitol_pipeline.config import Settings
from capitol_pipeline.models.congress import (
    FilingStub,
    HousePtrParseResult,
    HousePtrTransaction,
    MemberMatch,
    NormalizedTradeRow,
)
from capitol_pipeline.parsers import house_ptr, ptr_grid, ptr_vision, ptr_vision_provider
from capitol_pipeline.parsers.ptr_vision_provider import (
    CallBudget,
    GeminiProvider,
    VisionRunStopped,
)

np = pytest.importorskip("numpy")
fitz = pytest.importorskip("fitz")

PAGES = Path(__file__).parent / "fixtures" / "ptr_pages"


def _grid(name: str) -> dict[str, Any]:
    pixmap = fitz.Pixmap(str(PAGES / f"{name}.png"))
    gray = ptr_grid.gray_from_pixmap(pixmap.samples, pixmap.width, pixmap.height, pixmap.n)
    analysis = ptr_grid.analyze_amount_grid(gray)
    assert analysis is not None, name
    return analysis


def _row(**fields: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "page_number": 1,
        "owner": "self",
        "asset_description": "x",
        "ticker": None,
        "asset_type_code": None,
        "transaction_type": "purchase",
        "transaction_date": "2026-06-17",
        "notification_date": "2026-07-06",
        "amount_min": 0,
        "amount_max": 0,
        "amount_column_letter": None,
        "cap_gains_over_200": None,
        "comment": None,
        "legibility": "clear",
    }
    base.update(fields)
    return base


def _merge(rows_a: list[dict[str, Any]], rows_b: list[dict[str, Any]], year: int = 2026):
    """Exactly what extract_via_vision does with two reads of one page group."""

    rows_a, _ = ptr_vision.scrub_example_row_values(copy.deepcopy(rows_a), year)
    rows_a, _ = ptr_vision.apply_amount_letter_check(rows_a)
    rows_b, _ = ptr_vision.scrub_example_row_values(copy.deepcopy(rows_b), year)
    rows_b, _ = ptr_vision.apply_amount_letter_check(rows_b)
    return ptr_vision.reconcile_reads(rows_a, rows_b)


# -- The real reads -----------------------------------------------------------

#: Rogers 9116218, the only row. Both reads: no amount box ticked.
ROGERS_9116218 = _row(
    asset_description="Redeemed New York St Rev Rfdg Ser 2016A 5 000 Due 6/15/33 Full Call",
    transaction_type="sale",
    transaction_date="2026-06-15",
    notification_date="2026-07-14",
    legibility="partial",
)
ROGERS_READ_A = dict(
    ROGERS_9116218,
    comment="No amount box ticked in columns A-J; amount unreadable/unmarked. Asset name spans two printed lines. Owner column blank.",
)
ROGERS_READ_B = dict(
    ROGERS_9116218,
    comment="No amount bucket marked on the form. Asset description written across two grid lines.",
)

#: Malliotakis 9116217, the only row: a handwritten US Treasury Bill, Purchase, D.
MALLIOTAKIS_READ_A = _row(
    asset_description="US Treasury Bill",
    transaction_date="2026-06-25",
    notification_date="2026-09-07",
    amount_min=100001,
    amount_max=250000,
    amount_column_letter="D",
    legibility="partial",
)
MALLIOTAKIS_READ_B = dict(MALLIOTAKIS_READ_A, notification_date=None)


def _harshbarger_page_two(read: str) -> list[dict[str, Any]]:
    """Harshbarger 9116258 page 2: seven printed rows, MAIN STR ENERGY twice."""

    legibility = "partial" if read == "A" else "clear"

    def row(name: str, letter: str, band: tuple[int, int], date_: str = "2026-06-17") -> dict[str, Any]:
        return _row(
            page_number=2,
            asset_description=name,
            transaction_date=date_,
            amount_min=band[0],
            amount_max=band[1],
            amount_column_letter=letter,
            legibility=legibility,
        )

    c, d, b = (50001, 100000), (100001, 250000), (15001, 50000)
    return [
        row("NEW YORK NY BE/R/", "C", c),
        row("CITY & CNTY OF DENVER CO RV BE/R/", "D", d),
        row("MAIN STR ENERGY INC GA E SR A RV BE/R/", "C", c),
        row("CLARK CNTY NV ARPT SUB SR B RV BE/R/", "C", c),
        row("MAIN STR ENERGY INC GA E SR A RV BE/R/", "C", c),
        row("CITY & CNTY OF DENVER CO RV BE/R/", "B", b),
        row("NEW YORK NY CITY TRANSI SR I RV BE/R/", "B", b, "2026-06-18"),
    ]


# =============================================================================
# 1. The example row, and "no tick" is no amount
# =============================================================================


def test_9116218_a_row_nobody_ticked_gets_no_amount() -> None:
    merged, _agreement = _merge([ROGERS_READ_A], [ROGERS_READ_B])
    rows, pages = ptr_vision.apply_checkbox_detector(merged, [{"index": 1, "grid": _grid("9116218_p1")}])

    # Before: "resolved" to B, $15,001-$50,000, off the example's printed x,
    # on a filing the pipeline rated clean.
    assert not rows[0]["amount_min"] and not rows[0]["amount_max"]
    assert rows[0]["detectorStatus"] == "no-ticks"
    assert pages[0]["status"] == "no-ticks"
    assert pages[0]["resolved"] == 0
    totals = ptr_vision.detector_totals(pages)
    assert totals["noTickPages"] == [1]
    assert "checkbox detector found no ticked amount on page(s) 1" in ptr_vision.detector_review_reasons(totals)


def test_9116217_the_detector_confirms_the_read_it_used_to_contradict() -> None:
    merged, _agreement = _merge([MALLIOTAKIS_READ_A], [MALLIOTAKIS_READ_B])
    rows, pages = ptr_vision.apply_checkbox_detector(merged, [{"index": 1, "grid": _grid("9116217_p1")}])

    assert rows[0]["detectorLetter"] == "D" and rows[0]["detectorStatus"] == "agree"
    assert rows[0]["detectorType"] == "purchase" and rows[0]["detectorTypeStatus"] == "agree"
    assert (rows[0]["amount_min"], rows[0]["amount_max"]) == (100001, 250000)
    assert pages[0]["disagreed"] == 0 and pages[0]["typeDisagreed"] == 0
    assert pages[0]["exampleRow"] is not None


def test_the_detector_never_lends_an_amount_to_a_row_nobody_ticked() -> None:
    # Both reads agree no amount box is ticked; the page shows a tick in B. A
    # contradiction for a person, never an amount to publish.
    grid = ptr_grid.analyze_amount_grid(ptr_grid.draw_synthetic_grid(marks={0: 1}, rows=3))
    merged, _ = _merge([_row(asset_description="Ford")], [_row(asset_description="Ford")])
    rows, pages = ptr_vision.apply_checkbox_detector(merged, [{"index": 1, "grid": grid}])

    assert rows[0]["detectorStatus"] == "disagree"
    assert not rows[0]["amount_min"] and not rows[0]["amount_max"]
    assert rows[0]["legibility"] == "partial"
    assert "neither read saw an amount ticked" in rows[0]["comment"]
    assert pages[0]["disagreed"] == 1 and pages[0]["resolved"] == 0


def test_an_amount_the_reads_disputed_is_still_settled_by_the_detector() -> None:
    grid = ptr_grid.analyze_amount_grid(ptr_grid.draw_synthetic_grid(marks={0: 1}, rows=3))
    read_a = _row(asset_description="Ford", amount_min=15001, amount_max=50000, amount_column_letter="B")
    read_b = _row(asset_description="Ford", amount_min=50001, amount_max=100000, amount_column_letter="C")
    merged, _ = _merge([read_a], [read_b])
    assert merged[0]["amountDisputed"] is True
    rows, _pages = ptr_vision.apply_checkbox_detector(merged, [{"index": 1, "grid": grid}])

    assert rows[0]["detectorStatus"] == "resolved"
    assert (rows[0]["amount_min"], rows[0]["amount_max"]) == (15001, 50000)
    # The flag survives into the stored transcription, so a later reconcile
    # knows the same thing.
    assert ptr_vision.stored_transcription(rows)[0]["amountDisputed"] is True


def test_a_legacy_transcription_says_its_dispute_in_the_comment() -> None:
    assert ptr_vision._amount_disputed({"comment": "two reads disagreed on: amount_column_letter"})
    assert ptr_vision._amount_disputed(
        {"comment": "x; amount column letter C does not match the reported band"}
    )
    assert not ptr_vision._amount_disputed({"comment": "two reads disagreed on: owner"})
    assert not ptr_vision._amount_disputed({"comment": "No amount bucket marked on the form."})


# =============================================================================
# 3. Two identical printed rows are two trades
# =============================================================================


def _stub(doc_id: str = "9116258", **overrides: Any) -> FilingStub:
    fields: dict[str, Any] = dict(
        doc_id=doc_id,
        filing_year=2026,
        filing_date="2026-07-07",
        member=MemberMatch(id="m-H001086", name="Diana Harshbarger", slug="diana-harshbarger", state="TN"),
        source="house-clerk",
        source_url=f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/{doc_id}.pdf",
    )
    fields.update(overrides)
    return FilingStub(**fields)


def _fake_report(pdf: Path, rows: list[dict[str, Any]], **extra: Any) -> dict[str, Any]:
    report: dict[str, Any] = {
        "ok": True,
        "skipped": False,
        "reason": None,
        "provider": "gemini",
        "model": "gemini-3.8-flash",
        "model_b": "gemini-3.5-flash",
        "parser_version": "gemini-vision-v2",
        "pdf_sha256": hashlib.sha256(pdf.read_bytes()).hexdigest(),
        "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "transactions": rows,
        "confidence": 0.9,
        "needs_review": False,
        "needs_review_reasons": [],
        "chunk_pages": 1,
    }
    report.update(extra)
    return report


def test_9116258_both_main_str_energy_rows_are_published(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    merged, agreement = _merge(_harshbarger_page_two("A"), _harshbarger_page_two("B"))
    assert agreement["matched"] == 7 and agreement["rowCountsAgree"]
    rows, pages = ptr_vision.apply_checkbox_detector(merged, [{"index": 2, "grid": _grid("9116258_p2")}])
    assert pages[0]["agreed"] == 7

    pdf = tmp_path / "9116258.pdf"
    pdf.write_bytes(b"%PDF-1.4 stand-in")
    monkeypatch.setattr(house_ptr, "extract_via_vision", lambda *_a, **_k: _fake_report(pdf, rows))
    result, _metadata = house_ptr._run_vision_parse(pdf, _stub(), "")

    assert result is not None
    parsed, trades = result
    names = [row.asset_description for row in parsed.transactions]
    assert names.count("MAIN STR ENERGY INC GA E SR A RV BE/R/") == 2
    assert len(trades) == 7
    assert len({trade.source_id for trade in trades}) == 7


def test_a_row_only_one_read_doubled_is_not_a_second_trade() -> None:
    # What the dedupe stood guard against: a model reporting one row twice. A
    # row only one read saw is unmatched, rated illegible, and never publishes.
    read_a = _harshbarger_page_two("A")
    read_b = [row for index, row in enumerate(_harshbarger_page_two("B")) if index != 4]
    merged, agreement = _merge(read_a, read_b)
    assert agreement["rowCountsAgree"] is False
    assert sum(1 for row in merged if row["legibility"] == "illegible") == 1


# =============================================================================
# 2, 4, 5. The per-filing gate, stable ids, withdrawn ids
# =============================================================================


class _Calls:
    def __init__(self) -> None:
        self.upserts: list[list[NormalizedTradeRow]] = []
        self.marks: list[dict[str, Any]] = []
        self.references: tuple[list, list, list, list] = ([], [], [], [])


@pytest.fixture
def exporter(monkeypatch: pytest.MonkeyPatch) -> _Calls:
    calls = _Calls()
    monkeypatch.setattr(cli, "sync_house_stubs_to_neon", lambda _settings, _stubs: {"upserted": 1})

    def _upsert(_settings: Settings, trades: list[NormalizedTradeRow]) -> dict[str, Any]:
        calls.upserts.append(list(trades))
        return {"upserted": len(trades), "trade_ids": [trade.source_id for trade in trades]}

    monkeypatch.setattr(cli, "upsert_trade_rows_to_neon", _upsert)
    monkeypatch.setattr(cli, "fetch_house_line_references", lambda _settings, **_kwargs: calls.references)
    monkeypatch.setattr(cli, "mark_house_stub_processed", lambda _settings, _stub, **kwargs: calls.marks.append(kwargs))
    return calls


def _transaction(line: int, name: str, *, legibility: str | None = "clear", **fields: Any) -> HousePtrTransaction:
    values: dict[str, Any] = dict(
        line_number=line,
        asset_description=name,
        asset_type="Asset",
        transaction_type="purchase",
        transaction_date="2026-06-17",
        amount_min=50001,
        amount_max=100000,
        owner="self",
        legibility=legibility,
    )
    values.update(fields)
    return HousePtrTransaction(**values)


def _scanned(
    transactions: list[HousePtrTransaction],
    *,
    detector: list[str] | None = None,
    stub: FilingStub | None = None,
    **vision: Any,
) -> tuple[FilingStub, HousePtrParseResult, list[NormalizedTradeRow]]:
    stub = stub or _stub()
    statuses = detector or ["agree"] * len(transactions)
    report: dict[str, Any] = {
        "ok": True,
        "skipped": False,
        "parserVersion": "gemini-vision-v2",
        "visionVersion": ptr_vision.VISION_READ_VERSION,
        "detectorVersion": ptr_grid.DETECTOR_VERSION,
        "pdfSha256": "a" * 64,
        "needsReview": False,
        "needsReviewReasons": [],
        "rowsTranscribed": len(transactions),
        "rowsRecovered": len(transactions),
        "transcription": [
            {
                "line_number": row.line_number,
                "asset_description": row.asset_description,
                "transaction_type": row.transaction_type,
                "legibility": row.legibility,
                "detectorStatus": status,
                "detectorTypeStatus": "agree",
            }
            for row, status in zip(transactions, statuses)
        ],
    }
    report.update(vision)
    parsed = HousePtrParseResult(
        doc_id=stub.doc_id,
        parser_confidence=0.9,
        parser_version="gemini-vision-v2",
        transactions=transactions,
        vision_report=report,
    )
    return stub, parsed, house_ptr.build_trade_rows_from_house_ptr(parsed, stub)


def test_a_disputed_filing_is_withheld_whole_by_default(exporter: _Calls) -> None:
    stub, parsed, trades = _scanned(
        [_transaction(1, "NEW YORK NY BE/R/"), _transaction(2, "CLARK CNTY NV", legibility="partial")],
        needsReview=True,
        needsReviewReasons=["reads disagree on amount"],
    )
    summary = cli.persist_parsed_house_stub(Settings(), stub, parsed, trades)

    assert exporter.upserts == []
    assert summary["stubStatus"] == "needs_review"
    assert summary["trades"]["withheld"] == 2
    gate = summary["filingGate"]
    assert gate["decision"] == "withheld" and gate["override"] is False
    assert "reads disagree on amount" in gate["reasons"]
    assert "1 row(s): the read rated this row partial" in gate["reasons"]
    mark = exporter.marks[0]
    # The reads stay on the stub, both rows of them, for a person.
    assert [row["asset_description"] for row in mark["parsed_transactions"]] == ["NEW YORK NY BE/R/", "CLARK CNTY NV"]
    assert mark["metadata_extra"]["visionParse"]["filingGate"]["decision"] == "withheld"
    assert mark["metadata_extra"]["visionParse"]["publishedTrades"] == 0


def test_9116218_a_filing_rated_clean_with_a_partial_row_is_withheld(exporter: _Calls) -> None:
    # The read said needsReview False (the detector had "resolved" the
    # amount), and the old parsed branch published every row whatever its
    # rating. A partial row is a disputed row.
    stub, parsed, trades = _scanned(
        [_transaction(1, ROGERS_9116218["asset_description"], legibility="partial", transaction_type="sale",
                      transaction_date="2026-06-15", amount_min=15001, amount_max=50000)],
        stub=_stub("9116218", filing_date="2026-07-14"),
        detector=["resolved"],
    )
    summary = cli.persist_parsed_house_stub(Settings(), stub, parsed, trades)

    assert exporter.upserts == []
    assert summary["stubStatus"] == "needs_review"
    assert summary["filingGate"]["decision"] == "withheld"


def test_a_detector_conflict_withholds_the_filing_even_when_the_read_says_clean(exporter: _Calls) -> None:
    stub, parsed, trades = _scanned(
        [_transaction(1, "A"), _transaction(2, "B")], detector=["agree", "disagree"]
    )
    summary = cli.persist_parsed_house_stub(Settings(), stub, parsed, trades)

    assert exporter.upserts == []
    assert "checkbox detector: 1 row(s) disagree" in summary["filingGate"]["reasons"]


def test_a_clean_filing_publishes_every_row_and_leaves_review(exporter: _Calls) -> None:
    stub, parsed, trades = _scanned([_transaction(1, "A"), _transaction(2, "B")])
    summary = cli.persist_parsed_house_stub(Settings(), stub, parsed, trades)

    assert summary["stubStatus"] == "parsed"
    assert summary["filingGate"] == {**summary["filingGate"], "decision": "published", "reasons": []}
    assert [trade.source_id for trade in exporter.upserts[0]] == ["9116258:1", "9116258:2"]


def test_the_override_publishes_the_settled_rows_only(exporter: _Calls) -> None:
    stub, parsed, trades = _scanned(
        [_transaction(1, "A"), _transaction(2, "B", legibility="partial")],
        needsReview=True,
        needsReviewReasons=["reads disagree on amount"],
    )
    summary = cli.persist_parsed_house_stub(Settings(), stub, parsed, trades, allow_partial=True)

    assert [trade.source_id for trade in exporter.upserts[0]] == ["9116258:1"]
    assert summary["filingGate"]["decision"] == "published-partial"
    assert summary["filingGate"]["override"] is True
    assert summary["stubStatus"] == "needs_review"


def _live(doc: str, line: int, row: HousePtrTransaction) -> dict[str, Any]:
    return {
        "id": f"tr-house-{doc}-{line}",
        "ticker": row.ticker,
        "asset_description": row.asset_description,
        "transaction_type": row.transaction_type,
        "transaction_date": row.transaction_date,
        "amount_min": row.amount_min,
        "amount_max": row.amount_max,
        "owner": row.owner,
    }


def test_a_vision_re_read_keeps_the_ids_its_rows_already_have(exporter: _Calls) -> None:
    # The filing was published once with one row, Clark County, as line 1. A
    # re-read finds a row above it. Numbered by position, the upsert would
    # have written New York over Clark County and left nothing stale -- or,
    # with more rows, left the old last rows behind as duplicates.
    clark = _transaction(1, "CLARK CNTY NV ARPT SUB SR B RV BE/R/")
    exporter.references = ([], [_live("9116258", 1, clark)], [], [])
    stub, parsed, trades = _scanned(
        [_transaction(1, "NEW YORK NY BE/R/", amount_min=15001, amount_max=50000), _transaction(2, clark.asset_description)]
    )
    summary = cli.persist_parsed_house_stub(Settings(), stub, parsed, trades)

    published = {trade.asset_description: trade.source_id for trade in exporter.upserts[0]}
    assert published[clark.asset_description] == "9116258:1"  # kept its id
    assert published["NEW YORK NY BE/R/"] == "9116258:3"  # above every number used
    mark = exporter.marks[0]
    assert {row["asset_description"]: row["line_number"] for row in mark["parsed_transactions"]} == {
        "NEW YORK NY BE/R/": 3,
        clark.asset_description: 1,
    }
    # The transcription a later reconcile keys on moves with them.
    transcription = mark["metadata_extra"]["visionParse"]["transcription"]
    assert {row["asset_description"]: row["line_number"] for row in transcription} == {
        "NEW YORK NY BE/R/": 3,
        clark.asset_description: 1,
    }
    assert summary["filingGate"]["decision"] == "published"
    assert mark["metadata_extra"]["lineIds"]["matched"] == 1


def test_a_withdrawn_id_is_not_republished_from_the_same_pdf(exporter: _Calls) -> None:
    withdrawn = _transaction(2, "MAIN STR ENERGY INC GA E SR A RV BE/R/")
    exporter.references = ([], [], [], [_live("9116258", 2, withdrawn)])
    rows = [_transaction(1, "NEW YORK NY BE/R/"), _transaction(2, withdrawn.asset_description)]
    prior = {"visionParse": {"ok": True, "pdfSha256": "a" * 64}}

    # Default: the filing is held whole, and says why.
    stub, parsed, trades = _scanned(copy.deepcopy(rows), stub=_stub(prior_vision=prior))
    summary = cli.persist_parsed_house_stub(Settings(), stub, parsed, trades)
    assert exporter.upserts == []
    assert any("withdrawn" in reason for reason in summary["filingGate"]["reasons"])

    # Override: the rest publishes, the withdrawn row does not.
    stub, parsed, trades = _scanned(copy.deepcopy(rows), stub=_stub(prior_vision=prior))
    cli.persist_parsed_house_stub(Settings(), stub, parsed, trades, allow_partial=True)
    assert [trade.asset_description for trade in exporter.upserts[-1]] == ["NEW YORK NY BE/R/"]


def test_a_withdrawn_id_may_come_back_when_the_pdf_itself_changed(exporter: _Calls) -> None:
    withdrawn = _transaction(2, "MAIN STR ENERGY INC GA E SR A RV BE/R/")
    exporter.references = ([], [], [], [_live("9116258", 2, withdrawn)])
    rows = [_transaction(1, "NEW YORK NY BE/R/"), _transaction(2, withdrawn.asset_description)]
    # The last good read was of a different PDF: the Clerk replaced the filing.
    stub, parsed, trades = _scanned(rows, stub=_stub(prior_vision={"visionParse": {"ok": True, "pdfSha256": "b" * 64}}))
    summary = cli.persist_parsed_house_stub(Settings(), stub, parsed, trades)

    assert summary["filingGate"]["decision"] == "published"
    # Main Str keeps the number it was withdrawn under; New York matched
    # nothing and takes the next number above every one the filing used.
    assert sorted(trade.source_id for trade in exporter.upserts[0]) == ["9116258:2", "9116258:3"]
    assert exporter.marks[0]["metadata_extra"]["lineIds"]["sourceChanged"] is True


def test_withhold_rows_holds_withdrawn_ids_unless_the_source_changed() -> None:
    from capitol_pipeline.house_line_ids import LineReference, withhold_rows

    parsed = HousePtrParseResult(transactions=[_transaction(2, "MAIN STR")])
    trades = house_ptr.build_trade_rows_from_house_ptr(parsed, _stub())
    reference = LineReference(
        line_number=2, transaction_date="2026-06-17", transaction_type="purchase", amount_min=50001,
        amount_max=100000, ticker=None, asset_description="MAIN STR", owner="self", origin="withdrawn",
    )
    _parsed, kept, report = withhold_rows(parsed, trades, [reference], doc_id="9116258", filing_date=None)
    assert kept == [] and report[0]["line"] == 2
    _parsed, kept, report = withhold_rows(
        parsed, trades, [reference], doc_id="9116258", filing_date=None, source_changed=True
    )
    assert len(kept) == 1 and report == []


# =============================================================================
# 5. A saved read is replayed only when it is current
# =============================================================================


def test_saved_read_is_current_wants_both_versions() -> None:
    current = {"visionVersion": ptr_vision.VISION_READ_VERSION, "detectorVersion": ptr_grid.DETECTOR_VERSION}
    assert ptr_vision.saved_read_is_current(current) == (True, None)
    # Every read stored before 2026-10-02 carries neither.
    ok, why = ptr_vision.saved_read_is_current({"ok": True, "parserVersion": "gemini-vision-v2"})
    assert ok is False and "predates vision read version" in str(why)
    ok, why = ptr_vision.saved_read_is_current({**current, "detectorVersion": ptr_grid.DETECTOR_VERSION - 1})
    assert ok is False and "detector version" in str(why)


def _queue_row(vision: dict[str, Any]) -> dict[str, Any]:
    return {
        "doc_id": "8221358",
        "filing_year": 2026,
        "source": "house_clerk",
        "source_url": "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/8221358.pdf",
        "status": "needs_review",
        "metadata": {
            "memberId": "m-K000389",
            "memberName": "Ro Khanna",
            "filingDate": "2026-02-27",
            "parsedTransactions": [
                _transaction(1, "Apple Inc", transaction_date="2026-01-20", amount_min=1001, amount_max=15000).model_dump()
            ],
            "visionParse": vision,
        },
    }


def test_a_stale_saved_read_is_not_republished_from_the_stub() -> None:
    # 8221358's saved read still holds rows migrations 042/043 deleted from
    # trades, typed purchase and rated clear. repersist / reconcile must not
    # publish them; the filing has to be read again.
    stale = {"ok": True, "needsReview": False, "parserVersion": "gemini-vision-v2", "at": "2026-09-03T17:38:14+00:00"}
    _stub_, parsed, trades, skip = cli.rebuild_parsed_house_stub(_queue_row(stale))
    assert parsed is None and trades == []
    assert skip is not None and skip.startswith("stale vision read")

    current = {**stale, "visionVersion": ptr_vision.VISION_READ_VERSION, "detectorVersion": ptr_grid.DETECTOR_VERSION}
    _stub_, parsed, trades, skip = cli.rebuild_parsed_house_stub(_queue_row(current))
    assert skip is None and parsed is not None and len(trades) == 1


def test_a_reconcile_restamps_the_detector_but_not_the_read() -> None:
    # reconcile-house-vision re-runs today's detector over a stored read: the
    # rows are checked again, the read itself is as old as it was.
    vision = {"ok": True, "visionVersion": 1, "detectorVersion": 2}
    vision["detectorVersion"] = ptr_grid.DETECTOR_VERSION  # what the reconcile writes
    ok, why = ptr_vision.saved_read_is_current(vision)
    assert ok is False and "vision read version" in str(why)


# =============================================================================
# 6. Failed reads, 429s, and the call budget
# =============================================================================


def test_a_failed_read_never_replaces_the_saved_one() -> None:
    failed = {
        "ok": False,
        "skipped": True,
        "reason": "api error: gemini 403: Lightning dunning decision is deny for project",
        "provider": "gemini",
        "at": "2026-10-02T12:00:00+00:00",
        "calls": [{"label": "read A"}],
    }
    parsed = HousePtrParseResult(doc_id="9116141", vision_report=failed)
    extra = cli.build_house_stub_metadata_extra(parsed)

    assert extra is not None
    assert "visionParse" not in extra  # mark_house_stub_processed merges: the old read stays
    assert "403" in str(extra["visionLastFailure"]["reason"])
    assert extra["visionLastFailure"]["calls"] == 1

    good = cli.build_house_stub_metadata_extra(
        HousePtrParseResult(doc_id="9116141", vision_report={"ok": True, "skipped": False})
    )
    assert good is not None and good["visionParse"]["ok"] is True
    assert good["visionLastFailure"] is None


class _Response:
    def __init__(self, status_code: int, body: dict[str, Any]) -> None:
        self.status_code = status_code
        self._body = body
        self.headers: dict[str, str] = {}
        self.text = json.dumps(body)

    def json(self) -> dict[str, Any]:
        return self._body


def _gemini(monkeypatch: pytest.MonkeyPatch, responses: list[_Response]) -> list[int]:
    for name in ("CAPITOL_PTR_VISION_MODEL", "CAPITOL_PTR_VISION_MODEL_B", "CAPITOL_PTR_VISION_PROVIDER"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("GEMINI_API_KEY", "test-key-not-a-real-one")
    monkeypatch.setenv("CAPITOL_PTR_VISION_GEMINI_RPM", "0")
    attempts: list[int] = []

    def _post(_url: str, **_kwargs: Any) -> _Response:
        attempts.append(1)
        return responses[min(len(attempts) - 1, len(responses) - 1)]

    import httpx

    monkeypatch.setattr(httpx, "post", _post)
    return attempts


DUNNING = _Response(403, {"error": {"message": "Lightning dunning decision is deny for project: projects/1073647603491"}})
DAILY = _Response(
    429,
    {
        "error": {
            "message": "Quota exceeded for metric: generativelanguage.googleapis.com/generate_content_free_tier_requests, limit: 250",
            "details": [
                {
                    "@type": "type.googleapis.com/google.rpc.QuotaFailure",
                    "violations": [{"quotaId": "GenerateRequestsPerDayPerProjectPerModel-FreeTier"}],
                },
                {"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "41s"},
            ],
        }
    },
)
MINUTE = _Response(429, {"error": {"message": "Resource exhausted", "details": [{"retryDelay": "3s"}]}})
OK = _Response(200, {"candidates": [{"finishReason": "STOP", "content": {"parts": [{"text": "{}"}]}}]})


def test_a_refused_key_stops_the_run_at_once(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = _gemini(monkeypatch, [DUNNING])
    provider = GeminiProvider()
    with pytest.raises(VisionRunStopped) as caught:
        provider._post("gemini-3.8-flash", {"contents": []}, sleep=lambda _s: None)
    assert caught.value.kind == "denied"
    assert len(attempts) == 1  # no retry: the next one would get the same answer
    # Every later call in the run fails at once, without an HTTP request.
    with pytest.raises(VisionRunStopped):
        provider._post("gemini-3.5-flash", {"contents": []}, sleep=lambda _s: None)
    assert len(attempts) == 1
    assert ptr_vision_provider.run_budget().summary()["stopped"]["kind"] == "denied"


def test_a_daily_quota_429_stops_the_run_without_retrying(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = _gemini(monkeypatch, [DAILY])
    with pytest.raises(VisionRunStopped) as caught:
        GeminiProvider()._post("gemini-3.8-flash", {"contents": []}, sleep=lambda _s: None)
    assert caught.value.kind == "quota"
    assert len(attempts) == 1


def test_a_per_minute_429_backs_off_and_then_stops_if_it_never_clears(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = _gemini(monkeypatch, [MINUTE, OK])
    slept: list[float] = []
    assert GeminiProvider()._post("gemini-3.8-flash", {"contents": []}, sleep=slept.append)["candidates"]
    assert len(attempts) == 2 and 3.0 <= slept[0] <= 4.0

    ptr_vision_provider.start_run(None)
    monkeypatch.setenv("CAPITOL_PTR_VISION_GEMINI_MAX_ATTEMPTS", "3")
    attempts = _gemini(monkeypatch, [MINUTE])
    slept.clear()
    with pytest.raises(VisionRunStopped) as caught:
        GeminiProvider()._post("gemini-3.8-flash", {"contents": []}, sleep=slept.append)
    assert caught.value.kind == "quota"
    assert len(attempts) == 3 and len(slept) == 2


def test_the_quota_kind_is_read_from_googles_error() -> None:
    assert ptr_vision_provider.gemini_quota_kind(DAILY.json(), DAILY.json()["error"]["message"]) == "daily"
    assert ptr_vision_provider.gemini_quota_kind(MINUTE.json(), "Resource exhausted") == "minute"
    assert ptr_vision_provider.gemini_quota_kind({}, "limit: 0, model: gemini-3.8-flash") == "daily"


def test_every_attempt_is_charged_and_the_budget_ends_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = _gemini(monkeypatch, [MINUTE, OK])
    budget = ptr_vision_provider.start_run(2)
    provider = GeminiProvider()
    provider._post("gemini-3.8-flash", {"contents": []}, sleep=lambda _s: None)  # 429 then 200: two
    assert budget.used == 2 and budget.remaining() == 0
    with pytest.raises(VisionRunStopped) as caught:
        provider._post("gemini-3.8-flash", {"contents": []}, sleep=lambda _s: None)
    assert caught.value.kind == "budget"
    assert len(attempts) == 2  # the third was never sent


def test_call_budget_arithmetic() -> None:
    budget = CallBudget(5)
    assert budget.can_afford(5) and not budget.can_afford(6)
    for _ in range(5):
        budget.charge()
    assert budget.remaining() == 0
    with pytest.raises(VisionRunStopped):
        budget.charge()
    assert CallBudget(None).can_afford(10_000)
    assert ptr_vision_provider.resolve_call_budget(None) == ptr_vision_provider.DEFAULT_VISION_CALL_BUDGET
    assert ptr_vision_provider.resolve_call_budget(-1) is None
    assert ptr_vision_provider.resolve_call_budget(12) == 12


def _scan_pdf(tmp_path: Path, pages: int) -> Path:
    document = fitz.open()
    for _ in range(pages):
        document.new_page(width=792, height=612)
    pdf = tmp_path / f"scan{pages}.pdf"
    document.save(str(pdf))
    document.close()
    return pdf


def test_a_filing_that_will_not_fit_the_budget_is_not_started(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _gemini(monkeypatch, [OK])
    monkeypatch.delenv("CAPITOL_PTR_VISION_CHUNK_PAGES", raising=False)
    monkeypatch.setattr(GeminiProvider, "_post", lambda *_a, **_k: pytest.fail("a model was called"))
    ptr_vision_provider.start_run(3)

    result = ptr_vision.extract_via_vision(_scan_pdf(tmp_path, 2))  # two pages, two reads: four calls

    assert result["ok"] is False and result["budget_skipped"] is True
    assert result["calls_needed"] == 4
    metadata = ptr_vision.build_vision_metadata(result)
    assert metadata["budgetSkipped"] is True and metadata["callsNeeded"] == 4


def test_a_run_stopped_mid_filing_keeps_nothing_of_that_filing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _gemini(monkeypatch, [OK])
    monkeypatch.delenv("CAPITOL_PTR_VISION_CHUNK_PAGES", raising=False)
    sent: list[str] = []

    def _post(self: GeminiProvider, model: str, body: dict[str, Any], **_kwargs: Any) -> dict[str, Any]:
        sent.append(model)
        if len(sent) == 2:
            stop = VisionRunStopped("gemini-3.5-flash daily quota exhausted (429)", kind="quota")
            ptr_vision_provider.run_budget().stop(stop)
            raise stop
        return {"candidates": [{"finishReason": "STOP", "content": {"parts": [{"text": json.dumps(
            {"filer_name": None, "filing_date": None, "page_count": 1, "notes": None,
             "no_transactions_stated": False, "transactions": [_row(asset_description="Ford")]})}]}}]}

    monkeypatch.setattr(GeminiProvider, "_post", _post)
    result = ptr_vision.extract_via_vision(_scan_pdf(tmp_path, 2))

    assert result["ok"] is False and result["transactions"] == []
    assert result["stop_run"]["kind"] == "quota"
    assert len(sent) == 2  # page 2 was never read
    assert cli.is_good_vision_read(ptr_vision.build_vision_metadata(result)) is False


# -- The queue runner: budget skips, a clean stop, --max-filings, dry run -------


def _queue(doc_id: str, status: str = "needs_review") -> dict[str, Any]:
    return {
        "doc_id": doc_id,
        "filing_year": 2026,
        "source": "house-clerk",
        "source_url": f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/{doc_id}.pdf",
        "status": status,
        "extracted_trade_id": None,
        "metadata": {"memberId": "m-X", "memberName": "Pat Example", "filingDate": "2026-09-15", "reviewAttempts": 4},
    }


class _Runner:
    """Patches the queue runner's every seam; records what it writes."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, needs: dict[str, int], *, parse: Any = None) -> None:
        self.updates: list[dict[str, Any]] = []
        self.persisted: list[str] = []
        self.parsed: list[str] = []
        monkeypatch.setattr(cli, "download_house_pdf", lambda stub, _settings, path: path.write_bytes(b"%PDF"))
        monkeypatch.setattr(cli, "vision_calls_needed_for_pdf", lambda path, _backend: needs[path.stem])
        monkeypatch.setattr(
            cli,
            "update_house_stub_state",
            lambda _settings, **kwargs: self.updates.append(kwargs),
        )

        def _parse(stub: FilingStub, _settings: Any, _ocr: str, _vision: str, **_kwargs: Any):
            self.parsed.append(stub.doc_id)
            if parse is not None:
                return parse(stub)
            for _ in range(needs[stub.doc_id]):
                ptr_vision_provider.run_budget().charge()
            return HousePtrParseResult(doc_id=stub.doc_id, vision_report={"ok": True, "rowCount": 1}), []

        monkeypatch.setattr(cli, "parse_live_house_stub", _parse)

        def _persist(_settings: Any, stub: FilingStub, _parsed: Any, _trades: Any, **_kwargs: Any):
            self.persisted.append(stub.doc_id)
            return {"stubStatus": "needs_review", "trades": {"upserted": 0, "withheld": 0}}

        monkeypatch.setattr(cli, "persist_parsed_house_stub", _persist)

    def touched(self) -> list[str]:
        return sorted({update["doc_id"] for update in self.updates})


def test_filings_that_do_not_fit_are_skipped_untouched(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _Runner(monkeypatch, {"9116328": 36, "9116218": 2, "9116217": 2, "9115808": 2})
    ptr_vision_provider.start_run(6)
    summary = cli.process_house_queue_rows(
        Settings(),
        [_queue("9116328"), _queue("9116218"), _queue("9116217"), _queue("9115808")],
        ocr_backend="auto",
        vision_backend="on",
        review_mode=True,
    )

    # The 18-page filing needs 36 calls and is left exactly as it was; the
    # three one-page forms fit the six calls and are read.
    assert runner.parsed == ["9116218", "9116217", "9115808"]
    assert "9116328" not in runner.touched()
    skipped = [item for item in summary["processed"] if item["status"] == "skipped"]
    assert [item["docId"] for item in skipped] == ["9116328"]
    assert "needs 36 calls, 6 left" in skipped[0]["reason"]
    assert summary["visionCallBudget"]["used"] == 6


def test_a_stopped_run_puts_the_stub_back_and_skips_the_rest(monkeypatch: pytest.MonkeyPatch) -> None:
    def _denied(stub: FilingStub):
        stop = VisionRunStopped("gemini-3.8-flash refused the key (403)", kind="denied")
        ptr_vision_provider.run_budget().stop(stop)
        return (
            HousePtrParseResult(
                doc_id=stub.doc_id,
                vision_report={"ok": False, "skipped": True, "reason": "vision run stopped",
                               "stopRun": {"kind": "denied", "reason": stop.reason}},
            ),
            [],
        )

    runner = _Runner(monkeypatch, {"9116218": 2, "9116217": 2}, parse=_denied)
    summary = cli.process_house_queue_rows(
        Settings(), [_queue("9116218"), _queue("9116217")], ocr_backend="auto", vision_backend="on", review_mode=True
    )

    assert runner.persisted == []  # nothing about the failed read is written as a result
    first, restore = runner.updates
    assert first["status"] == "extracting"
    assert restore["status"] == "needs_review"
    assert restore["metadata_updates"]["reviewAttempts"] == 4  # as it was
    assert runner.parsed == ["9116218"]
    assert summary["stoppedEarly"] == "gemini-3.8-flash refused the key (403)"
    assert [item["status"] for item in summary["processed"]] == ["skipped", "skipped"]
    assert summary["processed"][1]["reason"].startswith("run stopped")


def test_max_filings_caps_what_is_attempted(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _Runner(monkeypatch, {"9116218": 2, "9116217": 2, "9115808": 2})
    summary = cli.process_house_queue_rows(
        Settings(),
        [_queue("9116218"), _queue("9116217"), _queue("9115808")],
        ocr_backend="auto",
        vision_backend="on",
        review_mode=True,
        max_filings=1,
    )
    assert runner.parsed == ["9116218"]
    assert [item.get("reason") for item in summary["processed"][1:]] == ["--max-filings 1 reached"] * 2


def test_a_dry_run_reads_and_writes_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    def _forbidden(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("a dry run wrote to the database")

    for name in (
        "update_house_stub_state",
        "sync_house_stubs_to_neon",
        "mark_house_stub_processed",
        "upsert_trade_rows_to_neon",
        "apply_house_amendment_changes",
        "index_search_document",
        "persist_parsed_house_stub",
    ):
        monkeypatch.setattr(cli, name, _forbidden)
    monkeypatch.setattr(cli, "download_house_pdf", lambda stub, _settings, path: path.write_bytes(b"%PDF"))
    monkeypatch.setattr(cli, "vision_calls_needed_for_pdf", lambda path, _backend: 2)
    monkeypatch.setattr(cli, "fetch_house_line_references", lambda _settings, **_kwargs: ([], [], [], []))
    stub_, parsed, trades = _scanned(
        [_transaction(1, ROGERS_9116218["asset_description"], legibility="partial", transaction_type="sale",
                      transaction_date="2026-06-15", amount_min=0, amount_max=0)],
        stub=_stub("9116218", filing_date="2026-07-14"),
        detector=["no-ticks"],
        needsReview=True,
        needsReviewReasons=["checkbox detector found no ticked amount on page(s) 1"],
    )
    monkeypatch.setattr(cli, "parse_live_house_stub", lambda *_a, **_k: (parsed, trades))

    summary = cli.process_house_queue_rows(
        Settings(), [_queue("9116218")], ocr_backend="auto", vision_backend="on",
        review_mode=True, dry_run=True, with_search_index=True,
    )

    item = summary["processed"][0]
    assert summary["dryRun"] is True
    assert item["status"] == "would be needs_review"
    assert item["plan"]["gate"]["decision"] == "withheld"
    assert "checkbox detector found no ticked amount on page(s) 1" in item["plan"]["gate"]["reasons"]
    assert item["plan"]["publish"] == []
    assert item["transcription"][0]["detector"][1] == "no-ticks"


# =============================================================================
# 7. Year scoping
# =============================================================================


def test_year_bounds_parse() -> None:
    today = date(2026, 10, 2)
    assert cli.parse_filing_year_bound("current-1", today=today) == 2025
    assert cli.parse_filing_year_bound("current", today=today) == 2026
    assert cli.parse_filing_year_bound("2019", today=today) == 2019
    assert cli.parse_filing_year_bound(None, today=today) is None
    import click

    with pytest.raises(click.BadParameter):
        cli.parse_filing_year_bound("last year", today=today)


def test_the_queue_query_bounds_the_filing_year(monkeypatch: pytest.MonkeyPatch) -> None:
    from capitol_pipeline.exporters import neon

    executed: list[tuple[str, tuple[Any, ...]]] = []

    class _Cursor:
        def __enter__(self) -> "_Cursor":
            return self

        def __exit__(self, *_exc: object) -> bool:
            return False

        def execute(self, sql: str, params: tuple[Any, ...]) -> None:
            executed.append((sql, params))

        def fetchall(self) -> list[dict[str, Any]]:
            return []

    class _Connection:
        def __enter__(self) -> "_Connection":
            return self

        def __exit__(self, *_exc: object) -> bool:
            return False

        def cursor(self) -> _Cursor:
            return _Cursor()

    monkeypatch.setattr(neon, "neon_connection", lambda _settings: _Connection())
    neon.fetch_house_stub_queue(Settings(), limit=12, only_needs_review=True, review_config="c", min_year=2025)
    sql, params = executed[-1]
    assert "filing_year >= %s" in sql and "filing_year <= %s" not in sql
    assert params == ("c", 2025, 12)

    neon.fetch_house_stub_queue(Settings(), limit=5, only_needs_review=True, min_year=2014, max_year=2024)
    sql, params = executed[-1]
    assert "filing_year >= %s" in sql and "filing_year <= %s" in sql
    assert params == (2014, 2024, 5)

    neon.fetch_house_stub_queue(Settings(), limit=5, only_needs_review=True)
    sql, params = executed[-1]
    assert "filing_year >=" not in sql and params == (5,)  # unchanged without the flags


def test_the_review_command_passes_the_scope_through(monkeypatch: pytest.MonkeyPatch) -> None:
    from click.testing import CliRunner

    seen: dict[str, Any] = {}
    monkeypatch.setattr(cli, "load_registry_if_available", lambda _settings, **kwargs: seen.update(registry=kwargs))
    monkeypatch.setattr(cli, "fetch_house_stub_queue", lambda _settings, **kwargs: seen.update(queue=kwargs) or [])

    def _rows(_settings: Any, rows: list[Any], **kwargs: Any) -> dict[str, Any]:
        seen["run"] = kwargs
        return {"processed": []}

    monkeypatch.setattr(cli, "process_house_queue_rows", _rows)
    result = CliRunner().invoke(
        cli.cli,
        ["process-house-review", "--min-year", "current-1", "--max-filings", "3",
         "--vision-call-budget", "24", "--dry-run", "--vision-backend", "on", "--ocr-backend", "auto"],
    )

    assert result.exit_code == 0, result.output
    assert seen["queue"]["min_year"] == date.today().year - 1 and seen["queue"]["max_year"] is None
    assert seen["run"]["max_filings"] == 3 and seen["run"]["dry_run"] is True
    assert seen["run"]["with_search_index"] is False  # a dry run indexes nothing
    assert seen["run"]["allow_partial"] is False  # the gate is on unless asked
    assert seen["registry"]["export_cache"] is False  # nor writes the registry cache
    assert json.loads(result.output)["visionCallBudget"]["limit"] == 24


# =============================================================================
# The providers: two different models, one rendered page per call
# =============================================================================


def test_gemini_reads_with_two_versions_and_anthropic_with_two_models(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("CAPITOL_PTR_VISION_MODEL", "CAPITOL_PTR_VISION_MODEL_B"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("GEMINI_API_KEY", "k")
    gemini = GeminiProvider()
    assert (gemini.read_model, gemini.read_model_b) == ("gemini-3.8-flash", "gemini-3.5-flash")
    anthropic = ptr_vision_provider.AnthropicProvider(lambda: None)
    assert anthropic.read_model == "claude-opus-5"
    assert anthropic.read_model_b == "claude-sonnet-5"
    assert anthropic.read_model != anthropic.read_model_b


def test_every_call_carries_exactly_one_rendered_page(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _gemini(monkeypatch, [OK])
    monkeypatch.delenv("CAPITOL_PTR_VISION_CHUNK_PAGES", raising=False)
    assert ptr_vision.resolve_chunk_pages() == 1
    bodies: list[tuple[str, dict[str, Any]]] = []

    def _post(self: GeminiProvider, model: str, body: dict[str, Any], **_kwargs: Any) -> dict[str, Any]:
        bodies.append((model, body))
        return OK.json()

    monkeypatch.setattr(GeminiProvider, "_post", _post)
    ptr_vision.extract_via_vision(_scan_pdf(tmp_path, 3))

    assert len(bodies) == 6  # three pages, two reads each
    labels = []
    for model, body in bodies:
        parts = body["contents"][0]["parts"]
        page_labels = [part["text"] for part in parts if "text" in part and part["text"].endswith(" of 3:")]
        assert len(page_labels) == 1, page_labels
        labels.append((model, page_labels[0]))
        # Rendered locally: PNG images only, never the PDF itself.
        assert all(part["inlineData"]["mimeType"] == "image/png" for part in parts if "inlineData" in part)
    assert labels == [
        ("gemini-3.8-flash", "Page 1 of 3:"), ("gemini-3.5-flash", "Page 1 of 3:"),
        ("gemini-3.8-flash", "Page 2 of 3:"), ("gemini-3.5-flash", "Page 2 of 3:"),
        ("gemini-3.8-flash", "Page 3 of 3:"), ("gemini-3.5-flash", "Page 3 of 3:"),
    ]


def test_a_read_that_leaves_nothing_to_publish_is_withheld_and_says_why(exporter: _Calls) -> None:
    # 9116218 read again on 2026-10-02 by the Gemini pair: one read said
    # purchase, the other sale, and the only row was dropped for its type.
    stub, parsed, _trades = _scanned([], stub=_stub("9116218", filing_date="2026-07-14"),
                                     needsReview=True, rowsDroppedForType=1,
                                     needsReviewReasons=["reads disagree on transaction_type"])
    summary = cli.persist_parsed_house_stub(Settings(), stub, parsed, [])

    assert exporter.upserts == []
    assert summary["stubStatus"] == "needs_review"
    gate = summary["filingGate"]
    assert gate["decision"] == "withheld"
    assert "1 row(s) dropped: the reads disagreed on the transaction type" in gate["reasons"]


def test_nothing_to_report_is_a_result_not_a_hold(exporter: _Calls) -> None:
    stub, parsed, _trades = _scanned([], noTransactions=True)
    summary = cli.persist_parsed_house_stub(Settings(), stub, parsed, [])
    assert summary["filingGate"] is None
    assert summary["stubStatus"] == "parsed"
