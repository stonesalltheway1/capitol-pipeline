"""A House PTR filing read again keeps the trade ids it already has.

The fixture is a real one: PTR 20009360 (2018) prints ten rows, six of them
sales in a small-capital font ("s", "s (partial)"). The case-blind parser read
the four purchases and published them as tr-house-20009360-1..4. Read with
today's parser the purchases sit at positions 6, 7, 8 and 10; numbered by
position, the upsert would have written Broadridge's sale over Kimberly-Clark's
purchase.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from capitol_pipeline import cli
from capitol_pipeline.config import Settings
from capitol_pipeline.house_line_ids import (
    LineReference,
    apply_line_numbers,
    assign_line_numbers,
    references_from,
    score_line,
    withhold_rows,
)
from capitol_pipeline.models.congress import (
    FilingStub,
    HousePtrTransaction,
    MemberMatch,
    NormalizedTradeRow,
)
from capitol_pipeline.parsers.house_ptr import parse_house_ptr_text

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "house_ptr"
DOC = "20009360"


def _stub(doc_id: str = DOC, filing_date: str = "2018-04-27") -> FilingStub:
    return FilingStub(
        doc_id=doc_id,
        filing_year=int(filing_date[:4]),
        filing_date=filing_date,
        member=MemberMatch(id="m-TEST", name="Test Member", slug="test-member", state="TX"),
        source="house-clerk",
        source_url=f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{filing_date[:4]}/{doc_id}.pdf",
    )


def _parse():
    return parse_house_ptr_text((FIXTURES_DIR / f"{DOC}.txt").read_text(encoding="utf-8"), _stub())


#: The transcription the case-blind parser stored for 20009360, as it stored it.
STORED_V1 = [
    {"line_number": 1, "asset_description": "Kimberly-Clark Corporation (KMb) [sT]", "ticker": None,
     "transaction_type": "purchase", "transaction_date": "2018-03-21", "amount_min": 1001, "amount_max": 15000,
     "owner": "self"},
    {"line_number": 2, "asset_description": "Lockheed Martin Corporation (LMT) [sT]", "ticker": None,
     "transaction_type": "purchase", "transaction_date": "2018-04-05", "amount_min": 1001, "amount_max": 15000,
     "owner": "self"},
    {"line_number": 3, "asset_description": "NuVasive, Inc. (NuVA) [sT]", "ticker": None,
     "transaction_type": "purchase", "transaction_date": "2018-04-04", "amount_min": 1001, "amount_max": 15000,
     "owner": "self"},
    {"line_number": 4, "asset_description": "Wells Fargo & Company (WFC) [sT]", "ticker": None,
     "transaction_type": "purchase", "transaction_date": "2018-03-28", "amount_min": 1001, "amount_max": 15000,
     "owner": "self"},
]


def _live_v1() -> list[dict[str, Any]]:
    return [
        {"id": f"tr-house-{DOC}-{row['line_number']}", "ticker": row["ticker"],
         "asset_description": row["asset_description"], "transaction_type": row["transaction_type"],
         "transaction_date": row["transaction_date"], "disclosure_date": "2018-04-27",
         "amount_min": row["amount_min"], "amount_max": row["amount_max"], "owner": row["owner"],
         "comment": "Filing Status: New", "source_url": ""}
        for row in STORED_V1
    ]


def test_a_filing_with_no_history_is_numbered_by_position() -> None:
    parsed, _trades = _parse()
    assignment = assign_line_numbers(parsed.transactions, [])
    assert assignment.numbers == {n: n for n in range(1, 11)}
    assert assignment.fresh == []


def test_rows_found_before_keep_their_ids_and_recovered_rows_get_new_ones() -> None:
    parsed, trades = _parse()
    by_position = {t.line_number: t.ticker for t in parsed.transactions}
    assert [by_position[p] for p in (6, 7, 8, 10)] == ["KMB", "LMT", "NUVA", "WFC"]

    assignment = assign_line_numbers(parsed.transactions, references_from(DOC, STORED_V1, _live_v1()))
    assert {p: assignment.numbers[p] for p in (6, 7, 8, 10)} == {6: 1, 7: 2, 8: 3, 10: 4}
    # The six sales are new: numbered above every number the filing has used
    # and above their own positions, in document order.
    assert [assignment.numbers[p] for p in (1, 2, 3, 4, 5, 9)] == [11, 12, 13, 14, 15, 16]
    assert assignment.fresh == [11, 12, 13, 14, 15, 16]
    assert assignment.unmatched_references == []

    parsed, trades = apply_line_numbers(parsed, trades, assignment.numbers)
    ids = {f"tr-house-{t.source_id.replace(':', '-')}": t.ticker for t in trades}
    assert {i: ids[i] for i in (f"tr-house-{DOC}-{n}" for n in (1, 2, 3, 4))} == {
        f"tr-house-{DOC}-1": "KMB", f"tr-house-{DOC}-2": "LMT", f"tr-house-{DOC}-3": "NUVA", f"tr-house-{DOC}-4": "WFC",
    }
    assert sorted(t.line_number for t in parsed.transactions) == [1, 2, 3, 4, 11, 12, 13, 14, 15, 16]


def test_reading_the_filing_a_third_time_changes_nothing() -> None:
    parsed, trades = _parse()
    first = assign_line_numbers(parsed.transactions, references_from(DOC, STORED_V1, _live_v1()))
    parsed, trades = apply_line_numbers(parsed, trades, first.numbers)
    stored_v2 = [t.model_dump() for t in parsed.transactions]
    live_v2 = [
        {"id": f"tr-house-{t.source_id.replace(':', '-')}", "ticker": t.ticker,
         "asset_description": t.asset_description, "transaction_type": t.transaction_type,
         "transaction_date": t.transaction_date, "amount_min": t.amount_min, "amount_max": t.amount_max,
         "owner": t.owner}
        for t in trades
    ]
    again, _ = _parse()
    second = assign_line_numbers(again.transactions, references_from(DOC, stored_v2, live_v2))
    assert second.numbers == first.numbers
    assert second.fresh == []


def test_an_owner_correction_does_not_change_identity() -> None:
    row = HousePtrTransaction(line_number=7, asset_description="Intuit Inc.", ticker="INTU", asset_type="Asset",
                              transaction_type="purchase", transaction_date="2014-01-07", amount_min=15001,
                              amount_max=50000, owner="spouse")
    ref = LineReference(line_number=1, transaction_date="2014-01-07", transaction_type="purchase", amount_min=15001,
                        amount_max=50000, ticker=None, asset_description="Intuit Inc. (INTu)", owner="self",
                        origin="trade")
    assert assign_line_numbers([row], [ref]).numbers == {7: 1}


def test_two_different_tickers_never_match() -> None:
    row = HousePtrTransaction(line_number=1, asset_description="Microsoft Corporation", ticker="MSFT",
                              asset_type="Stock", transaction_type="purchase", transaction_date="2020-01-02",
                              amount_min=1001, amount_max=15000, owner="self")
    ref = LineReference(line_number=1, transaction_date="2020-01-02", transaction_type="purchase", amount_min=1001,
                        amount_max=15000, ticker="AAPL", asset_description="Apple Inc.", owner="self",
                        origin="trade")
    assert score_line(row, ref, position=1) is None
    assignment = assign_line_numbers([row], [ref])
    assert assignment.numbers == {1: 2}
    assert assignment.unmatched_references == [1]


def test_the_stored_transcription_matches_a_row_an_amendment_changed() -> None:
    # The live row took an amendment's amount; the stored transcription still
    # holds what this filing printed, so the row is still recognised.
    row = HousePtrTransaction(line_number=3, asset_description="Exxon Mobil Corporation", ticker="XOM",
                              asset_type="Stock", transaction_type="sale", transaction_date="2020-03-18",
                              amount_min=15001, amount_max=50000, owner="self")
    stored = [{**row.model_dump(), "line_number": 2}]
    live = [{"id": "tr-house-20016358-2", "ticker": "XOM", "asset_description": "Exxon Mobil Corporation",
             "transaction_type": "purchase", "transaction_date": "2020-03-19", "amount_min": 1001,
             "amount_max": 15000, "owner": "self"}]
    assert assign_line_numbers([row], references_from("20016358", stored, live)).numbers == {3: 2}


def test_new_numbers_clear_every_number_the_filing_ever_used() -> None:
    row = HousePtrTransaction(line_number=1, asset_description="Tesla, Inc.", ticker="TSLA", asset_type="Stock",
                              transaction_type="sale", transaction_date="2021-05-03", amount_min=1001,
                              amount_max=15000, owner="self")
    stored = [{"line_number": 9, "ticker": "F", "asset_description": "Ford Motor Company",
               "transaction_type": "sale", "transaction_date": "2021-05-03", "amount_min": 1001,
               "amount_max": 15000, "owner": "self"}]
    assert assign_line_numbers([row], references_from("20019999", stored, [])).numbers == {1: 10}


def test_only_this_filings_line_ids_are_references() -> None:
    live = [{"id": "tr-house-20009360", "transaction_date": "2018-03-21"},
            {"id": "tr-house-200093601-2", "transaction_date": "2018-03-21"},
            {"id": "tr-house-20009360-7", "transaction_date": "2018-03-21", "transaction_type": "sale"}]
    assert [r.line_number for r in references_from(DOC, [], live)] == [7]


# ── Rows that must stay out of trades ───────────────────────────────────────


def _pelosi():
    text = (FIXTURES_DIR / "20015042.txt").read_text(encoding="utf-8")
    return parse_house_ptr_text(text, _stub("20015042", "2020-02-11"))


#: The 2026-10-02 amendment repair kept Pelosi's 20016961 restatement of the
#: $1,700 AMZN call sale and dated it from 20015042, where the sale was first
#: disclosed in a small-capital "s" the parser could not read.
STANDIN = {
    "id": "tr-house-20016961-1", "ticker": "AMZN", "asset_description": "Amazon.com, Inc.",
    "transaction_type": "sale", "transaction_date": "2020-01-16", "disclosure_date": "2020-02-11",
    "amount_min": 250001, "amount_max": 500000, "owner": "spouse", "source_url": "",
    "comment": "g f e d c | FIlINg STATuS: Amended | DESCRIPTION: Sold 20 call options with a strike price of "
               "$1,700 and an expiration date of 1/17/20. | First disclosed in House PTR 20015042 filed "
               "2020-02-11; this row is that transaction as restated in House PTR 20016961, so it carries the "
               "first filing's disclosure date",
}


def test_a_row_an_amendment_row_already_publishes_is_held_back_and_marked() -> None:
    parsed, trades = _pelosi()
    refs: list[LineReference] = []
    parsed, trades, report = withhold_rows(parsed, trades, refs, doc_id="20015042", filing_date="2020-02-11",
                                           standins=[STANDIN])
    assert report == [{"line": 2, "reason": "represented by tr-house-20016961-1"}]
    assert [int(t.source_id.split(":")[1]) for t in trades] == [1, 3, 4, 5]
    by_line = {t.line_number: t for t in parsed.transactions}
    assert by_line[2].withheld == "represented by tr-house-20016961-1"
    assert by_line[3].withheld is None


def test_a_withheld_mark_in_the_stored_transcription_holds_on_the_next_read() -> None:
    parsed, trades = _pelosi()
    stored = [{**t.model_dump(), "withheld": "represented by tr-house-20016961-1" if t.line_number == 2 else None}
              for t in parsed.transactions]
    refs = references_from("20015042", stored, [])
    assignment = assign_line_numbers(parsed.transactions, refs)
    assert assignment.numbers == {n: n for n in range(1, 6)}
    parsed, trades, report = withhold_rows(parsed, trades, refs, doc_id="20015042", filing_date="2020-02-11")
    assert report == [{"line": 2, "reason": "represented by tr-house-20016961-1"}]
    assert len(trades) == 4


def test_a_withdrawn_id_is_not_brought_back_by_a_new_read() -> None:
    parsed, trades = _pelosi()
    withdrawn = [{"id": "tr-house-20015042-9", "ticker": "FB", "asset_description": "Facebook, Inc. - Class a",
                  "transaction_type": "purchase", "transaction_date": "2020-01-16", "amount_min": 250001,
                  "amount_max": 500000, "owner": "spouse"}]
    refs = references_from("20015042", [], [], withdrawn)
    assignment = assign_line_numbers(parsed.transactions, refs)
    # Two identical Facebook purchases (rows 4 and 5); one of them was row 9.
    # A filing with any history numbers its other rows above it: 10 to 13.
    assert sorted(assignment.numbers[n] for n in (4, 5)) == [9, 13]
    parsed, trades = apply_line_numbers(parsed, trades, assignment.numbers)
    parsed, trades, report = withhold_rows(parsed, trades, refs, doc_id="20015042", filing_date="2020-02-11")
    assert [r["line"] for r in report] == [9]
    assert "trade change log" in report[0]["reason"]
    assert all(not t.source_id.endswith(":9") for t in trades)


def test_a_published_row_is_never_held_back() -> None:
    parsed, trades = _pelosi()
    live = [{"id": "tr-house-20015042-2", "ticker": "AMZN", "asset_description": "amazon.com, Inc.",
             "transaction_type": "sale", "transaction_date": "2020-01-16", "amount_min": 250001,
             "amount_max": 500000, "owner": "spouse"}]
    stored = [{**t.model_dump(), "withheld": "represented by tr-house-20016961-1" if t.line_number == 2 else None}
              for t in parsed.transactions]
    refs = references_from("20015042", stored, live)
    parsed, trades, report = withhold_rows(parsed, trades, refs, doc_id="20015042", filing_date="2020-02-11",
                                           standins=[STANDIN])
    assert report == []
    assert len(trades) == 5
    assert all(t.withheld is None for t in parsed.transactions)


def test_a_stand_in_for_another_filing_is_ignored() -> None:
    parsed, trades = _pelosi()
    other = {**STANDIN, "comment": STANDIN["comment"].replace("20015042", "20015043")}
    _parsed, kept, report = withhold_rows(parsed, trades, [], doc_id="20015042", filing_date="2020-02-11",
                                          standins=[other])
    assert report == [] and len(kept) == 5


# ── Persisting ──────────────────────────────────────────────────────────────


class _Calls:
    def __init__(self) -> None:
        self.upserts: list[list[NormalizedTradeRow]] = []
        self.marks: list[dict[str, Any]] = []
        self.fetches: list[dict[str, Any]] = []


@pytest.fixture
def exporter(monkeypatch: pytest.MonkeyPatch) -> _Calls:
    calls = _Calls()
    monkeypatch.setattr(cli, "sync_house_stubs_to_neon", lambda _settings, _stubs: {"upserted": 1})

    def _upsert(_settings: Settings, trades: list[NormalizedTradeRow]) -> dict[str, Any]:
        calls.upserts.append(list(trades))
        return {"upserted": len(trades), "trade_ids": [f"tr-house-{t.source_id.replace(':', '-')}" for t in trades]}

    def _references(_settings: Settings, *, doc_id: str, member_id: str | None):
        calls.fetches.append({"doc_id": doc_id, "member_id": member_id})
        return STORED_V1, _live_v1(), [], []

    def _mark(_settings: Settings, _stub: FilingStub, **kwargs: Any) -> None:
        calls.marks.append(kwargs)

    monkeypatch.setattr(cli, "upsert_trade_rows_to_neon", _upsert)
    monkeypatch.setattr(cli, "fetch_house_line_references", _references)
    monkeypatch.setattr(cli, "mark_house_stub_processed", _mark)
    return calls


def test_persisting_a_filing_read_again_keeps_its_ids(exporter: _Calls) -> None:
    parsed, trades = _parse()
    cli.persist_parsed_house_stub(Settings(), _stub(), parsed, trades)

    written = {f"tr-house-{t.source_id.replace(':', '-')}": (t.ticker, t.transaction_type) for t in exporter.upserts[0]}
    assert written[f"tr-house-{DOC}-1"] == ("KMB", "purchase")
    assert written[f"tr-house-{DOC}-4"] == ("WFC", "purchase")
    assert sorted(written) == sorted(f"tr-house-{DOC}-{n}" for n in (1, 2, 3, 4, 11, 12, 13, 14, 15, 16))
    mark = exporter.marks[0]
    # The stored transcription carries the same numbers, so the next run matches it.
    assert sorted(t["line_number"] for t in mark["parsed_transactions"]) == [1, 2, 3, 4, 11, 12, 13, 14, 15, 16]
    assert mark["metadata_extra"]["lineIds"] == {
        "matched": 4, "renumbered": 4, "fresh": [11, 12, 13, 14, 15, 16], "unmatchedReferences": []
    }
    assert exporter.fetches == [{"doc_id": DOC, "member_id": "m-TEST"}]


def test_a_filing_with_no_history_records_nothing(exporter: _Calls, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "fetch_house_line_references", lambda _settings, **_kwargs: ([], [], [], []))
    parsed, trades = _parse()
    cli.persist_parsed_house_stub(Settings(), _stub(), parsed, trades)
    assert sorted(int(t.source_id.split(":")[1]) for t in exporter.upserts[0]) == list(range(1, 11))
    assert "lineIds" not in (exporter.marks[0]["metadata_extra"] or {})


def test_position_never_outweighs_content() -> None:
    # PTR 20017356: the case-blind parser could not read the "$.25" Root 9B
    # sale, so the UBS sale was row 4. Today Root 9B is row 4 and UBS row 5;
    # same date, type and account prefix ("Charles Schwab - "), and Root 9B
    # sits where UBS used to. It must not take UBS's id.
    text = (FIXTURES_DIR / "20017356.txt").read_text(encoding="utf-8")
    parsed, _trades = parse_house_ptr_text(text, _stub("20017356", "2020-09-13"))
    names = {t.line_number: t.asset_description for t in parsed.transactions}
    assert "Root 9 B" in names[4] and "UBS" in names[5]
    stored = [
        {**t.model_dump(), "line_number": n}
        for n, t in zip((1, 2, 3, 4), [t for t in parsed.transactions if t.line_number != 4])
    ]
    assignment = assign_line_numbers(parsed.transactions, references_from("20017356", stored, []))
    assert assignment.numbers == {1: 1, 2: 2, 3: 3, 4: 6, 5: 4}
    assert assignment.fresh == [6]


def test_a_name_an_old_parse_could_not_read_still_matches_its_row() -> None:
    # "JT / ALLY FINANCIAL INC B/E 05.125% / 093024" was stored as "093024"
    # (or "Pending House PTR extraction") and published as "self".
    row = HousePtrTransaction(line_number=9, asset_description="ALLY FINANCIAL INC B/E 05.125% 093024",
                              asset_type="Asset", transaction_type="purchase", transaction_date="2015-01-22",
                              amount_min=1001, amount_max=15000, owner="joint")
    for name in ("093024", "Pending House PTR extraction"):
        ref = LineReference(line_number=16, transaction_date="2015-01-22", transaction_type="purchase",
                            amount_min=1001, amount_max=15000, ticker=None, asset_description=name, owner="self",
                            origin="trade")
        assert assign_line_numbers([row], [ref]).numbers == {9: 16}, name


def test_persisting_keeps_a_represented_row_out_and_marks_it(exporter: _Calls, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "fetch_house_line_references", lambda _settings, **_kwargs: ([], [], [STANDIN], []))
    parsed, trades = _pelosi()
    stub = _stub("20015042", "2020-02-11")
    cli.persist_parsed_house_stub(Settings(), stub, parsed, trades)
    assert sorted(int(t.source_id.split(":")[1]) for t in exporter.upserts[0]) == [1, 3, 4, 5]
    mark = exporter.marks[0]
    stored = {t["line_number"]: t for t in mark["parsed_transactions"]}
    assert stored[2]["withheld"] == "represented by tr-house-20016961-1"
    assert mark["metadata_extra"]["lineIds"]["withheld"] == [
        {"line": 2, "reason": "represented by tr-house-20016961-1"}
    ]


def test_a_401k_account_name_is_not_a_ticker() -> None:
    """Doc 20033916: an earlier parse lost the JPM ticker and left the account
    name in the asset ("... Sardinia Ready Mix 401(k) - Dave JP Morgan Chase &
    Co. Common"). The "(k)" read as ticker K kept the row from matching itself,
    so a re-read would have published it a second time under a new id."""

    row = HousePtrTransaction(
        line_number=16,
        asset_description="JP Morgan Chase & Co. Common Stock",
        ticker="JPM",
        asset_type="Stock",
        transaction_type="purchase",
        transaction_date="2026-01-16",
        amount_min=1001,
        amount_max=15000,
        owner="self",
    )
    reference = LineReference(
        line_number=16,
        transaction_date="2026-01-16",
        transaction_type="purchase",
        amount_min=1001,
        amount_max=15000,
        ticker=None,
        asset_description="David Taylor Trust > Sardinia Ready Mix 401(k) - Dave JP Morgan Chase & Co. Common",
        owner="self",
        origin="stored",
    )
    assert score_line(row, reference, position=16) is not None
    assert assign_line_numbers([row], [reference]).numbers == {16: 16}
    # A real one-letter ticker still counts.
    visa = reference.__class__(**{**reference.__dict__, "asset_description": "Visa Inc. (V)"})
    assert score_line(row, visa, position=16) is None
