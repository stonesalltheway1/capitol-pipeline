"""Publishing rules for House PTR results.

A filing read off page images publishes row by row while it is
``needs_review``: a row carrying a date, a type and an amount band goes to
``trades``, and a row missing any of the three stays in the stub metadata for
a reviewer. The text path keeps publishing as before. The database is never
touched: the exporter entry points are patched on the CLI module.
"""

from __future__ import annotations

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


def _stub(member_id: str | None = "m-K000389") -> FilingStub:
    return FilingStub(
        doc_id="8219444",
        filing_year=2023,
        filing_date="2023-04-07",
        member=MemberMatch(id=member_id, name="Ro Khanna", slug="ro-khanna", party="D", state="CA", district="17"),
        source="house-clerk",
        source_url="https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2023/8219444.pdf",
    )


def _transaction(*, line: int = 1, legibility: str | None = "clear") -> HousePtrTransaction:
    return HousePtrTransaction(
        legibility=legibility,
        line_number=line,
        asset_description="Lear Corporation",
        asset_type="Asset",
        transaction_type="purchase",
        transaction_date="2023-03-10",
        amount_min=1001,
        amount_max=15000,
        owner="spouse",
    )


def _trade(
    *,
    line: int = 1,
    transaction_date: str | None = "2023-03-10",
    amount_min: int = 1001,
    amount_max: int = 15000,
) -> NormalizedTradeRow:
    """One built trade row, settled unless a field is knocked out."""

    return NormalizedTradeRow(
        member=MemberMatch(
            id="m-K000389", name="Ro Khanna", slug="ro-khanna", party="D", state="CA", district="17"
        ),
        source="house-clerk",
        disclosure_kind="house-ptr",
        source_id=f"tr-house-8219444-{line}",
        asset_description="Lear Corporation",
        asset_type="Asset",
        transaction_type="purchase",
        transaction_date=transaction_date,
        amount_min=amount_min,
        amount_max=amount_max,
        owner="spouse",
    )


class _Calls:
    def __init__(self) -> None:
        self.upserts: list[list[Any]] = []
        self.marks: list[dict[str, Any]] = []


@pytest.fixture
def exporter(monkeypatch: pytest.MonkeyPatch) -> _Calls:
    calls = _Calls()
    monkeypatch.setattr(cli, "sync_house_stubs_to_neon", lambda _settings, _stubs: {"upserted": 1})

    def _upsert(_settings: Settings, trades: list[Any]) -> dict[str, Any]:
        calls.upserts.append(list(trades))
        return {"upserted": len(trades), "trade_ids": [f"tr-house-8219444-{i + 1}" for i in range(len(trades))]}

    def _mark(_settings: Settings, _stub: FilingStub, **kwargs: Any) -> None:
        calls.marks.append(kwargs)

    monkeypatch.setattr(cli, "upsert_trade_rows_to_neon", _upsert)
    monkeypatch.setattr(cli, "mark_house_stub_processed", _mark)
    return calls


def test_a_scan_in_review_publishes_the_rows_that_are_settled(exporter: _Calls) -> None:
    """One disputed row must not hold back the rows nothing disputes.

    On doc 9116141 the old rule did exactly that: 26 rows whose Type column
    the two reads read one column apart held back 108 rows both reads and the
    checkbox detector agreed on.
    """

    parsed = HousePtrParseResult(
        doc_id="8219444",
        parser_confidence=0.85,
        parser_version="claude-vision-v2",
        transactions=[_transaction()],
        vision_report={
            "ok": True,
            "needsReview": True,
            "needsReviewReasons": ["reads disagree on amount"],
            "rowsDroppedForType": 3,
        },
    )
    trades = [
        _trade(line=1),
        _trade(line=2, amount_min=0, amount_max=0),
        _trade(line=3, transaction_date=None),
    ]

    summary = cli.persist_parsed_house_stub(Settings(), _stub(), parsed, trades)

    assert summary["stubStatus"] == "needs_review"
    assert [row.source_id for row in exporter.upserts[0]] == ["tr-house-8219444-1"]
    assert summary["trades"]["upserted"] == 1
    assert summary["trades"]["withheld"] == 2
    mark = exporter.marks[0]
    assert mark["status"] == "needs_review"
    # The whole transcription stays on the stub for the reviewer.
    assert mark["parsed_transactions"][0]["asset_description"] == "Lear Corporation"
    vision = mark["metadata_extra"]["visionParse"]
    assert vision["publishedTrades"] == 1
    assert vision["withheldTrades"] == 2
    # Rows the reads disagreed on the type of never reached trade building,
    # so the reader-facing total has to carry them too.
    assert vision["rowsWithheldTotal"] == 5
    assert vision["withheldReasons"] == {
        "no amount band": 1,
        "no transaction date": 1,
        "reads disagree on transaction type": 3,
    }


def test_a_row_the_read_could_not_rate_clear_is_withheld(exporter: _Calls) -> None:
    """The reader's own rating is part of the publish rule, and it is measured.

    Docs 8221322 and 8221358 were read by the vision path and then transcribed
    again, twice, by two independent readers of a different model family that
    were never shown the result. Of the 798 rows where all three could be
    compared, 653 were rated ``clear`` and **not one** of those was wrong about
    its type or its amount; all 37 wrong types and all 8 wrong amounts were on
    rows already rated ``partial``. The 37 are two whole pages whose Type
    column both Gemini reads took one column to the left -- two reads by two
    versions of one model family are not independent of each other, so the
    rating has to be part of the rule rather than the agreement alone.
    """

    parsed = HousePtrParseResult(
        doc_id="8219444",
        parser_confidence=0.85,
        parser_version="gemini-vision-v2",
        transactions=[
            _transaction(line=1, legibility="clear"),
            _transaction(line=2, legibility="partial"),
            _transaction(line=3, legibility="illegible"),
            _transaction(line=4, legibility=None),
        ],
        vision_report={"ok": True, "needsReview": True, "needsReviewReasons": ["reads disagree on transaction_type"]},
    )
    trades = [_trade(line=1), _trade(line=2), _trade(line=3), _trade(line=4)]

    summary = cli.persist_parsed_house_stub(Settings(), _stub(), parsed, trades)

    # Rated clear publishes. Rated partial or illegible does not. An unrated
    # row is a transcription from before the reader recorded a rating, replayed
    # from the stub rather than read fresh, and it publishes.
    assert [row.source_id for row in exporter.upserts[0]] == [
        "tr-house-8219444-1",
        "tr-house-8219444-4",
    ]
    assert summary["trades"]["withheld"] == 2
    vision = exporter.marks[0]["metadata_extra"]["visionParse"]
    assert vision["withheldReasons"] == {
        "the read rated this row partial": 1,
        "the read rated this row illegible": 1,
    }


def test_a_scan_in_review_withholds_every_unsettled_row(exporter: _Calls) -> None:
    parsed = HousePtrParseResult(
        doc_id="8219444",
        parser_confidence=0.85,
        parser_version="claude-vision-v2",
        transactions=[_transaction()],
        vision_report={"ok": True, "needsReview": True, "needsReviewReasons": ["majority illegible"]},
    )
    trades = [_trade(line=1, amount_min=0, amount_max=0), _trade(line=2, transaction_date=None)]

    summary = cli.persist_parsed_house_stub(Settings(), _stub(), parsed, trades)

    assert exporter.upserts == []  # nothing reached trades
    assert summary["trades"] == {"upserted": 0, "trade_ids": [], "withheld": 2}
    assert exporter.marks[0]["extracted_trade_id"] is None


def test_a_relabelled_transcription_is_still_treated_as_a_scan(exporter: _Calls) -> None:
    """The publish gate must not key on a label any run can overwrite.

    doc 9116141 carried a Gemini transcription stamped ``regex-v1`` because
    every re-processing rewrote ``metadata.parserVersion``. Keyed on that
    label, the gate let 107 rows read off page images publish as text-parser
    output while the stub sat in review.
    """

    parsed = HousePtrParseResult(
        doc_id="9116141",
        parser_confidence=0.0,
        parser_version="regex-v1",
        transactions=[_transaction()],
        vision_report={
            "ok": True,
            "needsReview": True,
            "needsReviewReasons": ["reads disagree on transaction_type"],
        },
    )
    trades = [_trade(line=1), _trade(line=2, amount_min=0, amount_max=0)]

    summary = cli.persist_parsed_house_stub(Settings(), _stub(), parsed, trades)

    assert summary["trades"]["withheld"] == 1
    assert [row.source_id for row in exporter.upserts[0]] == ["tr-house-8219444-1"]


def test_vision_result_that_parses_publishes(exporter: _Calls) -> None:
    parsed = HousePtrParseResult(
        doc_id="8219444",
        parser_confidence=0.85,
        parser_version="claude-vision-v2",
        transactions=[_transaction()],
        vision_report={"ok": True, "needsReview": False},
    )
    trades = [object()]

    summary = cli.persist_parsed_house_stub(Settings(), _stub(), parsed, trades)  # type: ignore[arg-type]

    assert summary["stubStatus"] == "parsed"
    assert exporter.upserts == [trades]
    assert summary["trades"]["upserted"] == 1
    assert exporter.marks[0]["extracted_trade_id"] == "tr-house-8219444-1"
    assert "withheldTrades" not in exporter.marks[0]["metadata_extra"]["visionParse"]


def test_vision_result_with_unresolved_member_is_withheld(exporter: _Calls) -> None:
    parsed = HousePtrParseResult(
        doc_id="8219444",
        parser_confidence=0.85,
        parser_version="claude-vision-v2",
        transactions=[_transaction()],
        vision_report={"ok": True, "needsReview": False},
    )
    summary = cli.persist_parsed_house_stub(
        Settings(), _stub(member_id=None), parsed, [_trade(line=1, amount_min=0, amount_max=0)]
    )
    assert summary["stubStatus"] == "needs_review"
    assert exporter.upserts == []
    assert summary["trades"]["withheld"] == 1


def test_text_path_publishes_even_in_review(exporter: _Calls) -> None:
    # Unchanged behaviour for the regex/Haiku paths.
    parsed = HousePtrParseResult(
        doc_id="8219444",
        parser_confidence=0.3,
        parser_version="regex-v1",
        transactions=[_transaction()],
    )
    trades = [_trade(line=1)]

    summary = cli.persist_parsed_house_stub(Settings(), _stub(), parsed, trades)  # type: ignore[arg-type]

    assert summary["stubStatus"] == "needs_review"
    assert exporter.upserts == [trades]
    assert "withheld" not in summary["trades"]


def test_no_transactions_result_publishes_nothing_and_parses(exporter: _Calls) -> None:
    parsed = HousePtrParseResult(
        doc_id="8219362",
        parser_confidence=0.9,
        parser_version="claude-vision-v2",
        vision_report={"ok": True, "needsReview": False, "noTransactions": True},
    )
    summary = cli.persist_parsed_house_stub(Settings(), _stub(), parsed, [])
    assert summary["stubStatus"] == "parsed"
    assert exporter.upserts == [[]]
    assert exporter.marks[0]["last_error"] is None


def test_queue_fetch_can_target_doc_ids(monkeypatch: pytest.MonkeyPatch) -> None:
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
            return [{"doc_id": "8220068"}]

    class _Connection:
        def __enter__(self) -> "_Connection":
            return self

        def __exit__(self, *_exc: object) -> bool:
            return False

        def cursor(self) -> _Cursor:
            return _Cursor()

    monkeypatch.setattr(neon, "neon_connection", lambda _settings: _Connection())

    rows = neon.fetch_house_stub_queue(Settings(), limit=5, only_needs_review=True, doc_ids=["8220068", "8220100"])

    assert rows == [{"doc_id": "8220068"}]
    sql, params = executed[0]
    assert "doc_id = ANY(%s)" in sql
    assert params == (["8220068", "8220100"], 5)

    executed.clear()
    neon.fetch_house_stub_queue(Settings(), limit=3)
    sql, params = executed[0]
    assert "ANY" not in sql
    assert params == (3,)
