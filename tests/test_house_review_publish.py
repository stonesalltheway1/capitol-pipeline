"""Publishing rules for House PTR results.

A vision-parsed filing publishes trade rows only once it resolves to
``parsed``; while it is ``needs_review`` the rows stay in the stub metadata.
The text path keeps publishing as before. The database is never touched: the
exporter entry points are patched on the CLI module.
"""

from __future__ import annotations

from typing import Any

import pytest

from capitol_pipeline import cli
from capitol_pipeline.config import Settings
from capitol_pipeline.models.congress import FilingStub, HousePtrParseResult, HousePtrTransaction, MemberMatch


def _stub(member_id: str | None = "m-K000389") -> FilingStub:
    return FilingStub(
        doc_id="8219444",
        filing_year=2023,
        filing_date="2023-04-07",
        member=MemberMatch(id=member_id, name="Ro Khanna", slug="ro-khanna", party="D", state="CA", district="17"),
        source="house-clerk",
        source_url="https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2023/8219444.pdf",
    )


def _transaction() -> HousePtrTransaction:
    return HousePtrTransaction(
        line_number=1,
        asset_description="Lear Corporation",
        asset_type="Asset",
        transaction_type="purchase",
        transaction_date="2023-03-10",
        amount_min=1001,
        amount_max=15000,
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


def test_vision_result_in_review_publishes_no_trades(exporter: _Calls) -> None:
    parsed = HousePtrParseResult(
        doc_id="8219444",
        parser_confidence=0.85,
        parser_version="claude-vision-v2",
        transactions=[_transaction()],
        vision_report={"ok": True, "needsReview": True, "needsReviewReasons": ["reads disagree on amount"]},
    )
    trades = [object(), object()]

    summary = cli.persist_parsed_house_stub(Settings(), _stub(), parsed, trades)  # type: ignore[arg-type]

    assert summary["stubStatus"] == "needs_review"
    assert exporter.upserts == []  # nothing reached trades
    assert summary["trades"] == {"upserted": 0, "trade_ids": [], "withheld": 2}
    mark = exporter.marks[0]
    assert mark["status"] == "needs_review"
    assert mark["extracted_trade_id"] is None
    # The transcription stays on the stub for the reviewer.
    assert mark["parsed_transactions"][0]["asset_description"] == "Lear Corporation"
    assert mark["metadata_extra"]["visionParse"]["withheldTrades"] == 2


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
    summary = cli.persist_parsed_house_stub(Settings(), _stub(member_id=None), parsed, [object()])  # type: ignore[arg-type]
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
    trades = [object()]

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
