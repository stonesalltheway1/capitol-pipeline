"""Publishing a filing whose rows were parsed before its member resolved.

A PTR parsed for a filer with no member record keeps its rows on the stub and
writes nothing to trades. Loading the historical members afterwards resolves
the filer, but nothing re-runs, so 105 filings and 738 finished rows sat in the
review queue. This is the command that publishes them, and it costs nothing:
no PDF is downloaded and no model is called.
"""

from __future__ import annotations

from typing import Any

import pytest

from capitol_pipeline import cli
from capitol_pipeline.config import Settings
from capitol_pipeline.models.congress import FilingStub, MemberMatch
from capitol_pipeline.registries.members import MemberRegistry

MANNING_ROW: dict[str, Any] = {
    "owner": "spouse",
    "ticker": "CFG",
    "amount_max": 15000,
    "amount_min": 1001,
    "asset_type": "Stock",
    "line_number": 1,
    "transaction_date": "2023-12-12",
    "transaction_type": "sale",
    "asset_description": "Citizens Financial Group, Inc.",
    "notification_date": "2024-01-02",
}


def _row(**metadata: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "docId": "20024231",
        "state": "NC",
        "district": "6",
        "lastName": "Manning",
        "firstName": "Kathy",
        "memberId": "m-M001135",
        "memberName": "Kathy E. Manning",
        "memberSlug": "kathy-e-manning",
        "filingDate": "2024-01-12",
        "filingType": "P",
        "filingYear": 2024,
        "parserConfidence": 0.95,
        "parsedTransactionCount": 1,
        "parsedTransactions": [dict(MANNING_ROW)],
        "rawTextPreview": "Kathy Manning NC06 ...",
        "memberResolvedBy": "members_historical",
    }
    base.update(metadata)
    return {
        "doc_id": "20024231",
        "filing_year": 2024,
        "source": "house_clerk",
        "source_url": "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2024/20024231.pdf",
        "status": "needs_review",
        "extracted_trade_id": None,
        "metadata": base,
    }


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
        return {
            "upserted": len(trades),
            "trade_ids": [f"tr-house-20024231-{index + 1}" for index in range(len(trades))],
        }

    def _mark(_settings: Settings, _stub: FilingStub, **kwargs: Any) -> None:
        calls.marks.append(kwargs)

    monkeypatch.setattr(cli, "upsert_trade_rows_to_neon", _upsert)
    monkeypatch.setattr(cli, "mark_house_stub_processed", _mark)
    return calls


# ---------------------------------------------------------------------------
# Rebuilding
# ---------------------------------------------------------------------------


def test_a_stored_transcription_rebuilds_into_trade_rows() -> None:
    stub, parsed, trades, reason = cli.rebuild_parsed_house_stub(_row())

    assert reason is None
    assert parsed is not None
    assert stub.member.id == "m-M001135"
    assert [transaction.ticker for transaction in parsed.transactions] == ["CFG"]
    assert len(trades) == 1
    trade = trades[0]
    assert trade.source_id == "20024231:1"
    assert trade.member.id == "m-M001135"
    assert trade.transaction_date == "2023-12-12"
    assert trade.disclosure_date == "2024-01-12"
    # The row says where it came from, and does not claim a parser that never
    # ran: this filing was transcribed before the version was recorded.
    assert trade.parser_version == cli.REPLAY_PARSER_VERSION
    assert "published once the member record resolved" in trade.comment
    assert "House PTR 20024231" in trade.comment


def test_a_recorded_parser_version_is_kept() -> None:
    _stub, parsed, trades, _reason = cli.rebuild_parsed_house_stub(_row(parserVersion="regex-v1"))
    assert parsed is not None
    assert parsed.parser_version == "regex-v1"
    assert "[regex-v1]" in trades[0].comment


def test_the_registry_resolves_a_member_the_stub_never_had() -> None:
    registry = MemberRegistry.from_records(
        [
            MemberMatch(
                id="m-M001135",
                name="Kathy E. Manning",
                slug="kathy-e-manning",
                party="D",
                state="NC",
                district="6",
            )
        ]
    )
    row = _row(memberId="")
    stub, parsed, trades, reason = cli.rebuild_parsed_house_stub(row, registry=registry)

    assert reason is None and parsed is not None
    assert stub.member.id == "m-M001135"
    assert trades[0].member.id == "m-M001135"


def test_an_unresolved_member_is_reported_not_published() -> None:
    stub, parsed, trades, reason = cli.rebuild_parsed_house_stub(_row(memberId=""))
    assert parsed is None and trades == []
    assert reason == "member still unresolved"
    assert stub.doc_id == "20024231"


def test_a_row_dated_after_the_filing_is_dropped() -> None:
    late = dict(MANNING_ROW, transaction_date="2025-06-01")
    _stub, parsed, _trades, reason = cli.rebuild_parsed_house_stub(
        _row(parsedTransactions=[late])
    )
    assert parsed is None
    assert reason == "every stored row failed date validation"


def test_a_subset_font_run_is_repaired_before_publication() -> None:
    # Some House PTRs embed a subset font whose lowercase glyphs extract 0x222
    # code points high, as IPA characters. Transcriptions stored before the
    # text layer was run through fix_font_mojibake carry them, and 234 of the
    # rows waiting to be published did.
    shifted = "".join(
        chr(ord(char) + 0x222) if char.islower() else char
        for char in "Filing Status: New Paychex, Inc."
    )
    assert shifted != "Filing Status: New Paychex, Inc."
    row = dict(MANNING_ROW, asset_description=shifted, comment=shifted)
    _stub, parsed, trades, reason = cli.rebuild_parsed_house_stub(_row(parsedTransactions=[row]))

    assert reason is None and parsed is not None
    assert parsed.transactions[0].asset_description == "Filing Status: New Paychex, Inc."
    assert trades[0].asset_description == "Filing Status: New Paychex, Inc."
    assert trades[0].comment.startswith("Filing Status: New Paychex, Inc.")


def test_an_annotation_glued_to_the_asset_name_is_stripped() -> None:
    # The text parser has since learned to peel the form's "Filing Status:"
    # annotation off the front of the asset, but transcriptions stored before
    # that still carry it: 225 of the rows waiting to be published read
    # "iling Status: New Amazon.com, Inc. - Common Stock".
    row = dict(MANNING_ROW, asset_description="Filing Status: New Amazon.com, Inc. - Common Stock")
    _stub, parsed, _trades, reason = cli.rebuild_parsed_house_stub(_row(parsedTransactions=[row]))

    assert reason is None and parsed is not None
    assert parsed.transactions[0].asset_description == "Amazon.com, Inc. - Common Stock"


def test_a_transcription_the_cleaner_would_gut_is_kept() -> None:
    # The cleaner strips a run of three or more digits as a brokerage account
    # number, which eats a municipal bond's series and coupon.
    row = dict(MANNING_ROW, asset_description="MINNESOTA ST BD GRP 160 5%")
    _stub, parsed, _trades, _reason = cli.rebuild_parsed_house_stub(_row(parsedTransactions=[row]))

    assert parsed is not None
    assert parsed.transactions[0].asset_description == "MINNESOTA ST BD GRP 160 5%"


def test_a_stub_with_no_stored_rows_is_skipped() -> None:
    _stub, parsed, _trades, reason = cli.rebuild_parsed_house_stub(_row(parsedTransactions=[]))
    assert parsed is None
    assert reason == "no stored transcription"


# ---------------------------------------------------------------------------
# The batch
# ---------------------------------------------------------------------------


def test_the_batch_publishes_and_marks_the_stub_parsed(exporter: _Calls) -> None:
    summary = cli.repersist_house_stub_rows(Settings(), [_row()])

    assert summary["published"] == 1
    assert summary["skipped"] == 0
    assert summary["tradeRowsUpserted"] == 1
    assert len(exporter.upserts) == 1
    assert summary["stubs"][0]["stubStatus"] == "parsed"
    assert summary["stubs"][0]["memberId"] == "m-M001135"
    mark = exporter.marks[0]
    assert mark["status"] == "parsed"
    assert mark["extracted_trade_id"] == "tr-house-20024231-1"
    assert mark["parser_version"] == cli.REPLAY_PARSER_VERSION


def test_a_dry_run_writes_nothing(exporter: _Calls) -> None:
    summary = cli.repersist_house_stub_rows(Settings(), [_row()], dry_run=True)

    assert summary["dryRun"] is True
    assert summary["published"] == 1
    assert summary["tradeRowsUpserted"] == 1
    assert summary["stubs"][0]["status"] == "would publish"
    assert exporter.upserts == [] and exporter.marks == []


def test_a_low_confidence_filing_can_be_held_back(exporter: _Calls) -> None:
    summary = cli.repersist_house_stub_rows(
        Settings(), [_row(parserConfidence=0.4)], min_confidence=0.6
    )

    assert summary["published"] == 0
    assert summary["skipped"] == 1
    assert "below 0.60" in summary["stubs"][0]["reason"]
    assert exporter.upserts == []


def test_a_withheld_vision_transcription_still_publishes_nothing(exporter: _Calls) -> None:
    # --include-vision can put one in the batch; the withholding rule in
    # persist_parsed_house_stub is what keeps it out of trades, and it still
    # applies here.
    row = _row(
        parserVersion="claude-vision-v2",
        visionParse={"ok": True, "needsReview": True, "parserVersion": "claude-vision-v2"},
    )
    summary = cli.repersist_house_stub_rows(Settings(), [row])

    assert summary["published"] == 1
    assert summary["tradeRowsUpserted"] == 0
    assert summary["stubs"][0]["stubStatus"] == "needs_review"
    assert summary["stubs"][0]["withheld"] == 1
    assert exporter.upserts == []


def test_the_queue_query_excludes_vision_and_published_filings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
            return [{"doc_id": "20024231"}]

    class _Connection:
        def __enter__(self) -> "_Connection":
            return self

        def __exit__(self, *_exc: object) -> bool:
            return False

        def cursor(self) -> _Cursor:
            return _Cursor()

    monkeypatch.setattr(neon, "neon_connection", lambda _settings: _Connection())

    rows = neon.fetch_house_stubs_awaiting_publication(Settings(), limit=5)
    assert rows == [{"doc_id": "20024231"}]
    sql, params = executed[0]
    assert "NOT (s.metadata ? 'visionParse')" in sql
    assert "NOT EXISTS (SELECT 1 FROM trades t WHERE t.id LIKE 'tr-house-' || s.doc_id" in sql
    assert "s.status <> 'parsed'" in sql
    assert params == (5,)

    executed.clear()
    neon.fetch_house_stubs_awaiting_publication(
        Settings(), limit=2, doc_ids=["20024231"], include_vision=True
    )
    sql, params = executed[0]
    assert "visionParse" not in sql
    # Naming a filing runs exactly that filing, whatever state it is in: it is
    # how an already-published filing gets corrected.
    assert "s.status <> 'parsed'" not in sql
    assert "NOT EXISTS" not in sql
    assert params == (["20024231"], 2)
