"""House PTR rows marked "Filing Status: Amended" or "Deleted".

An amended row restates a transaction an earlier PTR disclosed and a deleted
row withdraws one; neither is a new trade. Published as trades, they counted
transactions twice and measured lateness from the amendment: Nancy Pelosi's
page showed ten late filings that were all amendment lines restating trades
she had disclosed on time. The fixtures below are cut from real filings'
text layers (the layout, the mixed-case "FIlINg STATuS" a subset font
prints, the Clerk's transaction id in front of the owner code on amended and
deleted rows); names and figures are taken from those filings.
"""

from __future__ import annotations

from typing import Any

import pytest

from capitol_pipeline import cli
from capitol_pipeline.config import Settings
from capitol_pipeline.house_amendments import (
    PriorLine,
    plan_house_amendments,
    prior_from_trade,
    priors_from_transcription,
)
from capitol_pipeline.models.congress import FilingStub, MemberMatch, NormalizedTradeRow
from capitol_pipeline.parsers.house_ptr import (
    parse_filing_status,
    parse_house_ptr_text,
    strip_form_annotation,
)

MEMBER = MemberMatch(id="m-P000197", name="Nancy Pelosi", slug="nancy-pelosi", party="D", state="CA", district="12")

HEADER = """PerIODIC TrANSACTION rePOrT
Clerk of the House of Representatives • legislative Resource Center • 135 Cannon Building • Washington, DC 20515
fIler INfOrmATION
Name:
Hon. Nancy Pelosi
Status:
Member
State/District: CA12
TrANSACTIONS
ID
Owner Asset
Transaction
Type
Date
Notification
Date
Amount
Cap.
Gains >
$200?
"""

FOOTER = """* For the complete list of asset type abbreviations, please visit https://fd.house.gov/reference/asset-type-codes.aspx.
INITIAl PublIC OfferINGS
n
m
l
k
j Yes n
m
l
k
j
i No
CerTIfICATION AND SIGNATure
I CERTIFY that the statements I have made on the attached Periodic Transaction Report are true, complete, and correct to the
best of my knowledge and belief.
Digitally Signed: Hon. Nancy Pelosi , 07/20/2020
"""

#: An amendment PTR (shape of doc 20016961): an amended row with the Clerk's
#: transaction id before the owner code, a new row, and a deleted row (shape
#: of doc 20003840).
AMENDMENT_TEXT = HEADER + """2000060675 SP
Amazon.com, Inc. (AMZN)
[OP]
S
01/16/2020
01/16/2020
$250,001 -
$500,000
g
f
e
d
c
FIlINg STATuS: Amended
DESCRIPTION: Sold 20 call options with a strike price of $1,700 and an expiration date of 1/17/20.
SP
American Express Company
(AXP) [OP]
P
06/24/2020 06/24/2020
$100,001 -
$250,000
g
f
e
d
c
FIlINg STATuS: New
DESCRIPTION: Purchased 50 call options with a strike price of $80 and an expiration date of 1/21/2022.
2000004250 JT
Caterpillar, Inc. (CAT)
E
03/11/2020 03/31/2020
$1,001 - $15,000
FILINg STATUS: Deleted
SUBHoLDINg oF: Brokerage #2 USAA 8425
DESCRIPTIoN: moved to new account
Filing ID #20016961
""" + FOOTER


def _stub(doc_id: str = "20016961", filing_date: str = "2020-07-20") -> FilingStub:
    return FilingStub(
        doc_id=doc_id,
        filing_year=int(filing_date[:4]),
        filing_date=filing_date,
        member=MEMBER,
        source="house-clerk",
        source_url=f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{filing_date[:4]}/{doc_id}.pdf",
    )


def _row(
    line: int,
    status: str | None,
    *,
    doc_id: str = "20016961",
    ticker: str | None = "AMZN",
    asset: str = "Amazon.com, Inc.",
    tx_type: str = "sale",
    tx_date: str = "2020-01-16",
    amount: tuple[int, int] = (250001, 500000),
    owner: str = "spouse",
    comment: str | None = None,
    filing_date: str = "2020-07-20",
) -> NormalizedTradeRow:
    return NormalizedTradeRow(
        member=MEMBER,
        source="house-clerk",
        disclosure_kind="house-ptr",
        source_id=f"{doc_id}:{line}",
        source_url=f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2020/{doc_id}.pdf",
        ticker=ticker,
        asset_description=asset,
        asset_type="Stock",
        transaction_type=tx_type,
        transaction_date=tx_date,
        disclosure_date=filing_date,
        amount_min=amount[0],
        amount_max=amount[1],
        owner=owner,
        comment=comment if comment is not None else (f"Filing Status: {status.capitalize()}" if status else None),
        filing_status=status,
    )


def _prior(
    trade_id: str | None,
    *,
    doc_id: str = "20015042",
    filing_date: str = "2020-02-11",
    ticker: str | None = "AMZN",
    asset: str = "Amazon.com, Inc.",
    tx_type: str = "sale",
    tx_date: str = "2020-01-16",
    amount: tuple[int, int] = (250001, 500000),
    owner: str = "spouse",
    description: str | None = None,
    key: str | None = None,
) -> PriorLine:
    return PriorLine(
        key=key or trade_id or f"{doc_id}:x",
        doc_id=doc_id,
        filing_date=filing_date,
        ticker=ticker,
        asset_description=asset,
        transaction_type=tx_type,
        transaction_date=tx_date,
        amount_min=amount[0],
        amount_max=amount[1],
        owner=owner,
        description=description,
        trade_id=trade_id,
    )


# ── Parser ──────────────────────────────────────────────────────────────────


def test_parser_reads_each_rows_filing_status() -> None:
    parsed, trades = parse_house_ptr_text(AMENDMENT_TEXT, _stub())
    assert [t.filing_status for t in parsed.transactions] == ["amended", "new", "deleted"]
    assert [t.filing_status for t in trades] == ["amended", "new", "deleted"]


def test_parser_spells_the_status_one_way_in_the_comment() -> None:
    parsed, _ = parse_house_ptr_text(AMENDMENT_TEXT, _stub())
    first, second, third = parsed.transactions
    assert "Filing Status: Amended" in (first.comment or "")
    assert "FIlINg STATuS" not in (first.comment or "")
    assert "Filing Status: New" in (second.comment or "")
    assert "Filing Status: Deleted" in (third.comment or "")


def test_parser_takes_the_clerk_id_off_the_owner_code() -> None:
    """ "2000060675 SP" hid the owner from parse_owner: amended rows came out
    as the member's own trades when the filing says spouse."""

    parsed, _ = parse_house_ptr_text(AMENDMENT_TEXT, _stub())
    first, second, third = parsed.transactions
    assert (first.filing_id, first.owner) == ("2000060675", "spouse")
    assert (second.filing_id, second.owner) == (None, "spouse")
    assert (third.filing_id, third.owner) == ("2000004250", "joint")
    assert first.asset_description == "Amazon.com, Inc."
    assert third.asset_description == "Caterpillar, Inc."
    assert third.transaction_type == "exchange"


def test_status_helpers() -> None:
    assert parse_filing_status("g f e d c | FIlINg STATuS: Amended | DESCRIPTION: x") == "amended"
    assert parse_filing_status("FILINg STATUS: Deleted") == "deleted"
    # The row's own status comes first; a second is a neighbour's swept in.
    assert parse_filing_status("Filing Status: New | Comments: x | Filing Status: Amended") == "new"
    assert parse_filing_status("Subholding Of: Fidelity") is None
    assert strip_form_annotation("Filing Status: Deleted Caterpillar, Inc.") == "Caterpillar, Inc."


def test_a_stored_transcription_without_the_field_keeps_its_status() -> None:
    """Stubs transcribed before filing_status existed carry it in the comment."""

    parsed, _ = parse_house_ptr_text(AMENDMENT_TEXT, _stub())
    stored = [t.model_dump(exclude={"filing_status"}) for t in parsed.transactions]
    from capitol_pipeline.models.congress import HousePtrTransaction
    from capitol_pipeline.parsers.house_ptr import build_trade_rows_from_house_ptr

    replayed = parsed.model_copy(update={"transactions": [HousePtrTransaction(**row) for row in stored]})
    rows = build_trade_rows_from_house_ptr(replayed, _stub())
    assert [row.filing_status for row in rows] == ["amended", "new", "deleted"]


# ── Planning ────────────────────────────────────────────────────────────────


def test_an_amended_row_folds_into_the_original_and_is_not_inserted() -> None:
    """Shape of Pelosi's AB purchase (doc 20018011, restated by 20018539): the
    original row keeps its id and its on-time disclosure date and takes the
    amended value. The changed amount here is illustrative."""

    rows = [
        _row(1, "amended", ticker="AB", asset="AllianceBernstein Holding L.P. Units", tx_type="purchase",
             tx_date="2020-12-22", amount=(1000001, 5000000), owner="spouse", doc_id="20018539",
             filing_date="2021-04-09"),
        _row(2, "new", ticker="AXP", asset="American Express Company", tx_type="purchase", tx_date="2021-03-24",
             doc_id="20018539", filing_date="2021-04-09"),
    ]
    priors = [_prior("tr-house-20018011-1", doc_id="20018011", filing_date="2021-01-21", ticker="AB",
                     asset="AllianceBernstein Holding l.P. units", tx_type="purchase", tx_date="2020-12-22",
                     amount=(500001, 1000000), owner="spouse")]
    plan = plan_house_amendments(rows, doc_id="20018539", filing_date="2021-04-09", trade_priors=priors)
    assert [row.ticker for row in plan.rows] == ["AXP"]
    assert plan.updates == {
        "tr-house-20018011-1": {
            "amount_min": 1000001,
            "amount_max": 5000000,
            "comment_note": "Amended by House PTR 20018539 filed 2021-04-09",
        }
    }
    # A row this filing published on an earlier run goes too.
    assert plan.deletes == ["tr-house-20018539-1"]
    assert plan.actions[0]["action"] == "applied_to_original"
    assert plan.actions[0]["originalFilingDate"] == "2021-01-21"


def test_a_deleted_row_withdraws_its_original_and_itself() -> None:
    rows = [_row(3, "deleted", ticker="CAT", asset="Caterpillar, Inc.", tx_type="exchange", tx_date="2020-03-11",
                 amount=(1001, 15000), owner="joint")]
    priors = [_prior("tr-house-20016300-2", doc_id="20016300", filing_date="2020-04-01", ticker="CAT",
                     asset="Caterpillar, Inc.", tx_type="exchange", tx_date="2020-03-11", amount=(1001, 15000),
                     owner="joint")]
    plan = plan_house_amendments(rows, doc_id="20016961", filing_date="2020-07-20", trade_priors=priors)
    assert plan.rows == []
    assert plan.updates == {}
    assert sorted(plan.deletes) == ["tr-house-20016300-2", "tr-house-20016961-3"]
    assert plan.actions[0]["action"] == "withdrew_original"


def test_a_deleted_row_with_nothing_to_withdraw_is_still_not_a_trade() -> None:
    rows = [_row(3, "deleted", ticker="CAT", asset="Caterpillar, Inc.", tx_type="exchange", tx_date="2020-03-11")]
    plan = plan_house_amendments(rows, doc_id="20016961", filing_date="2020-07-20", trade_priors=[])
    assert plan.rows == []
    assert plan.deletes == ["tr-house-20016961-3"]
    assert plan.actions[0]["action"] == "dropped_deleted_line"


def test_an_amendment_whose_original_never_became_a_row_takes_the_first_filings_date() -> None:
    """Pelosi's AMZN sales: doc 20015042 (filed 2020-02-11) disclosed them,
    but the parser never published those two lines, so the amendment rows
    are the only rows. They are dated from the first filing, not 2020-07-20."""

    rows = [
        _row(1, "amended", comment="Filing Status: Amended | DESCRIPTION: Sold 20 call options with a strike price of $1,700"),
        _row(2, "amended", comment="Filing Status: Amended | DESCRIPTION: Sold 20 call options with a strike price of $1,600"),
    ]
    transcribed = priors_from_transcription("20015042", "2020-02-11", [
        {"line_number": 1, "ticker": "AMZN", "asset_description": "amazon.com, Inc.", "transaction_type": "purchase",
         "transaction_date": "2020-01-16", "amount_min": 1000001, "amount_max": 5000000, "owner": "spouse",
         "comment": "Filing Status: New | DESCRIPTION: Exercised 30 call options"},
        {"line_number": 2, "ticker": "AMZN", "asset_description": "amazon.com, Inc.", "transaction_type": "sale",
         "transaction_date": "2020-01-16", "amount_min": 250001, "amount_max": 500000, "owner": "spouse",
         "comment": "Filing Status: New | DESCRIPTION: sold 20 call options with a strike price of $1,700"},
        {"line_number": 3, "ticker": "AMZN", "asset_description": "amazon.com, Inc.", "transaction_type": "sale",
         "transaction_date": "2020-01-16", "amount_min": 250001, "amount_max": 500000, "owner": "spouse",
         "comment": "Filing Status: New | DESCRIPTION: sold 20 call options with a strike price of $1,600"},
    ])
    plan = plan_house_amendments(rows, doc_id="20016961", filing_date="2020-07-20", trade_priors=[],
                                 transcribed_priors=transcribed)
    assert [row.disclosure_date for row in plan.rows] == ["2020-02-11", "2020-02-11"]
    assert all("First disclosed in House PTR 20015042 filed 2020-02-11" in (row.comment or "") for row in plan.rows)
    assert plan.updates == {} and plan.deletes == []
    assert [action["action"] for action in plan.actions] == ["inserted_with_original_date"] * 2


def test_an_amendment_with_no_original_on_file_is_inserted_as_it_is() -> None:
    rows = [_row(1, "amended")]
    plan = plan_house_amendments(rows, doc_id="20016961", filing_date="2020-07-20", trade_priors=[])
    assert len(plan.rows) == 1
    assert plan.rows[0].disclosure_date == "2020-07-20"
    # The comment keeps the status, which is what the site reads to leave the
    # row out of late-filing counts.
    assert "Filing Status: Amended" in (plan.rows[0].comment or "")
    assert plan.actions[0]["action"] == "inserted_original_not_located"


def test_identical_amended_lines_take_one_original_each() -> None:
    """Two AMZN sales on one day for the same amount: one original apiece,
    told apart by their descriptions."""

    rows = [
        _row(1, "amended", comment="Filing Status: Amended | DESCRIPTION: Sold 20 call options with a strike price of $1,700"),
        _row(2, "amended", comment="Filing Status: Amended | DESCRIPTION: Sold 20 call options with a strike price of $1,600"),
    ]
    priors = [
        _prior("tr-house-20015042-3", description="sold 20 call options with a strike price of $1,600"),
        _prior("tr-house-20015042-2", description="sold 20 call options with a strike price of $1,700"),
    ]
    plan = plan_house_amendments(rows, doc_id="20016961", filing_date="2020-07-20", trade_priors=priors)
    assert plan.rows == []
    originals = {action["line"]: action["original"] for action in plan.actions}
    assert originals == {"20016961:1": "tr-house-20015042-2", "20016961:2": "tr-house-20015042-3"}


def test_a_year_typo_is_what_the_amendment_corrects() -> None:
    """Virginia Foxx, doc 20020718 (filed 2022-04-05) typed March 2022 trades
    as 2021; amendment 20022213 fixed the year. Both rows read as a year late.
    The original row keeps its disclosure date and takes the corrected date."""

    rows = [_row(1, "amended", ticker="AEG", asset="AEGON N.V.", tx_type="sale", tx_date="2022-03-07",
                 amount=(15001, 50000), owner="joint", doc_id="20022213", filing_date="2023-01-03")]
    priors = [
        _prior("tr-house-20020718-1", doc_id="20020718", filing_date="2022-04-05", ticker="AEG", asset="AEgON N.V.",
               tx_type="sale", tx_date="2021-03-07", amount=(15001, 50000), owner="joint"),
        # A different, real AEGON trade two days off must not be the match.
        _prior("tr-house-20020543-1", doc_id="20020543", filing_date="2022-03-07", ticker="AEG", asset="AEgON N.V.",
               tx_type="purchase", tx_date="2022-02-15", amount=(15001, 50000), owner="joint"),
    ]
    plan = plan_house_amendments(rows, doc_id="20022213", filing_date="2023-01-03", trade_priors=priors)
    assert plan.updates["tr-house-20020718-1"]["transaction_date"] == "2022-03-07"
    assert plan.actions[0]["tier"] == "year_typo"


def test_a_corrected_ticker_still_finds_its_original() -> None:
    """Amendments exist to fix the asset too: SIMON PPTY notes first reported
    by CUSIP, then by name, same date, type and amount."""

    rows = [_row(2, "amended", ticker=None, asset="SIMON PPTY GROUP LP NOTE CALL MAKE WHOLE", tx_type="sale",
                 tx_date="2023-09-13", amount=(15001, 50000), owner="self", doc_id="20023767", filing_date="2024-01-16",
                 comment="Filing Status: Amended | Description: SIMON PPTY GROUP LP NOTE CALL MAKE WHOLE 2.45%")]
    priors = [_prior("tr-house-20023752-2", doc_id="20023752", filing_date="2023-09-28", ticker=None,
                     asset="828807DF1", tx_type="sale", tx_date="2023-09-13", amount=(15001, 50000), owner="self",
                     description="SIMON PPTY GROUP LP NOTE CALL MAKE WHOLE 2.45%")]
    plan = plan_house_amendments(rows, doc_id="20023767", filing_date="2024-01-16", trade_priors=priors)
    assert plan.actions[0]["tier"] == "asset_corrected"
    assert "tr-house-20023752-2" in plan.updates


def test_unrelated_names_are_not_a_corrected_asset() -> None:
    rows = [_row(1, "amended", ticker="ALLY", asset="Ally Financial Inc.", tx_type="purchase", tx_date="2021-06-15",
                 amount=(1001, 15000), owner="joint")]
    priors = [_prior("tr-house-1-1", ticker="LPLA", asset="LPL Financial Holdings Inc.", tx_type="purchase",
                     tx_date="2021-06-15", amount=(1001, 15000), owner="joint")]
    plan = plan_house_amendments(rows, doc_id="20016961", filing_date="2020-07-20", trade_priors=priors)
    assert plan.actions[0]["action"] == "inserted_original_not_located"


def test_a_later_filing_is_never_the_original() -> None:
    rows = [_row(1, "amended")]
    priors = [_prior("tr-house-20017000-1", doc_id="20017000", filing_date="2020-08-01")]
    plan = plan_house_amendments(rows, doc_id="20016961", filing_date="2020-07-20", trade_priors=priors)
    assert plan.actions[0]["action"] == "inserted_original_not_located"


def test_a_degenerate_amount_is_not_applied_over_a_band() -> None:
    rows = [_row(1, "amended", amount=(15001, 15001))]
    plan = plan_house_amendments(rows, doc_id="20016961", filing_date="2020-07-20",
                                 trade_priors=[_prior("tr-house-20015042-2")])
    assert "amount_min" not in plan.updates["tr-house-20015042-2"]


def test_prior_from_trade_reads_a_trades_row() -> None:
    prior = prior_from_trade({
        "id": "tr-house-20015042-2", "ticker": None, "asset_description": "amazon.com, Inc. (aMZN) [sT]",
        "transaction_type": "sale", "transaction_date": "2020-01-16", "disclosure_date": "2020-02-11",
        "amount_min": 250001, "amount_max": 500000, "owner": "spouse",
        "comment": "g f e d c | FIlINg sTaTus: New | DEsCRIPTIoN: sold 20 call options | Parsed from House PTR 20015042",
    })
    assert (prior.doc_id, prior.filing_date, prior.filing_status) == ("20015042", "2020-02-11", "new")
    assert prior.description == "sold 20 call options"


# ── Persisting a filing ─────────────────────────────────────────────────────


class _Calls:
    def __init__(self) -> None:
        self.upserts: list[list[NormalizedTradeRow]] = []
        self.applied: list[dict[str, Any]] = []
        self.marks: list[dict[str, Any]] = []


@pytest.fixture
def exporter(monkeypatch: pytest.MonkeyPatch) -> _Calls:
    calls = _Calls()
    monkeypatch.setattr(cli, "sync_house_stubs_to_neon", lambda _settings, _stubs: {"upserted": 1})

    def _upsert(_settings: Settings, trades: list[NormalizedTradeRow]) -> dict[str, Any]:
        calls.upserts.append(list(trades))
        return {"upserted": len(trades), "trade_ids": [f"tr-house-{t.source_id.replace(':', '-')}" for t in trades]}

    def _priors(_settings: Settings, *, member_id: str, doc_id: str, filing_date: str | None):
        assert (member_id, doc_id, filing_date) == ("m-P000197", "20016961", "2020-07-20")
        trades = [{
            "id": "tr-house-20016300-2", "ticker": "CAT", "asset_description": "Caterpillar, Inc.",
            "transaction_type": "exchange", "transaction_date": "2020-03-11", "disclosure_date": "2020-04-01",
            "amount_min": 1001, "amount_max": 15000, "owner": "joint", "comment": "Filing Status: New",
        }]
        stubs = [{"doc_id": "20015042", "filing_date": "2020-02-11", "parsed_transactions": [
            {"line_number": 2, "ticker": "AMZN", "asset_description": "amazon.com, Inc.", "transaction_type": "sale",
             "transaction_date": "2020-01-16", "amount_min": 250001, "amount_max": 500000, "owner": "spouse",
             "comment": "Filing Status: New | DESCRIPTION: sold 20 call options with a strike price of $1,700"},
        ]}]
        return trades, stubs

    def _apply(_settings: Settings, *, updates: dict, deletes: list) -> dict[str, int]:
        calls.applied.append({"updates": updates, "deletes": deletes})
        return {"updated": len(updates), "deleted": len(deletes)}

    def _mark(_settings: Settings, _stub: FilingStub, **kwargs: Any) -> None:
        calls.marks.append(kwargs)

    monkeypatch.setattr(cli, "upsert_trade_rows_to_neon", _upsert)
    monkeypatch.setattr(cli, "fetch_house_amendment_priors", _priors)
    monkeypatch.setattr(cli, "apply_house_amendment_changes", _apply)
    monkeypatch.setattr(cli, "mark_house_stub_processed", _mark)
    return calls


def test_persisting_an_amendment_ptr_publishes_only_the_new_row(exporter: _Calls) -> None:
    parsed, trades = parse_house_ptr_text(AMENDMENT_TEXT, _stub())
    summary = cli.persist_parsed_house_stub(Settings(), _stub(), parsed, trades)

    published = exporter.upserts[0]
    # The new AXP row, plus the AMZN restatement whose original line never
    # became a trade -- dated from the filing that first disclosed it.
    assert sorted((row.ticker, row.disclosure_date) for row in published) == [
        ("AMZN", "2020-02-11"), ("AXP", "2020-07-20")
    ]
    # The deleted CAT row withdrew the original and is not a trade itself.
    assert exporter.applied == [{"updates": {}, "deletes": ["tr-house-20016961-3", "tr-house-20016300-2"]}]
    amendments = exporter.marks[0]["metadata_extra"]["amendments"]
    assert amendments["counts"] == {"inserted_with_original_date": 1, "withdrew_original": 1}
    # The stub records every row the filing carries, statuses included.
    assert [t["filing_status"] for t in exporter.marks[0]["parsed_transactions"]] == ["amended", "new", "deleted"]
    assert summary["stubStatus"] == "parsed"


def test_a_filing_with_no_restated_rows_never_queries_for_originals(
    exporter: _Calls, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _fail(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("no amended or deleted rows: nothing to look up")

    monkeypatch.setattr(cli, "fetch_house_amendment_priors", _fail)
    text = AMENDMENT_TEXT.replace("STATuS: Amended", "STATuS: New").replace("STATUS: Deleted", "STATUS: New")
    parsed, trades = parse_house_ptr_text(text, _stub())
    cli.persist_parsed_house_stub(Settings(), _stub(), parsed, trades)
    assert len(exporter.upserts[0]) == 3
    assert "amendments" not in (exporter.marks[0]["metadata_extra"] or {})
