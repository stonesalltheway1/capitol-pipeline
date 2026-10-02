"""The Clerk's small-capital fonts, and the rows the case-blind parser lost.

Every fixture here is a real House PTR text layer, as ``probe_text_layer``
assembles it (pages joined by a blank line, the subset-font shift reversed).
In these fonts some capitals are drawn with small-capital glyphs and come out
of the text layer in lower case: the Sale code as "s" or "s (partial)",
tickers as "(aaPl)", type codes as "[sT]", the spouse code as "sP". Across
the 5,939 text-layer PTRs on file the old parser dropped 2,858 rows outright
(2,856 small-capital sales and two sub-dollar amounts), left 10,835 tickers
inside asset names and published 1,642 rows as the member's own that the form
assigns to a spouse, a dependent child or joint.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from capitol_pipeline.models.congress import FilingStub, MemberMatch
from capitol_pipeline.parsers.house_ptr import (
    REGEX_PARSER_VERSION,
    ROW_CORE_PATTERN,
    parse_amount_range,
    parse_house_ptr_text,
    parse_owner,
    parse_transactions,
    split_row_segment,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "house_ptr"


def load_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


def stub(doc_id: str, filing_date: str) -> FilingStub:
    return FilingStub(
        doc_id=doc_id,
        filing_year=int(filing_date[:4]),
        filing_date=filing_date,
        member=MemberMatch(id="m-TEST", name="Test Member", slug="test-member", state="NJ"),
        source="house-clerk",
        source_url=f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{filing_date[:4]}/{doc_id}.pdf",
    )


def rows(name: str):
    return parse_transactions(load_fixture(name))


# ── The row core ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("row", "code"),
    [
        ("Integrys Energy Group, Inc. (TEG)\ns\n01/3/2014\n01/3/2014\n$1,001 - $15,000", "s"),
        ("Chubb Limited (Cb) [sT]\ns (partial)\n04/11/2018\n04/11/2018\n$1,001 - $15,000", "s (partial)"),
        ("Hess Corporation (HES) [ST] S (partial) 04/10/2018 04/10/2018 $1,001 - $15,000", "S (partial)"),
        ("Acme Corp (ACME) [ST] P 01/02/2026 01/05/2026 $1,001 - $15,000", "P"),
    ],
)
def test_the_core_reads_the_type_code_in_any_case(row: str, code: str) -> None:
    match = ROW_CORE_PATTERN.search(row)
    assert match is not None
    assert match.group("tx_type") == code


def test_the_core_still_needs_a_standalone_code() -> None:
    # Lower case must not make "Corp" read as "Cor" + "p".
    assert ROW_CORE_PATTERN.search("Acme Corp 01/02/2026 01/05/2026 $1,001 - $15,000") is None
    assert ROW_CORE_PATTERN.search("Acme Corps 01/02/2026 01/05/2026 $1,001 - $15,000") is None


def test_the_core_takes_a_mixed_case_ticker_and_type_code() -> None:
    match = ROW_CORE_PATTERN.search("broadridge Financial solutions,\nInc. (bR) [sT]\ns (partial)\n03/21/2018 03/21/2018\n$1,001 - $15,000")
    assert match is not None
    assert (match.group("ticker"), match.group("asset_type")) == ("bR", "sT")


def test_a_sub_dollar_amount_is_an_amount() -> None:
    assert ROW_CORE_PATTERN.search("Root 9 B Technologies Inc [OT]\nS\n02/22/2019 09/12/2020\n$.25\n") is not None
    assert parse_amount_range("$.25") == (0, 0)
    assert parse_amount_range("$647.63") == (648, 648)


def test_parse_owner_reads_a_small_capital_code() -> None:
    assert parse_owner("sP Brookfield Global Listed Infrastructure") == "spouse"
    assert parse_owner("Washington DC Water Bonds") == "self"


# ── Lost rows ───────────────────────────────────────────────────────────────


def test_a_lower_case_sale_is_a_row() -> None:
    # 20000022 (2014): the old parser found one of these two rows.
    parsed = rows("20000022.txt")
    assert [(r.transaction_type, r.ticker, r.transaction_date) for r in parsed] == [
        ("sale", "TEG", "2014-01-03"),
        ("purchase", "TKR", "2013-12-18"),
    ]
    assert parsed[0].asset_description == "Integrys Energy Group, Inc."


def test_partial_sales_in_small_capitals_are_rows() -> None:
    # 20009360 (2018): ten rows, six of them "s" or "s (partial)"; the old
    # parser found the four purchases.
    parsed = rows("20009360.txt")
    assert len(parsed) == 10
    assert [r.ticker for r in parsed] == ["BR", "CB", "EFX", "GMED", "HES", "KMB", "LMT", "NUVA", "OXY", "WFC"]
    assert [r.transaction_type for r in parsed].count("sale") == 6
    # "[sT]" is the Stock type code.
    assert {r.asset_type for r in parsed} == {"Stock"}
    assert parsed[0].asset_description == "broadridge Financial solutions, Inc."


def test_pelosi_20015042_carries_both_amazon_sales() -> None:
    parsed, trades = parse_house_ptr_text(load_fixture("20015042.txt"), stub("20015042", "2020-02-11"))
    sales = [r for r in parsed.transactions if r.transaction_type == "sale"]
    assert [(r.line_number, r.ticker, r.amount_min, r.amount_max, r.owner) for r in sales] == [
        (2, "AMZN", 250001, 500000, "spouse"),
        (3, "AMZN", 250001, 500000, "spouse"),
    ]
    assert "$1,700" in (sales[0].comment or "") and "$1,600" in (sales[1].comment or "")
    assert len(trades) == 5
    assert trades[1].parser_version == REGEX_PARSER_VERSION == "regex-v2"


def test_a_row_with_a_sub_dollar_amount_is_kept() -> None:
    # 20017356: "Root 9 B Technologies" sold for $.25.
    parsed = rows("20017356.txt")
    assert len(parsed) == 5
    root9 = [r for r in parsed if "Root 9" in r.asset_description]
    assert len(root9) == 1
    assert (root9[0].transaction_type, root9[0].amount_min, root9[0].amount_max) == ("sale", 0, 0)


# ── Owners ──────────────────────────────────────────────────────────────────


def test_an_owner_line_after_a_long_description_opens_the_next_row() -> None:
    # 20001791: "sP" after a full-width DESCRIPTION line read as that line's
    # lower-case wrapped tail, so the iShares purchase was published as the
    # member's own.
    parsed = rows("20001791.txt")
    ishares = [r for r in parsed if r.ticker == "IEMG"]
    assert len(ishares) == 1
    assert ishares[0].owner == "spouse"
    assert ishares[0].asset_description == "ishares Core MsCI Emerging Markets ETF"
    before = parsed[parsed.index(ishares[0]) - 1]
    assert before.comment is not None and not before.comment.rstrip().endswith("sP")


def test_a_bond_whose_name_ends_in_its_maturity_keeps_its_owner_and_name() -> None:
    # 20002595 page 2: "JT / ALLIANT TECHSYSTEMS INC 06.875% / 091520" -- the
    # maturity line read as an account number, the name and the owner went to
    # the row above, and the trade was published as "self" named "091520"
    # (or "Pending House PTR extraction").
    parsed = rows("20002595-page-2.txt")
    by_line = {r.line_number: r for r in parsed}
    assert (by_line[8].owner, by_line[8].asset_description) == ("joint", "ALLIANT TECHSYSTEMS INC 06.875% 091520")
    assert (by_line[9].owner, by_line[9].asset_description) == ("joint", "ALLY FINANCIAL INC B/E 05.125% 093024")
    # Unchanged rows stay unchanged.
    assert (by_line[2].owner, by_line[2].ticker) == ("self", "EADSY")
    assert (by_line[4].owner, by_line[4].asset_description) == ("joint", "ALASKA ST SR A BE/R/ 5 DUE 080128 DTD 041409")
    assert by_line[7].comment is not None and "ALLIANT" not in by_line[7].comment


def test_small_capital_tickers_leave_the_asset_name() -> None:
    parsed = rows("20002595-page-2.txt")
    by_line = {r.line_number: r for r in parsed}
    assert (by_line[3].ticker, by_line[3].asset_description) == ("AKZOY", "Akzo Nobel N.V. American Depositary Shares")
    assert (by_line[7].ticker, by_line[7].asset_description) == ("AGN", "Allergan, Inc.")


def test_grijalva_20000236_owners_tickers_and_lost_sales() -> None:
    # The old parser: four rows, the first one "self" (its "sP" sat under the
    # unstripped column heading), tickers left in the names, three sales lost.
    parsed = rows("20000236.txt")
    assert [(r.transaction_type, r.ticker, r.owner) for r in parsed] == [
        ("purchase", "AA", "spouse"),
        ("purchase", "AAPL", "spouse"),
        ("purchase", "BAC", "spouse"),
        ("purchase", "INTC", "spouse"),
        ("sale", "IBM", "spouse"),
        ("sale", "JDD", "spouse"),
        ("sale", "CTY", "spouse"),
    ]
    assert parsed[0].asset_description == "alcoa Inc."
    assert parsed[1].asset_description == "apple Inc."
    # "2053" is the last line of the name, not an account number.
    assert parsed[6].asset_description == "Qwest Corporation 6.125% Notes due 2053"


# ── The segmenter on its own ────────────────────────────────────────────────


def test_split_row_segment_owner_line_after_a_full_width_line() -> None:
    segment = "\n".join([
        "FIlINg sTaTus: New",
        "DEsCRIPTIoN: Delaware statutory Trust in student lofts at st. louis university. sold by Inland Realty.",
        "sP",
        "ishares Core MsCI Emerging Markets",
        "ETF",
    ])
    annotation, asset = split_row_segment(segment)
    assert asset == ["sP", "ishares Core MsCI Emerging Markets", "ETF"]
    assert annotation[-1].startswith("DEsCRIPTIoN:")


def test_split_row_segment_owner_line_with_the_clerk_id() -> None:
    annotation, asset = split_row_segment("FIlINg STATuS: Amended\n2000090459 sP\nAllianceBernstein Holding l.P.\nunits")
    assert annotation == ["FIlINg STATuS: Amended"]
    assert asset == ["2000090459 sP", "AllianceBernstein Holding l.P.", "units"]


def test_split_row_segment_last_name_line_is_never_an_account_number() -> None:
    annotation, asset = split_row_segment("FIlING sTaTus: New\nQwest Corporation 6.125% Notes due\n2053")
    assert annotation == ["FIlING sTaTus: New"]
    assert asset == ["Qwest Corporation 6.125% Notes due", "2053"]


def test_split_row_segment_still_files_a_mid_segment_account_number_as_annotation() -> None:
    annotation, asset = split_row_segment("Filing Status: New\nSubholding Of: CETERA\n2000152177\nCVS Health Corporation Common\nStock")
    assert annotation == ["Filing Status: New", "Subholding Of: CETERA", "2000152177"]
    assert asset == ["CVS Health Corporation Common", "Stock"]
