"""Unit and fixture tests for the House PTR text parser."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from capitol_pipeline.models.congress import FilingStub, MemberMatch
from capitol_pipeline.parsers.house_ptr import (
    AMOUNT_RANGES,
    ROW_CORE_PATTERN,
    TRANSACTION_PATTERN,
    format_annotation,
    parse_amount_range,
    parse_house_ptr_text,
    parse_owner,
    parse_transactions,
    split_row_segment,
    strip_page_furniture,
)


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "house_ptr"

#: Text that must never survive into an asset description: comment fragments,
#: per-row annotation labels, and column headings from the House form.
LEAKED_TEXT = re.compile(
    r"influence the financial|assets of the portfolio|Description:|Comments:|Filing Status"
    r"|Subholding|Cap\. Gains|\$200\?|Notification Date|Filing ID|Periodic Transaction",
    re.I,
)


def load_fixture(name: str) -> str:
    """Return a text layer exactly as ``PyMuPDFBackend.extract`` produced it."""

    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


def build_stub(doc_id: str, name: str, state: str, filing_date: str) -> FilingStub:
    return FilingStub(
        doc_id=doc_id,
        filing_year=int(filing_date[:4]),
        filing_date=filing_date,
        member=MemberMatch(
            id=f"m-{doc_id}",
            name=name,
            slug=name.lower().replace(" ", "-"),
            party="D",
            state=state,
        ),
        source="house-clerk",
        source_url=f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{filing_date[:4]}/{doc_id}.pdf",
    )


def test_parse_amount_range_variants() -> None:
    cases: list[tuple[str, tuple[int, int]]] = [
        ("$1,001 - $15,000", (1001, 15000)),
        ("$1,001-$15,000", (1001, 15000)),
        ("$1,001–$15,000", (1001, 15000)),
        ("$1,001—$15,000", (1001, 15000)),
        ("$1,000 - $15,000", (1000, 15000)),
        ("$1,000-$15,000", (1000, 15000)),
        ("$15,001 - $50,000", (15001, 50000)),
        ("$50,001 - $100,000", (50001, 100000)),
        ("$100,001 - $250,000", (100001, 250000)),
        ("$250,001 - $500,000", (250001, 500000)),
        ("$500,001 - $1,000,000", (500001, 1000000)),
        ("$1,000,001 - $5,000,000", (1000001, 5000000)),
        ("$5,000,001 - $25,000,000", (5000001, 25000000)),
        ("$25,000,001 - $50,000,000", (25000001, 50000000)),
        ("Over $50,000,000", (50000001, 100000000)),
        ("over $50,000,000", (50000001, 100000000)),
        ("> $50,000,000", (50000001, 100000000)),
        ("$1001-$15000", (1001, 15000)),
        ("$647.63", (648, 648)),
        ("$25,000", (25000, 25000)),
        ("no amount here", (0, 0)),
    ]
    for raw, expected in cases:
        assert parse_amount_range(raw) == expected, f"failed: {raw!r}"


def test_amount_ranges_has_expected_length() -> None:
    assert len(AMOUNT_RANGES) >= 11


def test_transaction_pattern_handles_minimal_row() -> None:
    row = "Acme Holdings (ACME) [ST] P 01/02/2026 01/05/2026 $1,001 - $15,000"
    match = TRANSACTION_PATTERN.search(row)
    assert match is not None
    assert match.group(2) == "ACME"
    assert match.group(3) == "ST"
    assert match.group(4).upper() == "P"
    assert match.group(5) == "01/02/2026"
    assert match.group(6) == "01/05/2026"
    assert match.group(7) is None
    assert "$1,001" in match.group(8)


def test_transaction_pattern_handles_optional_owner_code() -> None:
    row = "Foo Bar Inc Common Stock (FOO) [ST] P 03/04/2026 03/06/2026 JT $1,001 - $15,000"
    match = TRANSACTION_PATTERN.search(row)
    assert match is not None
    assert match.group(7) == "JT"


def test_transaction_pattern_handles_missing_asset_bracket() -> None:
    row = "Widget Industries Common Stock (WDG) P 04/05/2026 04/07/2026 $15,001 - $50,000"
    match = TRANSACTION_PATTERN.search(row)
    assert match is not None
    assert match.group(2) == "WDG"
    assert match.group(3) is None
    assert match.group(4).upper() == "P"


def test_transaction_pattern_handles_em_dash_amount() -> None:
    row = "Zeta Corp (ZET) [ST] S 05/06/2026 05/09/2026 $1,001–$15,000"
    match = TRANSACTION_PATTERN.search(row)
    assert match is not None
    assert "–" in match.group(8) or "-" in match.group(8)


def test_transaction_pattern_handles_over_amount() -> None:
    row = "Mega Trust (MEG) [MF] P 06/07/2026 06/08/2026 Over $50,000,000"
    match = TRANSACTION_PATTERN.search(row)
    assert match is not None
    assert match.group(8).lower().startswith("over")


def test_transaction_pattern_allows_newlines_between_fields() -> None:
    row = "Acme Holdings (ACME) [ST] P\n01/02/2026\n01/05/2026\n$1,001 - $15,000"
    match = TRANSACTION_PATTERN.search(row)
    assert match is not None
    assert match.group(5) == "01/02/2026"
    assert match.group(6) == "01/05/2026"


def test_row_core_requires_a_standalone_type_code() -> None:
    # "Corp" must not be read as "Cor" + purchase code "p".
    row = "Acme Corp 01/02/2026 01/05/2026 $1,001 - $15,000"
    assert ROW_CORE_PATTERN.search(row) is None
    row = "Acme Corp P 01/02/2026 01/05/2026 $1,001 - $15,000"
    match = ROW_CORE_PATTERN.search(row)
    assert match is not None
    assert match.group("tx_type") == "P"


def test_parse_owner_only_honours_a_leading_code() -> None:
    assert parse_owner("JT Floyd County IN 4.00% 12/30/2031") == "joint"
    assert parse_owner("DC Accenture plc Class A Ordinary Shares") == "child"
    assert parse_owner("SP Apple Inc.") == "spouse"
    assert parse_owner("Washington DC Water Bonds") == "self"
    assert parse_owner("Apple Inc.", "Spouse/DC") == "spouse"


def test_split_row_segment_separates_wrapped_comment_from_asset() -> None:
    segment = "\n".join(
        [
            "Filing Status: New",
            "Comments: This investment is held within a publicly available, widely held independently managed portfolio over which I",
            "have no authority to exercise control over or influence the financial interests held by the portfolio. The assets of the portfolio",
            "are widely diversified, and all investment decisions are made solely by the independent manager, without my input or",
            "direction.",
            "Alexandria Real Estate Equities, Inc.",
            "Common Stock",
        ]
    )
    annotation, asset = split_row_segment(segment)
    assert asset == ["Alexandria Real Estate Equities, Inc.", "Common Stock"]
    assert annotation[0] == "Filing Status: New"
    assert annotation[-1] == "direction."


def test_split_row_segment_keeps_lowercase_brand_names_and_owner_codes() -> None:
    annotation, asset = split_row_segment("Filing Status: New\nSubholding Of: Fidelity\nDC\niShares Core S&P 500 ETF")
    assert annotation == ["Filing Status: New", "Subholding Of: Fidelity"]
    assert asset == ["DC", "iShares Core S&P 500 ETF"]


def test_split_row_segment_treats_account_number_as_annotation() -> None:
    annotation, asset = split_row_segment("Filing Status: New\nSubholding Of: CETERA\n2000152177\nCVS Health Corporation Common\nStock")
    assert annotation == ["Filing Status: New", "Subholding Of: CETERA", "2000152177"]
    assert asset == ["CVS Health Corporation Common", "Stock"]


def test_format_annotation_expands_legacy_shorthand() -> None:
    assert format_annotation(["F S: New S O: Fidelity Trust"]) == "Filing Status: New | Subholding Of: Fidelity Trust"
    assert format_annotation(["Filing Status: New", "Comments: first line", "second line"]) == (
        "Filing Status: New | Comments: first line second line"
    )
    assert format_annotation([]) is None


def test_strip_page_furniture_removes_every_page_heading_and_footer() -> None:
    stripped = strip_page_furniture(load_fixture("20030930.txt"))
    assert "Cap." not in stripped
    assert "Notification" not in stripped
    assert "Filing ID" not in stripped
    assert "Periodic Transaction Report" not in stripped
    assert "I CERTIFY" not in stripped
    assert "For the complete list of asset type" not in stripped


# ---------------------------------------------------------------------------
# Real filings (text layers captured from the Clerk's PDFs with pymupdf)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def torres() -> tuple:
    """Doc 20030930: 28 pages, every row carries a 330-character comment."""

    return parse_house_ptr_text(
        load_fixture("20030930.txt"),
        build_stub("20030930", "Ritchie Torres", "NY", "2025-08-20"),
    )


def test_torres_long_comments_never_leak_into_asset_names(torres) -> None:
    parsed, trades = torres
    assert parsed.member_name == "Ritchie John Torres"
    assert parsed.state == "NY"
    # 156 rows on the form; one is an exact duplicate (ASML purchase) that dedupe drops.
    assert len(parse_transactions(load_fixture("20030930.txt"))) == 156
    assert len(parsed.transactions) == 155
    assert len(trades) == 155
    leaked = [t.asset_description for t in parsed.transactions if LEAKED_TEXT.search(t.asset_description)]
    assert leaked == []
    assert all(t.amount_min == 1001 and t.amount_max == 15000 for t in parsed.transactions)
    # Two rows (ICE and LEN sales) carry 07/11/2025 as their notification
    # date on the form itself; every other row says 08/01/2025.
    odd_notification = sorted(t.ticker for t in parsed.transactions if t.notification_date != "2025-08-01")
    assert odd_notification == ["ICE", "LEN"]
    assert all(t.owner == "self" for t in parsed.transactions)
    # Only the Treasury notes have no ticker on the form.
    without_ticker = [t for t in parsed.transactions if not t.ticker]
    assert len(without_ticker) == 30
    assert all(t.asset_description.startswith("US Treasury Note ") for t in without_ticker)
    assert all(t.asset_type == "Government Security" for t in without_ticker)


def test_torres_first_rows_and_comment(torres) -> None:
    parsed, trades = torres
    first, second, third = parsed.transactions[:3]
    assert (first.ticker, first.asset_description, first.transaction_type, first.transaction_date) == (
        "ABBV", "AbbVie Inc. Common Stock", "purchase", "2024-09-26"
    )
    assert (second.ticker, second.transaction_type, second.transaction_date) == ("ABBV", "sale", "2025-07-11")
    assert third.asset_description == "Alexandria Real Estate Equities, Inc. Common Stock"
    assert first.comment is not None
    assert first.comment.startswith("Filing Status: New | Comments: This investment is held within a publicly available")
    assert first.comment.endswith("without my input or direction.")
    assert trades[0].comment is not None
    assert trades[0].comment.startswith(first.comment)
    assert trades[0].comment.endswith("[regex-v1]")


def test_torres_row_across_page_break(torres) -> None:
    parsed, _ = torres
    # Page 2 opens with the column heading, then "direction." (the tail of the
    # page-1 comment), then this row. Neither may end up in the asset name.
    row = parsed.transactions[4]
    assert row.ticker == "GOOGL"
    assert row.asset_description == "Alphabet Inc. - Class A Common Stock"
    assert (row.transaction_type, row.transaction_date) == ("sale", "2025-07-11")
    # And the previous row's comment still gets its wrapped last word.
    assert parsed.transactions[3].comment is not None
    assert parsed.transactions[3].comment.endswith("without my input or direction.")


def test_torres_last_rows_ignore_trailing_sections(torres) -> None:
    parsed, _ = torres
    last = parsed.transactions[-1]
    assert (last.ticker, last.asset_description, last.transaction_type, last.transaction_date) == (
        "WMB", "Williams Companies, Inc.", "purchase", "2024-09-26"
    )
    assert last.comment is not None
    assert last.comment.endswith("without my input or direction.")
    treasury = next(t for t in parsed.transactions if t.asset_description == "US Treasury Note 9128284V9")
    assert treasury.ticker is None
    assert treasury.transaction_type == "purchase"


def test_morrison_descriptions_go_to_comment_not_asset() -> None:
    """Doc 20034300: joint rows with a Description line under each one."""

    parsed, trades = parse_house_ptr_text(
        load_fixture("20034300.txt"),
        build_stub("20034300", "Kelly Morrison", "MN", "2026-05-05"),
    )
    assert parsed.member_name == "Kelly Louise Morrison"
    assert [t.asset_description for t in parsed.transactions] == [
        "Artistry in Motion, LLC",
        "Cavall",
        "Hermeus Corp",
        "Pelvital USA, Inc.",
        "Saronic Technologies",
        "Star Catcher Industries, Inc.",
        "Star Catcher Industries, Inc.",
    ]
    assert all(t.owner == "joint" for t in parsed.transactions)
    assert all(t.ticker is None for t in parsed.transactions)
    first = parsed.transactions[0]
    assert (first.transaction_type, first.transaction_date, first.notification_date) == ("purchase", "2026-04-15", "2026-04-15")
    assert (first.amount_min, first.amount_max) == (50001, 100000)
    assert first.comment == (
        "Filing Status: New | Subholding Of: Investment Fund 1 | Description: Live events special effects, Northridge, CA"
    )
    # First row on page 2, right after the repeated column heading.
    page_break = parsed.transactions[5]
    assert (page_break.transaction_type, page_break.transaction_date, page_break.amount_max) == ("sale", "2026-05-04", 50000)
    assert page_break.comment is not None
    assert page_break.comment.endswith("Description: Energy infrastructure in space, Jacksonville, FL")
    assert trades[1].comment == (
        "Filing Status: New | Subholding Of: Investment Fund 1 | Description: Sports Software, London, UK"
        " | Parsed from House PTR 20034300 at 95% confidence [regex-v1]"
    )


def test_moskowitz_page_split_row_and_sub_account_duplicates() -> None:
    """Doc 20034274: owner codes on their own line, a row whose asset name
    wraps onto the next page after the amount, and identical trades in
    different sub-accounts."""

    parsed, _ = parse_house_ptr_text(
        load_fixture("20034274.txt"),
        build_stub("20034274", "Jared Moskowitz", "FL", "2026-03-31"),
    )
    assert len(parsed.transactions) == 50
    assert all(t.ticker for t in parsed.transactions)
    assert not any(LEAKED_TEXT.search(t.asset_description) for t in parsed.transactions)
    # One "DC" owner line per dependent-child row in the text layer.
    child_rows = [t for t in parsed.transactions if t.owner == "child"]
    assert len(child_rows) == load_fixture("20034274.txt").count("\nDC\n") == 20
    assert child_rows[0].asset_description == "Accenture plc Class A Ordinary Shares"
    # pymupdf emits "Motorola Solutions, Inc. Common / P / dates / amount /
    # <page heading> / Stock (MSI) [ST]"; the tail must rejoin the row.
    split = [t for t in parsed.transactions if t.ticker == "MSI" and t.transaction_date == "2026-03-31"]
    assert len(split) == 1
    assert split[0].asset_description == "Motorola Solutions, Inc. Common Stock"
    assert split[0].asset_type == "Stock"
    assert split[0].comment == "Filing Status: New | Subholding Of: Morgan Stanley Active Assets (1)"
    accenture = [t for t in parsed.transactions if t.ticker == "ACN"]
    assert [t.comment.rsplit(" ", 1)[-1] for t in accenture if t.comment] == ["(5)", "(6)", "(1)"]


def test_miller_exact_amount_and_multiline_asset_name() -> None:
    """Doc 20030742: capital call with an exact dollar figure and a
    parenthesised fund name that wraps over two lines."""

    parsed, _ = parse_house_ptr_text(
        load_fixture("20030742.txt"),
        build_stub("20030742", "Max Miller", "OH", "2025-07-27"),
    )
    assert len(parsed.transactions) == 2
    first, second = parsed.transactions
    assert first.asset_description == "AIX Ventures Fund II, LP (GLAS Funds, LP)"
    assert first.ticker is None
    assert (first.amount_min, first.amount_max) == (648, 648)
    assert first.comment == (
        "Filing Status: New | Subholding Of: 2012 Trust"
        " | Description: Capital Call for AIX Ventures Fund II, LP (GLAS Funds, LP)"
    )
    assert second.asset_description == "GLAS Funds, LP"
    assert (second.amount_min, second.amount_max) == (15001, 50000)
    assert second.comment is not None
    assert second.comment.endswith("to fund capital calls for future investments.")
