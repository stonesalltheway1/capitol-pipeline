"""Pure-regex unit tests for House PTR parser internals."""

from __future__ import annotations

from pathlib import Path

import pytest

from capitol_pipeline.parsers.house_ptr import (
    AMOUNT_RANGES,
    TRANSACTION_PATTERN,
    parse_amount_range,
)


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "house_ptr"


def test_parse_amount_range_variants() -> None:
    cases: list[tuple[str, tuple[int, int]]] = [
        ("$1,001 - $15,000", (1001, 15000)),
        ("$1,001-$15,000", (1001, 15000)),
        ("$1,001\u2013$15,000", (1001, 15000)),
        ("$1,001\u2014$15,000", (1001, 15000)),
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
    row = "Zeta Corp (ZET) [ST] S 05/06/2026 05/09/2026 $1,001\u2013$15,000"
    match = TRANSACTION_PATTERN.search(row)
    assert match is not None
    assert "\u2013" in match.group(8) or "-" in match.group(8)


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


@pytest.mark.skipif(
    not FIXTURES_DIR.exists(),
    reason="TODO: add real House PTR PDF text fixtures under tests/fixtures/house_ptr",
)
def test_parse_real_fixture_text() -> None:
    # TODO: once fixtures exist, load a *.txt extracted from a real PTR PDF
    # and assert the full pipeline produces the expected transaction count.
    pass
