"""Regression tests for the official Senate eFD scraper and normalizer.

Fixtures under ``tests/fixtures/senate_efd`` mirror the real markup returned by
https://efdsearch.senate.gov (endpoint shapes verified 2026-09-02).
"""

from __future__ import annotations

from datetime import date
import json
from pathlib import Path

import pytest

from capitol_pipeline.bridges.capitol_exposed import (
    build_canonical_senate_trade_id,
    build_trade_id,
    build_trade_payload,
)
from capitol_pipeline.registries.members import MemberRegistry
from capitol_pipeline.sources.senate_efd import (
    EfdReport,
    EfdTransaction,
    build_efd_submitted_since,
    build_paper_report_stub,
    clean_efd_ticker,
    format_efd_date,
    list_paper_page_images,
    normalize_efd_asset_type,
    normalize_efd_owner,
    normalize_efd_transaction,
    parse_efd_amount_range,
    parse_efd_search_rows,
    parse_electronic_ptr_html,
)
from capitol_pipeline.sources.senate_ethics import (
    QuiverCongressTrade,
    normalize_quiver_senate_trade,
    normalize_senate_transaction_type,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "senate_efd"
BASE_URL = "https://efdsearch.senate.gov/"


def load_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


@pytest.fixture()
def search_payload() -> dict[str, object]:
    return json.loads(load_fixture("search_ptr_page.json"))


@pytest.fixture()
def electronic_transactions() -> list[EfdTransaction]:
    return parse_electronic_ptr_html(load_fixture("ptr_electronic.html"))


@pytest.fixture()
def senate_registry() -> MemberRegistry:
    return MemberRegistry.from_rows(
        [
            {
                "id": "m-W000802",
                "bioguide_id": "W000802",
                "name": "Sheldon Whitehouse",
                "slug": "sheldon-whitehouse",
                "party": "D",
                "state": "RI",
            },
            {
                "id": "m-B001277",
                "bioguide_id": "B001277",
                "name": "Richard Blumenthal",
                "slug": "richard-blumenthal",
                "party": "D",
                "state": "CT",
            },
        ]
    )


@pytest.fixture()
def whitehouse_report() -> EfdReport:
    return EfdReport(
        report_id="f8d003c0-ca1e-4c39-9d66-632d220180e1",
        kind="electronic",
        url=f"{BASE_URL}search/view/ptr/f8d003c0-ca1e-4c39-9d66-632d220180e1/",
        first_name="Sheldon",
        last_name="Whitehouse",
        office="Whitehouse, Sheldon (Senator)",
        title="Periodic Transaction Report for 09/02/2026",
        submitted_date="2026-09-02",
    )


# ---------------------------------------------------------------------------
# Search JSON parsing
# ---------------------------------------------------------------------------


def test_parse_efd_search_rows_splits_electronic_and_paper(search_payload) -> None:
    reports = parse_efd_search_rows(search_payload, base_url=BASE_URL)

    assert len(reports) == 6
    first = reports[0]
    assert first.report_id == "f8d003c0-ca1e-4c39-9d66-632d220180e1"
    assert first.kind == "electronic"
    assert first.url == (
        "https://efdsearch.senate.gov/search/view/ptr/"
        "f8d003c0-ca1e-4c39-9d66-632d220180e1/"
    )
    assert first.submitted_date == "2026-09-02"
    assert first.senator_name == "Sheldon Whitehouse"
    assert first.is_amendment is False

    paper = reports[1]
    assert paper.kind == "paper"
    assert paper.report_id == "929216d5-5dbd-429c-858c-1e9332924627"
    # The search feed pads and upper-cases some filer names.
    assert paper.senator_name == "RICHARD BLUMENTHAL"

    amendment = next(report for report in reports if report.last_name == "Boozman")
    assert amendment.is_amendment is True
    # The date column is the submission date, not the date named in the title.
    assert amendment.submitted_date == "2026-08-24"


def test_parse_efd_search_rows_rejects_a_payload_without_data() -> None:
    from capitol_pipeline.sources.senate_efd import SenateEfdError

    with pytest.raises(SenateEfdError):
        parse_efd_search_rows({"recordsTotal": 0}, base_url=BASE_URL)


# ---------------------------------------------------------------------------
# Electronic PTR table parsing
# ---------------------------------------------------------------------------


def test_parse_electronic_ptr_html_reads_every_transaction(electronic_transactions) -> None:
    assert len(electronic_transactions) == 7
    assert [row.line_number for row in electronic_transactions] == [7, 6, 5, 4, 3, 2, 1]


def test_parse_electronic_ptr_html_handles_missing_ticker(electronic_transactions) -> None:
    row = electronic_transactions[0]

    assert row.ticker is None  # the cell renders a literal "--"
    assert row.asset_description == "SDZNY- Sandoz Group AG ADR"
    assert row.transaction_type_raw == "Sale (Full)"
    assert row.transaction_date == "2026-08-06"
    assert row.owner_raw == "Self"
    assert row.comment is None  # a "--" comment is not a comment


def test_parse_electronic_ptr_html_handles_partial_sale_and_spouse(electronic_transactions) -> None:
    partial_self = electronic_transactions[1]
    assert partial_self.ticker == "NVDA"
    assert partial_self.transaction_type_raw == "Sale (Partial)"
    assert partial_self.owner_raw == "Self"

    partial_spouse = electronic_transactions[2]
    assert partial_spouse.ticker == "NVDA"
    assert partial_spouse.owner_raw == "Spouse"
    assert partial_spouse.amount_raw == "$1,001 - $15,000"


def test_parse_electronic_ptr_html_keeps_real_comments(electronic_transactions) -> None:
    row = electronic_transactions[3]

    assert row.ticker == "MSFT"
    assert row.owner_raw == "Joint"
    assert row.amount_raw == "$1,000,001 - $5,000,000"
    assert row.comment == "Purchased by an outside manager with no input from the filer."


def test_parse_electronic_ptr_html_handles_exchange_rows(electronic_transactions) -> None:
    row = electronic_transactions[4]

    assert row.transaction_type_raw == "Exchange"
    # The ticker cell carries "--" for the exchanged asset and the received
    # asset's symbol on a second line; the real symbol wins.
    assert row.ticker == "AMCR"
    assert row.asset_description == (
        "BERY - Berry Global Group, Inc. (Exchanged) / Amcor plc Ordinary Shares (Received)"
    )


def test_parse_electronic_ptr_html_splits_muted_asset_detail(electronic_transactions) -> None:
    row = electronic_transactions[5]

    assert row.asset_description == "PENNSYLVANIA ST TPK COMMN OIL REV"
    assert row.asset_detail == "Rate/Coupon: 5% Matures: 2026-12-01"
    assert row.asset_type_raw == "Municipal Security"
    assert row.amount_raw == "Over $50,000,000"


def test_parse_electronic_ptr_html_returns_nothing_without_a_table() -> None:
    assert parse_electronic_ptr_html("<html><body><p>No filings</p></body></html>") == []


def test_list_paper_page_images_returns_scan_urls() -> None:
    images = list_paper_page_images(load_fixture("ptr_paper.html"), base_url=BASE_URL)

    assert images == [
        "https://efd-media-public.senate.gov/media/2026/2/000/000/000000521.gif",
        "https://efd-media-public.senate.gov/media/2026/2/000/000/000000522.gif",
        "https://efd-media-public.senate.gov/media/2026/2/000/000/000000523.gif",
    ]
    # The site logo is an <img> too, and must not be mistaken for a filing page.
    assert not any(image.endswith("logo.svg") for image in images)


def test_build_paper_report_stub_marks_the_filing_for_review() -> None:
    report = EfdReport(
        report_id="929216d5-5dbd-429c-858c-1e9332924627",
        kind="paper",
        url=f"{BASE_URL}search/view/paper/929216d5-5dbd-429c-858c-1e9332924627/",
        first_name="RICHARD ",
        last_name="BLUMENTHAL",
        office="Senator",
        submitted_date="2026-08-31",
    )

    stub = build_paper_report_stub(report, ["https://example.test/page-1.gif"])

    assert stub["status"] == "needs_review"
    assert stub["reason"] == "paper_filing_requires_ocr"
    assert stub["pageImages"] == ["https://example.test/page-1.gif"]


# ---------------------------------------------------------------------------
# Value parsers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("$1,001 - $15,000", (1001, 15000)),
        ("$15,001 - $50,000", (15001, 50000)),
        ("$100,001 - $250,000", (100001, 250000)),
        ("$1,000,001 - $5,000,000", (1000001, 5000000)),
        ("Over $1,000,000", (1000000, 1000000)),
        ("Over $50,000,000", (50000000, 50000000)),
        ("--", (0, 0)),
        ("", (0, 0)),
        (None, (0, 0)),
    ],
)
def test_parse_efd_amount_range(raw, expected) -> None:
    assert parse_efd_amount_range(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Purchase", "purchase"),
        ("Sale (Full)", "sale"),
        ("Sale (Partial)", "sale"),
        ("sale (partial)", "sale"),
        ("Exchange", "exchange"),
    ],
)
def test_efd_transaction_type_mapping(raw, expected) -> None:
    assert normalize_senate_transaction_type(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Self", "self"),
        ("Spouse", "spouse"),
        ("Joint", "joint"),
        ("Dependent Child", "child"),
        ("", "self"),
    ],
)
def test_normalize_efd_owner(raw, expected) -> None:
    assert normalize_efd_owner(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("--", None),
        ("", None),
        (None, None),
        ("NVDA", "NVDA"),
        (["--", "AMCR"], "AMCR"),
        (["--"], None),
        (["BRK.B"], "BRK.B"),
    ],
)
def test_clean_efd_ticker(raw, expected) -> None:
    assert clean_efd_ticker(raw) == expected


def test_normalize_efd_asset_type_maps_site_vocabulary() -> None:
    assert normalize_efd_asset_type("Municipal Security") == "Bond"
    assert normalize_efd_asset_type("Corporate Bond") == "Bond"
    assert normalize_efd_asset_type("Stock") == "Stock"
    assert normalize_efd_asset_type("Stock Option") == "Option"
    assert normalize_efd_asset_type("Other Securities") == "Other"
    # A crypto classification always wins over the filing's own label.
    assert normalize_efd_asset_type("Other Securities", "direct_crypto") == "Cryptocurrency"


def test_format_efd_date_matches_the_search_form() -> None:
    assert format_efd_date(date(2026, 1, 5)) == "01/05/2026"


# ---------------------------------------------------------------------------
# Ingest window
# ---------------------------------------------------------------------------


def test_build_efd_submitted_since_backs_off_from_the_latest_known_disclosure() -> None:
    assert build_efd_submitted_since(
        "2026-08-30",
        lookback_days=14,
        floor_days=60,
        today=date(2026, 9, 2),
    ) == date(2026, 8, 16)


def test_build_efd_submitted_since_is_floored_at_sixty_days() -> None:
    assert build_efd_submitted_since(
        "2026-01-04",
        lookback_days=14,
        floor_days=60,
        today=date(2026, 9, 2),
    ) == date(2026, 7, 4)


def test_build_efd_submitted_since_without_history_uses_the_floor() -> None:
    assert build_efd_submitted_since(
        None,
        floor_days=60,
        today=date(2026, 9, 2),
    ) == date(2026, 7, 4)


# ---------------------------------------------------------------------------
# Normalization into site-ready trade rows
# ---------------------------------------------------------------------------


def test_normalize_efd_transaction_builds_a_site_ready_row(
    electronic_transactions, senate_registry, whitehouse_report
) -> None:
    row = normalize_efd_transaction(
        whitehouse_report, electronic_transactions[1], senate_registry
    )

    assert row is not None
    assert row.member.id == "m-W000802"
    assert row.source == "senate-efd"
    assert row.ticker == "NVDA"
    assert row.transaction_type == "sale"
    assert row.transaction_date == "2026-08-13"
    assert row.disclosure_date == "2026-09-02"  # the report's submitted date
    assert row.amount_min == 15001
    assert row.amount_max == 50000
    assert row.owner == "self"
    assert row.source_url == whitehouse_report.url
    assert row.asset_type == "Stock"

    payload = build_trade_payload(row)
    assert payload["source"] == "senate_efd"
    assert payload["id"] == build_trade_id(row)
    assert str(payload["id"]).startswith("tr-senate-")


def test_normalize_efd_transaction_carries_owner_and_asset_detail(
    electronic_transactions, senate_registry, whitehouse_report
) -> None:
    spouse_row = normalize_efd_transaction(
        whitehouse_report, electronic_transactions[2], senate_registry
    )
    assert spouse_row is not None
    assert spouse_row.owner == "spouse"

    municipal_row = normalize_efd_transaction(
        whitehouse_report, electronic_transactions[5], senate_registry
    )
    assert municipal_row is not None
    assert municipal_row.ticker is None
    assert municipal_row.asset_type == "Bond"
    assert municipal_row.comment == "Rate/Coupon: 5% Matures: 2026-12-01"
    assert municipal_row.amount_min == 50000000

    child_row = normalize_efd_transaction(
        whitehouse_report, electronic_transactions[6], senate_registry
    )
    assert child_row is not None
    assert child_row.owner == "child"
    assert child_row.normalized_asset is not None
    assert child_row.normalized_asset.kind == "direct_crypto"
    assert child_row.asset_type == "Cryptocurrency"


def test_normalize_efd_transaction_skips_unresolvable_filers(
    electronic_transactions, senate_registry
) -> None:
    unknown = EfdReport(
        report_id="00000000-0000-0000-0000-000000000000",
        kind="electronic",
        url=f"{BASE_URL}search/view/ptr/00000000-0000-0000-0000-000000000000/",
        first_name="Nobody",
        last_name="Atall",
        submitted_date="2026-09-02",
    )

    assert normalize_efd_transaction(unknown, electronic_transactions[1], senate_registry) is None


def test_normalize_efd_transaction_skips_rows_without_a_transaction_date(
    senate_registry, whitehouse_report
) -> None:
    row = EfdTransaction(
        line_number=1,
        transaction_date=None,
        owner_raw="Self",
        ticker="NVDA",
        asset_description="NVIDIA Corporation - Common Stock",
        asset_type_raw="Stock",
        transaction_type_raw="Purchase",
        amount_raw="$1,001 - $15,000",
    )

    assert normalize_efd_transaction(whitehouse_report, row, senate_registry) is None


# ---------------------------------------------------------------------------
# Canonical trade id
# ---------------------------------------------------------------------------


def test_canonical_id_matches_across_providers(
    electronic_transactions, senate_registry, whitehouse_report
) -> None:
    """The same trade seen through eFD and Quiver must collapse to one id."""

    efd_row = normalize_efd_transaction(
        whitehouse_report, electronic_transactions[1], senate_registry
    )
    quiver_row = normalize_quiver_senate_trade(
        QuiverCongressTrade(
            Name="Sheldon Whitehouse",
            BioGuideID="W000802",
            Filed="2026-09-02",
            Traded="2026-08-13",
            Ticker="NVDA",
            Transaction="Sale (Partial)",
            Trade_Size_USD="$15,001 - $50,000",
            Chamber="Senate",
            # Quiver words the asset differently and carries no source URL,
            # which is exactly what used to fork the id.
            Company="NVIDIA Corp",
            Description="NVIDIA Corp",
        ),
        senate_registry,
    )

    assert efd_row is not None and quiver_row is not None
    assert efd_row.asset_description != quiver_row.asset_description
    assert efd_row.source_url != quiver_row.source_url
    assert efd_row.source_id == quiver_row.source_id
    assert build_trade_payload(efd_row)["id"] == build_trade_payload(quiver_row)["id"]


def test_canonical_id_ignores_asset_description_when_a_ticker_exists(
    electronic_transactions, senate_registry, whitehouse_report
) -> None:
    row = normalize_efd_transaction(
        whitehouse_report, electronic_transactions[1], senate_registry
    )
    assert row is not None
    original = build_canonical_senate_trade_id(row)

    row.asset_description = "NVIDIA Corporation (renamed upstream)"
    row.source_url = "https://efdsearch.senate.gov/search/view/ptr/other/"
    assert build_canonical_senate_trade_id(row) == original


def test_canonical_id_falls_back_to_the_asset_description_without_a_ticker(
    electronic_transactions, senate_registry, whitehouse_report
) -> None:
    row = normalize_efd_transaction(
        whitehouse_report, electronic_transactions[5], senate_registry
    )
    assert row is not None
    assert row.ticker is None
    original = build_canonical_senate_trade_id(row)

    # Whitespace and case must not fork the id...
    row.asset_description = "  pennsylvania  ST TPK Commn OIL rev  "
    assert build_canonical_senate_trade_id(row) == original

    # ...but a genuinely different holding must.
    row.asset_description = "CALIFORNIA ST GO BONDS"
    assert build_canonical_senate_trade_id(row) != original


def test_client_accepts_the_agreement_before_searching() -> None:
    """The agreement/CSRF handshake runs once and both endpoints see the token."""

    import httpx

    from capitol_pipeline.config import Settings
    from capitol_pipeline.sources.senate_efd import SenateEfdClient, list_ptr_reports

    calls: list[tuple[str, str]] = []
    agreed = {"value": False}
    agreement_html = (
        '<form id="agreement_form"><input name="prohibition_agreement" />'
        '<input type="hidden" name="csrfmiddlewaretoken" value="test-token" /></form>'
    )

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        calls.append((request.method, path))

        if request.method == "GET" and path == "/search/home/":
            return httpx.Response(
                200,
                html=agreement_html,
                headers={"set-cookie": "csrftoken=test-token; Path=/"},
            )
        if request.method == "POST" and path == "/search/home/":
            assert request.headers["X-CSRFToken"] == "test-token"
            assert request.headers["Referer"].endswith("/search/home/")
            assert b"prohibition_agreement=1" in request.content
            agreed["value"] = True
            return httpx.Response(200, html="<html><body>Search</body></html>")
        if request.method == "POST" and path == "/search/report/data/":
            if not agreed["value"]:
                return httpx.Response(403, text="Forbidden")
            assert b"report_types=%5B11%5D" in request.content
            assert b"filer_types=%5B1%5D" in request.content
            assert b"submitted_start_date=07%2F04%2F2026+00%3A00%3A00" in request.content
            return httpx.Response(200, json=json.loads(load_fixture("search_ptr_page.json")))
        if path.startswith("/search/view/ptr/"):
            if not agreed["value"]:
                return httpx.Response(200, html=agreement_html)
            return httpx.Response(200, html=load_fixture("ptr_electronic.html"))
        raise AssertionError(f"unexpected request: {request.method} {path}")

    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport, base_url=BASE_URL, follow_redirects=True)
    settings = Settings()

    with SenateEfdClient(
        settings,
        client=http_client,
        min_interval_seconds=0.0,
        sleep=lambda _seconds: None,
    ) as client:
        reports = list_ptr_reports(settings, date(2026, 7, 4), None, 6, client=client)
        assert len(reports) == 6

        from capitol_pipeline.sources.senate_efd import fetch_electronic_ptr

        transactions = fetch_electronic_ptr(client, reports[0])
        assert len(transactions) == 7

        # The handshake is not repeated for every request.
        assert calls.count(("POST", "/search/home/")) == 1

    http_client.close()


def test_client_reaccepts_the_agreement_when_a_report_bounces() -> None:
    """A dropped session sends the report view back to the agreement page."""

    import httpx

    from capitol_pipeline.config import Settings
    from capitol_pipeline.sources.senate_efd import SenateEfdClient

    agreement_html = (
        '<form id="agreement_form"><input name="prohibition_agreement" />'
        '<input type="hidden" name="csrfmiddlewaretoken" value="test-token" /></form>'
    )
    state = {"report_hits": 0, "agreements": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "GET" and path == "/search/home/":
            return httpx.Response(
                200,
                html=agreement_html,
                headers={"set-cookie": "csrftoken=test-token; Path=/"},
            )
        if request.method == "POST" and path == "/search/home/":
            state["agreements"] += 1
            return httpx.Response(200, html="<html><body>Search</body></html>")
        if path.startswith("/search/view/ptr/"):
            state["report_hits"] += 1
            if state["report_hits"] == 1:
                # Session expired: the site serves the agreement form instead.
                return httpx.Response(200, html=agreement_html)
            return httpx.Response(200, html=load_fixture("ptr_electronic.html"))
        raise AssertionError(f"unexpected request: {request.method} {path}")

    http_client = httpx.Client(
        transport=httpx.MockTransport(handler), base_url=BASE_URL, follow_redirects=True
    )
    with SenateEfdClient(
        Settings(),
        client=http_client,
        min_interval_seconds=0.0,
        sleep=lambda _seconds: None,
    ) as client:
        html = client.get_report_html("/search/view/ptr/f8d003c0/")

    assert "table table-striped" in html
    assert state["agreements"] == 2  # once up front, once after the bounce
    http_client.close()


def test_client_retries_server_errors() -> None:
    import httpx

    from capitol_pipeline.config import Settings
    from capitol_pipeline.sources.senate_efd import SenateEfdClient

    attempts = {"count": 0}
    slept: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/search/home/" and request.method == "GET":
            attempts["count"] += 1
            if attempts["count"] < 3:
                return httpx.Response(502, text="Bad Gateway")
            return httpx.Response(
                200,
                html='<input name="csrfmiddlewaretoken" value="test-token" />',
                headers={"set-cookie": "csrftoken=test-token; Path=/"},
            )
        return httpx.Response(200, html="<html><body>Search</body></html>")

    http_client = httpx.Client(
        transport=httpx.MockTransport(handler), base_url=BASE_URL, follow_redirects=True
    )
    with SenateEfdClient(
        Settings(),
        client=http_client,
        min_interval_seconds=0.0,
        sleep=slept.append,
    ) as client:
        client.accept_agreement()

    assert attempts["count"] == 3
    assert len(slept) == 2  # backed off between the two 502s
    http_client.close()


def test_canonical_id_still_separates_real_differences(
    electronic_transactions, senate_registry, whitehouse_report
) -> None:
    self_row = normalize_efd_transaction(
        whitehouse_report, electronic_transactions[1], senate_registry
    )
    spouse_row = normalize_efd_transaction(
        whitehouse_report, electronic_transactions[2], senate_registry
    )
    assert self_row is not None and spouse_row is not None

    # Same ticker and date, different owner and amount bounds.
    assert self_row.source_id != spouse_row.source_id
