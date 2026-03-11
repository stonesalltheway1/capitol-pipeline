from capitol_pipeline.bridges.capitol_exposed import build_trade_id, build_trade_payload
from capitol_pipeline.bridges.search_documents import build_senate_trade_search_document
from capitol_pipeline.registries.members import MemberRegistry
from capitol_pipeline.sources.senate_ethics import (
    QuiverCongressTrade,
    QuiverLiveSenateTrade,
    normalize_quiver_live_senate_trade,
    normalize_quiver_senate_trade,
    SenateWatcherTrade,
    normalize_senate_watcher_trade,
    parse_senate_amount_range,
)


def test_parse_senate_amount_range_handles_ranges_and_over() -> None:
    assert parse_senate_amount_range("$1,001 - $15,000") == (1001, 15000)
    assert parse_senate_amount_range("Over $1,000,000") == (1000000, 1000000)
    assert parse_senate_amount_range("--") == (0, 0)


def test_normalize_senate_watcher_trade_resolves_member_and_crypto_asset() -> None:
    registry = MemberRegistry.from_rows(
        [
            {
                "id": "m-L000174",
                "name": "Cynthia M. Lummis",
                "slug": "cynthia-m-lummis",
                "party": "R",
                "state": "WY",
            }
        ]
    )
    raw_trade = SenateWatcherTrade(
        senator="Cynthia M. Lummis",
        transaction_date="08/14/2025",
        ticker="BTC",
        amount="$50,001 - $100,000",
        type="Purchase",
        asset_description="Bitcoin (CRYPTO:BTC)",
        ptr_link="https://example.test/lummis-btc",
        owner="Spouse",
    )

    normalized = normalize_senate_watcher_trade(raw_trade, registry)

    assert normalized is not None
    assert normalized.member.id == "m-L000174"
    assert normalized.transaction_date == "2025-08-14"
    assert normalized.amount_min == 50001
    assert normalized.amount_max == 100000
    assert normalized.normalized_asset is not None
    assert normalized.normalized_asset.kind == "direct_crypto"
    assert normalized.normalized_asset.canonical_symbol == "BTC"

    payload = build_trade_payload(normalized)
    assert payload["source"] == "senate_watcher"
    assert payload["id"] == build_trade_id(normalized)
    assert str(payload["id"]).startswith("tr-senate-")


def test_build_senate_trade_search_document_captures_trade_metadata() -> None:
    registry = MemberRegistry.from_rows(
        [
            {
                "id": "m-L000174",
                "name": "Cynthia M. Lummis",
                "slug": "cynthia-m-lummis",
                "party": "R",
                "state": "WY",
            }
        ]
    )
    normalized = normalize_senate_watcher_trade(
        SenateWatcherTrade(
            senator="Cynthia M. Lummis",
            transaction_date="08/14/2025",
            ticker="IBIT",
            amount="$1,001 - $15,000",
            type="Purchase",
            asset_description="iShares Bitcoin Trust ETF",
            ptr_link="https://example.test/lummis-ibit",
        ),
        registry,
    )

    assert normalized is not None
    document = build_senate_trade_search_document(normalized)
    assert document.source_document_id == f"senate-trade-{normalized.source_id}"
    assert document.source == normalized.source
    assert document.asset_tickers == ["IBIT"]
    assert document.metadata["cryptoKind"] == "crypto_etf"
    assert "Amount range: $1,001 to $15,000" in document.content


def test_normalize_quiver_senate_trade_prefers_bioguide_resolution() -> None:
    registry = MemberRegistry.from_rows(
        [
            {
                "id": "m-B001236",
                "bioguide_id": "B001236",
                "name": "John Boozman",
                "slug": "john-boozman",
                "party": "R",
                "state": "AR",
            }
        ]
    )
    normalized = normalize_quiver_senate_trade(
        QuiverCongressTrade(
            Name="John Boozman",
            BioGuideID="B001236",
            Filed="2026-03-06",
            Traded="2026-02-27",
            Ticker="AMAT",
            Transaction="Sale (Partial)",
            Trade_Size_USD="$1,001 - $15,000",
            Chamber="Senate",
            Company="Applied Materials, Inc. - Common Stock",
        ),
        registry,
    )

    assert normalized is not None
    assert normalized.member.id == "m-B001236"
    assert normalized.source == "senate-quiver"
    assert normalized.disclosure_date == "2026-03-06"
    assert normalized.transaction_type == "sale"
    payload = build_trade_payload(normalized)
    assert payload["source"] == "senate_quiver"


def test_normalize_quiver_live_senate_trade_maps_live_payload_fields() -> None:
    registry = MemberRegistry.from_rows(
        [
            {
                "id": "m-M001190",
                "bioguide_id": "M001190",
                "name": "Markwayne Mullin",
                "slug": "markwayne-mullin",
                "party": "R",
                "state": "OK",
            }
        ]
    )

    normalized = normalize_quiver_live_senate_trade(
        QuiverLiveSenateTrade(
            Senator="Markwayne Mullin",
            BioGuideID="M001190",
            Date="2026-02-25",
            Ticker="UNH",
            Transaction="Purchase",
            Range="$50,001 - $100,000",
            Amount="50001.0",
            last_modified="2026-03-10",
            TickerType="Stock",
        ),
        registry,
    )

    assert normalized is not None
    assert normalized.member.id == "m-M001190"
    assert normalized.transaction_date == "2026-02-25"
    assert normalized.disclosure_date == "2026-03-10"
    assert normalized.amount_min == 50001
    assert normalized.amount_max == 100000
    assert normalized.source == "senate-quiver"
