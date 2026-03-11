"""Bridge functions that shape Capitol Pipeline output for CapitolExposed."""

from __future__ import annotations

import hashlib

from capitol_pipeline.models.congress import FilingStub, NormalizedTradeRow


SOURCE_EXPORT_MAP = {
    "house-clerk": "house_clerk",
    "senate-quiver": "senate_quiver",
    "senate-watcher": "senate_watcher",
    "senate-ethics": "senate_efd",
    "congress-gov": "congress_gov",
}


def normalize_trade_source_for_site(source: str) -> str:
    """Map pipeline source ids to the source values CapitolExposed stores in trades."""

    return SOURCE_EXPORT_MAP.get(source, source.replace("-", "_"))


def build_canonical_senate_trade_id(row: NormalizedTradeRow) -> str:
    payload = "|".join(
        [
            row.member.id,
            (row.ticker or "").strip().upper(),
            " ".join(row.asset_description.split()).lower(),
            (row.transaction_type or "").strip().lower(),
            row.transaction_date or "",
            str(row.amount_min or 0),
            str(row.amount_max or 0),
            (row.owner or "self").strip().lower(),
            (row.source_url or "").strip().lower().rstrip("/"),
        ]
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
    return f"tr-senate-{digest}"


def build_trade_id(row: NormalizedTradeRow) -> str:
    """Create the canonical CapitolExposed trade id for a normalized row."""

    if row.disclosure_kind == "house-ptr":
        return f"tr-house-{row.source_id.replace(':', '-')}"
    if row.disclosure_kind == "senate-trade":
        return build_canonical_senate_trade_id(row)
    return f"tr-{row.source.replace(':', '-')}-{row.source_id.replace(':', '-')}"


def build_house_stub_payload(stub: FilingStub) -> dict[str, object]:
    """Build a row payload compatible with the site's house_filing_stubs table."""

    return {
        "doc_id": stub.doc_id,
        "filing_year": stub.filing_year,
        "source": stub.source,
        "source_url": stub.source_url,
        "status": "pending_extraction",
        "metadata": {
            "docId": stub.doc_id,
            "filingYear": stub.filing_year,
            "filingType": stub.filing_type,
            "filingDate": stub.filing_date,
            "firstName": stub.first_name,
            "lastName": stub.last_name,
            "memberId": stub.member.id,
            "memberName": stub.member.name,
            "memberSlug": stub.member.slug,
            "party": stub.member.party,
            "state": stub.member.state,
            "district": stub.member.district,
            "rawStateDistrict": stub.raw_state_district,
        },
    }


def build_trade_payload(row: NormalizedTradeRow) -> dict[str, object]:
    """Build a row payload compatible with the site's trades table."""

    normalized_asset = row.normalized_asset
    asset_type = row.asset_type
    if normalized_asset:
        if normalized_asset.kind == "direct_crypto":
            asset_type = "Cryptocurrency"
        elif normalized_asset.kind == "crypto_etf":
            asset_type = "Crypto ETF"
        elif normalized_asset.kind == "crypto_equity":
            asset_type = "Crypto-Adjacent Equity"

    return {
        "id": build_trade_id(row),
        "member_id": row.member.id,
        "ticker": row.ticker,
        "asset_description": row.asset_description,
        "asset_type": asset_type,
        "transaction_type": row.transaction_type,
        "transaction_date": row.transaction_date,
        "disclosure_date": row.disclosure_date,
        "amount_min": row.amount_min,
        "amount_max": row.amount_max,
        "owner": row.owner,
        "comment": row.comment,
        "source": normalize_trade_source_for_site(row.source),
        "source_url": row.source_url or "",
        "conflict_score": 0.0,
        "conflict_flags": [],
        "crypto_kind": normalized_asset.kind if normalized_asset else None,
        "crypto_symbol": normalized_asset.canonical_symbol if normalized_asset else None,
        "crypto_match_confidence": normalized_asset.confidence if normalized_asset else None,
    }
