"""Senate trade feed adapter and normalizer for Capitol Pipeline.

This starts with the Senate watcher aggregate feed because it is the same
interim source the site already uses. The long-term plan is to replace or
augment this with official Senate Ethics disclosure ingestion.
"""

from __future__ import annotations

import hashlib
import warnings

import httpx
from pydantic import BaseModel

from capitol_pipeline.bridges.capitol_exposed import build_canonical_senate_trade_id
from capitol_pipeline.config import Settings
from capitol_pipeline.models.congress import NormalizedTradeRow
from capitol_pipeline.normalizers.crypto_assets import classify_crypto_asset
from capitol_pipeline.registries.members import MemberRegistry


class SenateWatcherTrade(BaseModel):
    senator: str | None = None
    transaction_date: str | None = None
    ticker: str | None = None
    amount: str | None = None
    type: str | None = None
    asset_description: str | None = None
    comment: str | None = None
    ptr_link: str | None = None
    owner: str | None = None
    asset_type: str | None = None


def normalize_senate_date(raw: str | None) -> str | None:
    """Normalize Senate watcher dates into YYYY-MM-DD."""

    if not raw or raw in {"--", "N/A"}:
        return None
    if len(raw) >= 10 and raw[4:5] == "-" and raw[7:8] == "-":
        return raw[:10]
    parts = raw.split("/")
    if len(parts) != 3:
        return None
    month, day, year = (part.strip() for part in parts)
    if not (month.isdigit() and day.isdigit() and year.isdigit()):
        return None
    return f"{year.zfill(4)}-{month.zfill(2)}-{day.zfill(2)}"


def parse_senate_amount_range(raw: str | None) -> tuple[int, int]:
    """Convert a Senate watcher amount string into a min and max range."""

    if not raw or raw == "--":
        return 0, 0
    cleaned = raw.replace("$", "").replace(",", "").strip()
    lower = cleaned.lower()
    if lower.startswith("over "):
        over_value = lower.replace("over ", "", 1).strip()
        if over_value.isdigit():
            numeric = int(over_value)
            return numeric, numeric
        return 0, 0
    parts = [part.strip() for part in cleaned.split("-")]
    if len(parts) == 2 and all(part.isdigit() for part in parts):
        return int(parts[0]), int(parts[1])
    if cleaned.isdigit():
        numeric = int(cleaned)
        return numeric, numeric
    return 0, 0


def normalize_senate_transaction_type(raw: str | None) -> str:
    """Map Senate watcher action types to CapitolExposed transaction types."""

    normalized = (raw or "").strip().lower()
    if normalized.startswith("sale"):
        return "sale"
    if normalized == "exchange":
        return "exchange"
    return "purchase"


def normalize_senate_owner(raw: str | None) -> str:
    """Map Senate watcher owner values to the site's owner taxonomy."""

    normalized = (raw or "").strip().lower()
    if normalized in {"spouse", "child", "joint"}:
        return normalized
    return "self"


def build_senate_watcher_trade_key(
    *,
    member_name: str,
    ticker: str | None,
    transaction_date: str,
    transaction_type: str,
    raw_amount: str | None,
) -> str:
    """Reproduce CapitolExposed's stable Senate watcher hash id suffix.

    .. deprecated::
        Use :func:`capitol_pipeline.bridges.capitol_exposed.build_canonical_senate_trade_id`
        instead.  This legacy function produces IDs that diverge from the
        canonical trade IDs actually written to the database.  It is retained
        only for backward-compatibility with callers that still reference it
        and will be removed in a future release.
    """

    warnings.warn(
        "build_senate_watcher_trade_key is deprecated; use "
        "capitol_pipeline.bridges.capitol_exposed.build_canonical_senate_trade_id instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    payload = "|".join(
        [
            member_name,
            (ticker or "").upper(),
            transaction_date,
            transaction_type,
            raw_amount or "",
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def normalize_senate_watcher_trade(
    trade: SenateWatcherTrade,
    registry: MemberRegistry,
) -> NormalizedTradeRow | None:
    """Resolve and normalize one Senate watcher trade into a site-ready trade row."""

    senator_name = (trade.senator or "").strip()
    transaction_date = normalize_senate_date(trade.transaction_date)
    if not senator_name or not transaction_date:
        return None

    member = registry.resolve(name=senator_name)
    if not member or not member.id:
        return None

    raw_ticker = (trade.ticker or "").strip().upper()
    ticker = raw_ticker if raw_ticker and raw_ticker != "--" else None
    amount_min, amount_max = parse_senate_amount_range(trade.amount)
    transaction_type = normalize_senate_transaction_type(trade.type)
    asset_description = (trade.asset_description or "").strip() or ticker or "Unknown asset"
    normalized_asset = classify_crypto_asset(ticker, asset_description)

    asset_type = (trade.asset_type or "").strip() or "Stock"
    if normalized_asset.kind == "direct_crypto":
        asset_type = "Cryptocurrency"
    elif normalized_asset.kind == "crypto_etf":
        asset_type = "Crypto ETF"
    elif normalized_asset.kind == "crypto_equity":
        asset_type = "Crypto-Adjacent Equity"

    # Build the row with a temporary source_id, then overwrite it with the
    # canonical ID that the bridge actually writes to the database.  This
    # ensures source_id and the DB trade id are always in sync.
    row = NormalizedTradeRow(
        member=member,
        source="senate-watcher",
        disclosure_kind="senate-trade",
        source_id="",  # placeholder — set below
        source_url=(trade.ptr_link or "").strip() or None,
        ticker=ticker,
        asset_description=asset_description,
        asset_type=asset_type,
        transaction_type=transaction_type,
        transaction_date=transaction_date,
        disclosure_date=None,
        amount_min=amount_min,
        amount_max=amount_max,
        owner=normalize_senate_owner(trade.owner),
        comment=(trade.comment or "").strip() or None,
        normalized_asset=None if normalized_asset.kind == "unrelated" else normalized_asset,
    )
    row.source_id = build_canonical_senate_trade_id(row)
    return row


def fetch_senate_watcher_feed(
    settings: Settings | None = None,
    timeout_seconds: float = 20.0,
) -> list[SenateWatcherTrade]:
    """Fetch the current Senate watcher aggregate JSON feed."""

    settings = settings or Settings()
    with httpx.Client(
        headers={"User-Agent": settings.user_agent},
        follow_redirects=True,
        timeout=timeout_seconds,
    ) as client:
        response = client.get(settings.senate_watcher_url)
        response.raise_for_status()
    payload = response.json()
    return [SenateWatcherTrade.model_validate(row) for row in payload]
