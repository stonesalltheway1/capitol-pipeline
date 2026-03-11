"""Senate trade feed adapters and normalizers for Capitol Pipeline."""

from __future__ import annotations

import hashlib
import time
import warnings

import httpx
from pydantic import BaseModel, Field

from capitol_pipeline.bridges.capitol_exposed import build_canonical_senate_trade_id
from capitol_pipeline.config import Settings
from capitol_pipeline.models.congress import NormalizedTradeRow
from capitol_pipeline.normalizers.crypto_assets import classify_crypto_asset
from capitol_pipeline.registries.members import MemberRegistry

QUIVER_BULK_PAGE_SIZE = 1000


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


class QuiverCongressTrade(BaseModel):
    representative: str | None = Field(default=None, alias="Representative")
    bioguide_id: str | None = Field(default=None, alias="BioGuideID")
    report_date: str | None = Field(default=None, alias="ReportDate")
    transaction_date: str | None = Field(default=None, alias="TransactionDate")
    traded: str | None = Field(default=None, alias="Traded")
    ticker: str | None = Field(default=None, alias="Ticker")
    transaction: str | None = Field(default=None, alias="Transaction")
    trade_size_usd: str | None = Field(default=None, alias="Trade_Size_USD")
    house: str | None = Field(default=None, alias="House")
    amount: str | None = Field(default=None, alias="Amount")
    ticker_type: str | None = Field(default=None, alias="TickerType")
    description: str | None = Field(default=None, alias="Description")
    company: str | None = Field(default=None, alias="Company")
    name: str | None = Field(default=None, alias="Name")
    filed: str | None = Field(default=None, alias="Filed")
    party: str | None = Field(default=None, alias="Party")
    district: str | None = Field(default=None, alias="District")
    chamber: str | None = Field(default=None, alias="Chamber")
    comments: str | None = Field(default=None, alias="Comments")
    subholding: str | None = Field(default=None, alias="Subholding")
    state: str | None = Field(default=None, alias="State")
    quiver_upload_time: str | None = Field(default=None, alias="Quiver_Upload_Time")
    last_modified: str | None = Field(default=None, alias="last_modified")


def normalize_senate_date(raw: str | None) -> str | None:
    """Normalize Senate feed dates into YYYY-MM-DD."""

    if not raw or raw in {"--", "N/A"}:
        return None
    trimmed = raw.strip()
    if len(trimmed) >= 10 and trimmed[4:5] == "-" and trimmed[7:8] == "-":
        return trimmed[:10]
    if len(trimmed) == 8 and trimmed.isdigit():
        return f"{trimmed[:4]}-{trimmed[4:6]}-{trimmed[6:8]}"
    parts = trimmed.split("/")
    if len(parts) != 3:
        return None
    month, day, year = (part.strip() for part in parts)
    if not (month.isdigit() and day.isdigit() and year.isdigit()):
        return None
    return f"{year.zfill(4)}-{month.zfill(2)}-{day.zfill(2)}"


def parse_senate_amount_range(raw: str | None) -> tuple[int, int]:
    """Convert a Senate amount string into a min and max range."""

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
    """Map Senate action types to CapitolExposed transaction types."""

    normalized = (raw or "").strip().lower()
    if normalized.startswith("sale"):
        return "sale"
    if normalized == "exchange":
        return "exchange"
    if "receive" in normalized:
        return "receive"
    return "purchase"


def normalize_senate_owner(raw: str | None) -> str:
    """Map Senate owner values to the site's owner taxonomy."""

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
    """Legacy watcher hash retained for backward compatibility."""

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


def _normalize_asset_type(raw: str | None, normalized_asset_kind: str) -> str:
    if normalized_asset_kind == "direct_crypto":
        return "Cryptocurrency"
    if normalized_asset_kind == "crypto_etf":
        return "Crypto ETF"
    if normalized_asset_kind == "crypto_equity":
        return "Crypto-Adjacent Equity"

    normalized = (raw or "").strip().lower()
    if "etf" in normalized:
        return "ETF"
    if "option" in normalized:
        return "Option"
    if "bond" in normalized:
        return "Bond"
    if "fund" in normalized:
        return "Fund"
    return "Stock"


def _build_quiver_comment(trade: QuiverCongressTrade) -> str | None:
    parts = [
        (trade.subholding or "").strip(),
        (trade.comments or "").strip(),
    ]
    values = [part for part in parts if part]
    return " | ".join(values) if values else None


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
    asset_type = _normalize_asset_type(trade.asset_type, normalized_asset.kind)

    row = NormalizedTradeRow(
        member=member,
        source="senate-watcher",
        disclosure_kind="senate-trade",
        source_id="",
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


def normalize_quiver_senate_trade(
    trade: QuiverCongressTrade,
    registry: MemberRegistry,
) -> NormalizedTradeRow | None:
    """Resolve and normalize one Quiver Senate trade into a site-ready trade row."""

    chamber = (trade.chamber or trade.house or "").strip().lower()
    if chamber != "senate":
        return None

    member_name = (trade.name or trade.representative or "").strip()
    transaction_date = normalize_senate_date(trade.transaction_date or trade.traded)
    disclosure_date = normalize_senate_date(trade.filed or trade.report_date or trade.quiver_upload_time)
    if not member_name or not transaction_date:
        return None

    member = registry.resolve(
        bioguide_id=(trade.bioguide_id or "").strip().upper() or None,
        name=member_name,
        state=(trade.state or "").strip().upper() or None,
    )
    if not member or not member.id:
        return None

    raw_ticker = (trade.ticker or "").strip().upper()
    ticker = raw_ticker or None
    amount_min, amount_max = parse_senate_amount_range(trade.trade_size_usd or trade.amount)
    transaction_type = normalize_senate_transaction_type(trade.transaction)
    asset_description = (
        (trade.description or "").strip()
        or (trade.company or "").strip()
        or ticker
        or "Unknown asset"
    )
    normalized_asset = classify_crypto_asset(ticker, asset_description)
    asset_type = _normalize_asset_type(trade.ticker_type, normalized_asset.kind)

    row = NormalizedTradeRow(
        member=member,
        source="senate-quiver",
        disclosure_kind="senate-trade",
        source_id="",
        source_url=None,
        ticker=ticker,
        asset_description=asset_description,
        asset_type=asset_type,
        transaction_type=transaction_type,
        transaction_date=transaction_date,
        disclosure_date=disclosure_date,
        amount_min=amount_min,
        amount_max=amount_max,
        owner="self",
        comment=_build_quiver_comment(trade),
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


def fetch_quiver_bulk_congress_feed(
    settings: Settings | None = None,
    *,
    page: int = 1,
    page_size: int = QUIVER_BULK_PAGE_SIZE,
    date: str | None = None,
    timeout_seconds: float = 60.0,
) -> list[QuiverCongressTrade]:
    """Fetch one page from Quiver's bulk Congress trading feed."""

    settings = settings or Settings()
    token = settings.resolved_quiver_api_token
    if not token:
        raise RuntimeError("QUIVER_API_TOKEN is required for Quiver Senate ingestion.")

    params = {
        "normalized": "true",
        "version": "V2",
        "page_size": str(max(1, page_size)),
        "page": str(max(1, page)),
    }
    if date:
        params["date"] = normalize_senate_date(date).replace("-", "") if normalize_senate_date(date) else date

    with httpx.Client(
        headers={
            "User-Agent": settings.user_agent,
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
        follow_redirects=True,
        timeout=timeout_seconds,
    ) as client:
        for attempt in range(4):
            response = client.get(
                "https://api.quiverquant.com/beta/bulk/congresstrading",
                params=params,
            )
            if response.status_code != 429 and response.status_code < 500:
                response.raise_for_status()
                payload = response.json()
                return [QuiverCongressTrade.model_validate(row) for row in payload]

            retry_after = response.headers.get("retry-after")
            delay_seconds = float(retry_after) if retry_after and retry_after.isdigit() else 2.0 + attempt
            if attempt >= 3:
                response.raise_for_status()
            time.sleep(delay_seconds)

    return []
