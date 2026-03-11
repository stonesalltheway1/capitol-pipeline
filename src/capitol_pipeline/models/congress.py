"""Congressional disclosure models used by Capitol Pipeline."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

DisclosureSource = Literal[
    "house-clerk",
    "senate-quiver",
    "senate-watcher",
    "senate-ethics",
    "fec",
    "lda",
    "congress-gov",
    "sec",
    "manual",
    "other",
]

DisclosureKind = Literal[
    "house-ptr",
    "senate-trade",
    "campaign-finance",
    "lobbying",
    "bill",
    "vote",
    "other",
]

CryptoAssetKind = Literal[
    "direct_crypto",
    "crypto_etf",
    "crypto_equity",
    "unrelated",
]


class MemberMatch(BaseModel):
    """Resolved member metadata used during filing normalization."""

    id: str | None = None
    bioguide_id: str | None = None
    name: str
    slug: str | None = None
    party: str | None = None
    state: str | None = None
    district: str | None = None


class FilingStub(BaseModel):
    """A newly detected disclosure before transaction rows are parsed."""

    doc_id: str
    filing_year: int
    filing_type: str = "PTR"
    filing_date: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    member: MemberMatch
    source: DisclosureSource = "house-clerk"
    source_url: str
    raw_state_district: str | None = None


class HousePtrTransaction(BaseModel):
    """A single normalized transaction parsed from a House PTR filing."""

    line_number: int
    asset_description: str
    ticker: str | None = None
    asset_type: str
    transaction_type: Literal["purchase", "sale", "exchange"]
    transaction_date: str | None = None
    notification_date: str | None = None
    amount_min: int = 0
    amount_max: int = 0
    owner: Literal["self", "spouse", "joint", "child"] = "self"


class HousePtrParseResult(BaseModel):
    """Structured output from a parsed House PTR text block or PDF."""

    doc_id: str | None = None
    member_name: str | None = None
    state: str | None = None
    parser_confidence: float = 0.0
    raw_text_preview: str | None = None
    transactions: list[HousePtrTransaction] = Field(default_factory=list)


class NormalizedAsset(BaseModel):
    """Canonical asset classification emitted by the Capitol pipeline."""

    raw_ticker: str | None = None
    raw_description: str
    canonical_symbol: str | None = None
    canonical_name: str | None = None
    kind: CryptoAssetKind = "unrelated"
    matched_by: str | None = None
    confidence: float = 0.0
    aliases: list[str] = Field(default_factory=list)


class NormalizedTradeRow(BaseModel):
    """A normalized trade row ready for database export."""

    member: MemberMatch
    source: DisclosureSource
    disclosure_kind: DisclosureKind
    source_id: str
    source_url: str | None = None
    ticker: str | None = None
    asset_description: str
    asset_type: str
    transaction_type: str
    transaction_date: str | None = None
    disclosure_date: str | None = None
    amount_min: int = 0
    amount_max: int = 0
    owner: str = "self"
    comment: str | None = None
    parser_confidence: float | None = None
    normalized_asset: NormalizedAsset | None = None
