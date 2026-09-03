"""Congressional disclosure models used by Capitol Pipeline."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

DisclosureSource = Literal[
    "house-clerk",
    "senate-quiver",
    "senate-watcher",
    "senate-efd",
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
    # The stub's previous ``metadata.visionParse`` and ``parsedTransactions``,
    # hydrated from the review queue so an unchanged PDF is not read again.
    # Never serialized: the exporter writes stubs field by field.
    prior_vision: dict[str, Any] | None = Field(default=None, exclude=True)


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
    # Per-row annotations printed under the asset on the House form ("Filing
    # Status: New", "Subholding Of: <account>", "Description: ...",
    # "Comments: ..."), joined with " | ". Never part of asset_description.
    comment: str | None = None
    # How well the reader could read this row: "clear", "partial" or
    # "illegible". Set only on the vision path; None from the text parser,
    # where the question does not arise. It decides whether the row publishes
    # while its filing is under review -- see split_scanned_trades.
    legibility: str | None = None


class HousePtrParseResult(BaseModel):
    """Structured output from a parsed House PTR text block or PDF."""

    doc_id: str | None = None
    member_name: str | None = None
    state: str | None = None
    parser_confidence: float = 0.0
    parser_version: str = "regex-v1"
    raw_text_preview: str | None = None
    transactions: list[HousePtrTransaction] = Field(default_factory=list)
    # Compact record of a Claude vision transcription attempt (model, usage,
    # estimated cost, legibility counts, skip reason). Written to the stub's
    # ``metadata.visionParse``; None whenever the vision path never ran.
    vision_report: dict[str, object] | None = None
    # What pymupdf found before any OCR ran (page counts, text-layer chars,
    # whether OCR was skipped, capped or completed). Written to the stub's
    # ``metadata.textLayer``.
    text_layer: dict[str, object] | None = None
    # Why a filing produced no rows, for the stub's ``lastError``: a typed
    # text layer the segmenter could not split, an image-only scan whose OCR
    # timed out, and so on. None whenever at least one row was parsed.
    review_reason: str | None = None


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
    parser_version: str | None = None
    normalized_asset: NormalizedAsset | None = None
