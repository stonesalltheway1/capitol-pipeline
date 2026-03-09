"""Core document models for Capitol Pipeline."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Enums as Literal unions (mirrors the TS string-union types)
# ---------------------------------------------------------------------------

DocumentSource = Literal[
    "house-clerk",
    "senate-ethics",
    "senate-watcher",
    "fara",
    "fec",
    "lda",
    "congress-gov",
    "sec",
    "icij-offshore-leaks",
    "manual",
    "other",
]

DocumentCategory = Literal[
    "stock-act",
    "campaign-finance",
    "lobbying",
    "bill",
    "vote",
    "committee",
    "newsroom",
    "cross-reference",
    "other",
]

VerificationStatus = Literal[
    "verified",
    "unverified",
    "disputed",
    "redacted",
]


# ---------------------------------------------------------------------------
# Core models
# ---------------------------------------------------------------------------


class Document(BaseModel):
    """A single source document in the CapitolExposed corpus."""

    id: str
    title: str
    date: str | None = None  # YYYY-MM-DD format
    source: DocumentSource
    category: DocumentCategory
    summary: str | None = None
    memberIds: list[str] = Field(default_factory=list)
    committeeIds: list[str] = Field(default_factory=list)
    billIds: list[str] = Field(default_factory=list)
    assetTickers: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    pdfUrl: str | None = None
    sourceUrl: str | None = None
    archiveUrl: str | None = None
    pageCount: int | None = None
    documentKey: str | None = None
    ocrText: str | None = None
    verificationStatus: VerificationStatus | None = None


class EntityResult(BaseModel):
    """A single extracted entity from NER processing."""

    entity_type: str  # PERSON, ORG, PHONE, EMAIL_ADDR, CASE_NUMBER, FLIGHT_ID, etc.
    value: str
    confidence: float | None = None
    source: str = "spacy"  # 'spacy', 'gliner', 'regex'
    span: str | None = None  # original text context


class ProcessingResult(BaseModel):
    """Result of processing a single source file through the pipeline."""

    source_path: str
    document: Document | None = None
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    processing_time_ms: int

    # v1.0 additions
    ocr_confidence: float | None = None  # per-page average confidence
    classified_category: str | None = None  # auto-classified document type
    entities: list[EntityResult] = Field(default_factory=list)
    duplicate_cluster_id: str | None = None  # assigned dedup cluster
