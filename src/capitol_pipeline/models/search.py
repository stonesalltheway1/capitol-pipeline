"""Search and retrieval models for Capitol Pipeline."""

from __future__ import annotations

from pydantic import BaseModel, Field

from capitol_pipeline.models.document import Document


class SearchDocumentRecord(BaseModel):
    """A searchable canonical document record."""

    id: str
    source_document_id: str
    title: str
    content: str
    source: str
    category: str
    document_date: str | None = None
    summary: str | None = None
    source_url: str | None = None
    pdf_url: str | None = None
    member_ids: list[str] = Field(default_factory=list)
    committee_ids: list[str] = Field(default_factory=list)
    bill_ids: list[str] = Field(default_factory=list)
    asset_tickers: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)


class SearchChunkRecord(BaseModel):
    """A searchable chunk for lexical and vector retrieval."""

    id: str
    document_id: str
    chunk_index: int
    content: str
    token_estimate: int = 0
    metadata: dict[str, object] = Field(default_factory=dict)
    embedding: list[float] | None = None


class SearchHit(BaseModel):
    """A single hybrid retrieval hit."""

    document_id: str
    chunk_id: str | None = None
    title: str
    content: str
    source: str
    category: str
    source_url: str | None = None
    pdf_url: str | None = None
    lexical_score: float = 0.0
    semantic_score: float = 0.0
    combined_score: float = 0.0
    metadata: dict[str, object] = Field(default_factory=dict)


def build_search_document(
    document: Document,
    *,
    content: str | None = None,
    metadata: dict[str, object] | None = None,
) -> SearchDocumentRecord:
    """Convert a core document into a searchable record."""

    body = (content or document.ocrText or "").strip()
    return SearchDocumentRecord(
        id=f"doc-{document.id}",
        source_document_id=document.id,
        title=document.title,
        content=body,
        source=document.source,
        category=document.category,
        document_date=document.date,
        summary=document.summary,
        source_url=document.sourceUrl,
        pdf_url=document.pdfUrl,
        member_ids=list(document.memberIds),
        committee_ids=list(document.committeeIds),
        bill_ids=list(document.billIds),
        asset_tickers=list(document.assetTickers),
        tags=list(document.tags),
        metadata=metadata or {},
    )
