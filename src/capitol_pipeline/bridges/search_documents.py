"""Search document builders for Capitol Pipeline."""

from __future__ import annotations

from capitol_pipeline.models.congress import FilingStub, HousePtrParseResult, NormalizedTradeRow
from capitol_pipeline.models.document import Document
from capitol_pipeline.models.search import SearchDocumentRecord, build_search_document


def build_house_ptr_search_document(
    stub: FilingStub,
    parsed: HousePtrParseResult,
    trades: list[NormalizedTradeRow],
) -> SearchDocumentRecord:
    """Build a searchable document record from a parsed House PTR."""

    tickers = sorted({trade.ticker for trade in trades if trade.ticker})
    tags = ["house-ptr", "stock-act", "congressional-trades"]
    if tickers:
        tags.append("ticker-linked")

    summary = (
        f"House PTR filed by {stub.member.name} with {len(parsed.transactions)} parsed transactions."
        if parsed.transactions
        else f"House PTR filed by {stub.member.name} awaiting manual review."
    )

    document = Document(
        id=f"house-ptr-{stub.doc_id}",
        title=f"{stub.member.name} House PTR {stub.doc_id}",
        date=stub.filing_date,
        source="house-clerk",
        category="stock-act",
        summary=summary,
        memberIds=[stub.member.id] if stub.member.id else [],
        assetTickers=tickers,
        tags=tags,
        pdfUrl=stub.source_url,
        sourceUrl=stub.source_url,
        ocrText=parsed.raw_text_preview or "",
        verificationStatus="verified" if parsed.transactions else "unverified",
    )

    return build_search_document(
        document,
        content=parsed.raw_text_preview or "",
        metadata={
            "docId": stub.doc_id,
            "filingYear": stub.filing_year,
            "filingType": stub.filing_type,
            "parserConfidence": parsed.parser_confidence,
            "parsedTransactionCount": len(parsed.transactions),
        },
    )
