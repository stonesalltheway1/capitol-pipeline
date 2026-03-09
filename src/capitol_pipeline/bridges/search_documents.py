"""Search document builders for Capitol Pipeline."""

from __future__ import annotations

from typing import Any

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


def build_house_ptr_search_document_from_stub_row(
    row: dict[str, Any],
) -> SearchDocumentRecord:
    """Build a searchable record from a stored house_filing_stubs row."""

    metadata = row.get("metadata") or {}
    if not isinstance(metadata, dict):
        metadata = {}

    member_name = str(metadata.get("memberName") or "").strip() or f"House PTR {row.get('doc_id')}"
    raw_text_preview = str(metadata.get("rawTextPreview") or "").strip()
    parsed_transactions = metadata.get("parsedTransactions") or []
    if not isinstance(parsed_transactions, list):
        parsed_transactions = []

    tickers = sorted(
        {
            str(transaction.get("ticker") or "").strip().upper()
            for transaction in parsed_transactions
            if isinstance(transaction, dict) and transaction.get("ticker")
        }
    )
    transaction_count = len(parsed_transactions)
    status = str(row.get("status") or "parsed")
    summary = (
        f"House PTR filed by {member_name} with {transaction_count} parsed transactions."
        if transaction_count
        else f"House PTR filed by {member_name} awaiting deeper review."
    )

    document = Document(
        id=f"house-ptr-{row.get('doc_id')}",
        title=f"{member_name} House PTR {row.get('doc_id')}",
        date=str(metadata.get("filingDate") or "").strip() or None,
        source="house-clerk",
        category="stock-act",
        summary=summary,
        memberIds=[str(metadata.get("memberId"))] if metadata.get("memberId") else [],
        assetTickers=tickers,
        tags=["house-ptr", "stock-act", "congressional-trades"],
        pdfUrl=str(row.get("source_url") or "").strip() or None,
        sourceUrl=str(row.get("source_url") or "").strip() or None,
        ocrText=raw_text_preview,
        verificationStatus="verified" if status == "parsed" else "unverified",
    )

    return build_search_document(
        document,
        content=raw_text_preview,
        metadata={
            "docId": str(row.get("doc_id") or ""),
            "filingYear": row.get("filing_year"),
            "filingType": metadata.get("filingType") or "PTR",
            "parserConfidence": metadata.get("parserConfidence"),
            "parsedTransactionCount": transaction_count,
            "stubStatus": status,
        },
    )
