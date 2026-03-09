"""Search document builders for Capitol Pipeline."""

from __future__ import annotations

from typing import Any

from capitol_pipeline.models.congress import FilingStub, HousePtrParseResult, MemberMatch, NormalizedTradeRow
from capitol_pipeline.models.document import Document
from capitol_pipeline.models.fara import FaraMemberMatchRecord, FaraRegistrantBundle
from capitol_pipeline.models.offshore import OffshoreNodeRecord
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


def build_offshore_match_search_document(
    node: OffshoreNodeRecord,
    member: MemberMatch,
) -> SearchDocumentRecord:
    """Build a searchable cross-reference document for a Congress match."""

    document = Document(
        id=f"offshore-match-{node.node_key}",
        title=f"{member.name} cross-reference in {node.source_dataset}",
        source="icij-offshore-leaks",
        category="cross-reference",
        date=None,
        summary=f"{member.name} exactly matches an Offshore Leaks node in {node.source_dataset}.",
        memberIds=[member.id] if member.id else [],
        tags=["offshore-leaks", "cross-reference", node.node_type, node.source_dataset],
        sourceUrl="https://offshoreleaks.icij.org/",
        archiveUrl=node.metadata.get("sourceID") if isinstance(node.metadata.get("sourceID"), str) else None,
        ocrText="\n".join(
            [
                f"Matched member: {member.name}",
                f"Offshore dataset: {node.source_dataset}",
                f"Node type: {node.node_type}",
                f"Node name: {node.name}",
                node.content,
            ]
        ),
        verificationStatus="unverified",
    )

    return build_search_document(
        document,
        content=document.ocrText or "",
        metadata={
            "nodeKey": node.node_key,
            "nodeType": node.node_type,
            "sourceDataset": node.source_dataset,
            "matchType": "exact_name",
            "matchValue": node.name,
            "memberSlug": member.slug,
        },
    )


def build_fara_registrant_search_document(bundle: FaraRegistrantBundle) -> SearchDocumentRecord:
    """Build a searchable record for one FARA registrant bundle."""

    registrant = bundle.registrant
    foreign_principals = [principal.foreign_principal_name for principal in bundle.foreign_principals]
    short_forms = [item.full_name for item in bundle.short_forms]
    recent_docs = [f"{item.document_type} ({item.date_stamped or 'undated'})" for item in bundle.documents[:8]]
    content_lines = [
        registrant.content,
        f"Foreign principals: {', '.join(foreign_principals[:20])}" if foreign_principals else None,
        f"Short-form registrants: {', '.join(short_forms[:20])}" if short_forms else None,
        f"Recent filings: {', '.join(recent_docs)}" if recent_docs else None,
    ]
    document = Document(
        id=f"fara-registrant-{registrant.registration_number}",
        title=f"{registrant.name} FARA registrant profile",
        date=registrant.registration_date,
        source="fara",
        category="lobbying",
        summary=registrant.summary,
        sourceUrl="https://efile.fara.gov/",
        ocrText="\n".join(line for line in content_lines if line),
        tags=[
            "fara",
            "foreign-agent",
            "registrant",
        ],
        verificationStatus="verified",
    )
    return build_search_document(
        document,
        content=document.ocrText or "",
        metadata={
            "registrationNumber": registrant.registration_number,
            "foreignPrincipals": foreign_principals,
            "shortForms": short_forms,
            "recentDocuments": [item.url for item in bundle.documents[:12]],
        },
    )


def build_fara_member_match_search_document(
    match: FaraMemberMatchRecord,
    bundle: FaraRegistrantBundle,
) -> SearchDocumentRecord:
    """Build a cross-reference search document for an exact FARA name match."""

    registrant = bundle.registrant
    document = Document(
        id=f"fara-match-{match.entity_key}",
        title=f"{match.member_name} cross-reference in FARA registration {match.registration_number}",
        source="fara",
        category="cross-reference",
        summary=(
            f"{match.member_name} exactly matches a FARA {match.entity_kind.replace('_', ' ')} "
            f"linked to {registrant.name}."
        ),
        memberIds=[match.member_id],
        sourceUrl="https://efile.fara.gov/",
        ocrText="\n".join(
            [
                f"Matched member: {match.member_name}",
                f"Match value: {match.match_value}",
                f"Entity kind: {match.entity_kind}",
                f"Registrant: {registrant.name}",
                registrant.content,
            ]
        ),
        tags=["fara", "cross-reference", match.entity_kind],
        verificationStatus="unverified",
    )
    return build_search_document(
        document,
        content=document.ocrText or "",
        metadata={
            "registrationNumber": match.registration_number,
            "entityKind": match.entity_kind,
            "entityKey": match.entity_key,
            "matchType": match.match_type,
            "matchValue": match.match_value,
            "memberSlug": match.member_slug,
            "registrantName": registrant.name,
        },
    )
