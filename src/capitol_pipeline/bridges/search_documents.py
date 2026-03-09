"""Search document builders for Capitol Pipeline."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from capitol_pipeline.models.congress import FilingStub, HousePtrParseResult, MemberMatch, NormalizedTradeRow
from capitol_pipeline.models.document import Document
from capitol_pipeline.models.fara import FaraMemberMatchRecord, FaraRegistrantBundle
from capitol_pipeline.models.offshore import OffshoreNodeRecord
from capitol_pipeline.models.search import SearchDocumentRecord, build_search_document
from capitol_pipeline.registries.members import MemberRegistry


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


def normalize_document_date(value: Any) -> str | None:
    """Convert a timestamp-like value into YYYY-MM-DD for canonical document dates."""

    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    text = str(value).strip()
    if not text:
        return None
    if "T" in text:
        return text.split("T", 1)[0]
    if " " in text:
        return text.split(" ", 1)[0]
    return text[:10]


def build_registry_slug_index(registry: MemberRegistry | None) -> dict[str, str]:
    """Build a slug to member id index for CapitolExposed editorial records."""

    if registry is None:
        return {}
    return {
        str(record.slug): str(record.id)
        for record in registry.records
        if record.slug and record.id
    }


def resolve_story_member_ids(
    member_refs: Any,
    registry: MemberRegistry | None = None,
) -> tuple[list[str], list[str]]:
    """Resolve editorial member refs into member ids and slugs when available."""

    if not isinstance(member_refs, list):
        return [], []
    slug_index = build_registry_slug_index(registry)
    member_ids: list[str] = []
    member_slugs: list[str] = []
    seen_ids: set[str] = set()
    seen_slugs: set[str] = set()
    for item in member_refs:
        if not isinstance(item, dict):
            continue
        slug = str(item.get("slug") or "").strip()
        if slug and slug not in seen_slugs:
            seen_slugs.add(slug)
            member_slugs.append(slug)
        member_id = slug_index.get(slug) if slug else None
        if not member_id:
            name = str(item.get("name") or "").strip()
            if registry and name:
                resolved = registry.resolve(name=name)
                member_id = resolved.id if resolved and resolved.id else None
        if member_id and member_id not in seen_ids:
            seen_ids.add(member_id)
            member_ids.append(member_id)
    return member_ids, member_slugs


def build_news_post_search_document(
    row: dict[str, Any],
    *,
    base_url: str,
    registry: MemberRegistry | None = None,
) -> SearchDocumentRecord:
    """Build a searchable record from a published CapitolExposed story."""

    member_ids, member_slugs = resolve_story_member_ids(row.get("member_refs"), registry)
    title = str(row.get("title") or "").strip()
    subtitle = str(row.get("subtitle") or "").strip()
    excerpt = str(row.get("excerpt") or "").strip()
    body = str(row.get("body") or "").strip()
    author = str(row.get("author") or "").strip()
    category = str(row.get("category") or "").strip() or "general"
    tags = [
        "capitol-exposed",
        "published-story",
        "newsroom",
        category,
        *[
            str(tag).strip()
            for tag in (row.get("tags") or [])
            if str(tag).strip()
        ],
    ]
    source_url = f"{base_url.rstrip('/')}/news/{row.get('slug')}"
    content_lines = [
        subtitle,
        excerpt,
        body,
    ]
    document = Document(
        id=f"capitol-story-{row.get('slug')}",
        title=title,
        date=normalize_document_date(row.get("published_at") or row.get("updated_at")),
        source="capitol-exposed",
        category="newsroom",
        summary=excerpt or subtitle or None,
        memberIds=member_ids,
        tags=list(dict.fromkeys([tag for tag in tags if tag])),
        sourceUrl=source_url,
        archiveUrl=source_url,
        ocrText="\n\n".join(line for line in content_lines if line),
        verificationStatus="verified",
    )
    evidence_items = row.get("evidence") if isinstance(row.get("evidence"), list) else []
    return build_search_document(
        document,
        content=document.ocrText or "",
        metadata={
            "slug": row.get("slug"),
            "author": author or None,
            "storyCategory": category,
            "memberSlugs": member_slugs,
            "readingTime": row.get("reading_time"),
            "wordCount": row.get("word_count"),
            "publishedAt": str(row.get("published_at") or "") or None,
            "updatedAt": str(row.get("updated_at") or "") or None,
            "evidenceCount": len(evidence_items),
        },
    )


def build_dossier_search_document(
    row: dict[str, Any],
    *,
    base_url: str,
) -> SearchDocumentRecord:
    """Build a searchable record from a published CapitolExposed dossier."""

    slug = str(row.get("slug") or "").strip()
    summary = str(row.get("summary") or "").strip()
    executive_summary = str(row.get("executive_summary") or "").strip()
    methodology = str(row.get("methodology") or "").strip()
    findings = row.get("findings") if isinstance(row.get("findings"), list) else []
    finding_lines: list[str] = []
    finding_categories: list[str] = []
    for finding in findings:
        if not isinstance(finding, dict):
            continue
        category = str(finding.get("category") or "").strip()
        title = str(finding.get("title") or "").strip()
        narrative = str(finding.get("narrative") or "").strip()
        if category:
            finding_categories.append(category)
        if title:
            finding_lines.append(title)
        if narrative:
            finding_lines.append(narrative)
    content_lines = [summary, executive_summary, methodology, *finding_lines]
    severity = str(row.get("severity") or "").strip() or "unknown"
    verification_status = str(row.get("verification_status") or "").strip() or "unverified"
    source_url = f"{base_url.rstrip('/')}/dossier/{slug}"
    document = Document(
        id=f"capitol-dossier-{slug}",
        title=str(row.get("title") or "").strip(),
        date=normalize_document_date(
            row.get("updated_at") or row.get("generated_at") or row.get("reviewed_at")
        ),
        source="capitol-exposed",
        category="newsroom",
        summary=summary or executive_summary or None,
        memberIds=[str(row.get("member_id"))] if row.get("member_id") else [],
        tags=list(
            dict.fromkeys(
                [
                    "capitol-exposed",
                    "dossier",
                    "investigation",
                    severity,
                    verification_status,
                    *[category for category in finding_categories if category],
                ]
            )
        ),
        sourceUrl=source_url,
        archiveUrl=source_url,
        ocrText="\n\n".join(line for line in content_lines if line),
        verificationStatus="unverified",
    )
    return build_search_document(
        document,
        content=document.ocrText or "",
        metadata={
            "slug": slug,
            "memberId": row.get("member_id"),
            "severity": severity,
            "verificationStatus": verification_status,
            "findingCount": row.get("finding_count"),
            "generatedAt": str(row.get("generated_at") or "") or None,
            "reviewedAt": str(row.get("reviewed_at") or "") or None,
            "updatedAt": str(row.get("updated_at") or "") or None,
            "findingTitles": [
                str(finding.get("title") or "").strip()
                for finding in findings
                if isinstance(finding, dict) and str(finding.get("title") or "").strip()
            ],
        },
    )
