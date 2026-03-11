"""CLI for Capitol Pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import time
from typing import Iterable, TypeVar

import click
import httpx
import psycopg

from capitol_pipeline.bridges import (
    build_alert_search_document,
    build_bill_search_document,
    build_committee_search_document,
    build_congress_bill_context_search_document,
    build_dossier_search_document,
    build_fara_member_match_search_document,
    build_fara_registrant_search_document,
    build_house_ptr_search_document,
    build_house_ptr_search_document_from_stub_row,
    build_member_search_document,
    build_news_post_search_document,
    build_offshore_match_search_document,
    build_senate_trade_search_document,
    build_usaspending_company_match_search_document,
    build_trade_payload,
)
from capitol_pipeline.config import OcrBackend, Settings
from capitol_pipeline.exporters.neon import (
    backfill_crypto_trade_classification,
    ensure_congress_schema,
    ensure_fara_schema,
    ensure_offshore_schema,
    ensure_search_schema,
    ensure_usaspending_schema,
    fetch_existing_trade_ids,
    fetch_existing_fara_registration_numbers,
    fetch_alerts_for_search,
    fetch_bills_for_congress_sync,
    fetch_bills_for_search,
    fetch_company_candidates_for_usaspending,
    fetch_committees_for_search,
    fetch_published_dossiers,
    fetch_published_news_posts,
    fetch_members_for_search,
    fetch_search_chunk_embedding_backfill,
    fetch_house_stub_search_backfill,
    fetch_house_stub_queue,
    fetch_pipeline_corpus_status,
    fetch_senate_trade_search_backfill,
    hybrid_search,
    load_member_registry_from_neon,
    mark_house_stub_processed,
    sync_house_stubs_to_neon,
    update_search_chunk_embeddings,
    upsert_fara_documents,
    upsert_fara_foreign_principals,
    upsert_fara_member_matches,
    upsert_fara_registrants,
    upsert_fara_short_forms,
    upsert_offshore_member_matches,
    upsert_offshore_nodes,
    upsert_offshore_relationships,
    upsert_congress_bill_actions,
    upsert_congress_bill_summaries,
    upsert_congress_bill_sync,
    upsert_congress_bills,
    upsert_usaspending_awards,
    upsert_usaspending_company_matches,
    upsert_usaspending_company_sync,
    upsert_usaspending_recipients,
    update_house_stub_state,
    upsert_search_chunks,
    upsert_search_document,
    upsert_trade_rows_to_neon,
)
from capitol_pipeline.models.congress import FilingStub, HousePtrParseResult, MemberMatch, NormalizedTradeRow
from capitol_pipeline.models.fara import (
    FaraDocumentRecord,
    FaraForeignPrincipalRecord,
    FaraMemberMatchRecord,
    FaraRegistrantBundle,
    FaraRegistrantRecord,
    FaraShortFormRecord,
)
from capitol_pipeline.models.legislation import (
    CongressBillActionRecord,
    CongressBillRecord,
    CongressBillSummaryRecord,
)
from capitol_pipeline.models.offshore import OffshoreMemberMatchRecord, OffshoreNodeRecord
from capitol_pipeline.models.usaspending import (
    UsaspendingAwardRecord,
    UsaspendingCompanyMatchRecord,
    UsaspendingRecipientRecord,
)
from capitol_pipeline.normalizers.crypto_assets import classify_crypto_asset
from capitol_pipeline.parsers.house_ptr import parse_house_ptr_pdf
from capitol_pipeline.processors.chunking import build_search_chunks
from capitol_pipeline.processors.embeddings import get_embedder
from capitol_pipeline.processors.ocr import OcrProcessor
from capitol_pipeline.registries.members import MemberRegistry, load_member_registry_from_json
from capitol_pipeline.sources.congress_gov import (
    CongressGovApiClient,
    build_congress_bill_action_records,
    build_congress_bill_record,
    build_congress_bill_summary_records,
)
from capitol_pipeline.sources.house_clerk import fetch_house_feed
from capitol_pipeline.sources.fara import (
    FaraApiClient,
    fetch_bulk_foreign_principals,
    fetch_bulk_reg_documents,
    fetch_bulk_registrants,
    fetch_bulk_short_forms,
    fetch_active_registrants,
    fetch_foreign_principals,
    fetch_reg_documents,
    fetch_short_forms,
)
from capitol_pipeline.sources.icij_offshore_leaks import iter_offshore_nodes, iter_offshore_relationships
from capitol_pipeline.sources.senate_ethics import (
    fetch_quiver_bulk_congress_feed,
    fetch_senate_watcher_feed,
    normalize_quiver_senate_trade,
    normalize_senate_date,
    normalize_senate_watcher_trade,
)
from capitol_pipeline.sources.usaspending import (
    DEFAULT_USASPENDING_END_DATE,
    DEFAULT_USASPENDING_START_DATE,
    UsaspendingApiClient,
    build_company_search_queries,
    build_usaspending_award_records,
    build_usaspending_company_match_record,
    build_usaspending_recipient_record,
    fetch_recipient_profile,
    search_awards_for_recipient_name,
    search_awarding_agencies_for_recipient_name,
    search_recipient_summaries,
    score_recipient_profile_match,
)


T = TypeVar("T")


@click.group()
def cli() -> None:
    """Capitol Pipeline command line interface."""


def load_registry_if_available(
    settings: Settings,
    *,
    export_cache: bool = False,
) -> MemberRegistry | None:
    """Load the members registry from Neon or a cached JSON export if available."""

    if settings.resolved_neon_database_url:
        return load_member_registry_from_neon(settings, export_cache=export_cache)
    if settings.members_registry_path.exists():
        return load_member_registry_from_json(settings.members_registry_path)
    return None


def batched(items: Iterable[T], size: int) -> Iterable[list[T]]:
    """Yield stable batches from an iterable."""

    batch: list[T] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def group_rows_by_registration_number(items: Iterable[object]) -> dict[int, list[object]]:
    """Group FARA rows by registration number for bulk ingestion."""

    grouped: dict[int, list[object]] = {}
    for item in items:
        registration_number = getattr(item, "registration_number", None)
        if registration_number is None:
            continue
        grouped.setdefault(int(registration_number), []).append(item)
    return grouped


def now_iso() -> str:
    """Return the current UTC timestamp in ISO format."""

    return datetime.now(timezone.utc).isoformat()


def is_retryable_house_error(error: Exception) -> bool:
    """Return whether a House PTR processing error should be retried."""

    message = str(error)
    retryable_patterns = (
        "PTR PDF fetch failed with 404",
        "timeout",
        "timed out",
        "connection reset",
        "temporarily unavailable",
    )
    return any(pattern.lower() in message.lower() for pattern in retryable_patterns)


def build_retry_after_iso(error: Exception, attempts: int) -> str:
    """Return the next retry time for a transient House PTR failure."""

    base_minutes = 15 if "404" in str(error) else 10
    delay_minutes = min(base_minutes * max(1, attempts), 360)
    return datetime.fromtimestamp(
        datetime.now(timezone.utc).timestamp() + (delay_minutes * 60),
        tz=timezone.utc,
    ).isoformat()


def build_review_retry_after_iso(hours: int) -> str:
    """Return the next review retry time for a hard-to-parse House PTR."""

    delay_hours = max(1, hours)
    return (datetime.now(timezone.utc) + timedelta(hours=delay_hours)).isoformat()


def download_house_pdf(stub: FilingStub, settings: Settings, destination: Path) -> None:
    """Download a House PTR PDF to a local temporary file."""

    try:
        with httpx.Client(
            headers={"User-Agent": settings.user_agent},
            follow_redirects=True,
            timeout=30.0,
        ) as client:
            response = client.get(stub.source_url)
            response.raise_for_status()
    except httpx.HTTPStatusError as error:
        status_code = error.response.status_code if error.response is not None else "unknown"
        raise RuntimeError(
            f"PTR PDF fetch failed with {status_code} for {stub.source_url}"
        ) from error
    except httpx.HTTPError as error:
        raise RuntimeError(f"PTR PDF fetch failed for {stub.source_url}: {error}") from error
    destination.write_bytes(response.content)


def parse_live_house_stub(
    stub: FilingStub,
    settings: Settings,
    ocr_backend: str,
) -> tuple[HousePtrParseResult, list[NormalizedTradeRow]]:
    """Download and parse a live House PTR filing."""

    with TemporaryDirectory(prefix="capitol-ptr-") as temp_dir:
        pdf_path = Path(temp_dir) / f"{stub.doc_id}.pdf"
        download_house_pdf(stub, settings, pdf_path)
        return parse_house_ptr_pdf(
            pdf_path,
            stub=stub,
            settings=settings,
            backend=ocr_backend,
        )


def persist_parsed_house_stub(
    settings: Settings,
    stub: FilingStub,
    parsed: HousePtrParseResult,
    trades: list[NormalizedTradeRow],
) -> dict[str, object]:
    """Write a parsed House PTR result back into CapitolExposed."""

    sync_house_stubs_to_neon(settings, [stub])
    trade_summary = upsert_trade_rows_to_neon(settings, trades)
    status = "parsed" if trades and stub.member.id and parsed.parser_confidence >= 0.6 else "needs_review"
    mark_house_stub_processed(
        settings,
        stub,
        status=status,
        parser_confidence=parsed.parser_confidence,
        parsed_transaction_count=len(parsed.transactions),
        extracted_trade_id=trade_summary["trade_ids"][0] if trade_summary.get("trade_ids") else None,
        last_error=(
            None
            if parsed.transactions
            else "PTR text extracted but transactions need manual review"
        ),
        raw_text_preview=parsed.raw_text_preview,
        parsed_transactions=[transaction.model_dump() for transaction in parsed.transactions],
    )
    return {
        "stubs": {"upserted": 1},
        "trades": trade_summary,
        "stubStatus": status,
    }


def build_stub_from_queue_row(row: dict[str, object]) -> FilingStub:
    """Hydrate a FilingStub from a house_filing_stubs database row."""

    metadata = row.get("metadata") or {}
    if not isinstance(metadata, dict):
        metadata = {}

    first_name = str(metadata.get("firstName") or "").strip() or None
    last_name = str(metadata.get("lastName") or "").strip() or None
    member_name = str(metadata.get("memberName") or "").strip() or " ".join(
        part for part in [first_name, last_name] if part
    ).strip()

    return FilingStub(
        doc_id=str(row.get("doc_id") or ""),
        filing_year=int(row.get("filing_year") or 0),
        filing_type=str(metadata.get("filingType") or "PTR"),
        filing_date=str(metadata.get("filingDate") or "").strip() or None,
        first_name=first_name,
        last_name=last_name,
        member=MemberMatch(
            id=str(metadata.get("memberId") or "").strip() or None,
            name=member_name or f"House PTR {row.get('doc_id')}",
            slug=str(metadata.get("memberSlug") or "").strip() or None,
            party=str(metadata.get("party") or "").strip() or None,
            state=str(metadata.get("state") or "").strip() or None,
            district=str(metadata.get("district") or "").strip() or None,
        ),
        source=str(row.get("source") or "house-clerk").replace("_", "-"),
        source_url=str(row.get("source_url") or ""),
        raw_state_district=str(metadata.get("rawStateDistrict") or "").strip() or None,
    )


def index_search_document(
    settings: Settings,
    document,
    *,
    with_embeddings: bool,
    ensure_schema: bool = True,
) -> dict[str, object]:
    """Upsert a searchable document and its chunks."""

    if ensure_schema:
        ensure_search_schema(settings)
    document_summary = upsert_search_document(settings, document, ensure_schema=False)
    resolved_document_id = str(document_summary.get("document_id") or document.id)
    resolved_document = (
        document.model_copy(update={"id": resolved_document_id})
        if resolved_document_id != document.id
        else document
    )
    chunks = build_search_chunks(resolved_document, settings)
    if with_embeddings and chunks:
        embedder = get_embedder(settings)
        embeddings = embedder.embed_texts([chunk.content for chunk in chunks])
        for chunk, embedding in zip(chunks, embeddings, strict=False):
            chunk.embedding = embedding or None

    chunk_summary = upsert_search_chunks(settings, chunks, ensure_schema=False)
    return {
        "document": document_summary,
        "chunks": chunk_summary,
        "embedded": with_embeddings and any(chunk.embedding for chunk in chunks),
    }


def index_search_document_with_retry(
    settings: Settings,
    document,
    *,
    with_embeddings: bool,
    ensure_schema: bool = False,
    max_attempts: int = 3,
) -> dict[str, object]:
    """Index one document with lightweight retry for transient Neon disconnects."""

    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return index_search_document(
                settings,
                document,
                with_embeddings=with_embeddings,
                ensure_schema=ensure_schema,
            )
        except psycopg.OperationalError as error:
            last_error = error
            if attempt >= max_attempts:
                break
            time.sleep(min(5, attempt * 2))
    if last_error:
        raise last_error
    raise RuntimeError("Search indexing retry failed without a captured error.")


def process_house_queue_rows(
    settings: Settings,
    queue_rows: list[dict[str, object]],
    *,
    ocr_backend: str,
    with_search_index: bool = False,
    with_embeddings: bool = False,
    review_retry_hours: int = 12,
) -> dict[str, object]:
    """Process a batch of queued House PTR stubs from Neon."""

    summary = {
        "queued": len(queue_rows),
        "parsed": 0,
        "needsReview": 0,
        "deferred": 0,
        "failed": 0,
        "tradeRowsUpserted": 0,
        "searchDocumentsUpserted": 0,
        "searchChunksUpserted": 0,
        "processed": [],
    }

    for row in queue_rows:
        stub = build_stub_from_queue_row(row)
        current_status = str(row.get("status") or "")
        metadata = row.get("metadata") or {}
        if not isinstance(metadata, dict):
            metadata = {}
        attempts = int(metadata.get("extractionAttempts") or 0) + 1
        metadata_updates: dict[str, object] = {
            **metadata,
            "extractionStartedAt": now_iso(),
            "extractionAttempts": attempts,
            "retryAfter": None,
        }
        if current_status == "needs_review":
            metadata_updates.update(
                {
                    "reviewLastAttemptAt": now_iso(),
                    "reviewLastBackend": ocr_backend,
                    "reviewAttempts": int(metadata.get("reviewAttempts") or 0) + 1,
                }
            )
        update_house_stub_state(
            settings,
            doc_id=stub.doc_id,
            status="extracting",
            extracted_trade_id=None,
            metadata_updates=metadata_updates,
        )

        try:
            parsed, trades = parse_live_house_stub(stub, settings, ocr_backend)
            upsert_summary = persist_parsed_house_stub(settings, stub, parsed, trades)
            status = str(upsert_summary["stubStatus"])
            if status == "parsed":
                summary["parsed"] += 1
                if current_status == "needs_review":
                    update_house_stub_state(
                        settings,
                        doc_id=stub.doc_id,
                        status="parsed",
                        extracted_trade_id=None,
                        metadata_updates={
                            "retryAfter": None,
                            "reviewResolvedAt": now_iso(),
                            "reviewLastBackend": ocr_backend,
                        },
                    )
            else:
                summary["needsReview"] += 1
                review_updates: dict[str, object] = {
                    "retryAfter": build_review_retry_after_iso(review_retry_hours),
                    "needsReviewAt": now_iso(),
                }
                if current_status == "needs_review":
                    review_updates.update(
                        {
                            "reviewLastBackend": ocr_backend,
                            "reviewLastAttemptAt": now_iso(),
                        }
                    )
                update_house_stub_state(
                    settings,
                    doc_id=stub.doc_id,
                    status="needs_review",
                    extracted_trade_id=None,
                    metadata_updates=review_updates,
                )
            trade_rows = int((upsert_summary.get("trades") or {}).get("upserted", 0))  # type: ignore[union-attr]
            summary["tradeRowsUpserted"] += trade_rows

            index_summary: dict[str, object] | None = None
            if with_search_index:
                search_document = build_house_ptr_search_document(stub, parsed, trades)
                index_summary = index_search_document(
                    settings,
                    search_document,
                    with_embeddings=with_embeddings,
                )
                summary["searchDocumentsUpserted"] += int(
                    (index_summary.get("document") or {}).get("upserted", 0)  # type: ignore[union-attr]
                )
                summary["searchChunksUpserted"] += int(
                    (index_summary.get("chunks") or {}).get("upserted", 0)  # type: ignore[union-attr]
                )

            processed_item = {
                "docId": stub.doc_id,
                "status": status,
                "tradeRows": trade_rows,
            }
            if index_summary:
                processed_item["searchDocumentId"] = (index_summary.get("document") or {}).get("document_id")  # type: ignore[union-attr]
                processed_item["searchChunks"] = (index_summary.get("chunks") or {}).get("upserted", 0)  # type: ignore[union-attr]
            summary["processed"].append(processed_item)
        except Exception as error:  # pragma: no cover - depends on live upstream PDFs
            retryable = is_retryable_house_error(error)
            failed_status = "pending_extraction" if retryable else "needs_review"
            failure_updates: dict[str, object] = {
                **metadata,
                "failedAt": now_iso(),
                "lastError": str(error)[:500],
                "retryAfter": (
                    build_retry_after_iso(error, attempts)
                    if retryable
                    else build_review_retry_after_iso(review_retry_hours)
                ),
            }
            if not retryable:
                failure_updates["needsReviewAt"] = now_iso()
                if current_status == "needs_review":
                    failure_updates.update(
                        {
                            "reviewLastBackend": ocr_backend,
                            "reviewLastAttemptAt": now_iso(),
                        }
                    )
            update_house_stub_state(
                settings,
                doc_id=stub.doc_id,
                status=failed_status,
                extracted_trade_id=None,
                metadata_updates=failure_updates,
            )
            if retryable:
                summary["deferred"] += 1
            else:
                summary["failed"] += 1
            summary["processed"].append(
                {
                    "docId": stub.doc_id,
                    "status": "deferred" if retryable else "failed",
                    "error": str(error)[:200],
                }
            )

    return summary


def build_senate_trade_from_db_row(row: dict[str, object]) -> NormalizedTradeRow:
    """Hydrate a NormalizedTradeRow from an existing trades row."""

    normalized_asset = classify_crypto_asset(
        str(row.get("ticker") or "").strip() or None,
        str(row.get("asset_description") or "").strip() or None,
    )
    return NormalizedTradeRow(
        member=MemberMatch(
            id=str(row.get("member_id") or "").strip() or None,
            bioguide_id=str(row.get("member_bioguide_id") or "").strip().upper() or None,
            name=str(row.get("member_name") or "").strip() or "Unknown member",
            slug=str(row.get("member_slug") or "").strip() or None,
            party=str(row.get("member_party") or "").strip() or None,
            state=str(row.get("member_state") or "").strip() or None,
            district=str(row.get("member_district") or "").strip() or None,
        ),
        source=str(row.get("source") or "senate-watcher").replace("_", "-"),
        disclosure_kind="senate-trade",
        source_id=str(row.get("id") or ""),
        source_url=str(row.get("source_url") or "").strip() or None,
        ticker=str(row.get("ticker") or "").strip().upper() or None,
        asset_description=str(row.get("asset_description") or "").strip() or "Unknown asset",
        asset_type=str(row.get("asset_type") or "").strip() or "Stock",
        transaction_type=str(row.get("transaction_type") or "").strip() or "purchase",
        transaction_date=str(row.get("transaction_date") or "").strip() or None,
        disclosure_date=str(row.get("disclosure_date") or "").strip() or None,
        amount_min=int(row.get("amount_min") or 0),
        amount_max=int(row.get("amount_max") or 0),
        owner=str(row.get("owner") or "").strip().lower() or "self",
        comment=str(row.get("comment") or "").strip() or None,
        normalized_asset=None if normalized_asset.kind == "unrelated" else normalized_asset,
    )


def build_offshore_member_match(
    node: OffshoreNodeRecord,
    member: MemberMatch,
) -> OffshoreMemberMatchRecord:
    """Create a stable exact-name Congress match record for an Offshore node."""

    stable_key = hashlib.sha1(f"{member.id}|{node.node_key}|exact_name".encode("utf-8")).hexdigest()
    return OffshoreMemberMatchRecord(
        match_key=stable_key,
        member_id=member.id or "",
        member_name=member.name,
        member_slug=member.slug,
        node_key=node.node_key,
        node_type=node.node_type,
        source_dataset=node.source_dataset,
        match_value=node.name,
        metadata={
            "normalizedName": node.normalized_name,
            "countries": node.countries,
            "countryCodes": node.country_codes,
            "jurisdiction": node.jurisdiction_description or node.jurisdiction,
        },
    )


def build_fara_member_match(
    registrant: FaraRegistrantRecord,
    member: MemberMatch,
    *,
    entity_kind: str,
    entity_key: str,
    match_value: str,
    metadata: dict[str, object] | None = None,
) -> FaraMemberMatchRecord:
    """Create a stable exact-name Congress match record for a FARA entity."""

    stable_key = hashlib.sha1(
        f"{member.id}|{entity_kind}|{entity_key}|{match_value}|exact_name".encode("utf-8")
    ).hexdigest()
    return FaraMemberMatchRecord(
        match_key=stable_key,
        member_id=member.id or "",
        member_name=member.name,
        member_slug=member.slug,
        registration_number=registrant.registration_number,
        entity_kind=entity_kind,
        entity_key=entity_key,
        registrant_name=registrant.name,
        match_value=match_value,
        metadata=metadata or {},
    )


@cli.command("house-feed")
@click.option("--year", type=int, required=True, help="Disclosure year, for example 2026.")
@click.option("--limit", type=int, default=10, show_default=True)
@click.option("--resolve-members/--no-resolve-members", default=True, show_default=True)
@click.option("--export-registry/--no-export-registry", default=False, show_default=True)
def house_feed(
    year: int,
    limit: int,
    resolve_members: bool,
    export_registry: bool,
) -> None:
    """Fetch and print the latest House filing stubs."""

    settings = Settings()
    registry = load_registry_if_available(settings, export_cache=export_registry) if resolve_members else None
    rows = fetch_house_feed(
        year,
        resolver=registry.resolve_feed_member if registry else None,
        settings=settings,
    )
    click.echo(json.dumps([row.model_dump() for row in rows[:limit]], indent=2))


@cli.command("sync-house-feed")
@click.option("--year", type=int, required=True, help="Disclosure year, for example 2026.")
@click.option("--limit", type=int, default=0, show_default=True, help="0 means sync all rows.")
@click.option("--resolve-members/--no-resolve-members", default=True, show_default=True)
@click.option("--export-registry/--no-export-registry", default=True, show_default=True)
def sync_house_feed_command(
    year: int,
    limit: int,
    resolve_members: bool,
    export_registry: bool,
) -> None:
    """Fetch House filing stubs, resolve members, and upsert them into Neon."""

    settings = Settings()
    registry = load_registry_if_available(settings, export_cache=export_registry) if resolve_members else None
    rows = fetch_house_feed(
        year,
        resolver=registry.resolve_feed_member if registry else None,
        settings=settings,
    )
    if limit > 0:
        rows = rows[:limit]

    summary = sync_house_stubs_to_neon(settings, rows)
    resolved = sum(1 for row in rows if row.member.id)
    click.echo(
        json.dumps(
            {
                "year": year,
                "fetched": len(rows),
                "resolvedMembers": resolved,
                "unresolvedMembers": len(rows) - resolved,
                "summary": summary,
            },
            indent=2,
        )
    )


@cli.command("senate-feed")
@click.option("--limit", type=int, default=10, show_default=True)
@click.option(
    "--provider",
    type=click.Choice(["auto", "quiver", "watcher"]),
    default="auto",
    show_default=True,
)
def senate_feed(limit: int, provider: str) -> None:
    """Fetch and print the current Senate feed rows."""

    settings = Settings()
    feed_provider = (
        "quiver" if provider == "auto" and settings.resolved_quiver_api_token else provider
    )
    if feed_provider == "auto":
        feed_provider = "watcher"

    if feed_provider == "quiver":
        rows = fetch_quiver_bulk_congress_feed(
            settings,
            page=1,
            page_size=max(limit, 10),
        )
        senate_rows = [
            row.model_dump(by_alias=True)
            for row in rows
            if str(row.chamber or row.house or "").strip().lower() == "senate"
        ]
        click.echo(json.dumps(senate_rows[:limit], indent=2))
        return

    rows = fetch_senate_watcher_feed(settings)
    click.echo(json.dumps([row.model_dump() for row in rows[:limit]], indent=2))


@cli.command("senate-ingest")
@click.option("--limit", type=int, default=0, show_default=True, help="0 means ingest every newly detected Senate row.")
@click.option("--export-registry/--no-export-registry", default=True, show_default=True)
@click.option("--with-search-index/--no-search-index", default=True, show_default=True)
@click.option("--with-embeddings/--no-embeddings", default=False, show_default=True)
@click.option(
    "--provider",
    type=click.Choice(["auto", "quiver", "watcher"]),
    default="auto",
    show_default=True,
)
@click.option(
    "--start-date",
    type=str,
    default="2021-01-01",
    show_default=True,
    help="Ignore Quiver rows filed before this date.",
)
@click.option("--page-size", type=int, default=250, show_default=True)
def senate_ingest_command(
    limit: int,
    export_registry: bool,
    with_search_index: bool,
    with_embeddings: bool,
    provider: str,
    start_date: str,
    page_size: int,
) -> None:
    """Normalize new Senate trade rows and upsert them into CapitolExposed."""

    settings = Settings()
    registry = load_registry_if_available(settings, export_cache=export_registry)
    if not registry:
        raise click.ClickException("Member registry is required for Senate ingest.")

    feed_provider = "quiver" if provider == "auto" and settings.resolved_quiver_api_token else provider
    if feed_provider == "auto":
        feed_provider = "watcher"

    existing_trade_ids = fetch_existing_trade_ids(
        settings,
        sources=(
            ["senate_quiver", "senate-quiver"]
            if feed_provider == "quiver"
            else ["senate_watcher", "senate-watcher"]
        ),
    )
    if with_search_index:
        ensure_search_schema(settings)

    summary: dict[str, object] = {
        "provider": feed_provider,
        "fetched": 0,
        "normalized": 0,
        "skippedExisting": 0,
        "skippedUnresolved": 0,
        "skippedBeforeStartDate": 0,
        "tradesUpserted": 0,
        "searchDocumentsUpserted": 0,
        "searchChunksUpserted": 0,
        "embedded": 0,
        "processedSample": [],
    }

    normalized_rows: list[NormalizedTradeRow] = []
    search_documents = []

    def handle_normalized_row(normalized: NormalizedTradeRow | None) -> bool:
        if normalized is None:
            summary["skippedUnresolved"] = int(summary["skippedUnresolved"]) + 1
            return False
        if normalized.disclosure_date and normalized.disclosure_date < start_date:
            summary["skippedBeforeStartDate"] = int(summary["skippedBeforeStartDate"]) + 1
            return False
        trade_id = build_trade_payload(normalized)["id"]
        if trade_id in existing_trade_ids:
            summary["skippedExisting"] = int(summary["skippedExisting"]) + 1
            return False
        normalized_rows.append(normalized)
        existing_trade_ids.add(trade_id)
        summary["normalized"] = int(summary["normalized"]) + 1
        if with_search_index:
            search_documents.append(build_senate_trade_search_document(normalized))
        return limit > 0 and len(normalized_rows) >= limit

    if feed_provider == "quiver":
        page = 1
        while True:
            page_rows = fetch_quiver_bulk_congress_feed(
                settings,
                page=page,
                page_size=page_size,
            )
            if not page_rows:
                break
            summary["fetched"] = int(summary["fetched"]) + len(page_rows)
            stop = False
            for feed_row in page_rows:
                stop = handle_normalized_row(
                    normalize_quiver_senate_trade(feed_row, registry),
                )
                if stop:
                    break
            if stop or len(page_rows) < page_size:
                break
            page += 1
            time.sleep(0.35)
    else:
        feed_rows = sorted(
            fetch_senate_watcher_feed(settings),
            key=lambda row: normalize_senate_date(row.transaction_date) or "",
            reverse=True,
        )
        summary["fetched"] = len(feed_rows)
        for feed_row in feed_rows:
            stop = handle_normalized_row(
                normalize_senate_watcher_trade(feed_row, registry),
            )
            if stop:
                break

    if normalized_rows:
        trade_summary = upsert_trade_rows_to_neon(settings, normalized_rows)
        summary["tradesUpserted"] = int(trade_summary.get("upserted", 0))

        if with_search_index:
            for search_document in search_documents:
                index_summary = index_search_document_with_retry(
                    settings,
                    search_document,
                    with_embeddings=with_embeddings,
                    ensure_schema=False,
                )
                summary["searchDocumentsUpserted"] = int(summary["searchDocumentsUpserted"]) + int(
                    (index_summary.get("document") or {}).get("upserted", 0)  # type: ignore[union-attr]
                )
                summary["searchChunksUpserted"] = int(summary["searchChunksUpserted"]) + int(
                    (index_summary.get("chunks") or {}).get("upserted", 0)  # type: ignore[union-attr]
                )
                summary["embedded"] = int(summary["embedded"]) + (
                    1 if index_summary.get("embedded") else 0
                )
    for row in normalized_rows[:20]:
        summary["processedSample"].append(  # type: ignore[union-attr]
            {
                "id": build_trade_payload(row)["id"],
                "member": row.member.name,
                "ticker": row.ticker,
                "transactionDate": row.transaction_date,
                "transactionType": row.transaction_type,
            }
        )

    click.echo(json.dumps(summary, indent=2))


@cli.command("backfill-senate-search")
@click.option("--limit", type=int, default=0, show_default=True, help="0 means scan every eligible Senate trade row.")
@click.option("--source", type=str, default=None, help="Optional trades.source filter.")
@click.option("--only-missing/--include-existing", default=True, show_default=True)
@click.option("--with-embeddings/--no-embeddings", default=False, show_default=True)
def backfill_senate_search_command(
    limit: int,
    source: str | None,
    only_missing: bool,
    with_embeddings: bool,
) -> None:
    """Backfill pipeline search documents for existing Senate trade rows."""

    settings = Settings()
    source_filters = None
    if source:
        normalized = source.strip()
        source_filters = sorted({normalized, normalized.replace("_", "-"), normalized.replace("-", "_")})

    rows = fetch_senate_trade_search_backfill(
        settings,
        limit=limit,
        only_missing=only_missing,
        sources=source_filters,
    )
    summary = {
        "queued": len(rows),
        "documentsUpserted": 0,
        "chunksUpserted": 0,
        "embedded": 0,
        "source": source,
        "processedSample": [],
    }

    for row in rows:
        trade = build_senate_trade_from_db_row(row)
        index_summary = index_search_document_with_retry(
            settings,
            build_senate_trade_search_document(trade),
            with_embeddings=with_embeddings,
            ensure_schema=False,
        )
        summary["documentsUpserted"] += int(
            (index_summary.get("document") or {}).get("upserted", 0)  # type: ignore[union-attr]
        )
        summary["chunksUpserted"] += int(
            (index_summary.get("chunks") or {}).get("upserted", 0)  # type: ignore[union-attr]
        )
        summary["embedded"] += 1 if index_summary.get("embedded") else 0
        if len(summary["processedSample"]) < 25:
            summary["processedSample"].append(  # type: ignore[union-attr]
                {
                    "tradeId": trade.source_id,
                    "member": trade.member.name,
                    "ticker": trade.ticker,
                    "source": trade.source,
                }
            )

    click.echo(json.dumps(summary, indent=2))


@cli.command("classify-crypto")
@click.option("--ticker", type=str, default=None)
@click.option("--description", type=str, default=None)
def classify_crypto(ticker: str | None, description: str | None) -> None:
    """Classify a security as direct crypto, ETF/trust, adjacent equity, or unrelated."""

    click.echo(json.dumps(classify_crypto_asset(ticker, description).model_dump(), indent=2))


@cli.command("backfill-crypto-trades")
@click.option("--limit", type=int, default=0, show_default=True, help="0 means scan every likely crypto-linked trade.")
@click.option("--only-unclassified/--include-classified", default=True, show_default=True)
def backfill_crypto_trades_command(
    limit: int,
    only_unclassified: bool,
) -> None:
    """Normalize existing CapitolExposed trade rows into crypto classes."""

    settings = Settings()
    summary = backfill_crypto_trade_classification(
        settings,
        limit=limit,
        only_unclassified=only_unclassified,
    )
    click.echo(json.dumps(summary, indent=2))


@cli.command("ensure-search-schema")
def ensure_search_schema_command() -> None:
    """Create the search tables and indexes used for lexical and vector retrieval."""

    settings = Settings()
    summary = ensure_search_schema(settings)
    click.echo(json.dumps(summary, indent=2))


@cli.command("ensure-offshore-schema")
def ensure_offshore_schema_command() -> None:
    """Create the Offshore raw corpus tables and Congress match table."""

    settings = Settings()
    summary = ensure_offshore_schema(settings)
    click.echo(json.dumps(summary, indent=2))


@cli.command("ensure-fara-schema")
def ensure_fara_schema_command() -> None:
    """Create the official FARA raw corpus tables and Congress match table."""

    settings = Settings()
    summary = ensure_fara_schema(settings)
    click.echo(json.dumps(summary, indent=2))


@cli.command("ensure-congress-schema")
def ensure_congress_schema_command() -> None:
    """Create the official Congress.gov bill context tables."""

    settings = Settings()
    summary = ensure_congress_schema(settings)
    click.echo(json.dumps(summary, indent=2))


@cli.command("corpus-status")
def corpus_status_command() -> None:
    """Print a compact status snapshot for pipeline-managed corpora and retrieval."""

    settings = Settings()
    summary = fetch_pipeline_corpus_status(settings)
    click.echo(json.dumps(summary, indent=2))


@cli.command("ingest-usaspending")
@click.option("--limit-companies", type=int, default=25, show_default=True)
@click.option("--stale-after-days", type=int, default=30, show_default=True)
@click.option("--recipient-limit", type=int, default=8, show_default=True)
@click.option("--match-limit", type=int, default=2, show_default=True)
@click.option("--award-limit", type=int, default=12, show_default=True)
@click.option("--start-date", type=str, default=DEFAULT_USASPENDING_START_DATE, show_default=True)
@click.option("--end-date", type=str, default=DEFAULT_USASPENDING_END_DATE, show_default=True)
@click.option("--only-stale/--include-fresh", default=True, show_default=True)
@click.option("--with-search-index/--no-search-index", default=True, show_default=True)
@click.option("--with-embeddings/--no-embeddings", default=False, show_default=True)
def ingest_usaspending_command(
    limit_companies: int,
    stale_after_days: int,
    recipient_limit: int,
    match_limit: int,
    award_limit: int,
    start_date: str,
    end_date: str,
    only_stale: bool,
    with_search_index: bool,
    with_embeddings: bool,
) -> None:
    """Ingest federal recipient and award data from USAspending for tracked site companies."""

    settings = Settings()
    ensure_usaspending_schema(settings)
    if with_search_index:
        ensure_search_schema(settings)

    api_client = UsaspendingApiClient(settings)
    company_rows = fetch_company_candidates_for_usaspending(
        settings,
        limit=limit_companies,
        only_stale=only_stale,
        stale_after_days=stale_after_days,
    )

    summary = {
        "companiesQueued": len(company_rows),
        "companiesMatched": 0,
        "recipientsUpserted": 0,
        "companyMatchesUpserted": 0,
        "awardsUpserted": 0,
        "searchDocumentsUpserted": 0,
        "searchChunksUpserted": 0,
        "embedded": 0,
        "processed": [],
        "failed": [],
    }

    for row in company_rows:
        company_id = str(row.get("id") or "").strip()
        ticker = str(row.get("ticker") or "").strip().upper() or None
        company_name = str(row.get("name") or "").strip()
        sample_asset_description = str(row.get("sample_asset_description") or "").strip() or None
        queries = build_company_search_queries(
            raw_name=company_name,
            asset_description=sample_asset_description,
            ticker=ticker,
        )
        if not queries:
            upsert_usaspending_company_sync(
                settings,
                company_id=company_id,
                ticker=ticker,
                company_name=company_name,
                query_name=None,
                status="no_query",
                result_count=0,
                metadata={"sampleAssetDescription": sample_asset_description},
            )
            summary["processed"].append(
                {
                    "companyId": company_id,
                    "ticker": ticker,
                    "status": "no_query",
                }
            )
            continue

        try:
            recipient_candidates_by_id: dict[str, dict[str, object]] = {}
            attempted_queries: list[str] = []
            query_errors: list[dict[str, str]] = []

            for query_name in queries[:4]:
                attempted_queries.append(query_name)
                try:
                    recipient_rows = search_recipient_summaries(
                        settings,
                        query_name=query_name,
                        start_date=start_date,
                        end_date=end_date,
                        limit=recipient_limit,
                        client=api_client,
                    )
                    for recipient_row in recipient_rows:
                        recipient_id = str(recipient_row.get("recipient_id") or "").strip()
                        if not recipient_id:
                            continue
                        recipient_record = build_usaspending_recipient_record(
                            recipient_row,
                            query_name=query_name,
                        )
                        profile = fetch_recipient_profile(
                            settings,
                            recipient_id=recipient_record.recipient_id,
                            client=api_client,
                        )
                        match_score = score_recipient_profile_match(
                            query_name=query_name,
                            row=recipient_row,
                            profile=profile,
                        )
                        if match_score < 55:
                            continue
                        if profile:
                            merged_metadata = {
                                **recipient_record.metadata,
                                "recipientProfile": profile,
                                "totalTransactions": profile.get("total_transactions"),
                                "parentName": profile.get("parent_name"),
                                "recipientState": ((profile.get("location") or {}) if isinstance(profile.get("location"), dict) else {}).get("state_code"),
                            }
                            recipient_record = recipient_record.model_copy(
                                update={
                                    "uei": str(profile.get("uei") or recipient_record.uei or "").strip() or recipient_record.uei,
                                    "recipient_code": str(profile.get("duns") or recipient_record.recipient_code or "").strip() or recipient_record.recipient_code,
                                    "total_amount": (
                                        float(profile.get("total_transaction_amount"))
                                        if isinstance(profile.get("total_transaction_amount"), (int, float))
                                        else recipient_record.total_amount
                                    ),
                                    "metadata": merged_metadata,
                                }
                            )
                        candidate = {
                            "record": recipient_record,
                            "queryName": query_name,
                            "score": match_score,
                            "profile": profile,
                        }
                        existing = recipient_candidates_by_id.get(recipient_record.recipient_id)
                        if existing is None:
                            recipient_candidates_by_id[recipient_record.recipient_id] = candidate
                            continue
                        existing_record = existing["record"]
                        existing_score = int(existing.get("score") or 0)
                        existing_amount = (
                            existing_record.total_amount
                            if isinstance(existing_record, UsaspendingRecipientRecord)
                            else 0
                        ) or 0
                        current_amount = recipient_record.total_amount or 0
                        if match_score > existing_score or (
                            match_score == existing_score and current_amount > existing_amount
                        ):
                            recipient_candidates_by_id[recipient_record.recipient_id] = candidate
                except Exception as error:
                    query_errors.append(
                        {
                            "queryName": query_name,
                            "error": str(error)[:240],
                        }
                    )
                    continue

            recipient_candidates = sorted(
                recipient_candidates_by_id.values(),
                key=lambda item: (
                    -int(item.get("score") or 0),
                    -(
                        (
                            item["record"].total_amount
                            if isinstance(item.get("record"), UsaspendingRecipientRecord)
                            else 0
                        )
                        or 0
                    ),
                    (
                        item["record"].name
                        if isinstance(item.get("record"), UsaspendingRecipientRecord)
                        else ""
                    ),
                ),
            )[: max(1, match_limit)]
            recipient_records = [
                item["record"]
                for item in recipient_candidates
                if isinstance(item.get("record"), UsaspendingRecipientRecord)
            ]
            recipient_query_map = {
                item["record"].recipient_id: str(item.get("queryName") or item["record"].query_name)
                for item in recipient_candidates
                if isinstance(item.get("record"), UsaspendingRecipientRecord)
            }

            if not recipient_records:
                failure_status = "error" if query_errors else "no_match"
                upsert_usaspending_company_sync(
                    settings,
                    company_id=company_id,
                    ticker=ticker,
                    company_name=company_name,
                    query_name=queries[0],
                    status=failure_status,
                    result_count=0,
                    last_error=query_errors[0]["error"] if query_errors else None,
                    metadata={
                        "attemptedQueries": attempted_queries,
                        "queryErrors": query_errors,
                        "sampleAssetDescription": sample_asset_description,
                    },
                )
                summary["processed"].append(
                    {
                        "companyId": company_id,
                        "ticker": ticker,
                        "status": failure_status,
                        "attemptedQueries": attempted_queries,
                        "queryErrors": query_errors,
                    }
                )
                continue

            recipient_summary = upsert_usaspending_recipients(
                settings,
                recipient_records,
                ensure_schema=False,
            )
            summary["recipientsUpserted"] += int(recipient_summary["upserted"])

            company_match_records: list[UsaspendingCompanyMatchRecord] = []
            award_records: list[UsaspendingAwardRecord] = []
            recipient_errors: list[dict[str, str]] = []

            for recipient_record in recipient_records:
                award_rows: list[dict[str, object]] = []
                agency_rows: list[dict[str, object]] = []
                try:
                    award_rows = search_awards_for_recipient_name(
                        settings,
                        recipient_name=recipient_record.name,
                        start_date=start_date,
                        end_date=end_date,
                        limit=max(3, award_limit),
                        client=api_client,
                    )
                    if len(award_rows) < 3:
                        agency_rows = search_awarding_agencies_for_recipient_name(
                            settings,
                            recipient_name=recipient_record.name,
                            start_date=start_date,
                            end_date=end_date,
                            limit=max(5, award_limit),
                            client=api_client,
                        )
                except Exception as error:
                    recipient_errors.append(
                        {
                            "recipientId": recipient_record.recipient_id,
                            "recipientName": recipient_record.name,
                            "error": str(error)[:240],
                        }
                    )
                match_record = build_usaspending_company_match_record(
                    company_id=company_id,
                    company_name=company_name,
                    ticker=ticker,
                    query_name=recipient_query_map.get(recipient_record.recipient_id, recipient_record.query_name),
                    recipient=recipient_record,
                    awards=award_rows,
                    agencies=agency_rows,
                )
                company_match_records.append(match_record)
                award_records.extend(
                    build_usaspending_award_records(
                        match=match_record,
                        awards=award_rows,
                    )
                )

            company_match_summary = upsert_usaspending_company_matches(
                settings,
                company_match_records,
                ensure_schema=False,
            )
            summary["companyMatchesUpserted"] += int(company_match_summary["upserted"])

            awards_summary = upsert_usaspending_awards(
                settings,
                award_records,
                ensure_schema=False,
            )
            summary["awardsUpserted"] += int(awards_summary["upserted"])

            if with_search_index:
                for match_record in company_match_records:
                    match_awards = [
                        award
                        for award in award_records
                        if award.match_key == match_record.match_key
                    ]
                    index_summary = index_search_document(
                        settings,
                        build_usaspending_company_match_search_document(match_record, match_awards),
                        with_embeddings=with_embeddings,
                        ensure_schema=False,
                    )
                    summary["searchDocumentsUpserted"] += int(
                        (index_summary.get("document") or {}).get("upserted", 0)  # type: ignore[union-attr]
                    )
                    summary["searchChunksUpserted"] += int(
                        (index_summary.get("chunks") or {}).get("upserted", 0)  # type: ignore[union-attr]
                    )
                    summary["embedded"] += 1 if index_summary.get("embedded") else 0

            upsert_usaspending_company_sync(
                settings,
                company_id=company_id,
                ticker=ticker,
                company_name=company_name,
                query_name=queries[0],
                status="matched",
                result_count=len(company_match_records),
                metadata={
                    "attemptedQueries": attempted_queries,
                    "queryErrors": query_errors,
                    "recipientIds": [record.recipient_id for record in recipient_records],
                    "awardCount": len(award_records),
                    "recipientErrors": recipient_errors,
                    "sampleAssetDescription": sample_asset_description,
                },
            )
            summary["companiesMatched"] += 1
            summary["processed"].append(
                {
                    "companyId": company_id,
                    "ticker": ticker,
                    "status": "matched",
                    "recipientCount": len(recipient_records),
                    "federalAwardCount": sum(record.award_count for record in company_match_records),
                    "topAgencies": company_match_records[0].top_agencies[:3] if company_match_records else [],
                    "queries": attempted_queries,
                }
            )
        except Exception as error:
            upsert_usaspending_company_sync(
                settings,
                company_id=company_id,
                ticker=ticker,
                company_name=company_name,
                query_name=queries[0] if queries else None,
                status="error",
                result_count=0,
                last_error=str(error)[:500],
                metadata={
                    "attemptedQueries": queries,
                    "sampleAssetDescription": sample_asset_description,
                },
            )
            summary["failed"].append(
                {
                    "companyId": company_id,
                    "ticker": ticker,
                    "error": str(error)[:240],
                }
            )

    click.echo(json.dumps(summary, indent=2))


@cli.command("sync-congress-bills")
@click.option("--limit-bills", type=int, default=50, show_default=True)
@click.option("--stale-after-days", type=int, default=7, show_default=True)
@click.option("--rate-limit-cooldown-hours", type=int, default=12, show_default=True)
@click.option("--only-stale/--include-fresh", default=True, show_default=True)
@click.option("--with-search-index/--no-search-index", default=True, show_default=True)
@click.option("--with-embeddings/--no-embeddings", default=False, show_default=True)
def sync_congress_bills_command(
    limit_bills: int,
    stale_after_days: int,
    rate_limit_cooldown_hours: int,
    only_stale: bool,
    with_search_index: bool,
    with_embeddings: bool,
) -> None:
    """Sync official Congress.gov bill summaries and actions for tracked bills."""

    settings = Settings()
    ensure_congress_schema(settings)
    if with_search_index:
        ensure_search_schema(settings)

    effective_limit = limit_bills
    if settings.using_demo_congress_api_key and effective_limit > 5:
        effective_limit = 5

    client = CongressGovApiClient(settings)
    bill_rows = fetch_bills_for_congress_sync(
        settings,
        limit=effective_limit,
        only_stale=only_stale,
        stale_after_days=stale_after_days,
        rate_limit_cooldown_hours=rate_limit_cooldown_hours,
    )

    summary = {
        "billsQueued": len(bill_rows),
        "billLimitRequested": limit_bills,
        "billLimitUsed": effective_limit,
        "rateLimitCooldownHours": rate_limit_cooldown_hours,
        "usingDemoKey": settings.using_demo_congress_api_key,
        "billsSynced": 0,
        "billsErrored": 0,
        "rateLimited": False,
        "billRecordsUpserted": 0,
        "summariesUpserted": 0,
        "actionsUpserted": 0,
        "searchDocumentsUpserted": 0,
        "searchChunksUpserted": 0,
        "embedded": 0,
        "processed": [],
        "failed": [],
    }

    for row in bill_rows:
        site_bill_id = str(row.get("id") or "").strip()
        congress = int(row.get("congress") or 0)
        bill_type = str(row.get("bill_type") or "").strip()
        bill_number = str(row.get("number") or "").strip()
        try:
            bill_payload = client.fetch_bill_detail(congress, bill_type, bill_number)
            summary_items = client.fetch_bill_summaries(congress, bill_type, bill_number)
            action_items = client.fetch_bill_actions(congress, bill_type, bill_number)

            bill_record: CongressBillRecord = build_congress_bill_record(
                site_bill_row=row,
                bill_payload=bill_payload,
                summaries=summary_items,
                actions=action_items,
            )
            summary_records: list[CongressBillSummaryRecord] = build_congress_bill_summary_records(
                site_bill_id=site_bill_id,
                congress=bill_record.congress,
                bill_type=bill_record.bill_type,
                bill_number=bill_record.bill_number,
                bill_key=bill_record.bill_key,
                source_url=bill_record.legislation_url,
                items=summary_items,
            )
            action_records: list[CongressBillActionRecord] = build_congress_bill_action_records(
                site_bill_id=site_bill_id,
                congress=bill_record.congress,
                bill_type=bill_record.bill_type,
                bill_number=bill_record.bill_number,
                bill_key=bill_record.bill_key,
                source_url=bill_record.legislation_url,
                items=action_items,
            )

            bill_upsert_summary = upsert_congress_bills(settings, [bill_record])
            summaries_upsert_summary = upsert_congress_bill_summaries(settings, summary_records)
            actions_upsert_summary = upsert_congress_bill_actions(settings, action_records)

            index_summary: dict[str, object] | None = None
            if with_search_index:
                search_document = build_congress_bill_context_search_document(
                    bill_record,
                    summary_records,
                    action_records,
                    site_row=row,
                )
                index_summary = index_search_document_with_retry(
                    settings,
                    search_document,
                    with_embeddings=with_embeddings,
                    ensure_schema=False,
                )
                summary["searchDocumentsUpserted"] += int(
                    (index_summary.get("document") or {}).get("upserted", 0)  # type: ignore[union-attr]
                )
                summary["searchChunksUpserted"] += int(
                    (index_summary.get("chunks") or {}).get("upserted", 0)  # type: ignore[union-attr]
                )
                summary["embedded"] += 1 if index_summary.get("embedded") else 0

            upsert_congress_bill_sync(
                settings,
                site_bill_id=site_bill_id,
                bill_key=bill_record.bill_key,
                congress=bill_record.congress,
                bill_type=bill_record.bill_type,
                bill_number=bill_record.bill_number,
                last_status="ok",
                source_update_date=bill_record.update_date,
                source_update_date_including_text=bill_record.update_date_including_text,
                summaries_count=len(summary_records),
                actions_count=len(action_records),
                metadata={
                    "title": bill_record.title,
                    "policyArea": bill_record.policy_area,
                    "legislationUrl": bill_record.legislation_url,
                },
            )

            summary["billsSynced"] += 1
            summary["billRecordsUpserted"] += int(bill_upsert_summary["upserted"])
            summary["summariesUpserted"] += int(summaries_upsert_summary["upserted"])
            summary["actionsUpserted"] += int(actions_upsert_summary["upserted"])
            summary["processed"].append(
                {
                    "siteBillId": site_bill_id,
                    "billKey": bill_record.bill_key,
                    "title": bill_record.title,
                    "summaries": len(summary_records),
                    "actions": len(action_records),
                    "policyArea": bill_record.policy_area,
                    "documentId": (index_summary.get("document") or {}).get("document_id") if index_summary else None,  # type: ignore[union-attr]
                }
            )
        except httpx.HTTPStatusError as error:
            status_code = error.response.status_code if error.response is not None else None
            if status_code == 429:
                upsert_congress_bill_sync(
                    settings,
                    site_bill_id=site_bill_id,
                    bill_key=None,
                    congress=congress,
                    bill_type=bill_type,
                    bill_number=bill_number,
                    last_status="rate_limited",
                    source_update_date=None,
                    source_update_date_including_text=None,
                    summaries_count=0,
                    actions_count=0,
                    last_error=str(error)[:500],
                    metadata={"title": row.get("title"), "apiKeyMode": "demo" if settings.using_demo_congress_api_key else "configured"},
                )
                summary["rateLimited"] = True
                summary["failed"].append(
                    {
                        "siteBillId": site_bill_id,
                        "congress": congress,
                        "billType": bill_type,
                        "billNumber": bill_number,
                        "error": str(error)[:240],
                    }
                )
                break
            upsert_congress_bill_sync(
                settings,
                site_bill_id=site_bill_id,
                bill_key=None,
                congress=congress,
                bill_type=bill_type,
                bill_number=bill_number,
                last_status="error",
                source_update_date=None,
                source_update_date_including_text=None,
                summaries_count=0,
                actions_count=0,
                last_error=str(error)[:500],
                metadata={"title": row.get("title")},
            )
            summary["billsErrored"] += 1
            summary["failed"].append(
                {
                    "siteBillId": site_bill_id,
                    "congress": congress,
                    "billType": bill_type,
                    "billNumber": bill_number,
                    "error": str(error)[:240],
                }
            )
        except Exception as error:
            upsert_congress_bill_sync(
                settings,
                site_bill_id=site_bill_id,
                bill_key=None,
                congress=congress,
                bill_type=bill_type,
                bill_number=bill_number,
                last_status="error",
                source_update_date=None,
                source_update_date_including_text=None,
                summaries_count=0,
                actions_count=0,
                last_error=str(error)[:500],
                metadata={"title": row.get("title")},
            )
            summary["billsErrored"] += 1
            summary["failed"].append(
                {
                    "siteBillId": site_bill_id,
                    "congress": congress,
                    "billType": bill_type,
                    "billNumber": bill_number,
                    "error": str(error)[:240],
                }
            )

    click.echo(json.dumps(summary, indent=2))


@cli.command("ingest-offshore-leaks")
@click.option("--node-batch-size", type=int, default=5000, show_default=True)
@click.option("--relationship-batch-size", type=int, default=10000, show_default=True)
@click.option("--node-limit-per-type", type=int, default=0, show_default=True, help="0 means full file.")
@click.option("--relationship-limit", type=int, default=0, show_default=True, help="0 means full file.")
@click.option("--skip-nodes/--include-nodes", default=False, show_default=True)
@click.option("--skip-relationships/--include-relationships", default=False, show_default=True)
@click.option("--skip-match-index/--with-match-index", default=False, show_default=True)
@click.option("--with-embeddings/--no-embeddings", default=False, show_default=True)
@click.option("--export-registry/--no-export-registry", default=True, show_default=True)
def ingest_offshore_leaks_command(
    node_batch_size: int,
    relationship_batch_size: int,
    node_limit_per_type: int,
    relationship_limit: int,
    skip_nodes: bool,
    skip_relationships: bool,
    skip_match_index: bool,
    with_embeddings: bool,
    export_registry: bool,
) -> None:
    """Ingest the official ICIJ Offshore Leaks corpus into Neon."""

    settings = Settings()
    ensure_offshore_schema(settings)
    registry = load_registry_if_available(settings, export_cache=export_registry)
    if not registry:
        raise click.ClickException("Member registry is required for Offshore match extraction.")
    if not skip_match_index:
        ensure_search_schema(settings)

    summary: dict[str, object] = {
        "nodesUpserted": 0,
        "relationshipsUpserted": 0,
        "memberMatchesUpserted": 0,
        "matchDocumentsUpserted": 0,
        "matchChunksUpserted": 0,
        "nodeBatches": 0,
        "relationshipBatches": 0,
        "datasets": {},
    }

    if not skip_nodes:
        for batch in batched(
            iter_offshore_nodes(settings, limit_per_type=node_limit_per_type),
            max(1, node_batch_size),
        ):
            upsert_offshore_nodes(settings, batch, ensure_schema=False)
            summary["nodesUpserted"] = int(summary["nodesUpserted"]) + len(batch)
            summary["nodeBatches"] = int(summary["nodeBatches"]) + 1
            datasets = summary["datasets"]
            for node in batch:
                datasets[node.source_dataset] = int(datasets.get(node.source_dataset, 0)) + 1  # type: ignore[union-attr]

            matches = [
                build_offshore_member_match(node, match)
                for node in batch
                for match in [registry.resolve(name=node.name)]
                if match and match.id
            ]
            if matches:
                upsert_offshore_member_matches(settings, matches, ensure_schema=False)
                summary["memberMatchesUpserted"] = int(summary["memberMatchesUpserted"]) + len(matches)
                if not skip_match_index:
                    for match_record in matches:
                        node = next(item for item in batch if item.node_key == match_record.node_key)
                        member = registry.resolve(name=match_record.member_name)
                        if not member:
                            continue
                        search_document = build_offshore_match_search_document(node, member)
                        index_summary = index_search_document(
                            settings,
                            search_document,
                            with_embeddings=with_embeddings,
                            ensure_schema=False,
                        )
                        summary["matchDocumentsUpserted"] = int(summary["matchDocumentsUpserted"]) + int(
                            (index_summary.get("document") or {}).get("upserted", 0)  # type: ignore[union-attr]
                        )
                        summary["matchChunksUpserted"] = int(summary["matchChunksUpserted"]) + int(
                            (index_summary.get("chunks") or {}).get("upserted", 0)  # type: ignore[union-attr]
                        )

    if not skip_relationships:
        for batch in batched(
            iter_offshore_relationships(settings, limit=relationship_limit),
            max(1, relationship_batch_size),
        ):
            upsert_offshore_relationships(settings, batch, ensure_schema=False)
            summary["relationshipsUpserted"] = int(summary["relationshipsUpserted"]) + len(batch)
            summary["relationshipBatches"] = int(summary["relationshipBatches"]) + 1

    click.echo(json.dumps(summary, indent=2))


@cli.command("ingest-fara")
@click.option(
    "--mode",
    type=click.Choice(["bulk", "api"]),
    default="bulk",
    show_default=True,
    help="Use the official daily bulk ZIPs or the per-registrant API.",
)
@click.option("--limit-registrants", type=int, default=0, show_default=True, help="0 means all active registrants.")
@click.option("--offset-registrants", type=int, default=0, show_default=True)
@click.option("--skip-existing/--include-existing", default=True, show_default=True)
@click.option("--skip-match-index/--with-match-index", default=False, show_default=True)
@click.option("--with-embeddings/--no-embeddings", default=False, show_default=True)
@click.option("--export-registry/--no-export-registry", default=True, show_default=True)
def ingest_fara_command(
    mode: str,
    limit_registrants: int,
    offset_registrants: int,
    skip_existing: bool,
    skip_match_index: bool,
    with_embeddings: bool,
    export_registry: bool,
) -> None:
    """Ingest the official FARA corpus into Neon and search."""

    settings = Settings()
    ensure_fara_schema(settings)
    if not skip_match_index:
        ensure_search_schema(settings)
    registry = load_registry_if_available(settings, export_cache=export_registry)
    client = FaraApiClient(settings) if mode == "api" else None
    registrants = (
        fetch_bulk_registrants(settings)
        if mode == "bulk"
        else fetch_active_registrants(settings, client)
    )
    skipped_existing = 0
    if skip_existing:
        existing_registration_numbers = fetch_existing_fara_registration_numbers(settings)
        skipped_existing = sum(
            1 for registrant in registrants if registrant.registration_number in existing_registration_numbers
        )
        registrants = [
            registrant
            for registrant in registrants
            if registrant.registration_number not in existing_registration_numbers
        ]
    if offset_registrants > 0:
        registrants = registrants[offset_registrants:]
    if limit_registrants > 0:
        registrants = registrants[:limit_registrants]

    bulk_principal_map: dict[int, list[FaraForeignPrincipalRecord]] = {}
    bulk_short_form_map: dict[int, list[FaraShortFormRecord]] = {}
    bulk_document_map: dict[int, list[FaraDocumentRecord]] = {}
    if mode == "bulk":
        bulk_principal_map = {
            registration_number: list(rows)  # type: ignore[list-item]
            for registration_number, rows in group_rows_by_registration_number(
                fetch_bulk_foreign_principals(settings)
            ).items()
        }
        bulk_short_form_map = {
            registration_number: list(rows)  # type: ignore[list-item]
            for registration_number, rows in group_rows_by_registration_number(
                fetch_bulk_short_forms(settings)
            ).items()
        }
        bulk_document_map = {
            registration_number: list(rows)  # type: ignore[list-item]
            for registration_number, rows in group_rows_by_registration_number(
                fetch_bulk_reg_documents(settings)
            ).items()
        }

    summary: dict[str, object] = {
        "mode": mode,
        "registrantsFetched": len(registrants) + skipped_existing + offset_registrants,
        "registrantsSkippedExisting": skipped_existing,
        "registrantsQueued": len(registrants),
        "registrantsUpserted": 0,
        "foreignPrincipalsUpserted": 0,
        "shortFormsUpserted": 0,
        "documentsUpserted": 0,
        "memberMatchesUpserted": 0,
        "searchDocumentsUpserted": 0,
        "searchChunksUpserted": 0,
        "failedRegistrants": [],
    }

    for registrant in registrants:
        principals: list[FaraForeignPrincipalRecord] = []
        short_forms: list[FaraShortFormRecord] = []
        documents: list[FaraDocumentRecord] = []
        partial_errors: list[str] = []

        if mode == "bulk":
            principals = bulk_principal_map.get(registrant.registration_number, [])
            short_forms = bulk_short_form_map.get(registrant.registration_number, [])
            documents = bulk_document_map.get(registrant.registration_number, [])
        else:
            for label, fetcher in (
                ("foreignPrincipals", lambda: fetch_foreign_principals(settings, registrant.registration_number, client)),
                ("shortForms", lambda: fetch_short_forms(settings, registrant.registration_number, client)),
                ("documents", lambda: fetch_reg_documents(settings, registrant.registration_number, client)),
            ):
                try:
                    rows = fetcher()
                    if label == "foreignPrincipals":
                        principals = rows  # type: ignore[assignment]
                    elif label == "shortForms":
                        short_forms = rows  # type: ignore[assignment]
                    else:
                        documents = rows  # type: ignore[assignment]
                except Exception as error:  # pragma: no cover - depends on live API variance
                    partial_errors.append(f"{label}: {error}")

        upsert_fara_registrants(settings, [registrant], ensure_schema=False)
        upsert_fara_foreign_principals(settings, principals, ensure_schema=False)
        upsert_fara_short_forms(settings, short_forms, ensure_schema=False)
        upsert_fara_documents(settings, documents, ensure_schema=False)
        summary["registrantsUpserted"] = int(summary["registrantsUpserted"]) + 1
        summary["foreignPrincipalsUpserted"] = int(summary["foreignPrincipalsUpserted"]) + len(principals)
        summary["shortFormsUpserted"] = int(summary["shortFormsUpserted"]) + len(short_forms)
        summary["documentsUpserted"] = int(summary["documentsUpserted"]) + len(documents)

        bundle = FaraRegistrantBundle(
            registrant=registrant,
            foreign_principals=principals,
            short_forms=short_forms,
            documents=documents,
        )

        matches: list[FaraMemberMatchRecord] = []
        if registry:
            registrant_match = registry.resolve(name=registrant.name, state=registrant.state)
            if registrant_match and registrant_match.id:
                matches.append(
                    build_fara_member_match(
                        registrant,
                        registrant_match,
                        entity_kind="registrant",
                        entity_key=f"registrant:{registrant.registration_number}",
                        match_value=registrant.name,
                        metadata={"registrationDate": registrant.registration_date},
                    )
                )
            for principal in principals:
                member = registry.resolve(name=principal.foreign_principal_name, state=principal.state)
                if member and member.id:
                    matches.append(
                        build_fara_member_match(
                            registrant,
                            member,
                            entity_kind="foreign_principal",
                            entity_key=principal.principal_key,
                            match_value=principal.foreign_principal_name,
                            metadata={"country": principal.country_name},
                        )
                    )
            for short_form in short_forms:
                member = registry.resolve(
                    first_name=short_form.first_name,
                    last_name=short_form.last_name,
                    state=short_form.state,
                )
                if member and member.id:
                    matches.append(
                        build_fara_member_match(
                            registrant,
                            member,
                            entity_kind="short_form",
                            entity_key=short_form.short_form_key,
                            match_value=short_form.full_name,
                            metadata={"shortFormDate": short_form.short_form_date},
                        )
                    )
        if matches:
            deduped: dict[str, FaraMemberMatchRecord] = {match.match_key: match for match in matches}
            fara_matches = list(deduped.values())
            upsert_fara_member_matches(settings, fara_matches, ensure_schema=False)
            summary["memberMatchesUpserted"] = int(summary["memberMatchesUpserted"]) + len(fara_matches)
        else:
            fara_matches = []

        if not skip_match_index:
            registrant_index_summary = index_search_document(
                settings,
                build_fara_registrant_search_document(bundle),
                with_embeddings=with_embeddings,
                ensure_schema=False,
            )
            summary["searchDocumentsUpserted"] = int(summary["searchDocumentsUpserted"]) + int(
                (registrant_index_summary.get("document") or {}).get("upserted", 0)  # type: ignore[union-attr]
            )
            summary["searchChunksUpserted"] = int(summary["searchChunksUpserted"]) + int(
                (registrant_index_summary.get("chunks") or {}).get("upserted", 0)  # type: ignore[union-attr]
            )
            for match in fara_matches:
                match_index_summary = index_search_document(
                    settings,
                    build_fara_member_match_search_document(match, bundle),
                    with_embeddings=with_embeddings,
                    ensure_schema=False,
                )
                summary["searchDocumentsUpserted"] = int(summary["searchDocumentsUpserted"]) + int(
                    (match_index_summary.get("document") or {}).get("upserted", 0)  # type: ignore[union-attr]
                )
                summary["searchChunksUpserted"] = int(summary["searchChunksUpserted"]) + int(
                    (match_index_summary.get("chunks") or {}).get("upserted", 0)  # type: ignore[union-attr]
                )

        if partial_errors:
            summary["failedRegistrants"].append(  # type: ignore[union-attr]
                {
                    "registrationNumber": registrant.registration_number,
                    "name": registrant.name,
                    "errors": partial_errors,
                }
            )

    click.echo(json.dumps(summary, indent=2))


@cli.command("ocr")
@click.argument("pdf_path", type=click.Path(exists=True, path_type=Path))
def ocr_file(pdf_path: Path) -> None:
    """Run the OCR pipeline against a single PDF file."""

    processor = OcrProcessor(Settings())
    result = processor.process_file(pdf_path)
    click.echo(
        json.dumps(
            {
                "source_path": result.source_path,
                "errors": result.errors,
                "warnings": result.warnings,
                "ocr_confidence": result.ocr_confidence,
                "document": result.document.model_dump() if result.document else None,
            },
            indent=2,
        )
    )


@cli.command("parse-house-ptr")
@click.argument("pdf_path", type=click.Path(exists=True, path_type=Path))
@click.option("--doc-id", type=str, required=True, help="House PTR filing document id.")
@click.option("--filing-year", type=int, required=True, help="Disclosure year, for example 2026.")
@click.option("--filing-date", type=str, default=None, help="Disclosure filing date in YYYY-MM-DD format.")
@click.option("--member-name", type=str, required=True, help="Resolved member name.")
@click.option("--member-slug", type=str, default=None, help="CapitolExposed member slug.")
@click.option("--member-id", type=str, default=None, help="CapitolExposed member id.")
@click.option("--party", type=str, default=None, help="Member party.")
@click.option("--state", type=str, default=None, help="Member state.")
@click.option("--district", type=str, default=None, help="Member district.")
@click.option("--upsert/--no-upsert", default=False, show_default=True)
@click.option(
    "--ocr-backend",
    type=click.Choice([choice.value for choice in OcrBackend]),
    default=OcrBackend.AUTO.value,
    show_default=True,
)
def parse_house_ptr_command(
    pdf_path: Path,
    doc_id: str,
    filing_year: int,
    filing_date: str | None,
    member_name: str,
    member_slug: str | None,
    member_id: str | None,
    party: str | None,
    state: str | None,
    district: str | None,
    upsert: bool,
    ocr_backend: str,
) -> None:
    """OCR and parse a House PTR PDF into structured transactions."""

    settings = Settings()
    stub = FilingStub(
        doc_id=doc_id,
        filing_year=filing_year,
        filing_date=filing_date,
        member=MemberMatch(
            id=member_id,
            name=member_name,
            slug=member_slug,
            party=party,
            state=state,
            district=district,
        ),
        source="house-clerk",
        source_url=str(pdf_path),
    )
    parsed, trades = parse_house_ptr_pdf(
        pdf_path,
        stub=stub,
        settings=settings,
        backend=ocr_backend,
    )

    upsert_summary: dict[str, object] | None = None
    if upsert:
        upsert_summary = persist_parsed_house_stub(settings, stub, parsed, trades)

    click.echo(
        json.dumps(
            {
                "parsed": parsed.model_dump(),
                "trades": [trade.model_dump() for trade in trades],
                "upsert": upsert_summary,
            },
            indent=2,
        )
    )


@cli.command("process-house-doc")
@click.option("--year", type=int, required=True, help="Disclosure year, for example 2026.")
@click.option("--doc-id", type=str, required=True, help="House PTR filing document id.")
@click.option("--export-registry/--no-export-registry", default=True, show_default=True)
@click.option("--upsert/--no-upsert", default=False, show_default=True)
@click.option(
    "--ocr-backend",
    type=click.Choice([choice.value for choice in OcrBackend]),
    default=OcrBackend.AUTO.value,
    show_default=True,
)
def process_house_doc_command(
    year: int,
    doc_id: str,
    export_registry: bool,
    upsert: bool,
    ocr_backend: str,
) -> None:
    """Fetch a House PTR by doc id, resolve the member, parse it, and optionally upsert it."""

    settings = Settings()
    registry = load_registry_if_available(settings, export_cache=export_registry)
    feed = fetch_house_feed(
        year,
        resolver=registry.resolve_feed_member if registry else None,
        settings=settings,
    )
    stub = next((row for row in feed if row.doc_id == doc_id), None)
    if not stub:
        raise click.ClickException(f"Doc id {doc_id} was not found in the {year} House feed.")

    parsed, trades = parse_live_house_stub(stub, settings, ocr_backend)

    upsert_summary: dict[str, object] | None = None
    if upsert:
        upsert_summary = persist_parsed_house_stub(settings, stub, parsed, trades)

    click.echo(
        json.dumps(
            {
                "stub": stub.model_dump(),
                "parsed": parsed.model_dump(),
                "trades": [trade.model_dump() for trade in trades],
                "upsert": upsert_summary,
            },
            indent=2,
        )
    )


@cli.command("index-house-doc-search")
@click.option("--year", type=int, required=True, help="Disclosure year, for example 2026.")
@click.option("--doc-id", type=str, required=True, help="House PTR filing document id.")
@click.option("--export-registry/--no-export-registry", default=True, show_default=True)
@click.option("--with-embeddings/--no-embeddings", default=False, show_default=True)
@click.option(
    "--ocr-backend",
    type=click.Choice([choice.value for choice in OcrBackend]),
    default=OcrBackend.AUTO.value,
    show_default=True,
)
def index_house_doc_search_command(
    year: int,
    doc_id: str,
    export_registry: bool,
    with_embeddings: bool,
    ocr_backend: str,
) -> None:
    """Index a live House PTR into searchable documents and chunks."""

    settings = Settings()
    registry = load_registry_if_available(settings, export_cache=export_registry)
    feed = fetch_house_feed(
        year,
        resolver=registry.resolve_feed_member if registry else None,
        settings=settings,
    )
    stub = next((row for row in feed if row.doc_id == doc_id), None)
    if not stub:
        raise click.ClickException(f"Doc id {doc_id} was not found in the {year} House feed.")

    parsed, trades = parse_live_house_stub(stub, settings, ocr_backend)
    search_document = build_house_ptr_search_document(stub, parsed, trades)
    summary = index_search_document(settings, search_document, with_embeddings=with_embeddings)
    click.echo(
        json.dumps(
            {
                "stub": stub.model_dump(),
                "parsedTransactionCount": len(parsed.transactions),
                "searchDocument": search_document.model_dump(),
                "indexing": summary,
            },
            indent=2,
        )
    )


@cli.command("index-house-search-backfill")
@click.option("--limit", type=int, default=0, show_default=True, help="0 means index every eligible stored PTR.")
@click.option("--include-needs-review/--parsed-only", default=True, show_default=True)
@click.option("--only-missing/--reindex-all", default=True, show_default=True)
@click.option("--with-embeddings/--no-embeddings", default=False, show_default=True)
def index_house_search_backfill_command(
    limit: int,
    include_needs_review: bool,
    only_missing: bool,
    with_embeddings: bool,
) -> None:
    """Backfill indexed House PTR search documents from stored Neon stub rows."""

    settings = Settings()
    ensure_search_schema(settings)
    rows = fetch_house_stub_search_backfill(
        settings,
        limit=limit,
        include_needs_review=include_needs_review,
        only_missing=only_missing,
    )

    summary = {
        "queued": len(rows),
        "documentsUpserted": 0,
        "chunksUpserted": 0,
        "embedded": 0,
        "processed": [],
    }

    for row in rows:
        search_document = build_house_ptr_search_document_from_stub_row(row)
        index_summary = index_search_document(
            settings,
            search_document,
            with_embeddings=with_embeddings,
            ensure_schema=False,
        )
        summary["documentsUpserted"] += int(
            (index_summary.get("document") or {}).get("upserted", 0)  # type: ignore[union-attr]
        )
        summary["chunksUpserted"] += int(
            (index_summary.get("chunks") or {}).get("upserted", 0)  # type: ignore[union-attr]
        )
        summary["embedded"] += 1 if index_summary.get("embedded") else 0
        summary["processed"].append(
            {
                "docId": row.get("doc_id"),
                "status": row.get("status"),
                "documentId": (index_summary.get("document") or {}).get("document_id"),  # type: ignore[union-attr]
                "chunks": (index_summary.get("chunks") or {}).get("upserted", 0),  # type: ignore[union-attr]
            }
        )

    click.echo(json.dumps(summary, indent=2))


@cli.command("index-site-editorial")
@click.option("--limit-stories", type=int, default=0, show_default=True, help="0 means index every eligible published story.")
@click.option("--limit-dossiers", type=int, default=0, show_default=True, help="0 means index every eligible published dossier.")
@click.option("--only-missing/--reindex-all", default=True, show_default=True)
@click.option("--with-embeddings/--no-embeddings", default=False, show_default=True)
@click.option("--export-registry/--no-export-registry", default=True, show_default=True)
def index_site_editorial_command(
    limit_stories: int,
    limit_dossiers: int,
    only_missing: bool,
    with_embeddings: bool,
    export_registry: bool,
) -> None:
    """Index published CapitolExposed stories and dossiers into the shared search corpus."""

    settings = Settings()
    ensure_search_schema(settings)
    registry = load_registry_if_available(settings, export_cache=export_registry)

    story_rows = fetch_published_news_posts(
        settings,
        limit=limit_stories,
        only_missing=only_missing,
    )
    dossier_rows = fetch_published_dossiers(
        settings,
        limit=limit_dossiers,
        only_missing=only_missing,
    )

    summary = {
        "storiesQueued": len(story_rows),
        "dossiersQueued": len(dossier_rows),
        "documentsUpserted": 0,
        "chunksUpserted": 0,
        "embedded": 0,
        "processed": [],
    }

    for row in story_rows:
        search_document = build_news_post_search_document(
            row,
            base_url=settings.site_base_url,
            registry=registry,
        )
        index_summary = index_search_document(
            settings,
            search_document,
            with_embeddings=with_embeddings,
            ensure_schema=False,
        )
        summary["documentsUpserted"] += int(
            (index_summary.get("document") or {}).get("upserted", 0)  # type: ignore[union-attr]
        )
        summary["chunksUpserted"] += int(
            (index_summary.get("chunks") or {}).get("upserted", 0)  # type: ignore[union-attr]
        )
        summary["embedded"] += 1 if index_summary.get("embedded") else 0
        summary["processed"].append(
            {
                "kind": "story",
                "slug": row.get("slug"),
                "documentId": (index_summary.get("document") or {}).get("document_id"),  # type: ignore[union-attr]
                "chunks": (index_summary.get("chunks") or {}).get("upserted", 0),  # type: ignore[union-attr]
            }
        )

    for row in dossier_rows:
        search_document = build_dossier_search_document(
            row,
            base_url=settings.site_base_url,
        )
        index_summary = index_search_document(
            settings,
            search_document,
            with_embeddings=with_embeddings,
            ensure_schema=False,
        )
        summary["documentsUpserted"] += int(
            (index_summary.get("document") or {}).get("upserted", 0)  # type: ignore[union-attr]
        )
        summary["chunksUpserted"] += int(
            (index_summary.get("chunks") or {}).get("upserted", 0)  # type: ignore[union-attr]
        )
        summary["embedded"] += 1 if index_summary.get("embedded") else 0
        summary["processed"].append(
            {
                "kind": "dossier",
                "slug": row.get("slug"),
                "documentId": (index_summary.get("document") or {}).get("document_id"),  # type: ignore[union-attr]
                "chunks": (index_summary.get("chunks") or {}).get("upserted", 0),  # type: ignore[union-attr]
            }
        )

    click.echo(json.dumps(summary, indent=2))


@cli.command("index-site-core")
@click.option("--limit-members", type=int, default=0, show_default=True, help="0 means index every eligible member.")
@click.option("--limit-committees", type=int, default=0, show_default=True, help="0 means index every eligible committee.")
@click.option("--limit-bills", type=int, default=0, show_default=True, help="0 means index every eligible bill.")
@click.option("--limit-alerts", type=int, default=0, show_default=True, help="0 means index every eligible alert.")
@click.option("--only-missing/--reindex-all", default=True, show_default=True)
@click.option("--with-embeddings/--no-embeddings", default=False, show_default=True)
def index_site_core_command(
    limit_members: int,
    limit_committees: int,
    limit_bills: int,
    limit_alerts: int,
    only_missing: bool,
    with_embeddings: bool,
) -> None:
    """Index core CapitolExposed entities into the shared search corpus."""

    settings = Settings()
    ensure_search_schema(settings)

    summary = {
        "membersQueued": 0,
        "committeesQueued": 0,
        "billsQueued": 0,
        "alertsQueued": 0,
        "documentsUpserted": 0,
        "chunksUpserted": 0,
        "embedded": 0,
        "processedCount": 0,
        "processedSample": [],
        "failed": [],
    }

    def process_rows(
        kind: str,
        rows: list[dict[str, object]],
        build_document,
        label_key: str,
    ) -> None:
        summary[f"{kind}sQueued"] += len(rows)
        for row in rows:
            search_document = build_document(row, base_url=settings.site_base_url)
            try:
                index_summary = index_search_document_with_retry(
                    settings,
                    search_document,
                    with_embeddings=with_embeddings,
                    ensure_schema=False,
                )
            except Exception as error:
                summary["failed"].append(
                    {
                        "kind": kind,
                        label_key: row.get(label_key),
                        "error": str(error)[:240],
                    }
                )
                continue
            summary["documentsUpserted"] += int(
                (index_summary.get("document") or {}).get("upserted", 0)  # type: ignore[union-attr]
            )
            summary["chunksUpserted"] += int(
                (index_summary.get("chunks") or {}).get("upserted", 0)  # type: ignore[union-attr]
            )
            summary["embedded"] += 1 if index_summary.get("embedded") else 0
            summary["processedCount"] += 1
            if len(summary["processedSample"]) < 40:
                summary["processedSample"].append(
                    {
                        "kind": kind,
                        label_key: row.get(label_key),
                        "documentId": (index_summary.get("document") or {}).get("document_id"),  # type: ignore[union-attr]
                        "chunks": (index_summary.get("chunks") or {}).get("upserted", 0),  # type: ignore[union-attr]
                    }
                )

    def drain_rows(kind: str, fetch_rows, limit: int, batch_size: int, build_document, label_key: str) -> None:
        if only_missing and limit <= 0:
            while True:
                rows = fetch_rows(settings, limit=batch_size, only_missing=True)
                if not rows:
                    break
                process_rows(kind, rows, build_document, label_key)
        else:
            rows = fetch_rows(settings, limit=limit, only_missing=only_missing)
            process_rows(kind, rows, build_document, label_key)

    drain_rows("member", fetch_members_for_search, limit_members, 50, build_member_search_document, "slug")
    drain_rows("committee", fetch_committees_for_search, limit_committees, 50, build_committee_search_document, "id")
    drain_rows("bill", fetch_bills_for_search, limit_bills, 100, build_bill_search_document, "id")
    drain_rows("alert", fetch_alerts_for_search, limit_alerts, 100, build_alert_search_document, "id")

    click.echo(json.dumps(summary, indent=2))


@cli.command("embed-search-backfill")
@click.option("--limit", type=int, default=100, show_default=True)
@click.option("--source", type=str, default=None, help="Optional pipeline_search_documents.source filter.")
def embed_search_backfill_command(
    limit: int,
    source: str | None,
) -> None:
    """Embed existing search chunks that still have null embeddings."""

    settings = Settings()
    rows = fetch_search_chunk_embedding_backfill(settings, limit=limit, source=source)
    embedder = get_embedder(settings)
    embeddings = embedder.embed_texts([str(row.get("content") or "") for row in rows])
    updates = [
        (str(row.get("id") or ""), embedding)
        for row, embedding in zip(rows, embeddings, strict=False)
        if embedding
    ]
    summary = update_search_chunk_embeddings(settings, updates)
    click.echo(
        json.dumps(
            {
                "queued": len(rows),
                "updated": summary["updated"],
                "source": source,
            },
            indent=2,
        )
    )


@cli.command("embed-search-corpus")
@click.option("--batch-size", type=int, default=100, show_default=True)
@click.option("--max-batches", type=int, default=0, show_default=True, help="0 means run until the queue is empty.")
@click.option("--source", type=str, default=None, help="Optional pipeline_search_documents.source filter.")
def embed_search_corpus_command(
    batch_size: int,
    max_batches: int,
    source: str | None,
) -> None:
    """Embed the search corpus in stable batches until the queue is drained."""

    settings = Settings()
    embedder = get_embedder(settings)
    summary = {
        "source": source,
        "batchSize": batch_size,
        "batches": 0,
        "queued": 0,
        "updated": 0,
    }

    while True:
        if max_batches > 0 and summary["batches"] >= max_batches:
            break
        rows = fetch_search_chunk_embedding_backfill(
            settings,
            limit=batch_size,
            source=source,
        )
        if not rows:
            break
        embeddings = embedder.embed_texts([str(row.get("content") or "") for row in rows])
        updates = [
            (str(row.get("id") or ""), embedding)
            for row, embedding in zip(rows, embeddings, strict=False)
            if embedding
        ]
        batch_summary = update_search_chunk_embeddings(settings, updates)
        summary["batches"] += 1
        summary["queued"] += len(rows)
        summary["updated"] += int(batch_summary["updated"])

    click.echo(json.dumps(summary, indent=2))


@cli.command("process-house-backlog")
@click.option("--limit", type=int, default=5, show_default=True)
@click.option("--export-registry/--no-export-registry", default=True, show_default=True)
@click.option("--with-search-index/--no-search-index", default=False, show_default=True)
@click.option("--with-embeddings/--no-embeddings", default=False, show_default=True)
@click.option("--include-needs-review/--queued-only", default=False, show_default=True)
@click.option("--review-retry-hours", type=int, default=12, show_default=True)
@click.option(
    "--ocr-backend",
    type=click.Choice([choice.value for choice in OcrBackend]),
    default=OcrBackend.AUTO.value,
    show_default=True,
)
def process_house_backlog_command(
    limit: int,
    export_registry: bool,
    with_search_index: bool,
    with_embeddings: bool,
    include_needs_review: bool,
    review_retry_hours: int,
    ocr_backend: str,
) -> None:
    """Process queued House PTR stubs from Neon in batch order."""

    settings = Settings()
    load_registry_if_available(settings, export_cache=export_registry)
    queue_rows = fetch_house_stub_queue(
        settings,
        limit=limit,
        include_needs_review=include_needs_review,
    )
    summary = process_house_queue_rows(
        settings,
        queue_rows,
        ocr_backend=ocr_backend,
        with_search_index=with_search_index,
        with_embeddings=with_embeddings,
        review_retry_hours=review_retry_hours,
    )
    click.echo(json.dumps(summary, indent=2))


@cli.command("process-house-review")
@click.option("--limit", type=int, default=2, show_default=True)
@click.option("--export-registry/--no-export-registry", default=True, show_default=True)
@click.option("--with-search-index/--no-search-index", default=True, show_default=True)
@click.option("--with-embeddings/--no-embeddings", default=False, show_default=True)
@click.option("--review-retry-hours", type=int, default=12, show_default=True)
@click.option(
    "--ocr-backend",
    type=click.Choice([choice.value for choice in OcrBackend]),
    default=OcrBackend.DOCLING.value,
    show_default=True,
)
def process_house_review_command(
    limit: int,
    export_registry: bool,
    with_search_index: bool,
    with_embeddings: bool,
    review_retry_hours: int,
    ocr_backend: str,
) -> None:
    """Reprocess the House PTR review queue with an alternate OCR backend."""

    settings = Settings()
    load_registry_if_available(settings, export_cache=export_registry)
    queue_rows = fetch_house_stub_queue(
        settings,
        limit=limit,
        only_needs_review=True,
    )
    summary = process_house_queue_rows(
        settings,
        queue_rows,
        ocr_backend=ocr_backend,
        with_search_index=with_search_index,
        with_embeddings=with_embeddings,
        review_retry_hours=review_retry_hours,
    )
    summary["mode"] = "needs_review"
    click.echo(json.dumps(summary, indent=2))


@cli.command("house-ingest")
@click.option("--year", type=int, required=True, help="Disclosure year, for example 2026.")
@click.option("--sync-limit", type=int, default=0, show_default=True, help="0 means sync all feed rows.")
@click.option("--batch-size", type=int, default=10, show_default=True)
@click.option("--max-batches", type=int, default=0, show_default=True, help="0 means run until the queue is drained.")
@click.option("--export-registry/--no-export-registry", default=True, show_default=True)
@click.option("--with-search-index/--no-search-index", default=True, show_default=True)
@click.option("--with-embeddings/--no-embeddings", default=False, show_default=True)
@click.option("--include-needs-review/--fresh-only", default=False, show_default=True)
@click.option("--review-retry-hours", type=int, default=12, show_default=True)
@click.option(
    "--ocr-backend",
    type=click.Choice([choice.value for choice in OcrBackend]),
    default=OcrBackend.AUTO.value,
    show_default=True,
)
def house_ingest_command(
    year: int,
    sync_limit: int,
    batch_size: int,
    max_batches: int,
    export_registry: bool,
    with_search_index: bool,
    with_embeddings: bool,
    include_needs_review: bool,
    review_retry_hours: int,
    ocr_backend: str,
) -> None:
    """Run the end-to-end House feed sync and backlog processing cycle."""

    settings = Settings()
    registry = load_registry_if_available(settings, export_cache=export_registry)
    feed = fetch_house_feed(
        year,
        resolver=registry.resolve_feed_member if registry else None,
        settings=settings,
    )
    if sync_limit > 0:
        feed = feed[:sync_limit]
    sync_summary = sync_house_stubs_to_neon(settings, feed)

    if with_search_index:
        ensure_search_schema(settings)

    total_summary: dict[str, object] = {
        "year": year,
        "synced": len(feed),
        "resolvedMembers": sum(1 for row in feed if row.member.id),
        "syncSummary": sync_summary,
        "batches": [],
        "totals": {
            "queued": 0,
            "parsed": 0,
            "needsReview": 0,
            "deferred": 0,
            "failed": 0,
            "tradeRowsUpserted": 0,
            "searchDocumentsUpserted": 0,
            "searchChunksUpserted": 0,
        },
    }

    batch_number = 0
    while True:
        if max_batches > 0 and batch_number >= max_batches:
            break
        queue_rows = fetch_house_stub_queue(
            settings,
            limit=batch_size,
            include_needs_review=include_needs_review,
        )
        if not queue_rows:
            break
        batch_number += 1
        batch_summary = process_house_queue_rows(
            settings,
            queue_rows,
            ocr_backend=ocr_backend,
            with_search_index=with_search_index,
            with_embeddings=with_embeddings,
            review_retry_hours=review_retry_hours,
        )
        total_summary["batches"].append(batch_summary)  # type: ignore[union-attr]
        totals = total_summary["totals"]  # type: ignore[assignment]
        for key in (
            "queued",
            "parsed",
            "needsReview",
            "deferred",
            "failed",
            "tradeRowsUpserted",
            "searchDocumentsUpserted",
            "searchChunksUpserted",
        ):
            totals[key] = int(totals[key]) + int(batch_summary.get(key, 0))  # type: ignore[index]
        if len(queue_rows) < batch_size:
            break

    click.echo(json.dumps(total_summary, indent=2))


@cli.command("hybrid-search")
@click.option("--query", "query_text", type=str, required=True, help="Search query text.")
@click.option("--limit", type=int, default=10, show_default=True)
@click.option("--with-embeddings/--no-embeddings", default=False, show_default=True)
@click.option("--source", type=str, default=None, help="Optional document source filter.")
@click.option("--category", type=str, default=None, help="Optional document category filter.")
@click.option("--member-id", type=str, default=None, help="Optional member scope filter.")
@click.option("--committee-id", type=str, default=None, help="Optional committee scope filter.")
@click.option("--bill-id", type=str, default=None, help="Optional bill scope filter.")
@click.option("--ticker", type=str, default=None, help="Optional asset ticker filter.")
def hybrid_search_command(
    query_text: str,
    limit: int,
    with_embeddings: bool,
    source: str | None,
    category: str | None,
    member_id: str | None,
    committee_id: str | None,
    bill_id: str | None,
    ticker: str | None,
) -> None:
    """Run a hybrid lexical and vector search against indexed chunks."""

    settings = Settings()
    query_embedding = None
    if with_embeddings:
        query_embedding = get_embedder(settings).embed_texts([query_text])[0]
    hits = hybrid_search(
        settings,
        query_text=query_text,
        query_embedding=query_embedding,
        limit=limit,
        source=source,
        category=category,
        member_id=member_id,
        committee_id=committee_id,
        bill_id=bill_id,
        ticker=ticker,
    )
    click.echo(json.dumps([hit.model_dump() for hit in hits], indent=2))


if __name__ == "__main__":
    cli()
