"""CLI for Capitol Pipeline."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import logging
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
from capitol_pipeline.exporters.members_bio_schema import ensure_members_bio_schema
from capitol_pipeline.exporters.neon import (
    DEFAULT_SENATE_DEDUPE_SOURCES,
    backfill_crypto_trade_classification,
    delete_duplicate_senate_trades,
    ensure_congress_schema,
    ensure_fara_schema,
    ensure_offshore_schema,
    ensure_search_schema,
    ensure_usaspending_schema,
    fetch_existing_trade_ids,
    fetch_latest_trade_disclosure_date,
    fetch_existing_fara_registration_numbers,
    fetch_alerts_for_search,
    fetch_bills_for_congress_sync,
    fetch_bills_for_search,
    fetch_company_candidates_for_usaspending,
    fetch_committees_for_search,
    fetch_duplicate_senate_trade_groups,
    fetch_published_dossiers,
    fetch_published_news_posts,
    fetch_members_for_search,
    fetch_search_chunk_embedding_backfill,
    fetch_house_stub_search_backfill,
    fetch_house_stub_queue,
    fetch_house_stubs_awaiting_publication,
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
from capitol_pipeline.models.congress import (
    FilingStub,
    HousePtrParseResult,
    HousePtrTransaction,
    MemberMatch,
    NormalizedTradeRow,
)
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
from capitol_pipeline.parsers.house_ptr import (
    REPLAY_PARSER_VERSION,
    VISION_BACKENDS,
    build_trade_rows_from_house_ptr,
    clean_asset_description,
    cleaning_gutted_description,
    get_transaction_date_issue,
    parse_house_ptr_pdf,
    strip_form_annotation,
)
from capitol_pipeline.parsers.ptr_vision import (
    MAX_ROW_SUMMARIES_IN_METADATA,
    detector_review_reasons,
    is_vision_parser_version,
    reconcile_stored_transcription,
    row_summary,
    stored_transcription,
    transcription_from_metadata,
)
from capitol_pipeline.processors.chunking import build_search_chunks
from capitol_pipeline.processors.embeddings import get_embedder
from capitol_pipeline.processors.headshots import (
    fetch_headshot_targets,
    run_headshot_verification,
    upsert_headshot_verifications,
)
from capitol_pipeline.processors.ocr import OcrProcessor, fix_font_mojibake
from capitol_pipeline.registries.members import MemberRegistry, load_member_registry_from_json
from capitol_pipeline.sources.congress_gov import (
    CongressGovApiClient,
    build_congress_bill_action_records,
    build_congress_bill_record,
    build_congress_bill_summary_records,
)
from capitol_pipeline.sources.bioguide import (
    fetch_bioguide_records,
    fetch_member_bioguides,
    upsert_bioguide_records,
)
from capitol_pipeline.sources.congress_legislators import (
    fetch_legislator_crosswalk,
    upsert_member_crosswalk,
)
from capitol_pipeline.sources.house_clerk import fetch_house_feed
from capitol_pipeline.sources.wikidata import (
    fetch_member_qids,
    fetch_wikidata_infoboxes,
    upsert_wikidata_infoboxes,
)
from capitol_pipeline.sources.wikipedia import (
    fetch_member_wiki_pairs,
    fetch_wikipedia_summaries,
    upsert_wikipedia_summaries,
)
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
from capitol_pipeline.sources.senate_efd import (
    SenateEfdClient,
    build_efd_submitted_since,
    build_paper_report_stub,
    fetch_electronic_ptr,
    fetch_paper_ptr_pages,
    list_ptr_reports,
    normalize_efd_transaction,
)
from capitol_pipeline.sources.senate_ethics import (
    build_quiver_bulk_reconcile_dates,
    fetch_quiver_bulk_congress_feed,
    fetch_quiver_live_senate_feed,
    fetch_senate_watcher_feed,
    normalize_quiver_live_senate_trade,
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


logger = logging.getLogger(__name__)

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


VISION_BACKEND_HELP = (
    "Whether to read scanned or handwritten PTRs as page images. "
    "'off' never calls a model; 'auto' calls one only when the text parser "
    "scores under 0.5 or produced no OCR text; 'on' always calls one "
    "('claude' and 'gemini' are accepted spellings of 'on'). Which vendor "
    "answers is CAPITOL_PTR_VISION_PROVIDER (default gemini, free tier). "
    "Disable globally with CAPITOL_PTR_VISION_DISABLED=1."
)


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
    vision_backend: str = "off",
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
            vision_backend=vision_backend,
        )


def resolve_house_stub_status(
    stub: FilingStub,
    parsed: HousePtrParseResult,
    trades: list[NormalizedTradeRow],
) -> str:
    """Decide whether a parsed House PTR leaves the review queue.

    A vision transcription overrides the confidence threshold with the model's
    own legibility verdict: more than half the rows illegible keeps the stub in
    ``needs_review``, anything better marks it ``parsed``. A vision result that
    found the form states there is nothing to report is terminal: ``parsed``
    with zero rows (there is no separate "nothing to import" status; the
    ``visionParse.noTransactions`` flag records why). A stub whose member never
    resolved, or that otherwise produced no trade rows, can never be ``parsed``.
    """

    if not stub.member.id:
        return "needs_review"
    vision = parsed.vision_report
    vision_ok = (
        is_vision_parser_version(parsed.parser_version)
        and isinstance(vision, dict)
        and bool(vision.get("ok"))
    )
    if vision_ok and vision.get("noTransactions") and not parsed.transactions:  # type: ignore[union-attr]
        return "needs_review" if vision.get("needsReview") else "parsed"  # type: ignore[union-attr]
    if not trades:
        return "needs_review"
    if vision_ok:
        return "needs_review" if vision.get("needsReview") else "parsed"  # type: ignore[union-attr]
    return "parsed" if parsed.parser_confidence >= 0.6 else "needs_review"


#: Prefix the needs-review queue query matches on; keep it stable.
HOUSE_REVIEW_LAST_ERROR = "PTR text extracted but transactions need manual review"


def house_stub_last_error(parsed: HousePtrParseResult) -> str | None:
    """The ``lastError`` for a parsed stub: None once any row was parsed.

    The parser's ``review_reason`` (typed text the segmenter could not split,
    image-only scan whose OCR timed out, ...) is appended after the stable
    prefix so the queue query still recognises the row.
    """

    if parsed.transactions:
        return None
    vision = parsed.vision_report
    if isinstance(vision, dict) and vision.get("ok") and vision.get("noTransactions"):
        # The form states there is nothing to report: a clean zero-row result.
        return None
    if parsed.review_reason:
        return f"{HOUSE_REVIEW_LAST_ERROR}: {parsed.review_reason}"
    return HOUSE_REVIEW_LAST_ERROR


def build_house_stub_metadata_extra(parsed: HousePtrParseResult) -> dict[str, object] | None:
    """Vision and text-layer reports to merge into the stub metadata."""

    extra: dict[str, object] = {}
    if parsed.vision_report:
        extra["visionParse"] = parsed.vision_report
    if parsed.text_layer:
        extra["textLayer"] = parsed.text_layer
    return extra or None


def persist_parsed_house_stub(
    settings: Settings,
    stub: FilingStub,
    parsed: HousePtrParseResult,
    trades: list[NormalizedTradeRow],
) -> dict[str, object]:
    """Write a parsed House PTR result back into CapitolExposed.

    A vision-parsed filing (``parser_version`` starting ``claude-``) publishes
    trade rows only when it resolves to ``parsed``; while it is
    ``needs_review`` the transcription stays in the stub metadata
    (``parsedTransactions`` and ``visionParse.rows``) and nothing reaches
    ``trades``. The text path is unchanged.
    """

    sync_house_stubs_to_neon(settings, [stub])
    status = resolve_house_stub_status(stub, parsed, trades)
    withheld = bool(trades) and is_vision_parser_version(parsed.parser_version) and status != "parsed"
    if withheld:
        trade_summary: dict[str, object] = {"upserted": 0, "trade_ids": [], "withheld": len(trades)}
        if isinstance(parsed.vision_report, dict):
            parsed.vision_report["withheldTrades"] = len(trades)
    else:
        trade_summary = upsert_trade_rows_to_neon(settings, trades)
    metadata_extra = build_house_stub_metadata_extra(parsed)
    mark_house_stub_processed(
        settings,
        stub,
        status=status,
        parser_confidence=parsed.parser_confidence,
        parser_version=parsed.parser_version,
        parsed_transaction_count=len(parsed.transactions),
        extracted_trade_id=trade_summary["trade_ids"][0] if trade_summary.get("trade_ids") else None,
        last_error=house_stub_last_error(parsed),
        raw_text_preview=parsed.raw_text_preview,
        parsed_transactions=[transaction.model_dump() for transaction in parsed.transactions],
        metadata_extra=metadata_extra,
    )
    return {
        "stubs": {"upserted": 1},
        "trades": trade_summary,
        "stubStatus": status,
        "visionParse": parsed.vision_report,
    }


#: What a replayed stub reports when the original transcription is unusable.
REPLAY_SKIPPED = "skipped"


def _replay_asset_description(row: dict[str, object]) -> str:
    """The asset name a stored row should be published under.

    Transcriptions stored months ago were cleaned by whatever the cleaner did
    then, and rows still carry the artefacts it has since learned to strip --
    a "Filing Status: New" annotation glued to the front of the asset, and the
    subset-font shift the text layer now reverses. Both are removed here,
    unless cleaning eats most of the name, in which case the transcription is
    the better record.
    """

    raw = fix_font_mojibake(str(row.get("asset_description") or "")).strip()
    if not raw:
        return raw
    # The annotation is form furniture, not part of the name, so it comes off
    # before the "did cleaning eat this?" test -- otherwise removing it from a
    # short name ("Filing Status: New Visa Inc.") looks like the cleaner
    # eating half the row.
    core = strip_form_annotation(raw) or raw
    ticker = row.get("ticker")
    ticker = ticker.strip().upper() if isinstance(ticker, str) and ticker.strip() else None
    cleaned = clean_asset_description(raw, ticker).strip()
    if not cleaned or cleaning_gutted_description(cleaned, core):
        return core
    return cleaned


def rebuild_parsed_house_stub(
    row: dict[str, object],
    *,
    registry: MemberRegistry | None = None,
) -> tuple[FilingStub, HousePtrParseResult | None, list[NormalizedTradeRow], str | None]:
    """Rebuild a finished parse from a stub's stored transcription.

    The rows in ``metadata.parsedTransactions`` were transcribed in an earlier
    run; they never reached ``trades`` because the filer had no member record
    at the time. Nothing here re-reads the PDF and nothing calls a model.

    Returns ``(stub, parsed, trades, skip_reason)``. ``skip_reason`` is set,
    and ``parsed`` is None, whenever the filing still cannot be published.
    """

    stub = build_stub_from_queue_row(row)
    metadata = row.get("metadata") or {}
    if not isinstance(metadata, dict):
        return stub, None, [], "stub metadata is not an object"

    if not stub.member.id and registry is not None:
        resolved = registry.resolve_feed_member(
            stub.first_name or "", stub.last_name or "", stub.member.state
        )
        if resolved is not None:
            stub = stub.model_copy(update={"member": resolved})
    if not stub.member.id:
        return stub, None, [], "member still unresolved"

    stored = metadata.get("parsedTransactions")
    if not isinstance(stored, list) or not stored:
        return stub, None, [], "no stored transcription"

    transactions: list[HousePtrTransaction] = []
    for entry in stored:
        if not isinstance(entry, dict):
            continue
        # Some House PTRs embed a subset font whose lowercase glyphs extract as
        # IPA characters, and transcriptions stored before the text layer was
        # run through fix_font_mojibake carry them: "Filing Status: New" comes
        # back as an unreadable run. Reversing a known character shift is not
        # a rewrite of the record.
        entry = {
            **entry,
            "asset_description": _replay_asset_description(entry),
            "comment": (
                fix_font_mojibake(str(entry["comment"]))
                if isinstance(entry.get("comment"), str)
                else entry.get("comment")
            ),
        }
        try:
            transactions.append(HousePtrTransaction(**entry))
        except Exception as error:  # noqa: BLE001 - one bad row must not lose the filing
            logger.warning(
                "repersist: %s row %s unusable (%s)", stub.doc_id, entry.get("line_number"), error
            )
    if not transactions:
        return stub, None, [], "stored transcription could not be read back"

    valid = [
        transaction
        for transaction in transactions
        if get_transaction_date_issue(transaction.transaction_date, stub.filing_date) is None
    ]
    if not valid:
        return stub, None, [], "every stored row failed date validation"

    vision = metadata.get("visionParse")
    stored_version = str(metadata.get("parserVersion") or "").strip()
    if not stored_version and isinstance(vision, dict):
        stored_version = str(vision.get("parserVersion") or "").strip()
    parser_version = stored_version or REPLAY_PARSER_VERSION
    confidence = float(metadata.get("parserConfidence") or 0.0)

    parsed = HousePtrParseResult(
        doc_id=stub.doc_id,
        member_name=str(metadata.get("memberName") or "").strip() or stub.member.name,
        state=str(metadata.get("state") or "").strip() or stub.member.state,
        parser_confidence=confidence,
        parser_version=parser_version,
        raw_text_preview=str(metadata.get("rawTextPreview") or "") or None,
        transactions=valid,
        vision_report=vision if isinstance(vision, dict) else None,
        text_layer=metadata.get("textLayer") if isinstance(metadata.get("textLayer"), dict) else None,
    )
    provenance = (
        f"House PTR {stub.doc_id}: transcribed at {round(confidence * 100)}% confidence "
        f"[{parser_version}], published once the member record resolved"
    )
    trades = build_trade_rows_from_house_ptr(parsed, stub, provenance=provenance)
    return stub, parsed, trades, None


def repersist_house_stub_rows(
    settings: Settings,
    rows: list[dict[str, object]],
    *,
    registry: MemberRegistry | None = None,
    min_confidence: float = 0.0,
    dry_run: bool = False,
) -> dict[str, object]:
    """Publish stored transcriptions for a batch of stubs. No model, no PDF."""

    summary: dict[str, object] = {
        "candidates": len(rows),
        "published": 0,
        "skipped": 0,
        "failed": 0,
        "tradeRowsUpserted": 0,
        "rowsDropped": 0,
        "dryRun": dry_run,
        "stubs": [],
    }
    for row in rows:
        doc_id = str(row.get("doc_id") or "")
        stub, parsed, trades, skip_reason = rebuild_parsed_house_stub(row, registry=registry)
        stored_count = len(
            [entry for entry in ((row.get("metadata") or {}).get("parsedTransactions") or []) if entry]  # type: ignore[union-attr]
        )
        if parsed is None:
            summary["skipped"] = int(summary["skipped"]) + 1
            summary["stubs"].append(  # type: ignore[union-attr]
                {"docId": doc_id, "status": REPLAY_SKIPPED, "reason": skip_reason}
            )
            continue
        if parsed.parser_confidence < min_confidence:
            summary["skipped"] = int(summary["skipped"]) + 1
            summary["stubs"].append(  # type: ignore[union-attr]
                {
                    "docId": doc_id,
                    "status": REPLAY_SKIPPED,
                    "reason": (
                        f"parser confidence {parsed.parser_confidence:.2f} below "
                        f"{min_confidence:.2f}"
                    ),
                }
            )
            continue

        dropped = stored_count - len(parsed.transactions)
        summary["rowsDropped"] = int(summary["rowsDropped"]) + max(0, dropped)
        entry: dict[str, object] = {
            "docId": doc_id,
            "memberId": stub.member.id,
            "memberName": stub.member.name,
            "parserVersion": parsed.parser_version,
            "parserConfidence": parsed.parser_confidence,
            "rows": len(trades),
            "rowsDropped": max(0, dropped),
        }
        if dry_run:
            entry["status"] = "would publish"
            entry["stubStatus"] = resolve_house_stub_status(stub, parsed, trades)
            summary["published"] = int(summary["published"]) + 1
            summary["tradeRowsUpserted"] = int(summary["tradeRowsUpserted"]) + len(trades)
            summary["stubs"].append(entry)  # type: ignore[union-attr]
            continue
        try:
            result = persist_parsed_house_stub(settings, stub, parsed, trades)
        except Exception as error:  # pragma: no cover - depends on the live database
            logger.exception("repersist: %s failed: %s", doc_id, error)
            summary["failed"] = int(summary["failed"]) + 1
            entry["status"] = "failed"
            entry["error"] = str(error)[:500]
            summary["stubs"].append(entry)  # type: ignore[union-attr]
            continue
        upserted = int((result.get("trades") or {}).get("upserted", 0))  # type: ignore[union-attr]
        summary["published"] = int(summary["published"]) + 1
        summary["tradeRowsUpserted"] = int(summary["tradeRowsUpserted"]) + upserted
        entry["status"] = "published"
        entry["stubStatus"] = result.get("stubStatus")
        entry["rows"] = upserted
        entry["withheld"] = int((result.get("trades") or {}).get("withheld", 0))  # type: ignore[union-attr]
        summary["stubs"].append(entry)  # type: ignore[union-attr]
    return summary


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
        prior_vision=(
            {
                "visionParse": metadata.get("visionParse"),
                "parsedTransactions": metadata.get("parsedTransactions"),
            }
            if isinstance(metadata.get("visionParse"), dict)
            else None
        ),
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
    vision_backend: str = "off",
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
        "visionBackend": vision_backend,
        "visionCalls": 0,
        "visionRowsRecovered": 0,
        "visionCostUsd": 0.0,
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
            parsed, trades = parse_live_house_stub(stub, settings, ocr_backend, vision_backend)
            upsert_summary = persist_parsed_house_stub(settings, stub, parsed, trades)
            status = str(upsert_summary["stubStatus"])
            vision_report = parsed.vision_report if isinstance(parsed.vision_report, dict) else None
            if vision_report:
                summary["visionCalls"] += 1
                summary["visionRowsRecovered"] += int(vision_report.get("rowCount") or 0)
                summary["visionCostUsd"] = round(
                    float(summary["visionCostUsd"]) + float(vision_report.get("costUsd") or 0.0),
                    6,
                )
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
                "tradesWithheld": int((upsert_summary.get("trades") or {}).get("withheld", 0)),  # type: ignore[union-attr]
                "parserVersion": parsed.parser_version,
            }
            if vision_report:
                processed_item["visionParse"] = {
                    "ok": vision_report.get("ok"),
                    "reason": vision_report.get("reason"),
                    "rowCount": vision_report.get("rowCount"),
                    "rowsRecovered": vision_report.get("rowsRecovered"),
                    "noTransactions": vision_report.get("noTransactions"),
                    "reused": vision_report.get("reused", False),
                    "chunks": len(vision_report.get("chunks") or []),
                    "legibility": vision_report.get("legibility"),
                    "needsReviewReasons": vision_report.get("needsReviewReasons"),
                    "costUsd": vision_report.get("costUsd"),
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


SENATE_TRADE_SOURCE_FAMILIES: dict[str, list[str]] = {
    "quiver": ["senate_quiver", "senate-quiver"],
    "watcher": ["senate_watcher", "senate-watcher"],
    # eFD ids are provider independent now, so an eFD run must also see the
    # Quiver rows or it will re-insert trades Quiver already wrote.
    "efd": [
        "senate_efd",
        "senate-efd",
        "senate-ethics",
        "senate_quiver",
        "senate-quiver",
    ],
}


def resolve_senate_provider(provider: str, settings: Settings) -> str:
    """Resolve the ``auto`` Senate provider.

    ``auto`` prefers Quiver while a token is configured and otherwise falls back
    to the official eFD scraper. It never falls back to the senate-stock-watcher
    feed, which has been frozen since 2020.
    """

    if provider != "auto":
        return provider
    return "quiver" if settings.resolved_quiver_api_token else "efd"


def senate_source_family(feed_provider: str) -> list[str]:
    """Return the trades.source values one Senate provider should reconcile."""

    if feed_provider.startswith("quiver"):
        return SENATE_TRADE_SOURCE_FAMILIES["quiver"]
    if feed_provider == "efd":
        return SENATE_TRADE_SOURCE_FAMILIES["efd"]
    return SENATE_TRADE_SOURCE_FAMILIES["watcher"]


@cli.command("senate-feed")
@click.option("--limit", type=int, default=10, show_default=True)
@click.option(
    "--provider",
    type=click.Choice(["auto", "quiver", "watcher", "efd"]),
    default="auto",
    show_default=True,
)
@click.option(
    "--since",
    type=str,
    default=None,
    help="For --provider efd: only list reports submitted on or after YYYY-MM-DD.",
)
@click.option(
    "--with-transactions/--no-transactions",
    default=False,
    show_default=True,
    help="For --provider efd: also fetch and print each report's parsed rows.",
)
def senate_feed(limit: int, provider: str, since: str | None, with_transactions: bool) -> None:
    """Fetch and print the current Senate feed rows."""

    settings = Settings()
    feed_provider = resolve_senate_provider(provider, settings)

    if feed_provider == "efd":
        submitted_since = (
            date.fromisoformat(since)
            if since
            else build_efd_submitted_since(None, floor_days=settings.senate_efd_floor_days)
        )
        payload: list[dict[str, object]] = []
        with SenateEfdClient(settings) as client:
            reports = list_ptr_reports(
                settings,
                submitted_since,
                None,
                max(1, limit),
                client=client,
            )
            for report in reports:
                entry = report.model_dump()
                if with_transactions:
                    if report.kind == "electronic":
                        entry["transactions"] = [
                            transaction.model_dump()
                            for transaction in fetch_electronic_ptr(client, report)
                        ]
                    else:
                        entry["pageImages"] = fetch_paper_ptr_pages(client, report)
                payload.append(entry)
        click.echo(
            json.dumps(
                {
                    "provider": "efd",
                    "submittedSince": submitted_since.isoformat(),
                    "reports": payload,
                },
                indent=2,
            )
        )
        return

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
    type=click.Choice(["auto", "quiver", "quiver-live", "quiver-bulk", "watcher", "efd"]),
    default="auto",
    show_default=True,
)
@click.option(
    "--start-date",
    type=str,
    default="2021-01-01",
    show_default=True,
    help="Ignore rows filed before this date.",
)
@click.option("--page-size", type=int, default=250, show_default=True)
@click.option(
    "--reconcile-lookback-days",
    type=int,
    default=14,
    show_default=True,
    help="For quiver-bulk, replay only the most recent disclosure window to catch late amendments.",
)
@click.option(
    "--since",
    type=str,
    default=None,
    help=(
        "For --provider efd: override the submitted-date window start (YYYY-MM-DD). "
        "Defaults to the newest known Senate disclosure date minus the eFD lookback, "
        "floored so a scheduled run never scans more than the last 60 days."
    ),
)
@click.option(
    "--max-reports",
    type=int,
    default=0,
    show_default=True,
    help="For --provider efd: hard cap on reports opened per run. 0 uses the configured cap.",
)
def senate_ingest_command(
    limit: int,
    export_registry: bool,
    with_search_index: bool,
    with_embeddings: bool,
    provider: str,
    start_date: str,
    page_size: int,
    reconcile_lookback_days: int,
    since: str | None,
    max_reports: int,
) -> None:
    """Normalize new Senate trade rows and upsert them into CapitolExposed."""

    settings = Settings()
    registry = load_registry_if_available(settings, export_cache=export_registry)
    if not registry:
        raise click.ClickException("Member registry is required for Senate ingest.")

    feed_provider = resolve_senate_provider(provider, settings)
    if feed_provider == "quiver":
        feed_provider = "quiver-live"

    source_family = senate_source_family(feed_provider)
    existing_trade_ids = fetch_existing_trade_ids(settings, sources=source_family)
    latest_known_disclosure_date = fetch_latest_trade_disclosure_date(
        settings,
        sources=source_family,
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
        "latestKnownDisclosureDate": latest_known_disclosure_date,
        "reconcileLookbackDays": reconcile_lookback_days,
        "windowStartDate": None,
        "windowEndDate": None,
        "reportsListed": 0,
        "electronicParsed": 0,
        "paperDeferred": 0,
        "paperDeferredReports": [],
        "errors": [],
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

    if feed_provider == "efd":
        submitted_since = (
            date.fromisoformat(since)
            if since
            else build_efd_submitted_since(
                latest_known_disclosure_date,
                lookback_days=settings.senate_efd_lookback_days,
                floor_days=settings.senate_efd_floor_days,
            )
        )
        summary["windowStartDate"] = submitted_since.isoformat()
        summary["windowEndDate"] = datetime.now(timezone.utc).date().isoformat()
        report_cap = max_reports if max_reports > 0 else settings.senate_efd_max_reports_per_run

        stop = False
        with SenateEfdClient(settings) as efd_client:
            reports = list_ptr_reports(
                settings,
                submitted_since,
                None,
                report_cap,
                client=efd_client,
            )
            summary["reportsListed"] = len(reports)

            for report in reports:
                if report.kind != "electronic":
                    page_images: list[str] = []
                    try:
                        page_images = fetch_paper_ptr_pages(efd_client, report)
                    except Exception as error:  # noqa: BLE001 - one bad scan cannot stop the run
                        summary["errors"].append(  # type: ignore[union-attr]
                            {"reportId": report.report_id, "error": str(error)[:200]}
                        )
                    summary["paperDeferred"] = int(summary["paperDeferred"]) + 1
                    if len(summary["paperDeferredReports"]) < 25:  # type: ignore[arg-type]
                        summary["paperDeferredReports"].append(  # type: ignore[union-attr]
                            build_paper_report_stub(report, page_images)
                        )
                    continue

                try:
                    transactions = fetch_electronic_ptr(efd_client, report)
                except Exception as error:  # noqa: BLE001 - skip the report and keep going
                    summary["errors"].append(  # type: ignore[union-attr]
                        {"reportId": report.report_id, "error": str(error)[:200]}
                    )
                    continue

                summary["electronicParsed"] = int(summary["electronicParsed"]) + 1
                summary["fetched"] = int(summary["fetched"]) + len(transactions)
                for transaction in transactions:
                    stop = handle_normalized_row(
                        normalize_efd_transaction(report, transaction, registry),
                    )
                    if stop:
                        break
                if stop:
                    break
    elif feed_provider == "quiver-bulk":
        reconcile_dates = build_quiver_bulk_reconcile_dates(
            start_date=start_date,
            latest_known_disclosure_date=latest_known_disclosure_date,
            lookback_days=reconcile_lookback_days,
        )
        if reconcile_dates:
            summary["windowStartDate"] = reconcile_dates[0]
            summary["windowEndDate"] = reconcile_dates[-1]

        stop = False
        for disclosure_date in reversed(reconcile_dates):
            page = 1
            while True:
                page_rows = fetch_quiver_bulk_congress_feed(
                    settings,
                    page=page,
                    page_size=page_size,
                    date=disclosure_date,
                )
                if not page_rows:
                    break
                summary["fetched"] = int(summary["fetched"]) + len(page_rows)
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
            if stop:
                break
    elif feed_provider == "quiver-live":
        feed_rows = sorted(
            fetch_quiver_live_senate_feed(settings),
            key=lambda row: normalize_senate_date(row.disclosure_date) or "",
            reverse=True,
        )
        summary["fetched"] = len(feed_rows)
        for feed_row in feed_rows:
            normalized = normalize_quiver_live_senate_trade(feed_row, registry)
            if (
                normalized
                and latest_known_disclosure_date
                and normalized.disclosure_date
                and normalized.disclosure_date < latest_known_disclosure_date
            ):
                break
            stop = handle_normalized_row(normalized)
            if stop:
                break
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

    summary["tradesInserted"] = summary["tradesUpserted"]
    summary["skipped"] = (
        int(summary["skippedExisting"])
        + int(summary["skippedUnresolved"])
        + int(summary["skippedBeforeStartDate"])
    )

    click.echo(json.dumps(summary, indent=2))


@cli.command("dedupe-senate-trades")
@click.option(
    "--source",
    "sources",
    multiple=True,
    default=DEFAULT_SENATE_DEDUPE_SOURCES,
    show_default=True,
    help="trades.source values to sweep. Repeat the flag for more than one.",
)
@click.option(
    "--dry-run/--apply",
    default=True,
    show_default=True,
    help="Dry run only reports. --apply deletes the duplicates inside one transaction.",
)
@click.option("--limit", type=int, default=25, show_default=True, help="Sample groups to print.")
def dedupe_senate_trades_command(sources: tuple[str, ...], dry_run: bool, limit: int) -> None:
    """Collapse Senate trade rows the old canonical id scheme duplicated.

    Groups on (member_id, ticker, transaction_type, transaction_date,
    amount_min, amount_max), falling back to the normalized asset description
    when a row has no ticker, and keeps the most complete row: a non-null
    disclosure_date first, then the longest asset_description, then the oldest
    created_at.
    """

    settings = Settings()
    source_list = [source.strip() for source in sources if source.strip()]

    groups = fetch_duplicate_senate_trade_groups(settings, sources=source_list, limit=0)
    duplicate_rows = sum(max(0, int(group.get("row_count") or 0) - 1) for group in groups)

    summary: dict[str, object] = {
        "mode": "dry-run" if dry_run else "apply",
        "sources": source_list,
        "duplicateGroups": len(groups),
        "rowsToDelete": duplicate_rows,
        "deleted": 0,
        "sampleGroups": [
            {
                "memberId": group.get("member_id"),
                "assetKey": group.get("asset_key"),
                "transactionType": group.get("transaction_type"),
                "transactionDate": group.get("transaction_date"),
                "amountMin": group.get("amount_min"),
                "amountMax": group.get("amount_max"),
                "rowCount": group.get("row_count"),
                "keepId": group.get("keep_id"),
                "deleteIds": list(group.get("delete_ids") or []),
            }
            for group in groups[: max(0, limit)]
        ],
    }

    if not dry_run and duplicate_rows:
        result = delete_duplicate_senate_trades(settings, sources=source_list)
        summary["deleted"] = result.get("deleted", 0)
        summary["scanned"] = result.get("scanned", 0)

    click.echo(json.dumps(summary, indent=2, default=str))


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
@click.option(
    "--vision-backend",
    type=click.Choice(VISION_BACKENDS),
    default="off",
    show_default=True,
    help=VISION_BACKEND_HELP,
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
    vision_backend: str,
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
        vision_backend=vision_backend,
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
@click.option(
    "--vision-backend",
    type=click.Choice(VISION_BACKENDS),
    default="off",
    show_default=True,
    help=VISION_BACKEND_HELP,
)
def process_house_backlog_command(
    limit: int,
    export_registry: bool,
    with_search_index: bool,
    with_embeddings: bool,
    include_needs_review: bool,
    review_retry_hours: int,
    ocr_backend: str,
    vision_backend: str,
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
        vision_backend=vision_backend,
    )
    click.echo(json.dumps(summary, indent=2))


@cli.command("process-house-review")
@click.option(
    "--limit",
    type=int,
    default=2,
    show_default=True,
    help="Hard cap on filings per run. Every one may cost a vision call.",
)
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
@click.option(
    "--vision-backend",
    type=click.Choice(VISION_BACKENDS),
    default="auto",
    show_default=True,
    help=VISION_BACKEND_HELP,
)
@click.option(
    "--doc-id",
    "doc_ids",
    multiple=True,
    help="Only these filings (repeatable); the queue's status rules and --limit still apply.",
)
def process_house_review_command(
    limit: int,
    export_registry: bool,
    with_search_index: bool,
    with_embeddings: bool,
    review_retry_hours: int,
    ocr_backend: str,
    vision_backend: str,
    doc_ids: tuple[str, ...],
) -> None:
    """Reprocess the House PTR review queue with an alternate OCR backend."""

    settings = Settings()
    load_registry_if_available(settings, export_cache=export_registry)
    queue_rows = fetch_house_stub_queue(
        settings,
        limit=limit,
        only_needs_review=True,
        doc_ids=[doc_id.strip() for doc_id in doc_ids if doc_id.strip()] or None,
    )
    summary = process_house_queue_rows(
        settings,
        queue_rows,
        ocr_backend=ocr_backend,
        with_search_index=with_search_index,
        with_embeddings=with_embeddings,
        review_retry_hours=review_retry_hours,
        vision_backend=vision_backend,
    )
    summary["mode"] = "needs_review"
    click.echo(json.dumps(summary, indent=2))


@cli.command("repersist-house-stubs")
@click.option(
    "--limit",
    type=int,
    default=25,
    show_default=True,
    help="Filings per run. The command is restartable: publishing a filing takes it out of the queue.",
)
@click.option(
    "--doc-id",
    "doc_ids",
    multiple=True,
    help="Only these filings (repeatable). Overrides the status filter.",
)
@click.option(
    "--include-vision/--text-only",
    default=False,
    show_default=True,
    help=(
        "Vision transcriptions are withheld from trades on purpose while they "
        "wait for a human. --include-vision publishes them anyway; only use it "
        "on a filing you have read."
    ),
)
@click.option(
    "--min-confidence",
    type=float,
    default=0.0,
    show_default=True,
    help="Skip a filing whose stored parser confidence is below this.",
)
@click.option("--dry-run/--apply", default=False, show_default=True)
@click.option("--export-registry/--no-export-registry", default=True, show_default=True)
def repersist_house_stubs_command(
    limit: int,
    doc_ids: tuple[str, ...],
    include_vision: bool,
    min_confidence: float,
    dry_run: bool,
    export_registry: bool,
) -> None:
    """Publish House filings whose rows were parsed before their member resolved.

    When a PTR is parsed for a filer with no member record, the transcription
    is kept on the stub and nothing is written to trades. Loading the historical
    members afterwards resolves the filer, but nothing re-runs -- so finished,
    correct rows sit in the review queue indefinitely. This republishes them
    from what is already stored: no PDF is downloaded and no model is called.
    """

    settings = Settings()
    registry = load_registry_if_available(settings, export_cache=export_registry)
    rows = fetch_house_stubs_awaiting_publication(
        settings,
        limit=limit,
        doc_ids=[doc_id.strip() for doc_id in doc_ids if doc_id.strip()] or None,
        include_vision=include_vision,
    )
    summary = repersist_house_stub_rows(
        settings,
        rows,
        registry=registry,
        min_confidence=min_confidence,
        dry_run=dry_run,
    )
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
    sync_summary = sync_house_stubs_to_neon(
        settings,
        feed,
        prune_missing=sync_limit <= 0,
    )

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


@cli.command("ensure-members-bio-schema")
def ensure_members_bio_schema_command() -> None:
    """Add Wikipedia-style infobox + verified-headshot columns to the members table."""

    settings = Settings()
    summary = ensure_members_bio_schema(settings)
    click.echo(json.dumps(summary, indent=2))


@cli.command("sync-member-crosswalk")
def sync_member_crosswalk_command() -> None:
    """Pull Wikidata/Wikipedia/Ballotpedia/GovTrack/VoteSmart IDs from
    unitedstates/congress-legislators into the members table."""

    settings = Settings()
    ensure_members_bio_schema(settings)
    records = list(fetch_legislator_crosswalk(settings))
    summary = upsert_member_crosswalk(settings, records)
    click.echo(json.dumps({"fetched": len(records), **summary}, indent=2))


@cli.command("enrich-members-wikidata")
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Limit the number of QIDs enriched (smoke testing).",
)
def enrich_members_wikidata_command(limit: int | None) -> None:
    """Pull infobox facts (birthplace, education, religion, family, predecessor) from Wikidata."""

    settings = Settings()
    ensure_members_bio_schema(settings)
    pairs = fetch_member_qids(settings)
    if limit:
        pairs = pairs[: int(limit)]
    qids = [qid for _, qid in pairs]
    infoboxes = fetch_wikidata_infoboxes(settings, qids)
    applied = upsert_wikidata_infoboxes(settings, infoboxes)
    click.echo(
        json.dumps(
            {
                "members_with_qid": len(pairs),
                "infoboxes_returned": len(infoboxes),
                "rows_updated": applied,
            },
            indent=2,
        )
    )


@cli.command("import-bioguide-bios")
@click.option(
    "--only-missing/--all",
    default=True,
    show_default=True,
    help="Only fetch members without an existing bio_text.",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Optional cap on the number of bioguide IDs processed.",
)
@click.option(
    "--no-cache",
    is_flag=True,
    default=False,
    help="Bypass the on-disk Bioguide JSON cache.",
)
def import_bioguide_bios_command(
    only_missing: bool, limit: int | None, no_cache: bool
) -> None:
    """Pull profileText prose biographies from the Bioguide JSON feed."""

    settings = Settings()
    ensure_members_bio_schema(settings)
    bioguide_ids = fetch_member_bioguides(settings, only_missing=only_missing, limit=limit)
    records = list(
        fetch_bioguide_records(settings, bioguide_ids, use_cache=not no_cache)
    )
    applied = upsert_bioguide_records(settings, records)
    click.echo(
        json.dumps(
            {
                "candidates": len(bioguide_ids),
                "records_with_bio": len(records),
                "rows_updated": applied,
            },
            indent=2,
        )
    )


@cli.command("import-wikipedia-bios")
@click.option(
    "--only-missing/--all",
    default=True,
    show_default=True,
    help="Only fetch members without an existing bio_text.",
)
def import_wikipedia_bios_command(only_missing: bool) -> None:
    """Pull Wikipedia REST summaries (CC BY-SA) for the bio_text column.

    Used in place of the Bioguide profileText feed, which is gated by Akamai
    and returns 403 to every non-browser request.
    """

    settings = Settings()
    ensure_members_bio_schema(settings)
    pairs = fetch_member_wiki_pairs(settings, only_missing=only_missing)
    summaries = list(fetch_wikipedia_summaries(settings, pairs))
    applied = upsert_wikipedia_summaries(settings, summaries)
    click.echo(
        json.dumps(
            {
                "candidates": len(pairs),
                "summaries_returned": len(summaries),
                "rows_updated": applied,
            },
            indent=2,
        )
    )


@cli.command("sync-member-headshots")
@click.option(
    "--only-unverified/--all",
    default=False,
    show_default=True,
    help="Only re-run on members without a verified headshot or in needs_review.",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Optional cap on the number of members processed (smoke testing).",
)
@click.option(
    "--no-vlm",
    is_flag=True,
    default=False,
    help="Skip the Claude Haiku VLM fallback even if ANTHROPIC_API_KEY is set.",
)
def sync_member_headshots_command(
    only_unverified: bool, limit: int | None, no_vlm: bool
) -> None:
    """Fetch + verify member headshots from unitedstates/images, congress.gov, and Wikidata."""

    settings = Settings()
    ensure_members_bio_schema(settings)
    targets = fetch_headshot_targets(
        settings, only_unverified=only_unverified, limit=limit
    )
    verifications, summary = run_headshot_verification(
        settings, targets, enable_vlm_fallback=not no_vlm
    )
    applied = upsert_headshot_verifications(settings, verifications)
    click.echo(
        json.dumps(
            {**summary, "rows_updated": applied},
            indent=2,
        )
    )


# ---------------------------------------------------------------------------
# Reconciling a stored transcription
#
# The two reads of a scanned filing disagree on which amount box is ticked far
# more often than they disagree on anything else -- one of them counts the
# ladder from the wrong end and is a whole column out for a page at a time --
# and the merge nulls the amount rather than pick a side. The checkbox
# detector reads the same boxes off the pixels, for nothing, and can settle
# most of them. It runs during the read; this runs it again afterwards over
# what the stub already holds, so a filing withheld under the old behaviour
# can be released without a second read.
# ---------------------------------------------------------------------------

#: The review reason the letter conflict raises; dropped once nothing is left
#: unresolved. Keep in step with ``ptr_vision.extract_via_vision``.
AMOUNT_LETTER_REVIEW_REASON = "amount nulled after column-letter conflict"


def _short_pages(vision: dict[str, object], rows: list[dict[str, object]]) -> frozenset[int]:
    """Pages holding fewer rows now than the read saw on them.

    The stored ``detector`` block counted every merged row on each page. A page
    that no longer has that many cannot be aligned safely.
    """

    detector = vision.get("detector")
    pages = (detector or {}).get("pages") if isinstance(detector, dict) else None
    if not isinstance(pages, list):
        return frozenset()
    counts: dict[int, int] = {}
    for entry in rows:
        page = entry.get("page_number")
        if isinstance(page, int):
            counts[page] = counts.get(page, 0) + 1
    return frozenset(
        int(page["page"])
        for page in pages
        if isinstance(page, dict) and counts.get(int(page["page"]), 0) != int(page.get("rows") or 0)
    )


def _amount_from_row(row: dict[str, object]) -> tuple[int, int] | None:
    try:
        low = int(row.get("amount_min") or 0)
        high = int(row.get("amount_max") or 0)
    except (TypeError, ValueError):
        return None
    return (low, high) if low and high else None


def reconcile_stored_house_vision(
    settings: Settings,
    row: dict[str, object],
) -> dict[str, object]:
    """Settle a stub's withheld amounts from what it already stores.

    Downloads the filing's PDF, checks it is byte-for-byte the one that was
    read, and re-runs the free numpy checkbox detector over the stored
    transcription. No model is called and nothing is spent.

    Mutates ``row["metadata"]`` in place and returns a report. ``changed`` is
    False whenever nothing could be settled, in which case the caller has
    nothing to write.
    """

    doc_id = str(row.get("doc_id") or "")
    metadata = row.get("metadata")
    report: dict[str, object] = {"docId": doc_id, "changed": False}
    if not isinstance(metadata, dict):
        return {**report, "skipped": "stub metadata is not an object"}
    vision = metadata.get("visionParse")
    if not isinstance(vision, dict) or not vision.get("ok"):
        return {**report, "skipped": "no usable visionParse on the stub"}

    transcription, complete = transcription_from_metadata(vision)
    if not transcription:
        return {**report, "skipped": "no stored transcription to reconcile"}

    keyed = any(str(entry.get("line_number") or "").isdigit() for entry in transcription)
    # A transcription stored before line numbers were kept holds only the rows
    # that were published: any page that lost a row to a disputed transaction
    # type is now short, and the detector's bands would line up against the
    # wrong rows. Those pages are left alone rather than guessed at.
    skip_pages = frozenset() if keyed else _short_pages(vision, transcription)

    stub = build_stub_from_queue_row(row)
    with TemporaryDirectory(prefix="capitol-reconcile-") as temp_dir:
        pdf_path = Path(temp_dir) / f"{doc_id}.pdf"
        download_house_pdf(stub, settings, pdf_path)
        digest = hashlib.sha256(pdf_path.read_bytes()).hexdigest()
        expected = str(vision.get("pdfSha256") or "")
        if expected and digest != expected:
            # A different PDF is a different filing; reconciling one against
            # the other would move amounts onto the wrong rows.
            return {**report, "skipped": f"pdf changed since the read ({digest[:12]}...)"}
        outcome = reconcile_stored_transcription(pdf_path, transcription, skip_pages=skip_pages)

    if outcome is None:
        return {**report, "skipped": "the pages could not be rendered"}
    rows, detector = outcome
    resolved = [row_ for row_ in rows if row_.get("detectorStatus") == "resolved"]
    report.update(
        {
            "rows": len(rows),
            "transcriptionComplete": complete,
            "skippedPages": sorted(skip_pages),
            "resolved": len(resolved),
            "detector": {key: value for key, value in detector.items() if key != "pages"},
        }
    )
    if not resolved:
        return {**report, "skipped": "the detector settled nothing new"}

    # Carry the settled amounts onto the published rows. line_number is the
    # 1-based index into this very list, assigned by _vision_to_transactions.
    published = metadata.get("parsedTransactions")
    published = published if isinstance(published, list) else []
    by_line = {
        int(entry["line_number"]): entry
        for entry in published
        if isinstance(entry, dict) and str(entry.get("line_number") or "").isdigit()
    }
    republished = 0
    for index, transcribed in enumerate(rows, start=1):
        if transcribed.get("detectorStatus") != "resolved":
            continue
        line = transcribed.get("line_number") if keyed else index
        if not str(line or "").isdigit():
            continue
        entry = by_line.get(int(line))
        band = _amount_from_row(transcribed)
        if entry is None or band is None or _amount_from_row(entry) is not None:
            continue
        entry["amount_min"], entry["amount_max"] = band
        entry["comment"] = transcribed.get("comment") or entry.get("comment")
        republished += 1

    unresolved = sum(
        1
        for transcribed in rows
        if _amount_from_row(transcribed) is None and transcribed.get("transaction_type")
    )
    reasons = [
        reason
        for reason in (vision.get("needsReviewReasons") or [])
        if reason != AMOUNT_LETTER_REVIEW_REASON and not str(reason).startswith("checkbox detector")
    ]
    if unresolved and complete:
        reasons.append(AMOUNT_LETTER_REVIEW_REASON)
    elif not complete:
        # Only part of the transcription survived on the stub, so the rows past
        # the cap were never looked at. Releasing the filing on that basis
        # would be a guess; the amounts recovered here are not.
        reasons.append("stored transcription is truncated; not all rows were reconciled")
    reasons.extend(detector_review_reasons(detector))

    vision["detector"] = detector
    vision["transcription"] = stored_transcription(rows)
    published_rows = [entry for entry in rows if entry.get("transaction_type")]
    vision["rows"] = [row_summary(entry) for entry in published_rows[:MAX_ROW_SUMMARIES_IN_METADATA]]
    vision["amountsUnresolved"] = unresolved
    vision["needsReviewReasons"] = reasons
    vision["needsReview"] = bool(reasons)
    vision["reconciledAt"] = datetime.now(timezone.utc).isoformat()
    vision["reconcileCostUsd"] = 0.0
    report.update(
        {
            "changed": True,
            "amountsRestored": republished,
            "amountsUnresolved": unresolved,
            "needsReview": bool(reasons),
            "needsReviewReasons": reasons,
        }
    )
    return report


def reconcile_house_stub_rows(
    settings: Settings,
    rows: list[dict[str, object]],
    *,
    registry: MemberRegistry | None = None,
    dry_run: bool = True,
) -> dict[str, object]:
    """Reconcile a batch of stubs and, unless dry running, publish what clears."""

    summary: dict[str, object] = {
        "candidates": len(rows),
        "reconciled": 0,
        "skipped": 0,
        "failed": 0,
        "amountsRestored": 0,
        "published": 0,
        "tradeRowsUpserted": 0,
        "dryRun": dry_run,
        "costUsd": 0.0,
        "stubs": [],
    }
    for row in rows:
        doc_id = str(row.get("doc_id") or "")
        try:
            report = reconcile_stored_house_vision(settings, row)
        except Exception as error:  # noqa: BLE001 - one bad filing must not stop the batch
            logger.exception("reconcile: %s failed: %s", doc_id, error)
            summary["failed"] = int(summary["failed"]) + 1
            summary["stubs"].append({"docId": doc_id, "status": "failed", "error": str(error)[:500]})  # type: ignore[union-attr]
            continue
        if not report.get("changed"):
            summary["skipped"] = int(summary["skipped"]) + 1
            summary["stubs"].append({**report, "status": "skipped"})  # type: ignore[union-attr]
            continue
        summary["reconciled"] = int(summary["reconciled"]) + 1
        summary["amountsRestored"] = int(summary["amountsRestored"]) + int(report["amountsRestored"])  # type: ignore[arg-type]
        entry = {**report, "status": "reconciled"}
        if dry_run:
            summary["stubs"].append({**entry, "status": "would reconcile"})  # type: ignore[union-attr]
            continue
        stub, parsed, trades, skip_reason = rebuild_parsed_house_stub(row, registry=registry)
        if parsed is None:
            entry["publish"] = skip_reason
            summary["stubs"].append(entry)  # type: ignore[union-attr]
            continue
        result = persist_parsed_house_stub(settings, stub, parsed, trades)
        upserted = int((result.get("trades") or {}).get("upserted", 0))  # type: ignore[union-attr]
        summary["published"] = int(summary["published"]) + 1
        summary["tradeRowsUpserted"] = int(summary["tradeRowsUpserted"]) + upserted
        entry["stubStatus"] = result.get("stubStatus")
        entry["tradeRowsUpserted"] = upserted
        summary["stubs"].append(entry)  # type: ignore[union-attr]
    return summary


@cli.command("reconcile-house-vision")
@click.option(
    "--doc-id",
    "doc_ids",
    multiple=True,
    help="Only these filings (repeatable).",
)
@click.option("--limit", type=int, default=10, show_default=True)
@click.option(
    "--dry-run/--apply",
    default=True,
    show_default=True,
    help="Dry run reports what would be settled and writes nothing.",
)
@click.option("--export-registry/--no-export-registry", default=True, show_default=True)
def reconcile_house_vision_command(
    doc_ids: tuple[str, ...],
    limit: int,
    dry_run: bool,
    export_registry: bool,
) -> None:
    """Settle withheld amounts from a stored transcription. No model, no cost.

    A scanned filing is read twice, and where the two reads name different
    amount columns the merge keeps neither. The checkbox detector reads the
    ticked box off the page itself with numpy, for nothing; this re-runs it
    over the rows the stub already holds and fills in what it can settle.
    Nothing here calls a model -- not for the transcription, not for the page
    orientation -- and nothing is spent. A row the detector cannot settle keeps
    no amount, and a filing with any left keeps its place in the review queue.
    """

    settings = Settings()
    registry = load_registry_if_available(settings, export_cache=export_registry)
    queue_rows = fetch_house_stub_queue(
        settings,
        limit=limit,
        only_needs_review=True,
        doc_ids=[doc_id.strip() for doc_id in doc_ids if doc_id.strip()] or None,
    )
    summary = reconcile_house_stub_rows(settings, queue_rows, registry=registry, dry_run=dry_run)
    click.echo(json.dumps(summary, indent=2))


if __name__ == "__main__":
    cli()

from capitol_pipeline.members_historical import sync_members_historical_command  # noqa: E402

cli.add_command(sync_members_historical_command)
