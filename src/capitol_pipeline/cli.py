"""CLI for Capitol Pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

import click
import httpx

from capitol_pipeline.config import OcrBackend, Settings
from capitol_pipeline.exporters.neon import (
    load_member_registry_from_neon,
    mark_house_stub_processed,
    sync_house_stubs_to_neon,
    upsert_trade_rows_to_neon,
)
from capitol_pipeline.models.congress import FilingStub, MemberMatch
from capitol_pipeline.normalizers.crypto_assets import classify_crypto_asset
from capitol_pipeline.parsers.house_ptr import parse_house_ptr_pdf
from capitol_pipeline.processors.ocr import OcrProcessor
from capitol_pipeline.registries.members import MemberRegistry, load_member_registry_from_json
from capitol_pipeline.sources.house_clerk import fetch_house_feed
from capitol_pipeline.sources.senate_ethics import fetch_senate_watcher_feed


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
def senate_feed(limit: int) -> None:
    """Fetch and print the current Senate watcher rows."""

    rows = fetch_senate_watcher_feed()
    click.echo(json.dumps([row.model_dump() for row in rows[:limit]], indent=2))


@cli.command("classify-crypto")
@click.option("--ticker", type=str, default=None)
@click.option("--description", type=str, default=None)
def classify_crypto(ticker: str | None, description: str | None) -> None:
    """Classify a security as direct crypto, ETF/trust, adjacent equity, or unrelated."""

    click.echo(json.dumps(classify_crypto_asset(ticker, description).model_dump(), indent=2))


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
        upsert_summary = {
            "stubs": {"upserted": 1},
            "trades": trade_summary,
            "stubStatus": status,
        }

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

    with TemporaryDirectory(prefix="capitol-ptr-") as temp_dir:
        pdf_path = Path(temp_dir) / f"{doc_id}.pdf"
        with httpx.Client(
            headers={"User-Agent": settings.user_agent},
            follow_redirects=True,
            timeout=30.0,
        ) as client:
            response = client.get(stub.source_url)
            response.raise_for_status()
        pdf_path.write_bytes(response.content)
        parsed, trades = parse_house_ptr_pdf(
            pdf_path,
            stub=stub,
            settings=settings,
            backend=ocr_backend,
        )

    upsert_summary: dict[str, object] | None = None
    if upsert:
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
        upsert_summary = {
            "stubs": {"upserted": 1},
            "trades": trade_summary,
            "stubStatus": status,
        }

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


if __name__ == "__main__":
    cli()
