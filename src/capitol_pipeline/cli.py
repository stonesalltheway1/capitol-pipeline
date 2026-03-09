"""CLI for Capitol Pipeline."""

from __future__ import annotations

import json
from pathlib import Path

import click

from capitol_pipeline.normalizers.crypto_assets import classify_crypto_asset
from capitol_pipeline.processors.ocr import OcrProcessor
from capitol_pipeline.sources.house_clerk import fetch_house_feed
from capitol_pipeline.sources.senate_ethics import fetch_senate_watcher_feed


@click.group()
def cli() -> None:
    """Capitol Pipeline command line interface."""


@cli.command("house-feed")
@click.option("--year", type=int, required=True, help="Disclosure year, for example 2026.")
@click.option("--limit", type=int, default=10, show_default=True)
def house_feed(year: int, limit: int) -> None:
    """Fetch and print the latest House filing stubs."""

    rows = fetch_house_feed(year)
    click.echo(json.dumps([row.model_dump() for row in rows[:limit]], indent=2))


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

    processor = OcrProcessor()
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


if __name__ == "__main__":
    cli()
