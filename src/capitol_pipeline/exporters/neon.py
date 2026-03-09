"""Neon Postgres adapters for CapitolExposed table shapes."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator

from capitol_pipeline.bridges.capitol_exposed import (
    build_house_stub_payload,
    build_trade_payload,
)
from capitol_pipeline.config import Settings
from capitol_pipeline.models.congress import FilingStub, NormalizedTradeRow
from capitol_pipeline.registries.members import MemberRegistry

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Jsonb
except ImportError:  # pragma: no cover - optional dependency in local env
    psycopg = None
    dict_row = None
    Jsonb = None


def ensure_neon_available() -> None:
    """Raise a clear error if the optional Neon dependency is not installed."""

    if psycopg is None or dict_row is None or Jsonb is None:
        raise RuntimeError(
            "Neon export requires psycopg. Install with `pip install -e .[neon]`."
        )


def _require_database_url(settings: Settings) -> str:
    database_url = settings.resolved_neon_database_url
    if not database_url:
        raise RuntimeError(
            "No database URL configured. Set CAPITOL_NEON_DATABASE_URL or DATABASE_URL."
        )
    return database_url


@contextmanager
def neon_connection(settings: Settings) -> Iterator["psycopg.Connection"]:  # type: ignore[name-defined]
    """Open a short-lived psycopg connection to Neon."""

    ensure_neon_available()
    database_url = _require_database_url(settings)
    connection = psycopg.connect(database_url, row_factory=dict_row)  # type: ignore[union-attr]
    try:
        yield connection
    finally:
        connection.close()


def load_member_registry_from_neon(
    settings: Settings,
    *,
    export_cache: bool = False,
) -> MemberRegistry:
    """Load the CapitolExposed members table into a local registry."""

    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, name, slug, party, state, district, first_name, last_name
                FROM members
                ORDER BY in_office DESC NULLS LAST, name ASC
                """
            )
            rows = cursor.fetchall()

    registry = MemberRegistry.from_rows(rows)
    if export_cache:
        registry.save_json(settings.members_registry_path)
    return registry


def sync_house_stubs_to_neon(
    settings: Settings,
    stubs: list[FilingStub],
) -> dict[str, int]:
    """Upsert House filing stubs into CapitolExposed's house_filing_stubs table."""

    payloads = [build_house_stub_payload(stub) for stub in stubs]
    if not payloads:
        return {"upserted": 0}

    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO house_filing_stubs (
                    doc_id,
                    filing_year,
                    source,
                    source_url,
                    status,
                    metadata
                )
                VALUES (
                    %(doc_id)s,
                    %(filing_year)s,
                    %(source)s,
                    %(source_url)s,
                    %(status)s,
                    %(metadata)s
                )
                ON CONFLICT (doc_id) DO UPDATE SET
                    filing_year = EXCLUDED.filing_year,
                    source = EXCLUDED.source,
                    source_url = EXCLUDED.source_url,
                    metadata = COALESCE(house_filing_stubs.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                    last_seen_at = NOW(),
                    status = CASE
                        WHEN house_filing_stubs.status IN ('extracting', 'parsed') THEN house_filing_stubs.status
                        ELSE EXCLUDED.status
                    END
                """,
                [
                    {
                        **payload,
                        "metadata": Jsonb(payload["metadata"]),  # type: ignore[arg-type]
                    }
                    for payload in payloads
                ],
            )
        connection.commit()

    return {"upserted": len(payloads)}


def fetch_house_stub_queue(
    settings: Settings,
    *,
    limit: int = 10,
) -> list[dict[str, object]]:
    """Load queued House filing stubs that are ready for extraction or retry."""

    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT doc_id, filing_year, source, source_url, status, extracted_trade_id, metadata,
                       detected_at, last_seen_at
                FROM house_filing_stubs
                WHERE status IN ('pending_extraction', 'extracting', 'needs_review')
                  AND COALESCE(NULLIF(metadata->>'retryAfter', '')::timestamptz, NOW() - INTERVAL '1 second') <= NOW()
                ORDER BY
                    CASE
                        WHEN status = 'pending_extraction'
                          AND COALESCE(metadata->>'lastError', '') NOT ILIKE 'PTR PDF fetch failed with 404%%'
                          THEN 0
                        WHEN status = 'extracting' THEN 1
                        WHEN status = 'needs_review' THEN 2
                        ELSE 3
                    END,
                    detected_at DESC
                LIMIT %s
                """,
                (max(1, limit),),
            )
            return list(cursor.fetchall())


def upsert_trade_rows_to_neon(
    settings: Settings,
    rows: list[NormalizedTradeRow],
) -> dict[str, object]:
    """Upsert parsed trade rows into CapitolExposed's trades table."""

    exportable_rows = [row for row in rows if row.member.id]
    payloads = [build_trade_payload(row) for row in exportable_rows]
    if not payloads:
        return {"upserted": 0, "trade_ids": []}

    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO trades (
                    id,
                    member_id,
                    ticker,
                    asset_description,
                    asset_type,
                    transaction_type,
                    transaction_date,
                    disclosure_date,
                    amount_min,
                    amount_max,
                    owner,
                    comment,
                    source,
                    source_url,
                    conflict_score,
                    conflict_flags
                )
                VALUES (
                    %(id)s,
                    %(member_id)s,
                    %(ticker)s,
                    %(asset_description)s,
                    %(asset_type)s,
                    %(transaction_type)s,
                    %(transaction_date)s,
                    %(disclosure_date)s,
                    %(amount_min)s,
                    %(amount_max)s,
                    %(owner)s,
                    %(comment)s,
                    %(source)s,
                    %(source_url)s,
                    %(conflict_score)s,
                    %(conflict_flags)s
                )
                ON CONFLICT (id) DO UPDATE SET
                    member_id = EXCLUDED.member_id,
                    ticker = EXCLUDED.ticker,
                    asset_description = EXCLUDED.asset_description,
                    asset_type = EXCLUDED.asset_type,
                    transaction_type = EXCLUDED.transaction_type,
                    transaction_date = EXCLUDED.transaction_date,
                    disclosure_date = EXCLUDED.disclosure_date,
                    amount_min = EXCLUDED.amount_min,
                    amount_max = EXCLUDED.amount_max,
                    owner = EXCLUDED.owner,
                    comment = EXCLUDED.comment,
                    source = EXCLUDED.source,
                    source_url = EXCLUDED.source_url,
                    conflict_score = EXCLUDED.conflict_score,
                    conflict_flags = EXCLUDED.conflict_flags
                """,
                [
                    {
                        **payload,
                        "conflict_flags": Jsonb(payload["conflict_flags"]),  # type: ignore[arg-type]
                    }
                    for payload in payloads
                ],
            )
        connection.commit()

    return {
        "upserted": len(payloads),
        "trade_ids": [str(payload["id"]) for payload in payloads],
    }


def update_house_stub_state(
    settings: Settings,
    *,
    doc_id: str,
    status: str,
    metadata_updates: dict[str, object],
    extracted_trade_id: str | None = None,
) -> None:
    """Merge metadata into a stub row and update its extraction state."""

    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE house_filing_stubs
                SET status = %s,
                    extracted_trade_id = %s,
                    metadata = COALESCE(metadata, '{}'::jsonb) || %s
                WHERE doc_id = %s
                """,
                (
                    status,
                    extracted_trade_id,
                    Jsonb(metadata_updates),  # type: ignore[arg-type]
                    doc_id,
                ),
            )
        connection.commit()


def mark_house_stub_processed(
    settings: Settings,
    stub: FilingStub,
    *,
    status: str,
    parser_confidence: float,
    parsed_transaction_count: int,
    extracted_trade_id: str | None = None,
    last_error: str | None = None,
    raw_text_preview: str | None = None,
    parsed_transactions: list[dict[str, object]] | None = None,
) -> None:
    """Update a House stub after parsing so the site can surface its current state."""

    metadata = build_house_stub_payload(stub)["metadata"]
    metadata.update(
        {
            "parserConfidence": parser_confidence,
            "parsedAt": datetime.now(timezone.utc).isoformat(),
            "parsedTransactionCount": parsed_transaction_count,
            "lastError": last_error,
            "rawTextPreview": raw_text_preview,
            "parsedTransactions": parsed_transactions or [],
        }
    )

    update_house_stub_state(
        settings,
        doc_id=stub.doc_id,
        status=status,
        extracted_trade_id=extracted_trade_id,
        metadata_updates=metadata,
    )
