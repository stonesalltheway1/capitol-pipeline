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
from capitol_pipeline.models.search import SearchChunkRecord, SearchDocumentRecord, SearchHit
from capitol_pipeline.registries.members import MemberRegistry

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Jsonb
except ImportError:  # pragma: no cover - optional dependency in local env
    psycopg = None
    dict_row = None
    Jsonb = None

try:
    from pgvector.psycopg import register_vector
except ImportError:  # pragma: no cover - optional dependency in local env
    register_vector = None


PIPELINE_SEARCH_DOCUMENTS_TABLE = "pipeline_search_documents"
PIPELINE_SEARCH_CHUNKS_TABLE = "pipeline_search_chunks"


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
    if register_vector is not None:
        register_vector(connection)
    try:
        yield connection
    finally:
        connection.close()


def vector_literal(values: list[float] | None) -> str | None:
    """Serialize a Python list into pgvector text literal format."""

    if not values:
        return None
    return "[" + ",".join(f"{value:.8f}" for value in values) + "]"


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


def ensure_search_schema(settings: Settings) -> dict[str, object]:
    """Create the lexical and vector search tables used by the pipeline."""

    dimensions = (
        settings.openai_embedding_dimensions
        if settings.embedding_provider == "openai"
        else settings.embedding_dimensions
    )
    if dimensions <= 0:
        raise RuntimeError("Embedding dimensions must be greater than zero.")

    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS pipeline_search_documents (
                    id TEXT PRIMARY KEY,
                    source_document_id TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    content TEXT NOT NULL,
                    source TEXT NOT NULL,
                    category TEXT NOT NULL,
                    document_date DATE NULL,
                    summary TEXT NULL,
                    source_url TEXT NULL,
                    pdf_url TEXT NULL,
                    member_ids TEXT[] NOT NULL DEFAULT '{}'::text[],
                    committee_ids TEXT[] NOT NULL DEFAULT '{}'::text[],
                    bill_ids TEXT[] NOT NULL DEFAULT '{}'::text[],
                    asset_tickers TEXT[] NOT NULL DEFAULT '{}'::text[],
                    tags TEXT[] NOT NULL DEFAULT '{}'::text[],
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    content_tsv tsvector GENERATED ALWAYS AS (
                        to_tsvector(
                            'english',
                            coalesce(title, '') || ' ' || coalesce(summary, '') || ' ' || coalesce(content, '')
                        )
                    ) STORED,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS pipeline_search_chunks (
                    id TEXT PRIMARY KEY,
                    document_id TEXT NOT NULL REFERENCES pipeline_search_documents(id) ON DELETE CASCADE,
                    chunk_index INT NOT NULL,
                    content TEXT NOT NULL,
                    token_estimate INT NOT NULL DEFAULT 0,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    text_tsv tsvector GENERATED ALWAYS AS (
                        to_tsvector('english', coalesce(content, ''))
                    ) STORED,
                    embedding vector({dimensions}),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_pipeline_search_documents_tsv ON pipeline_search_documents USING GIN (content_tsv)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_pipeline_search_chunks_tsv ON pipeline_search_chunks USING GIN (text_tsv)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_pipeline_search_chunks_document ON pipeline_search_chunks USING BTREE (document_id, chunk_index)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_pipeline_search_documents_source ON pipeline_search_documents USING BTREE (source, category, document_date DESC)"
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_pipeline_search_chunks_embedding
                ON pipeline_search_chunks
                USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100)
                """
            )
        connection.commit()

    return {
        "dimensions": dimensions,
        "tables": [PIPELINE_SEARCH_DOCUMENTS_TABLE, PIPELINE_SEARCH_CHUNKS_TABLE],
    }


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


def upsert_search_document(
    settings: Settings,
    document: SearchDocumentRecord,
    *,
    ensure_schema: bool = True,
) -> dict[str, object]:
    """Upsert a searchable document record."""

    if ensure_schema:
        ensure_search_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO pipeline_search_documents (
                    id,
                    source_document_id,
                    title,
                    content,
                    source,
                    category,
                    document_date,
                    summary,
                    source_url,
                    pdf_url,
                    member_ids,
                    committee_ids,
                    bill_ids,
                    asset_tickers,
                    tags,
                    metadata,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (id) DO UPDATE SET
                    source_document_id = EXCLUDED.source_document_id,
                    title = EXCLUDED.title,
                    content = EXCLUDED.content,
                    source = EXCLUDED.source,
                    category = EXCLUDED.category,
                    document_date = EXCLUDED.document_date,
                    summary = EXCLUDED.summary,
                    source_url = EXCLUDED.source_url,
                    pdf_url = EXCLUDED.pdf_url,
                    member_ids = EXCLUDED.member_ids,
                    committee_ids = EXCLUDED.committee_ids,
                    bill_ids = EXCLUDED.bill_ids,
                    asset_tickers = EXCLUDED.asset_tickers,
                    tags = EXCLUDED.tags,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                """,
                (
                    document.id,
                    document.source_document_id,
                    document.title,
                    document.content,
                    document.source,
                    document.category,
                    document.document_date,
                    document.summary,
                    document.source_url,
                    document.pdf_url,
                    document.member_ids,
                    document.committee_ids,
                    document.bill_ids,
                    document.asset_tickers,
                    document.tags,
                    Jsonb(document.metadata),  # type: ignore[arg-type]
                ),
            )
        connection.commit()
    return {"upserted": 1, "document_id": document.id}


def upsert_search_chunks(
    settings: Settings,
    chunks: list[SearchChunkRecord],
    *,
    ensure_schema: bool = True,
) -> dict[str, object]:
    """Replace and insert search chunks for a document."""

    if ensure_schema:
        ensure_search_schema(settings)
    if not chunks:
        return {"upserted": 0, "document_id": None}

    document_id = chunks[0].document_id
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute("DELETE FROM pipeline_search_chunks WHERE document_id = %s", (document_id,))
            cursor.executemany(
                """
                INSERT INTO pipeline_search_chunks (
                    id,
                    document_id,
                    chunk_index,
                    content,
                    token_estimate,
                    metadata,
                    embedding,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s::vector, NOW()
                )
                """,
                [
                    (
                        chunk.id,
                        chunk.document_id,
                        chunk.chunk_index,
                        chunk.content,
                        chunk.token_estimate,
                        Jsonb(chunk.metadata),  # type: ignore[arg-type]
                        vector_literal(chunk.embedding),
                    )
                    for chunk in chunks
                ],
            )
        connection.commit()
    return {"upserted": len(chunks), "document_id": document_id}


def hybrid_search(
    settings: Settings,
    *,
    query_text: str,
    query_embedding: list[float] | None = None,
    limit: int = 10,
) -> list[SearchHit]:
    """Run a hybrid lexical and vector search across indexed chunks."""

    ensure_search_schema(settings)
    rows: list[dict[str, object]]
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            if query_embedding:
                cursor.execute(
                    """
                    WITH ranked AS (
                        SELECT
                            d.id AS document_id,
                            c.id AS chunk_id,
                            d.title,
                            c.content,
                            d.source,
                            d.category,
                            d.source_url,
                            d.pdf_url,
                            (
                                ts_rank_cd(d.content_tsv, websearch_to_tsquery('english', %s))
                                + ts_rank_cd(c.text_tsv, websearch_to_tsquery('english', %s))
                            ) AS lexical_score,
                            1 - (c.embedding <=> %s::vector) AS semantic_score,
                            (
                                (
                                    ts_rank_cd(d.content_tsv, websearch_to_tsquery('english', %s))
                                    + ts_rank_cd(c.text_tsv, websearch_to_tsquery('english', %s))
                                ) * 0.45
                                + (1 - (c.embedding <=> %s::vector)) * 0.55
                            ) AS combined_score,
                            c.metadata
                        FROM pipeline_search_chunks c
                        JOIN pipeline_search_documents d ON d.id = c.document_id
                        WHERE d.content_tsv @@ websearch_to_tsquery('english', %s)
                           OR c.text_tsv @@ websearch_to_tsquery('english', %s)
                           OR c.embedding IS NOT NULL
                        ORDER BY combined_score DESC
                        LIMIT %s
                    )
                    SELECT * FROM ranked ORDER BY combined_score DESC
                    """,
                    (
                        query_text,
                        query_text,
                        query_text,
                        vector_literal(query_embedding),
                        query_text,
                        query_text,
                        vector_literal(query_embedding),
                        query_text,
                        query_text,
                        max(1, limit),
                    ),
                )
            else:
                cursor.execute(
                    """
                    SELECT
                        d.id AS document_id,
                        c.id AS chunk_id,
                        d.title,
                        c.content,
                        d.source,
                        d.category,
                        d.source_url,
                        d.pdf_url,
                        (
                            ts_rank_cd(d.content_tsv, websearch_to_tsquery('english', %s))
                            + ts_rank_cd(c.text_tsv, websearch_to_tsquery('english', %s))
                        ) AS lexical_score,
                        0.0 AS semantic_score,
                        (
                            ts_rank_cd(d.content_tsv, websearch_to_tsquery('english', %s))
                            + ts_rank_cd(c.text_tsv, websearch_to_tsquery('english', %s))
                        ) AS combined_score,
                        c.metadata
                    FROM pipeline_search_chunks c
                    JOIN pipeline_search_documents d ON d.id = c.document_id
                    WHERE d.content_tsv @@ websearch_to_tsquery('english', %s)
                       OR c.text_tsv @@ websearch_to_tsquery('english', %s)
                    ORDER BY combined_score DESC
                    LIMIT %s
                    """,
                    (
                        query_text,
                        query_text,
                        query_text,
                        query_text,
                        query_text,
                        query_text,
                        max(1, limit),
                    ),
                )
            rows = list(cursor.fetchall())

    return [
        SearchHit(
            document_id=str(row["document_id"]),
            chunk_id=str(row["chunk_id"]) if row.get("chunk_id") else None,
            title=str(row["title"]),
            content=str(row["content"]),
            source=str(row["source"]),
            category=str(row["category"]),
            source_url=str(row["source_url"]) if row.get("source_url") else None,
            pdf_url=str(row["pdf_url"]) if row.get("pdf_url") else None,
            lexical_score=float(row.get("lexical_score") or 0.0),
            semantic_score=float(row.get("semantic_score") or 0.0),
            combined_score=float(row.get("combined_score") or 0.0),
            metadata=dict(row.get("metadata") or {}),
        )
        for row in rows
    ]


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
                WHERE status IN ('pending_extraction', 'extracting')
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


def fetch_house_stub_search_backfill(
    settings: Settings,
    *,
    limit: int = 0,
    include_needs_review: bool = True,
    only_missing: bool = True,
) -> list[dict[str, object]]:
    """Load parsed House PTR stubs that should be indexed into search."""

    statuses = ["parsed"]
    if include_needs_review:
        statuses.append("needs_review")

    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            query = f"""
                SELECT h.doc_id, h.filing_year, h.source_url, h.status, h.metadata, h.detected_at
                FROM house_filing_stubs h
                WHERE h.status = ANY(%s)
                  AND COALESCE(h.metadata->>'rawTextPreview', '') <> ''
                  {"AND NOT EXISTS (SELECT 1 FROM pipeline_search_documents d WHERE d.source_document_id = CONCAT('house-ptr-', h.doc_id))" if only_missing else ""}
                ORDER BY h.detected_at DESC
                {"" if limit <= 0 else "LIMIT %s"}
            """
            params: list[object] = [statuses]
            if limit > 0:
                params.append(limit)
            cursor.execute(query, tuple(params))
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
