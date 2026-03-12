"""Neon Postgres adapters for CapitolExposed table shapes."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
from typing import Iterator

from capitol_pipeline.bridges.capitol_exposed import (
    build_house_stub_payload,
    build_trade_payload,
)
from capitol_pipeline.config import Settings
from capitol_pipeline.models.congress import FilingStub, NormalizedTradeRow
from capitol_pipeline.models.fara import (
    FaraDocumentRecord,
    FaraForeignPrincipalRecord,
    FaraMemberMatchRecord,
    FaraRegistrantRecord,
    FaraShortFormRecord,
)
from capitol_pipeline.models.legislation import (
    CongressBillActionRecord,
    CongressBillRecord,
    CongressBillSummaryRecord,
)
from capitol_pipeline.models.offshore import (
    OffshoreMemberMatchRecord,
    OffshoreNodeRecord,
    OffshoreRelationshipRecord,
)
from capitol_pipeline.models.search import SearchChunkRecord, SearchDocumentRecord, SearchHit
from capitol_pipeline.models.usaspending import (
    UsaspendingAwardRecord,
    UsaspendingCompanyMatchRecord,
    UsaspendingRecipientRecord,
)
from capitol_pipeline.normalizers.crypto_assets import (
    CRYPTO_EQUITY_TICKERS,
    CRYPTO_ETF_TICKERS,
    DIRECT_CRYPTO_SYMBOLS,
    classify_crypto_asset,
)
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
PIPELINE_OFFSHORE_NODES_TABLE = "pipeline_offshore_nodes"
PIPELINE_OFFSHORE_RELATIONSHIPS_TABLE = "pipeline_offshore_relationships"
PIPELINE_OFFSHORE_MEMBER_MATCHES_TABLE = "pipeline_offshore_member_matches"
PIPELINE_FARA_REGISTRANTS_TABLE = "pipeline_fara_registrants"
PIPELINE_FARA_FOREIGN_PRINCIPALS_TABLE = "pipeline_fara_foreign_principals"
PIPELINE_FARA_SHORT_FORMS_TABLE = "pipeline_fara_short_forms"
PIPELINE_FARA_DOCUMENTS_TABLE = "pipeline_fara_documents"
PIPELINE_FARA_MEMBER_MATCHES_TABLE = "pipeline_fara_member_matches"
PIPELINE_USASPENDING_RECIPIENTS_TABLE = "pipeline_usaspending_recipients"
PIPELINE_USASPENDING_COMPANY_MATCHES_TABLE = "pipeline_usaspending_company_matches"
PIPELINE_USASPENDING_AWARDS_TABLE = "pipeline_usaspending_awards"
PIPELINE_USASPENDING_COMPANY_SYNCS_TABLE = "pipeline_usaspending_company_syncs"
PIPELINE_CONGRESS_BILLS_TABLE = "pipeline_congress_bills"
PIPELINE_CONGRESS_BILL_SUMMARIES_TABLE = "pipeline_congress_bill_summaries"
PIPELINE_CONGRESS_BILL_ACTIONS_TABLE = "pipeline_congress_bill_actions"
PIPELINE_CONGRESS_BILL_SYNCS_TABLE = "pipeline_congress_bill_syncs"
CRYPTO_TRADE_SCAN_REGEX = (
    "(bitcoin|ethereum|ether|solana|xrp|cardano|dogecoin|litecoin|polkadot|"
    "avalanche|chainlink|crypto|digital asset|coinbase|microstrategy|"
    "marathon digital|riot platforms|bitcoin trust|bitcoin etf|ethereum trust|ethereum etf)"
)

# House filing stub doc_ids known to 404 permanently.  These are retained here
# as a blocklist so the pipeline never re-inserts them into the queue.
# Add confirmed dead doc_ids here as they are identified.
HOUSE_STUB_BLOCKLIST: frozenset[str] = frozenset()


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


def advisory_lock_key(name: str) -> int:
    """Create a stable advisory lock key from a schema name."""

    digest = hashlib.sha1(name.encode("utf-8")).digest()
    unsigned = int.from_bytes(digest[:8], "big", signed=False)
    return unsigned - (1 << 63) if unsigned >= (1 << 63) else unsigned


@contextmanager
def advisory_lock(connection: "psycopg.Connection", name: str) -> Iterator[None]:  # type: ignore[name-defined]
    """Hold a Postgres advisory lock for the duration of a schema update."""

    key = advisory_lock_key(name)
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_lock(%s)", (key,))
    try:
        yield
    finally:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_unlock(%s)", (key,))


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
                SELECT id, bioguide_id, name, slug, party, state, district, first_name, last_name
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
        with advisory_lock(connection, "pipeline-search-schema"):
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
                    """
                    SELECT format_type(a.atttypid, a.atttypmod)
                    FROM pg_attribute a
                    JOIN pg_class c ON a.attrelid = c.oid
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    WHERE c.relname = 'pipeline_search_chunks'
                      AND a.attname = 'embedding'
                      AND n.nspname = current_schema()
                    """
                )
                row = cursor.fetchone()
                current_type = str(row["format_type"]) if row and row.get("format_type") else None
                expected_type = f"vector({dimensions})"
                if current_type and current_type != expected_type:
                    raise RuntimeError(
                        f"Existing pipeline_search_chunks.embedding uses {current_type}. "
                        f"Expected {expected_type}. Set matching embedding dimensions before indexing."
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
                    "CREATE INDEX IF NOT EXISTS idx_pipeline_search_documents_member_ids ON pipeline_search_documents USING GIN (member_ids)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_pipeline_search_documents_committee_ids ON pipeline_search_documents USING GIN (committee_ids)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_pipeline_search_documents_bill_ids ON pipeline_search_documents USING GIN (bill_ids)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_pipeline_search_documents_asset_tickers ON pipeline_search_documents USING GIN (asset_tickers)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_pipeline_search_documents_tags ON pipeline_search_documents USING GIN (tags)"
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


def ensure_offshore_schema(settings: Settings) -> dict[str, object]:
    """Create the Offshore Leaks raw corpus tables and Congress match table."""

    with neon_connection(settings) as connection:
        with advisory_lock(connection, "pipeline-offshore-schema"):
            with connection.cursor() as cursor:
                cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {PIPELINE_OFFSHORE_NODES_TABLE} (
                    node_key TEXT PRIMARY KEY,
                    node_id TEXT NOT NULL,
                    node_type TEXT NOT NULL,
                    name TEXT NOT NULL,
                    normalized_name TEXT NOT NULL,
                    source_dataset TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    content TEXT NOT NULL,
                    countries TEXT[] NOT NULL DEFAULT '{{}}'::text[],
                    country_codes TEXT[] NOT NULL DEFAULT '{{}}'::text[],
                    jurisdiction TEXT NULL,
                    jurisdiction_description TEXT NULL,
                    company_type TEXT NULL,
                    address TEXT NULL,
                    status TEXT NULL,
                    service_provider TEXT NULL,
                    note TEXT NULL,
                    valid_until TEXT NULL,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    content_tsv tsvector GENERATED ALWAYS AS (
                        to_tsvector('english', coalesce(name, '') || ' ' || coalesce(summary, '') || ' ' || coalesce(content, ''))
                    ) STORED,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
                cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {PIPELINE_OFFSHORE_RELATIONSHIPS_TABLE} (
                    relationship_key TEXT PRIMARY KEY,
                    start_node_id TEXT NOT NULL,
                    end_node_id TEXT NOT NULL,
                    rel_type TEXT NOT NULL,
                    link TEXT NULL,
                    status TEXT NULL,
                    start_date TEXT NULL,
                    end_date TEXT NULL,
                    source_dataset TEXT NOT NULL,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
                cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {PIPELINE_OFFSHORE_MEMBER_MATCHES_TABLE} (
                    match_key TEXT PRIMARY KEY,
                    member_id TEXT NOT NULL,
                    member_name TEXT NOT NULL,
                    member_slug TEXT NULL,
                    node_key TEXT NOT NULL REFERENCES {PIPELINE_OFFSHORE_NODES_TABLE}(node_key) ON DELETE CASCADE,
                    node_type TEXT NOT NULL,
                    source_dataset TEXT NOT NULL,
                    match_type TEXT NOT NULL,
                    match_value TEXT NOT NULL,
                    metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
                cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipeline_offshore_nodes_name ON {PIPELINE_OFFSHORE_NODES_TABLE} USING BTREE (normalized_name)"
                )
                cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipeline_offshore_nodes_dataset ON {PIPELINE_OFFSHORE_NODES_TABLE} USING BTREE (source_dataset, node_type)"
                )
                cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipeline_offshore_nodes_tsv ON {PIPELINE_OFFSHORE_NODES_TABLE} USING GIN (content_tsv)"
                )
                cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipeline_offshore_relationships_start ON {PIPELINE_OFFSHORE_RELATIONSHIPS_TABLE} USING BTREE (start_node_id, rel_type)"
                )
                cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipeline_offshore_relationships_end ON {PIPELINE_OFFSHORE_RELATIONSHIPS_TABLE} USING BTREE (end_node_id, rel_type)"
                )
                cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_pipeline_offshore_member_matches_member ON {PIPELINE_OFFSHORE_MEMBER_MATCHES_TABLE} USING BTREE (member_id, source_dataset)"
                )
            connection.commit()

    return {
        "tables": [
            PIPELINE_OFFSHORE_NODES_TABLE,
            PIPELINE_OFFSHORE_RELATIONSHIPS_TABLE,
            PIPELINE_OFFSHORE_MEMBER_MATCHES_TABLE,
        ]
    }


def ensure_fara_schema(settings: Settings) -> dict[str, object]:
    """Create the official FARA raw corpus tables and Congress match table."""

    with neon_connection(settings) as connection:
        with advisory_lock(connection, "pipeline-fara-schema"):
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {PIPELINE_FARA_REGISTRANTS_TABLE} (
                        registration_number INT PRIMARY KEY,
                        name TEXT NOT NULL,
                        normalized_name TEXT NOT NULL,
                        registration_date DATE NULL,
                        address_1 TEXT NULL,
                        address_2 TEXT NULL,
                        city TEXT NULL,
                        state TEXT NULL,
                        zip_code TEXT NULL,
                        summary TEXT NOT NULL,
                        content TEXT NOT NULL,
                        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                        content_tsv tsvector GENERATED ALWAYS AS (
                            to_tsvector('english', coalesce(name, '') || ' ' || coalesce(summary, '') || ' ' || coalesce(content, ''))
                        ) STORED,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {PIPELINE_FARA_FOREIGN_PRINCIPALS_TABLE} (
                        principal_key TEXT PRIMARY KEY,
                        registration_number INT NOT NULL REFERENCES {PIPELINE_FARA_REGISTRANTS_TABLE}(registration_number) ON DELETE CASCADE,
                        foreign_principal_name TEXT NOT NULL,
                        normalized_name TEXT NOT NULL,
                        registrant_name TEXT NOT NULL,
                        country_name TEXT NULL,
                        registration_date DATE NULL,
                        foreign_principal_registration_date DATE NULL,
                        address_1 TEXT NULL,
                        address_2 TEXT NULL,
                        city TEXT NULL,
                        state TEXT NULL,
                        zip_code TEXT NULL,
                        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {PIPELINE_FARA_SHORT_FORMS_TABLE} (
                        short_form_key TEXT PRIMARY KEY,
                        registration_number INT NOT NULL REFERENCES {PIPELINE_FARA_REGISTRANTS_TABLE}(registration_number) ON DELETE CASCADE,
                        registrant_name TEXT NOT NULL,
                        first_name TEXT NOT NULL,
                        last_name TEXT NOT NULL,
                        full_name TEXT NOT NULL,
                        normalized_name TEXT NOT NULL,
                        short_form_date DATE NULL,
                        registration_date DATE NULL,
                        address_1 TEXT NULL,
                        address_2 TEXT NULL,
                        city TEXT NULL,
                        state TEXT NULL,
                        zip_code TEXT NULL,
                        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {PIPELINE_FARA_DOCUMENTS_TABLE} (
                        document_key TEXT PRIMARY KEY,
                        registration_number INT NOT NULL REFERENCES {PIPELINE_FARA_REGISTRANTS_TABLE}(registration_number) ON DELETE CASCADE,
                        registrant_name TEXT NOT NULL,
                        document_type TEXT NOT NULL,
                        date_stamped DATE NULL,
                        url TEXT NOT NULL,
                        short_form_name TEXT NULL,
                        foreign_principal_name TEXT NULL,
                        foreign_principal_country TEXT NULL,
                        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {PIPELINE_FARA_MEMBER_MATCHES_TABLE} (
                        match_key TEXT PRIMARY KEY,
                        member_id TEXT NOT NULL,
                        member_name TEXT NOT NULL,
                        member_slug TEXT NULL,
                        registration_number INT NOT NULL REFERENCES {PIPELINE_FARA_REGISTRANTS_TABLE}(registration_number) ON DELETE CASCADE,
                        entity_kind TEXT NOT NULL,
                        entity_key TEXT NOT NULL,
                        registrant_name TEXT NOT NULL,
                        match_type TEXT NOT NULL,
                        match_value TEXT NOT NULL,
                        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_fara_registrants_name ON {PIPELINE_FARA_REGISTRANTS_TABLE} USING BTREE (normalized_name)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_fara_registrants_tsv ON {PIPELINE_FARA_REGISTRANTS_TABLE} USING GIN (content_tsv)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_fara_foreign_principals_reg ON {PIPELINE_FARA_FOREIGN_PRINCIPALS_TABLE} USING BTREE (registration_number)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_fara_foreign_principals_name ON {PIPELINE_FARA_FOREIGN_PRINCIPALS_TABLE} USING BTREE (normalized_name)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_fara_short_forms_reg ON {PIPELINE_FARA_SHORT_FORMS_TABLE} USING BTREE (registration_number)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_fara_short_forms_name ON {PIPELINE_FARA_SHORT_FORMS_TABLE} USING BTREE (normalized_name)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_fara_documents_reg ON {PIPELINE_FARA_DOCUMENTS_TABLE} USING BTREE (registration_number, date_stamped DESC)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_fara_member_matches_member ON {PIPELINE_FARA_MEMBER_MATCHES_TABLE} USING BTREE (member_id, entity_kind)"
                )
            connection.commit()

    return {
        "tables": [
            PIPELINE_FARA_REGISTRANTS_TABLE,
            PIPELINE_FARA_FOREIGN_PRINCIPALS_TABLE,
            PIPELINE_FARA_SHORT_FORMS_TABLE,
            PIPELINE_FARA_DOCUMENTS_TABLE,
            PIPELINE_FARA_MEMBER_MATCHES_TABLE,
        ]
    }


def ensure_usaspending_schema(settings: Settings) -> dict[str, object]:
    """Create the USAspending recipient, match, award, and sync tables."""

    with neon_connection(settings) as connection:
        with advisory_lock(connection, "pipeline-usaspending-schema"):
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {PIPELINE_USASPENDING_RECIPIENTS_TABLE} (
                        recipient_id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        normalized_name TEXT NOT NULL,
                        query_name TEXT NOT NULL,
                        recipient_code TEXT NULL,
                        uei TEXT NULL,
                        total_amount DOUBLE PRECISION NULL,
                        total_outlays DOUBLE PRECISION NULL,
                        source_url TEXT NULL,
                        summary TEXT NOT NULL,
                        content TEXT NOT NULL,
                        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                        content_tsv tsvector GENERATED ALWAYS AS (
                            to_tsvector('english', coalesce(name, '') || ' ' || coalesce(summary, '') || ' ' || coalesce(content, ''))
                        ) STORED,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {PIPELINE_USASPENDING_COMPANY_MATCHES_TABLE} (
                        match_key TEXT PRIMARY KEY,
                        company_id TEXT NOT NULL,
                        company_name TEXT NOT NULL,
                        ticker TEXT NULL,
                        query_name TEXT NOT NULL,
                        canonical_recipient_id TEXT NOT NULL REFERENCES {PIPELINE_USASPENDING_RECIPIENTS_TABLE}(recipient_id) ON DELETE CASCADE,
                        recipient_name TEXT NOT NULL,
                        normalized_recipient_name TEXT NOT NULL,
                        recipient_code TEXT NULL,
                        uei TEXT NULL,
                        match_type TEXT NOT NULL,
                        match_value TEXT NOT NULL,
                        total_amount DOUBLE PRECISION NULL,
                        award_count INT NOT NULL DEFAULT 0,
                        top_agencies TEXT[] NOT NULL DEFAULT '{{}}'::text[],
                        source_url TEXT NULL,
                        summary TEXT NOT NULL,
                        content TEXT NOT NULL,
                        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                        content_tsv tsvector GENERATED ALWAYS AS (
                            to_tsvector('english', coalesce(company_name, '') || ' ' || coalesce(recipient_name, '') || ' ' || coalesce(summary, '') || ' ' || coalesce(content, ''))
                        ) STORED,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {PIPELINE_USASPENDING_AWARDS_TABLE} (
                        award_key TEXT PRIMARY KEY,
                        match_key TEXT NOT NULL REFERENCES {PIPELINE_USASPENDING_COMPANY_MATCHES_TABLE}(match_key) ON DELETE CASCADE,
                        canonical_recipient_id TEXT NOT NULL REFERENCES {PIPELINE_USASPENDING_RECIPIENTS_TABLE}(recipient_id) ON DELETE CASCADE,
                        recipient_name TEXT NOT NULL,
                        award_id TEXT NOT NULL,
                        internal_id BIGINT NULL,
                        generated_internal_id TEXT NULL,
                        action_date DATE NULL,
                        award_amount DOUBLE PRECISION NULL,
                        award_type TEXT NULL,
                        awarding_agency TEXT NULL,
                        awarding_agency_id INT NULL,
                        agency_slug TEXT NULL,
                        description TEXT NULL,
                        source_url TEXT NULL,
                        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {PIPELINE_USASPENDING_COMPANY_SYNCS_TABLE} (
                        company_id TEXT PRIMARY KEY,
                        ticker TEXT NULL,
                        company_name TEXT NOT NULL,
                        query_name TEXT NULL,
                        status TEXT NOT NULL,
                        result_count INT NOT NULL DEFAULT 0,
                        last_error TEXT NULL,
                        synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb
                    )
                    """
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_usaspending_recipients_name ON {PIPELINE_USASPENDING_RECIPIENTS_TABLE} USING BTREE (normalized_name)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_usaspending_recipients_tsv ON {PIPELINE_USASPENDING_RECIPIENTS_TABLE} USING GIN (content_tsv)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_usaspending_matches_company ON {PIPELINE_USASPENDING_COMPANY_MATCHES_TABLE} USING BTREE (company_id, ticker)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_usaspending_matches_recipient ON {PIPELINE_USASPENDING_COMPANY_MATCHES_TABLE} USING BTREE (canonical_recipient_id)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_usaspending_matches_tsv ON {PIPELINE_USASPENDING_COMPANY_MATCHES_TABLE} USING GIN (content_tsv)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_usaspending_awards_match ON {PIPELINE_USASPENDING_AWARDS_TABLE} USING BTREE (match_key, award_amount DESC)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_usaspending_awards_recipient ON {PIPELINE_USASPENDING_AWARDS_TABLE} USING BTREE (canonical_recipient_id, action_date DESC)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_usaspending_syncs_status ON {PIPELINE_USASPENDING_COMPANY_SYNCS_TABLE} USING BTREE (status, synced_at DESC)"
                )
            connection.commit()

    return {
        "tables": [
            PIPELINE_USASPENDING_RECIPIENTS_TABLE,
            PIPELINE_USASPENDING_COMPANY_MATCHES_TABLE,
            PIPELINE_USASPENDING_AWARDS_TABLE,
            PIPELINE_USASPENDING_COMPANY_SYNCS_TABLE,
        ]
    }


def ensure_congress_schema(settings: Settings) -> dict[str, object]:
    """Create the official Congress.gov bill-context tables."""

    with neon_connection(settings) as connection:
        with advisory_lock(connection, "pipeline-congress-schema"):
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {PIPELINE_CONGRESS_BILLS_TABLE} (
                        bill_key TEXT PRIMARY KEY,
                        site_bill_id TEXT NOT NULL UNIQUE,
                        congress INT NOT NULL,
                        bill_type TEXT NOT NULL,
                        bill_number TEXT NOT NULL,
                        title TEXT NOT NULL,
                        short_title TEXT NULL,
                        origin_chamber TEXT NULL,
                        policy_area TEXT NULL,
                        sponsor_bioguide_id TEXT NULL,
                        sponsor_full_name TEXT NULL,
                        sponsor_party TEXT NULL,
                        sponsor_state TEXT NULL,
                        sponsor_district TEXT NULL,
                        introduced_date DATE NULL,
                        latest_action_date DATE NULL,
                        latest_action_text TEXT NULL,
                        update_date TIMESTAMPTZ NULL,
                        update_date_including_text TIMESTAMPTZ NULL,
                        legislation_url TEXT NULL,
                        api_url TEXT NULL,
                        summaries_count INT NOT NULL DEFAULT 0,
                        actions_count INT NOT NULL DEFAULT 0,
                        committees_count INT NOT NULL DEFAULT 0,
                        cosponsors_count INT NOT NULL DEFAULT 0,
                        subject_count INT NOT NULL DEFAULT 0,
                        text_version_count INT NOT NULL DEFAULT 0,
                        summary TEXT NULL,
                        content TEXT NOT NULL DEFAULT '',
                        committee_names TEXT[] NOT NULL DEFAULT '{{}}'::text[],
                        committee_codes TEXT[] NOT NULL DEFAULT '{{}}'::text[],
                        subjects TEXT[] NOT NULL DEFAULT '{{}}'::text[],
                        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                        content_tsv tsvector GENERATED ALWAYS AS (
                            to_tsvector('english', coalesce(title, '') || ' ' || coalesce(summary, '') || ' ' || coalesce(content, ''))
                        ) STORED,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {PIPELINE_CONGRESS_BILL_SUMMARIES_TABLE} (
                        summary_key TEXT PRIMARY KEY,
                        bill_key TEXT NOT NULL REFERENCES {PIPELINE_CONGRESS_BILLS_TABLE}(bill_key) ON DELETE CASCADE,
                        site_bill_id TEXT NOT NULL,
                        congress INT NOT NULL,
                        bill_type TEXT NOT NULL,
                        bill_number TEXT NOT NULL,
                        version_code TEXT NULL,
                        action_date DATE NULL,
                        action_desc TEXT NULL,
                        update_date TIMESTAMPTZ NULL,
                        text TEXT NOT NULL,
                        source_url TEXT NULL,
                        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                        text_tsv tsvector GENERATED ALWAYS AS (
                            to_tsvector('english', coalesce(text, ''))
                        ) STORED,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {PIPELINE_CONGRESS_BILL_ACTIONS_TABLE} (
                        action_key TEXT PRIMARY KEY,
                        bill_key TEXT NOT NULL REFERENCES {PIPELINE_CONGRESS_BILLS_TABLE}(bill_key) ON DELETE CASCADE,
                        site_bill_id TEXT NOT NULL,
                        congress INT NOT NULL,
                        bill_type TEXT NOT NULL,
                        bill_number TEXT NOT NULL,
                        action_date DATE NULL,
                        action_time TEXT NULL,
                        action_code TEXT NULL,
                        action_type TEXT NULL,
                        text TEXT NOT NULL,
                        source_system_name TEXT NULL,
                        source_system_code INT NULL,
                        committee_names TEXT[] NOT NULL DEFAULT '{{}}'::text[],
                        committee_codes TEXT[] NOT NULL DEFAULT '{{}}'::text[],
                        source_url TEXT NULL,
                        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                        text_tsv tsvector GENERATED ALWAYS AS (
                            to_tsvector('english', coalesce(text, ''))
                        ) STORED,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {PIPELINE_CONGRESS_BILL_SYNCS_TABLE} (
                        site_bill_id TEXT PRIMARY KEY,
                        bill_key TEXT NULL UNIQUE,
                        congress INT NOT NULL,
                        bill_type TEXT NOT NULL,
                        bill_number TEXT NOT NULL,
                        last_status TEXT NOT NULL,
                        last_error TEXT NULL,
                        source_update_date TIMESTAMPTZ NULL,
                        source_update_date_including_text TIMESTAMPTZ NULL,
                        summaries_count INT NOT NULL DEFAULT 0,
                        actions_count INT NOT NULL DEFAULT 0,
                        synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb
                    )
                    """
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_congress_bills_type ON {PIPELINE_CONGRESS_BILLS_TABLE} USING BTREE (congress, bill_type, bill_number)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_congress_bills_action_date ON {PIPELINE_CONGRESS_BILLS_TABLE} USING BTREE (latest_action_date DESC)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_congress_bills_tsv ON {PIPELINE_CONGRESS_BILLS_TABLE} USING GIN (content_tsv)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_congress_bills_committee_codes ON {PIPELINE_CONGRESS_BILLS_TABLE} USING GIN (committee_codes)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_congress_bills_subjects ON {PIPELINE_CONGRESS_BILLS_TABLE} USING GIN (subjects)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_congress_bill_summaries_bill ON {PIPELINE_CONGRESS_BILL_SUMMARIES_TABLE} USING BTREE (bill_key, action_date DESC, update_date DESC)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_congress_bill_summaries_tsv ON {PIPELINE_CONGRESS_BILL_SUMMARIES_TABLE} USING GIN (text_tsv)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_congress_bill_actions_bill ON {PIPELINE_CONGRESS_BILL_ACTIONS_TABLE} USING BTREE (bill_key, action_date DESC)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_congress_bill_actions_tsv ON {PIPELINE_CONGRESS_BILL_ACTIONS_TABLE} USING GIN (text_tsv)"
                )
                cursor.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_pipeline_congress_bill_syncs_status ON {PIPELINE_CONGRESS_BILL_SYNCS_TABLE} USING BTREE (last_status, synced_at ASC)"
                )
            connection.commit()

    return {
        "tables": [
            PIPELINE_CONGRESS_BILLS_TABLE,
            PIPELINE_CONGRESS_BILL_SUMMARIES_TABLE,
            PIPELINE_CONGRESS_BILL_ACTIONS_TABLE,
            PIPELINE_CONGRESS_BILL_SYNCS_TABLE,
        ]
    }


def sync_house_stubs_to_neon(
    settings: Settings,
    stubs: list[FilingStub],
    *,
    prune_missing: bool = False,
) -> dict[str, int]:
    """Upsert House filing stubs into CapitolExposed's house_filing_stubs table."""

    stubs = [s for s in stubs if s.doc_id not in HOUSE_STUB_BLOCKLIST]
    payloads = [build_house_stub_payload(stub) for stub in stubs]
    if not payloads:
        return {"upserted": 0, "pruned": 0}

    live_doc_ids = sorted({stub.doc_id for stub in stubs if stub.doc_id})
    filing_years = sorted({stub.filing_year for stub in stubs if stub.filing_year})
    pruned = 0

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
            if prune_missing and live_doc_ids and filing_years:
                cursor.execute(
                    """
                    WITH deleted AS (
                        DELETE FROM house_filing_stubs
                        WHERE filing_year = ANY(%s)
                          AND source IN ('house_clerk', 'house-clerk')
                          AND status = 'pending_extraction'
                          AND COALESCE(metadata->>'lastError', '') ILIKE 'PTR PDF fetch failed with 404%%'
                          AND NOT (doc_id = ANY(%s))
                        RETURNING doc_id
                    )
                    SELECT COUNT(*)::int AS pruned
                    FROM deleted
                    """,
                    (filing_years, live_doc_ids),
                )
                row = cursor.fetchone() or {}
                pruned = int(row.get("pruned") or 0)
        connection.commit()

    return {"upserted": len(payloads), "pruned": pruned}


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
                ON CONFLICT (source_document_id) DO UPDATE SET
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
                RETURNING id
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
            row = cursor.fetchone()
        connection.commit()
    document_id = row.get("id") if isinstance(row, dict) else row[0] if row else document.id
    return {"upserted": 1, "document_id": document_id or document.id}


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
    chunk_ids = [chunk.id for chunk in chunks]
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM pipeline_search_chunks
                WHERE document_id = %s
                  AND NOT (id = ANY(%s))
                """,
                (document_id, chunk_ids),
            )
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
                ON CONFLICT (id) DO UPDATE SET
                    document_id = EXCLUDED.document_id,
                    chunk_index = EXCLUDED.chunk_index,
                    content = EXCLUDED.content,
                    token_estimate = EXCLUDED.token_estimate,
                    metadata = EXCLUDED.metadata,
                    embedding = EXCLUDED.embedding,
                    updated_at = NOW()
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
    source: str | None = None,
    category: str | None = None,
    member_id: str | None = None,
    committee_id: str | None = None,
    bill_id: str | None = None,
    ticker: str | None = None,
) -> list[SearchHit]:
    """Run a hybrid lexical and vector search across indexed chunks."""

    ensure_search_schema(settings)
    filters: list[str] = []
    filter_params: list[object] = []
    if source:
        filters.append("d.source = %s")
        filter_params.append(source)
    if category:
        filters.append("d.category = %s")
        filter_params.append(category)
    if member_id:
        filters.append("%s = ANY(d.member_ids)")
        filter_params.append(member_id)
    if committee_id:
        filters.append("%s = ANY(d.committee_ids)")
        filter_params.append(committee_id)
    if bill_id:
        filters.append("%s = ANY(d.bill_ids)")
        filter_params.append(bill_id)
    if ticker:
        filters.append("%s = ANY(d.asset_tickers)")
        filter_params.append(ticker.upper())

    filter_sql = ""
    if filters:
        filter_sql = " AND " + " AND ".join(filters)

    rows: list[dict[str, object]]
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            if query_embedding:
                params: list[object] = [
                    query_text,
                    query_text,
                    vector_literal(query_embedding),
                    query_text,
                    query_text,
                    vector_literal(query_embedding),
                    query_text,
                    query_text,
                    *filter_params,
                    max(1, limit),
                ]
                cursor.execute(
                    f"""
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
                        WHERE (
                            d.content_tsv @@ websearch_to_tsquery('english', %s)
                            OR c.text_tsv @@ websearch_to_tsquery('english', %s)
                            OR c.embedding IS NOT NULL
                        )
                          {filter_sql}
                        ORDER BY combined_score DESC
                        LIMIT %s
                    )
                    SELECT * FROM ranked ORDER BY combined_score DESC
                    """,
                    tuple(params),
                )
            else:
                params = [
                    query_text,
                    query_text,
                    query_text,
                    query_text,
                    query_text,
                    query_text,
                    *filter_params,
                    max(1, limit),
                ]
                cursor.execute(
                    f"""
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
                    WHERE (
                        d.content_tsv @@ websearch_to_tsquery('english', %s)
                        OR c.text_tsv @@ websearch_to_tsquery('english', %s)
                    )
                      {filter_sql}
                    ORDER BY combined_score DESC
                    LIMIT %s
                    """,
                    tuple(params),
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
    include_needs_review: bool = False,
    only_needs_review: bool = False,
) -> list[dict[str, object]]:
    """Load queued House filing stubs that are ready for extraction or retry."""

    if only_needs_review:
        status_clause = "status = 'needs_review'"
    elif include_needs_review:
        status_clause = "status IN ('pending_extraction', 'extracting', 'needs_review')"
    else:
        status_clause = "status IN ('pending_extraction', 'extracting')"

    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT doc_id, filing_year, source, source_url, status, extracted_trade_id, metadata,
                       detected_at, last_seen_at
                FROM house_filing_stubs
                WHERE {status_clause}
                  AND COALESCE(NULLIF(metadata->>'retryAfter', '')::timestamptz, NOW() - INTERVAL '1 second') <= NOW()
                  AND (
                        status <> 'pending_extraction'
                        OR COALESCE(metadata->>'lastError', '') NOT ILIKE 'PTR PDF fetch failed with 404%%'
                  )
                ORDER BY
                    CASE
                        WHEN status = 'pending_extraction'
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


def upsert_offshore_nodes(
    settings: Settings,
    rows: list[OffshoreNodeRecord],
    *,
    ensure_schema: bool = True,
) -> dict[str, int]:
    """Upsert Offshore Leaks nodes into the raw corpus table."""

    if not rows:
        return {"upserted": 0}
    if ensure_schema:
        ensure_offshore_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                f"""
                INSERT INTO {PIPELINE_OFFSHORE_NODES_TABLE} (
                    node_key,
                    node_id,
                    node_type,
                    name,
                    normalized_name,
                    source_dataset,
                    summary,
                    content,
                    countries,
                    country_codes,
                    jurisdiction,
                    jurisdiction_description,
                    company_type,
                    address,
                    status,
                    service_provider,
                    note,
                    valid_until,
                    metadata,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (node_key) DO UPDATE SET
                    name = EXCLUDED.name,
                    normalized_name = EXCLUDED.normalized_name,
                    source_dataset = EXCLUDED.source_dataset,
                    summary = EXCLUDED.summary,
                    content = EXCLUDED.content,
                    countries = EXCLUDED.countries,
                    country_codes = EXCLUDED.country_codes,
                    jurisdiction = EXCLUDED.jurisdiction,
                    jurisdiction_description = EXCLUDED.jurisdiction_description,
                    company_type = EXCLUDED.company_type,
                    address = EXCLUDED.address,
                    status = EXCLUDED.status,
                    service_provider = EXCLUDED.service_provider,
                    note = EXCLUDED.note,
                    valid_until = EXCLUDED.valid_until,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                """,
                [
                    (
                        row.node_key,
                        row.node_id,
                        row.node_type,
                        row.name,
                        row.normalized_name,
                        row.source_dataset,
                        row.summary,
                        row.content,
                        row.countries,
                        row.country_codes,
                        row.jurisdiction,
                        row.jurisdiction_description,
                        row.company_type,
                        row.address,
                        row.status,
                        row.service_provider,
                        row.note,
                        row.valid_until,
                        Jsonb(row.metadata),  # type: ignore[arg-type]
                    )
                    for row in rows
                ],
            )
        connection.commit()
    return {"upserted": len(rows)}


def upsert_offshore_relationships(
    settings: Settings,
    rows: list[OffshoreRelationshipRecord],
    *,
    ensure_schema: bool = True,
) -> dict[str, int]:
    """Upsert Offshore Leaks relationships into the raw corpus table."""

    if not rows:
        return {"upserted": 0}
    if ensure_schema:
        ensure_offshore_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                f"""
                INSERT INTO {PIPELINE_OFFSHORE_RELATIONSHIPS_TABLE} (
                    relationship_key,
                    start_node_id,
                    end_node_id,
                    rel_type,
                    link,
                    status,
                    start_date,
                    end_date,
                    source_dataset,
                    metadata,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (relationship_key) DO UPDATE SET
                    link = EXCLUDED.link,
                    status = EXCLUDED.status,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    source_dataset = EXCLUDED.source_dataset,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                """,
                [
                    (
                        row.relationship_key,
                        row.start_node_id,
                        row.end_node_id,
                        row.rel_type,
                        row.link,
                        row.status,
                        row.start_date,
                        row.end_date,
                        row.source_dataset,
                        Jsonb(row.metadata),  # type: ignore[arg-type]
                    )
                    for row in rows
                ],
            )
        connection.commit()
    return {"upserted": len(rows)}


def upsert_offshore_member_matches(
    settings: Settings,
    rows: list[OffshoreMemberMatchRecord],
    *,
    ensure_schema: bool = True,
) -> dict[str, int]:
    """Upsert exact Congress matches against Offshore Leaks nodes."""

    if not rows:
        return {"upserted": 0}
    if ensure_schema:
        ensure_offshore_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                f"""
                INSERT INTO {PIPELINE_OFFSHORE_MEMBER_MATCHES_TABLE} (
                    match_key,
                    member_id,
                    member_name,
                    member_slug,
                    node_key,
                    node_type,
                    source_dataset,
                    match_type,
                    match_value,
                    metadata,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (match_key) DO UPDATE SET
                    member_name = EXCLUDED.member_name,
                    member_slug = EXCLUDED.member_slug,
                    node_type = EXCLUDED.node_type,
                    source_dataset = EXCLUDED.source_dataset,
                    match_type = EXCLUDED.match_type,
                    match_value = EXCLUDED.match_value,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                """,
                [
                    (
                        row.match_key,
                        row.member_id,
                        row.member_name,
                        row.member_slug,
                        row.node_key,
                        row.node_type,
                        row.source_dataset,
                        row.match_type,
                        row.match_value,
                        Jsonb(row.metadata),  # type: ignore[arg-type]
                    )
                    for row in rows
                ],
            )
        connection.commit()
    return {"upserted": len(rows)}


def upsert_fara_registrants(
    settings: Settings,
    rows: list[FaraRegistrantRecord],
    *,
    ensure_schema: bool = True,
) -> dict[str, int]:
    """Upsert FARA registrants into the raw corpus table."""

    if not rows:
        return {"upserted": 0}
    if ensure_schema:
        ensure_fara_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                f"""
                INSERT INTO {PIPELINE_FARA_REGISTRANTS_TABLE} (
                    registration_number,
                    name,
                    normalized_name,
                    registration_date,
                    address_1,
                    address_2,
                    city,
                    state,
                    zip_code,
                    summary,
                    content,
                    metadata,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (registration_number) DO UPDATE SET
                    name = EXCLUDED.name,
                    normalized_name = EXCLUDED.normalized_name,
                    registration_date = EXCLUDED.registration_date,
                    address_1 = EXCLUDED.address_1,
                    address_2 = EXCLUDED.address_2,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    zip_code = EXCLUDED.zip_code,
                    summary = EXCLUDED.summary,
                    content = EXCLUDED.content,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                """,
                [
                    (
                        row.registration_number,
                        row.name,
                        row.normalized_name,
                        row.registration_date,
                        row.address_1,
                        row.address_2,
                        row.city,
                        row.state,
                        row.zip_code,
                        row.summary,
                        row.content,
                        Jsonb(row.metadata),  # type: ignore[arg-type]
                    )
                    for row in rows
                ],
            )
        connection.commit()
    return {"upserted": len(rows)}


def upsert_fara_foreign_principals(
    settings: Settings,
    rows: list[FaraForeignPrincipalRecord],
    *,
    ensure_schema: bool = True,
) -> dict[str, int]:
    """Upsert FARA foreign principals into the raw corpus table."""

    if not rows:
        return {"upserted": 0}
    if ensure_schema:
        ensure_fara_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                f"""
                INSERT INTO {PIPELINE_FARA_FOREIGN_PRINCIPALS_TABLE} (
                    principal_key,
                    registration_number,
                    foreign_principal_name,
                    normalized_name,
                    registrant_name,
                    country_name,
                    registration_date,
                    foreign_principal_registration_date,
                    address_1,
                    address_2,
                    city,
                    state,
                    zip_code,
                    metadata,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (principal_key) DO UPDATE SET
                    registration_number = EXCLUDED.registration_number,
                    foreign_principal_name = EXCLUDED.foreign_principal_name,
                    normalized_name = EXCLUDED.normalized_name,
                    registrant_name = EXCLUDED.registrant_name,
                    country_name = EXCLUDED.country_name,
                    registration_date = EXCLUDED.registration_date,
                    foreign_principal_registration_date = EXCLUDED.foreign_principal_registration_date,
                    address_1 = EXCLUDED.address_1,
                    address_2 = EXCLUDED.address_2,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    zip_code = EXCLUDED.zip_code,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                """,
                [
                    (
                        row.principal_key,
                        row.registration_number,
                        row.foreign_principal_name,
                        row.normalized_name,
                        row.registrant_name,
                        row.country_name,
                        row.registration_date,
                        row.foreign_principal_registration_date,
                        row.address_1,
                        row.address_2,
                        row.city,
                        row.state,
                        row.zip_code,
                        Jsonb(row.metadata),  # type: ignore[arg-type]
                    )
                    for row in rows
                ],
            )
        connection.commit()
    return {"upserted": len(rows)}


def upsert_fara_short_forms(
    settings: Settings,
    rows: list[FaraShortFormRecord],
    *,
    ensure_schema: bool = True,
) -> dict[str, int]:
    """Upsert FARA short-form registrants into the raw corpus table."""

    if not rows:
        return {"upserted": 0}
    if ensure_schema:
        ensure_fara_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                f"""
                INSERT INTO {PIPELINE_FARA_SHORT_FORMS_TABLE} (
                    short_form_key,
                    registration_number,
                    registrant_name,
                    first_name,
                    last_name,
                    full_name,
                    normalized_name,
                    short_form_date,
                    registration_date,
                    address_1,
                    address_2,
                    city,
                    state,
                    zip_code,
                    metadata,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (short_form_key) DO UPDATE SET
                    registration_number = EXCLUDED.registration_number,
                    registrant_name = EXCLUDED.registrant_name,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    full_name = EXCLUDED.full_name,
                    normalized_name = EXCLUDED.normalized_name,
                    short_form_date = EXCLUDED.short_form_date,
                    registration_date = EXCLUDED.registration_date,
                    address_1 = EXCLUDED.address_1,
                    address_2 = EXCLUDED.address_2,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    zip_code = EXCLUDED.zip_code,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                """,
                [
                    (
                        row.short_form_key,
                        row.registration_number,
                        row.registrant_name,
                        row.first_name,
                        row.last_name,
                        row.full_name,
                        row.normalized_name,
                        row.short_form_date,
                        row.registration_date,
                        row.address_1,
                        row.address_2,
                        row.city,
                        row.state,
                        row.zip_code,
                        Jsonb(row.metadata),  # type: ignore[arg-type]
                    )
                    for row in rows
                ],
            )
        connection.commit()
    return {"upserted": len(rows)}


def upsert_fara_documents(
    settings: Settings,
    rows: list[FaraDocumentRecord],
    *,
    ensure_schema: bool = True,
) -> dict[str, int]:
    """Upsert FARA document metadata rows into the raw corpus table."""

    if not rows:
        return {"upserted": 0}
    if ensure_schema:
        ensure_fara_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                f"""
                INSERT INTO {PIPELINE_FARA_DOCUMENTS_TABLE} (
                    document_key,
                    registration_number,
                    registrant_name,
                    document_type,
                    date_stamped,
                    url,
                    short_form_name,
                    foreign_principal_name,
                    foreign_principal_country,
                    metadata,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (document_key) DO UPDATE SET
                    registration_number = EXCLUDED.registration_number,
                    registrant_name = EXCLUDED.registrant_name,
                    document_type = EXCLUDED.document_type,
                    date_stamped = EXCLUDED.date_stamped,
                    url = EXCLUDED.url,
                    short_form_name = EXCLUDED.short_form_name,
                    foreign_principal_name = EXCLUDED.foreign_principal_name,
                    foreign_principal_country = EXCLUDED.foreign_principal_country,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                """,
                [
                    (
                        row.document_key,
                        row.registration_number,
                        row.registrant_name,
                        row.document_type,
                        row.date_stamped,
                        row.url,
                        row.short_form_name,
                        row.foreign_principal_name,
                        row.foreign_principal_country,
                        Jsonb(row.metadata),  # type: ignore[arg-type]
                    )
                    for row in rows
                ],
            )
        connection.commit()
    return {"upserted": len(rows)}


def upsert_fara_member_matches(
    settings: Settings,
    rows: list[FaraMemberMatchRecord],
    *,
    ensure_schema: bool = True,
) -> dict[str, int]:
    """Upsert exact Congress matches against FARA entities."""

    if not rows:
        return {"upserted": 0}
    if ensure_schema:
        ensure_fara_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                f"""
                INSERT INTO {PIPELINE_FARA_MEMBER_MATCHES_TABLE} (
                    match_key,
                    member_id,
                    member_name,
                    member_slug,
                    registration_number,
                    entity_kind,
                    entity_key,
                    registrant_name,
                    match_type,
                    match_value,
                    metadata,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (match_key) DO UPDATE SET
                    member_name = EXCLUDED.member_name,
                    member_slug = EXCLUDED.member_slug,
                    registration_number = EXCLUDED.registration_number,
                    entity_kind = EXCLUDED.entity_kind,
                    entity_key = EXCLUDED.entity_key,
                    registrant_name = EXCLUDED.registrant_name,
                    match_type = EXCLUDED.match_type,
                    match_value = EXCLUDED.match_value,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                """,
                [
                    (
                        row.match_key,
                        row.member_id,
                        row.member_name,
                        row.member_slug,
                        row.registration_number,
                        row.entity_kind,
                        row.entity_key,
                        row.registrant_name,
                        row.match_type,
                        row.match_value,
                        Jsonb(row.metadata),  # type: ignore[arg-type]
                    )
                    for row in rows
                ],
            )
        connection.commit()
    return {"upserted": len(rows)}


def upsert_usaspending_recipients(
    settings: Settings,
    rows: list[UsaspendingRecipientRecord],
    *,
    ensure_schema: bool = True,
) -> dict[str, int]:
    """Upsert USAspending recipient summaries into the raw corpus table."""

    if not rows:
        return {"upserted": 0}
    if ensure_schema:
        ensure_usaspending_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                f"""
                INSERT INTO {PIPELINE_USASPENDING_RECIPIENTS_TABLE} (
                    recipient_id,
                    name,
                    normalized_name,
                    query_name,
                    recipient_code,
                    uei,
                    total_amount,
                    total_outlays,
                    source_url,
                    summary,
                    content,
                    metadata,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (recipient_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    normalized_name = EXCLUDED.normalized_name,
                    query_name = EXCLUDED.query_name,
                    recipient_code = EXCLUDED.recipient_code,
                    uei = EXCLUDED.uei,
                    total_amount = EXCLUDED.total_amount,
                    total_outlays = EXCLUDED.total_outlays,
                    source_url = EXCLUDED.source_url,
                    summary = EXCLUDED.summary,
                    content = EXCLUDED.content,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                """,
                [
                    (
                        row.recipient_id,
                        row.name,
                        row.normalized_name,
                        row.query_name,
                        row.recipient_code,
                        row.uei,
                        row.total_amount,
                        row.total_outlays,
                        row.source_url,
                        row.summary,
                        row.content,
                        Jsonb(row.metadata),  # type: ignore[arg-type]
                    )
                    for row in rows
                ],
            )
        connection.commit()
    return {"upserted": len(rows)}


def upsert_usaspending_company_matches(
    settings: Settings,
    rows: list[UsaspendingCompanyMatchRecord],
    *,
    ensure_schema: bool = True,
) -> dict[str, int]:
    """Upsert site-company to USAspending recipient matches."""

    if not rows:
        return {"upserted": 0}
    if ensure_schema:
        ensure_usaspending_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                f"""
                INSERT INTO {PIPELINE_USASPENDING_COMPANY_MATCHES_TABLE} (
                    match_key,
                    company_id,
                    company_name,
                    ticker,
                    query_name,
                    canonical_recipient_id,
                    recipient_name,
                    normalized_recipient_name,
                    recipient_code,
                    uei,
                    match_type,
                    match_value,
                    total_amount,
                    award_count,
                    top_agencies,
                    source_url,
                    summary,
                    content,
                    metadata,
                    updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (match_key) DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    ticker = EXCLUDED.ticker,
                    query_name = EXCLUDED.query_name,
                    canonical_recipient_id = EXCLUDED.canonical_recipient_id,
                    recipient_name = EXCLUDED.recipient_name,
                    normalized_recipient_name = EXCLUDED.normalized_recipient_name,
                    recipient_code = EXCLUDED.recipient_code,
                    uei = EXCLUDED.uei,
                    match_type = EXCLUDED.match_type,
                    match_value = EXCLUDED.match_value,
                    total_amount = EXCLUDED.total_amount,
                    award_count = EXCLUDED.award_count,
                    top_agencies = EXCLUDED.top_agencies,
                    source_url = EXCLUDED.source_url,
                    summary = EXCLUDED.summary,
                    content = EXCLUDED.content,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                """,
                [
                    (
                        row.match_key,
                        row.company_id,
                        row.company_name,
                        row.ticker,
                        row.query_name,
                        row.canonical_recipient_id,
                        row.recipient_name,
                        row.normalized_recipient_name,
                        row.recipient_code,
                        row.uei,
                        row.match_type,
                        row.match_value,
                        row.total_amount,
                        row.award_count,
                        row.top_agencies,
                        row.source_url,
                        row.summary,
                        row.content,
                        Jsonb(row.metadata),  # type: ignore[arg-type]
                    )
                    for row in rows
                ],
            )
        connection.commit()
    return {"upserted": len(rows)}


def upsert_usaspending_awards(
    settings: Settings,
    rows: list[UsaspendingAwardRecord],
    *,
    ensure_schema: bool = True,
) -> dict[str, int]:
    """Upsert USAspending awards tied to company matches."""

    if not rows:
        return {"upserted": 0}
    if ensure_schema:
        ensure_usaspending_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                f"""
                INSERT INTO {PIPELINE_USASPENDING_AWARDS_TABLE} (
                    award_key,
                    match_key,
                    canonical_recipient_id,
                    recipient_name,
                    award_id,
                    internal_id,
                    generated_internal_id,
                    action_date,
                    award_amount,
                    award_type,
                    awarding_agency,
                    awarding_agency_id,
                    agency_slug,
                    description,
                    source_url,
                    metadata,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (award_key) DO UPDATE SET
                    match_key = EXCLUDED.match_key,
                    canonical_recipient_id = EXCLUDED.canonical_recipient_id,
                    recipient_name = EXCLUDED.recipient_name,
                    award_id = EXCLUDED.award_id,
                    internal_id = EXCLUDED.internal_id,
                    generated_internal_id = EXCLUDED.generated_internal_id,
                    action_date = EXCLUDED.action_date,
                    award_amount = EXCLUDED.award_amount,
                    award_type = EXCLUDED.award_type,
                    awarding_agency = EXCLUDED.awarding_agency,
                    awarding_agency_id = EXCLUDED.awarding_agency_id,
                    agency_slug = EXCLUDED.agency_slug,
                    description = EXCLUDED.description,
                    source_url = EXCLUDED.source_url,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                """,
                [
                    (
                        row.award_key,
                        row.match_key,
                        row.canonical_recipient_id,
                        row.recipient_name,
                        row.award_id,
                        row.internal_id,
                        row.generated_internal_id,
                        row.action_date,
                        row.award_amount,
                        row.award_type,
                        row.awarding_agency,
                        row.awarding_agency_id,
                        row.agency_slug,
                        row.description,
                        row.source_url,
                        Jsonb(row.metadata),  # type: ignore[arg-type]
                    )
                    for row in rows
                ],
            )
        connection.commit()
    return {"upserted": len(rows)}


def upsert_usaspending_company_sync(
    settings: Settings,
    *,
    company_id: str,
    ticker: str | None,
    company_name: str,
    query_name: str | None,
    status: str,
    result_count: int,
    last_error: str | None = None,
    metadata: dict[str, object] | None = None,
) -> dict[str, int]:
    """Record the last USAspending sync result for one tracked company."""

    ensure_usaspending_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {PIPELINE_USASPENDING_COMPANY_SYNCS_TABLE} (
                    company_id,
                    ticker,
                    company_name,
                    query_name,
                    status,
                    result_count,
                    last_error,
                    synced_at,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s)
                ON CONFLICT (company_id) DO UPDATE SET
                    ticker = EXCLUDED.ticker,
                    company_name = EXCLUDED.company_name,
                    query_name = EXCLUDED.query_name,
                    status = EXCLUDED.status,
                    result_count = EXCLUDED.result_count,
                    last_error = EXCLUDED.last_error,
                    synced_at = NOW(),
                    metadata = EXCLUDED.metadata
                """,
                (
                    company_id,
                    ticker,
                    company_name,
                    query_name,
                    status,
                    result_count,
                    last_error,
                    Jsonb(metadata or {}),  # type: ignore[arg-type]
                ),
            )
        connection.commit()
    return {"upserted": 1}


def upsert_congress_bills(
    settings: Settings,
    rows: list[CongressBillRecord],
) -> dict[str, int]:
    """Upsert canonical Congress.gov bill records."""

    if not rows:
        return {"upserted": 0}
    ensure_congress_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                f"""
                INSERT INTO {PIPELINE_CONGRESS_BILLS_TABLE} (
                    bill_key,
                    site_bill_id,
                    congress,
                    bill_type,
                    bill_number,
                    title,
                    short_title,
                    origin_chamber,
                    policy_area,
                    sponsor_bioguide_id,
                    sponsor_full_name,
                    sponsor_party,
                    sponsor_state,
                    sponsor_district,
                    introduced_date,
                    latest_action_date,
                    latest_action_text,
                    update_date,
                    update_date_including_text,
                    legislation_url,
                    api_url,
                    summaries_count,
                    actions_count,
                    committees_count,
                    cosponsors_count,
                    subject_count,
                    text_version_count,
                    summary,
                    content,
                    committee_names,
                    committee_codes,
                    subjects,
                    metadata
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (bill_key) DO UPDATE SET
                    site_bill_id = EXCLUDED.site_bill_id,
                    congress = EXCLUDED.congress,
                    bill_type = EXCLUDED.bill_type,
                    bill_number = EXCLUDED.bill_number,
                    title = EXCLUDED.title,
                    short_title = EXCLUDED.short_title,
                    origin_chamber = EXCLUDED.origin_chamber,
                    policy_area = EXCLUDED.policy_area,
                    sponsor_bioguide_id = EXCLUDED.sponsor_bioguide_id,
                    sponsor_full_name = EXCLUDED.sponsor_full_name,
                    sponsor_party = EXCLUDED.sponsor_party,
                    sponsor_state = EXCLUDED.sponsor_state,
                    sponsor_district = EXCLUDED.sponsor_district,
                    introduced_date = EXCLUDED.introduced_date,
                    latest_action_date = EXCLUDED.latest_action_date,
                    latest_action_text = EXCLUDED.latest_action_text,
                    update_date = EXCLUDED.update_date,
                    update_date_including_text = EXCLUDED.update_date_including_text,
                    legislation_url = EXCLUDED.legislation_url,
                    api_url = EXCLUDED.api_url,
                    summaries_count = EXCLUDED.summaries_count,
                    actions_count = EXCLUDED.actions_count,
                    committees_count = EXCLUDED.committees_count,
                    cosponsors_count = EXCLUDED.cosponsors_count,
                    subject_count = EXCLUDED.subject_count,
                    text_version_count = EXCLUDED.text_version_count,
                    summary = EXCLUDED.summary,
                    content = EXCLUDED.content,
                    committee_names = EXCLUDED.committee_names,
                    committee_codes = EXCLUDED.committee_codes,
                    subjects = EXCLUDED.subjects,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                """,
                [
                    (
                        row.bill_key,
                        row.site_bill_id,
                        row.congress,
                        row.bill_type,
                        row.bill_number,
                        row.title,
                        row.short_title,
                        row.origin_chamber,
                        row.policy_area,
                        row.sponsor_bioguide_id,
                        row.sponsor_full_name,
                        row.sponsor_party,
                        row.sponsor_state,
                        row.sponsor_district,
                        row.introduced_date,
                        row.latest_action_date,
                        row.latest_action_text,
                        row.update_date,
                        row.update_date_including_text,
                        row.legislation_url,
                        row.api_url,
                        row.summaries_count,
                        row.actions_count,
                        row.committees_count,
                        row.cosponsors_count,
                        row.subject_count,
                        row.text_version_count,
                        row.summary,
                        row.content,
                        row.committee_names,
                        row.committee_codes,
                        row.subjects,
                        Jsonb(row.metadata),  # type: ignore[arg-type]
                    )
                    for row in rows
                ],
            )
        connection.commit()
    return {"upserted": len(rows)}


def upsert_congress_bill_summaries(
    settings: Settings,
    rows: list[CongressBillSummaryRecord],
) -> dict[str, int]:
    """Upsert official bill summary versions."""

    if not rows:
        return {"upserted": 0}
    ensure_congress_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                f"""
                INSERT INTO {PIPELINE_CONGRESS_BILL_SUMMARIES_TABLE} (
                    summary_key,
                    bill_key,
                    site_bill_id,
                    congress,
                    bill_type,
                    bill_number,
                    version_code,
                    action_date,
                    action_desc,
                    update_date,
                    text,
                    source_url,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (summary_key) DO UPDATE SET
                    bill_key = EXCLUDED.bill_key,
                    site_bill_id = EXCLUDED.site_bill_id,
                    congress = EXCLUDED.congress,
                    bill_type = EXCLUDED.bill_type,
                    bill_number = EXCLUDED.bill_number,
                    version_code = EXCLUDED.version_code,
                    action_date = EXCLUDED.action_date,
                    action_desc = EXCLUDED.action_desc,
                    update_date = EXCLUDED.update_date,
                    text = EXCLUDED.text,
                    source_url = EXCLUDED.source_url,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                """,
                [
                    (
                        row.summary_key,
                        row.bill_key,
                        row.site_bill_id,
                        row.congress,
                        row.bill_type,
                        row.bill_number,
                        row.version_code,
                        row.action_date,
                        row.action_desc,
                        row.update_date,
                        row.text,
                        row.source_url,
                        Jsonb(row.metadata),  # type: ignore[arg-type]
                    )
                    for row in rows
                ],
            )
        connection.commit()
    return {"upserted": len(rows)}


def upsert_congress_bill_actions(
    settings: Settings,
    rows: list[CongressBillActionRecord],
) -> dict[str, int]:
    """Upsert official bill actions."""

    if not rows:
        return {"upserted": 0}
    ensure_congress_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                f"""
                INSERT INTO {PIPELINE_CONGRESS_BILL_ACTIONS_TABLE} (
                    action_key,
                    bill_key,
                    site_bill_id,
                    congress,
                    bill_type,
                    bill_number,
                    action_date,
                    action_time,
                    action_code,
                    action_type,
                    text,
                    source_system_name,
                    source_system_code,
                    committee_names,
                    committee_codes,
                    source_url,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (action_key) DO UPDATE SET
                    bill_key = EXCLUDED.bill_key,
                    site_bill_id = EXCLUDED.site_bill_id,
                    congress = EXCLUDED.congress,
                    bill_type = EXCLUDED.bill_type,
                    bill_number = EXCLUDED.bill_number,
                    action_date = EXCLUDED.action_date,
                    action_time = EXCLUDED.action_time,
                    action_code = EXCLUDED.action_code,
                    action_type = EXCLUDED.action_type,
                    text = EXCLUDED.text,
                    source_system_name = EXCLUDED.source_system_name,
                    source_system_code = EXCLUDED.source_system_code,
                    committee_names = EXCLUDED.committee_names,
                    committee_codes = EXCLUDED.committee_codes,
                    source_url = EXCLUDED.source_url,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                """,
                [
                    (
                        row.action_key,
                        row.bill_key,
                        row.site_bill_id,
                        row.congress,
                        row.bill_type,
                        row.bill_number,
                        row.action_date,
                        row.action_time,
                        row.action_code,
                        row.action_type,
                        row.text,
                        row.source_system_name,
                        row.source_system_code,
                        row.committee_names,
                        row.committee_codes,
                        row.source_url,
                        Jsonb(row.metadata),  # type: ignore[arg-type]
                    )
                    for row in rows
                ],
            )
        connection.commit()
    return {"upserted": len(rows)}


def upsert_congress_bill_sync(
    settings: Settings,
    *,
    site_bill_id: str,
    congress: int,
    bill_type: str,
    bill_number: str,
    bill_key: str | None,
    last_status: str,
    source_update_date: str | None,
    source_update_date_including_text: str | None,
    summaries_count: int,
    actions_count: int,
    last_error: str | None = None,
    metadata: dict[str, object] | None = None,
) -> dict[str, int]:
    """Record the last Congress.gov sync result for one tracked bill."""

    ensure_congress_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {PIPELINE_CONGRESS_BILL_SYNCS_TABLE} (
                    site_bill_id,
                    bill_key,
                    congress,
                    bill_type,
                    bill_number,
                    last_status,
                    last_error,
                    source_update_date,
                    source_update_date_including_text,
                    summaries_count,
                    actions_count,
                    synced_at,
                    metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
                ON CONFLICT (site_bill_id) DO UPDATE SET
                    bill_key = EXCLUDED.bill_key,
                    congress = EXCLUDED.congress,
                    bill_type = EXCLUDED.bill_type,
                    bill_number = EXCLUDED.bill_number,
                    last_status = EXCLUDED.last_status,
                    last_error = EXCLUDED.last_error,
                    source_update_date = EXCLUDED.source_update_date,
                    source_update_date_including_text = EXCLUDED.source_update_date_including_text,
                    summaries_count = EXCLUDED.summaries_count,
                    actions_count = EXCLUDED.actions_count,
                    synced_at = NOW(),
                    metadata = EXCLUDED.metadata
                """,
                (
                    site_bill_id,
                    bill_key,
                    congress,
                    bill_type,
                    bill_number,
                    last_status,
                    last_error,
                    source_update_date,
                    source_update_date_including_text,
                    summaries_count,
                    actions_count,
                    Jsonb(metadata or {}),  # type: ignore[arg-type]
                ),
            )
        connection.commit()
    return {"upserted": 1}


def fetch_company_candidates_for_usaspending(
    settings: Settings,
    *,
    limit: int = 0,
    only_stale: bool = True,
    stale_after_days: int = 30,
) -> list[dict[str, object]]:
    """Load the highest-value tracked companies for USAspending recipient matching."""

    ensure_usaspending_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            query = f"""
                WITH trade_company_activity AS (
                    SELECT
                        c.id AS company_id,
                        COUNT(t.id)::int AS trade_count,
                        MAX(
                            COALESCE(
                                t.created_at,
                                t.disclosure_date::timestamp,
                                t.transaction_date::timestamp
                            )
                        ) AS latest_trade_at
                    FROM companies c
                    JOIN trades t
                      ON (
                            t.company_id = c.id
                            OR (
                                COALESCE(t.company_id, '') = ''
                                AND COALESCE(c.ticker, '') <> ''
                                AND UPPER(COALESCE(t.ticker, '')) = UPPER(c.ticker)
                            )
                        )
                    GROUP BY c.id
                ),
                trade_company_sample AS (
                    SELECT DISTINCT ON (c.id)
                        c.id AS company_id,
                        t.asset_description,
                        COALESCE(
                            t.created_at,
                            t.disclosure_date::timestamp,
                            t.transaction_date::timestamp
                        ) AS activity_at
                    FROM companies c
                    JOIN trades t
                      ON (
                            t.company_id = c.id
                            OR (
                                COALESCE(t.company_id, '') = ''
                                AND COALESCE(c.ticker, '') <> ''
                                AND UPPER(COALESCE(t.ticker, '')) = UPPER(c.ticker)
                            )
                        )
                    WHERE COALESCE(t.asset_description, '') <> ''
                    ORDER BY c.id, activity_at DESC NULLS LAST
                )
                SELECT
                    c.id,
                    c.ticker,
                    c.name,
                    COALESCE(a.trade_count, 0) AS trade_count,
                    a.latest_trade_at,
                    s.asset_description AS sample_asset_description,
                    sync.synced_at,
                    sync.status AS sync_status,
                    sync.result_count,
                    sync.last_error
                FROM companies c
                JOIN trade_company_activity a ON a.company_id = c.id
                LEFT JOIN trade_company_sample s ON s.company_id = c.id
                LEFT JOIN {PIPELINE_USASPENDING_COMPANY_SYNCS_TABLE} sync ON sync.company_id = c.id
                WHERE COALESCE(c.ticker, '') <> ''
                  AND COALESCE(a.trade_count, 0) > 0
                  AND (
                        NOT %s
                        OR sync.synced_at IS NULL
                        OR sync.status IN ('error', 'no_match')
                        OR sync.synced_at < (NOW() - (%s * INTERVAL '1 day'))
                  )
                ORDER BY
                    COALESCE(a.trade_count, 0) DESC,
                    a.latest_trade_at DESC NULLS LAST,
                    c.name ASC
            """
            params: list[object] = [only_stale, max(1, stale_after_days)]
            if limit > 0:
                query += " LIMIT %s"
                params.append(limit)
            cursor.execute(query, tuple(params))
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


def fetch_senate_trade_search_backfill(
    settings: Settings,
    *,
    limit: int = 0,
    only_missing: bool = True,
    sources: list[str] | None = None,
) -> list[dict[str, object]]:
    """Load Senate trade rows that should be indexed into pipeline search."""

    normalized_sources = sorted(
        {
            source.strip()
            for source in (
                sources
                or [
                    "senate_quiver",
                    "senate-quiver",
                    "senate_watcher",
                    "senate-watcher",
                    "senate_efd",
                    "senate-ethics",
                ]
            )
            if source and source.strip()
        }
    )
    if not normalized_sources:
        return []

    ensure_search_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            query = f"""
                SELECT DISTINCT ON (t.id)
                    t.id,
                    t.member_id,
                    t.ticker,
                    t.asset_description,
                    t.asset_type,
                    t.transaction_type,
                    t.transaction_date,
                    t.disclosure_date,
                    t.amount_min,
                    t.amount_max,
                    t.owner,
                    t.comment,
                    t.source,
                    t.source_url,
                    m.bioguide_id AS member_bioguide_id,
                    m.name AS member_name,
                    m.slug AS member_slug,
                    m.party AS member_party,
                    m.state AS member_state,
                    m.district AS member_district
                FROM trades t
                LEFT JOIN members m ON m.id = t.member_id
                WHERE t.source = ANY(%s)
                  AND COALESCE(t.member_id, '') <> ''
                  AND COALESCE(t.transaction_date, t.disclosure_date) IS NOT NULL
                  {"AND NOT EXISTS (SELECT 1 FROM pipeline_search_documents d WHERE d.source_document_id = CONCAT('senate-trade-', t.id))" if only_missing else ""}
                ORDER BY
                    t.id,
                    COALESCE(t.disclosure_date, t.transaction_date) DESC,
                    t.created_at DESC NULLS LAST,
                    t.id DESC
                {"" if limit <= 0 else "LIMIT %s"}
            """
            params: list[object] = [normalized_sources]
            if limit > 0:
                params.append(limit)
            cursor.execute(query, tuple(params))
            return list(cursor.fetchall())


def fetch_published_news_posts(
    settings: Settings,
    *,
    limit: int = 0,
    only_missing: bool = True,
) -> list[dict[str, object]]:
    """Load published CapitolExposed stories for shared search indexing."""

    ensure_search_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            query = """
                SELECT
                    n.id,
                    n.slug,
                    n.title,
                    n.subtitle,
                    n.excerpt,
                    n.body,
                    n.category,
                    n.tags,
                    n.author,
                    n.member_refs,
                    n.evidence,
                    n.word_count,
                    n.reading_time,
                    n.published_at,
                    n.updated_at
                FROM news_posts n
                WHERE n.status = 'published'
                  AND COALESCE(n.slug, '') <> ''
                  AND COALESCE(n.body, '') <> ''
            """
            if only_missing:
                query += """
                  AND NOT EXISTS (
                        SELECT 1
                        FROM pipeline_search_documents d
                        WHERE d.source_document_id = CONCAT('capitol-story-', n.slug)
                    )
                """
            query += """
                ORDER BY n.published_at DESC NULLS LAST, n.updated_at DESC NULLS LAST
            """
            params: list[object] = []
            if limit > 0:
                query += " LIMIT %s"
                params.append(limit)
            cursor.execute(query, tuple(params))
            return list(cursor.fetchall())


def fetch_published_dossiers(
    settings: Settings,
    *,
    limit: int = 0,
    only_missing: bool = True,
) -> list[dict[str, object]]:
    """Load published CapitolExposed dossiers with finding narratives for indexing."""

    ensure_search_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            query = """
                SELECT
                    d.id,
                    d.member_id,
                    d.title,
                    d.slug,
                    d.summary,
                    d.severity,
                    d.verification_status,
                    d.generated_at,
                    d.reviewed_at,
                    d.metadata,
                    d.executive_summary,
                    d.methodology,
                    d.finding_count,
                    d.updated_at,
                    COALESCE(
                        jsonb_agg(
                            jsonb_build_object(
                                'id', f.id,
                                'category', f.category,
                                'title', f.title,
                                'narrative', f.narrative,
                                'verification_status', f.verification_status,
                                'severity_score', f.severity_score,
                                'sort_order', f.sort_order
                            )
                            ORDER BY f.sort_order ASC, f.id ASC
                        ) FILTER (WHERE f.id IS NOT NULL),
                        '[]'::jsonb
                    ) AS findings
                FROM dossiers d
                LEFT JOIN dossier_findings f ON f.dossier_id = d.id
                WHERE d.published = TRUE
                  AND COALESCE(d.slug, '') <> ''
            """
            if only_missing:
                query += """
                  AND NOT EXISTS (
                        SELECT 1
                        FROM pipeline_search_documents s
                        WHERE s.source_document_id = CONCAT('capitol-dossier-', d.slug)
                    )
                """
            query += """
                GROUP BY
                    d.id,
                    d.member_id,
                    d.title,
                    d.slug,
                    d.summary,
                    d.severity,
                    d.verification_status,
                    d.generated_at,
                    d.reviewed_at,
                    d.metadata,
                    d.executive_summary,
                    d.methodology,
                    d.finding_count,
                    d.updated_at
                ORDER BY d.updated_at DESC NULLS LAST, d.generated_at DESC NULLS LAST
            """
            params: list[object] = []
            if limit > 0:
                query += " LIMIT %s"
                params.append(limit)
            cursor.execute(query, tuple(params))
            return list(cursor.fetchall())


def fetch_existing_trade_ids(
    settings: Settings,
    *,
    sources: list[str] | tuple[str, ...],
) -> set[str]:
    """Load existing trade ids for one or more source families."""

    normalized_sources = [source.strip() for source in sources if source.strip()]
    if not normalized_sources:
        return set()
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id
                FROM trades
                WHERE source = ANY(%s)
                """,
                (normalized_sources,),
            )
            return {
                str(row["id"])
                for row in cursor.fetchall()
                if row.get("id") is not None
            }


def fetch_latest_trade_disclosure_date(
    settings: Settings,
    *,
    sources: list[str] | tuple[str, ...],
) -> str | None:
    """Return the newest disclosure_date for a family of trade sources."""

    normalized_sources = [source.strip() for source in sources if source.strip()]
    if not normalized_sources:
        return None
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT MAX(disclosure_date)::text AS latest_disclosure_date
                FROM trades
                WHERE source = ANY(%s)
                """,
                (normalized_sources,),
            )
            row = cursor.fetchone() or {}
            value = row.get("latest_disclosure_date")
            return str(value) if value else None


def fetch_members_for_search(
    settings: Settings,
    *,
    limit: int = 0,
    only_missing: bool = True,
) -> list[dict[str, object]]:
    """Load in-office member profiles for shared search indexing."""

    ensure_search_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            query = """
                SELECT
                    m.id,
                    m.slug,
                    m.name,
                    m.party,
                    m.state,
                    m.district,
                    m.chamber,
                    m.office,
                    m.website,
                    m.twitter_handle,
                    COALESCE(
                        (
                            SELECT jsonb_agg(
                                jsonb_build_object(
                                    'id', c.id,
                                    'name', c.name,
                                    'role', mc.role
                                )
                                ORDER BY c.name
                            )
                            FROM member_committees mc
                            JOIN committees c ON c.id = mc.committee_id
                            WHERE mc.member_id = m.id
                              AND (mc.end_date IS NULL OR mc.end_date >= CURRENT_DATE)
                        ),
                        '[]'::jsonb
                    ) AS committees,
                    COALESCE(
                        (
                            SELECT jsonb_agg(
                                jsonb_build_object(
                                    'ticker', ticker_counts.ticker,
                                    'tradeCount', ticker_counts.trade_count
                                )
                                ORDER BY ticker_counts.trade_count DESC, ticker_counts.ticker
                            )
                            FROM (
                                SELECT
                                    UPPER(t.ticker) AS ticker,
                                    COUNT(*)::int AS trade_count
                                FROM trades t
                                WHERE t.member_id = m.id
                                  AND COALESCE(t.ticker, '') <> ''
                                GROUP BY UPPER(t.ticker)
                                ORDER BY trade_count DESC, UPPER(t.ticker)
                                LIMIT 8
                            ) AS ticker_counts
                        ),
                        '[]'::jsonb
                    ) AS top_tickers
                FROM members m
                WHERE m.in_office = TRUE
                  AND COALESCE(m.slug, '') <> ''
            """
            if only_missing:
                query += """
                  AND NOT EXISTS (
                        SELECT 1
                        FROM pipeline_search_documents d
                        WHERE d.source_document_id = CONCAT('capitol-member-', m.slug)
                    )
                """
            query += """
                ORDER BY m.last_name ASC, m.first_name ASC
            """
            params: list[object] = []
            if limit > 0:
                query += " LIMIT %s"
                params.append(limit)
            cursor.execute(query, tuple(params))
            return list(cursor.fetchall())


def fetch_committees_for_search(
    settings: Settings,
    *,
    limit: int = 0,
    only_missing: bool = True,
) -> list[dict[str, object]]:
    """Load committee profiles for shared search indexing."""

    ensure_search_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            query = """
                SELECT
                    c.id,
                    c.name,
                    c.chamber,
                    c.code,
                    c.parent_id,
                    c.url,
                    c.jurisdiction_industries,
                    COALESCE(
                        (
                            SELECT jsonb_agg(
                                jsonb_build_object(
                                    'id', m.id,
                                    'name', m.name,
                                    'party', m.party,
                                    'role', mc.role
                                )
                                ORDER BY
                                    CASE
                                        WHEN mc.role ILIKE '%%chair%%' THEN 0
                                        WHEN mc.role ILIKE '%%ranking%%' THEN 1
                                        ELSE 2
                                    END,
                                    m.last_name,
                                    m.first_name
                            )
                            FROM member_committees mc
                            JOIN members m ON m.id = mc.member_id
                            WHERE mc.committee_id = c.id
                              AND (mc.end_date IS NULL OR mc.end_date >= CURRENT_DATE)
                        ),
                        '[]'::jsonb
                    ) AS members
                FROM committees c
                WHERE COALESCE(c.id, '') <> ''
            """
            if only_missing:
                query += """
                  AND NOT EXISTS (
                        SELECT 1
                        FROM pipeline_search_documents d
                        WHERE d.source_document_id = CONCAT('capitol-committee-', c.id)
                    )
                """
            query += """
                ORDER BY c.chamber ASC, c.name ASC
            """
            params: list[object] = []
            if limit > 0:
                query += " LIMIT %s"
                params.append(limit)
            cursor.execute(query, tuple(params))
            return list(cursor.fetchall())


def fetch_bills_for_search(
    settings: Settings,
    *,
    limit: int = 0,
    only_missing: bool = True,
) -> list[dict[str, object]]:
    """Load bill records for shared search indexing."""

    ensure_search_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            query = """
                SELECT
                    b.id,
                    b.congress,
                    b.bill_type,
                    b.number,
                    b.title,
                    b.short_title,
                    b.subjects,
                    b.sponsor_id,
                    m.name AS sponsor_name,
                    m.slug AS sponsor_slug,
                    b.committees,
                    b.status,
                    b.introduced_date,
                    b.last_action_date,
                    b.text_url,
                    b.industries
                FROM bills b
                LEFT JOIN members m ON m.id = b.sponsor_id
                WHERE COALESCE(b.title, '') <> ''
            """
            if only_missing:
                query += """
                  AND NOT EXISTS (
                        SELECT 1
                        FROM pipeline_search_documents d
                        WHERE d.source_document_id = CONCAT('capitol-bill-', b.id)
                    )
                """
            query += """
                ORDER BY b.last_action_date DESC NULLS LAST, b.introduced_date DESC NULLS LAST, b.id ASC
            """
            params: list[object] = []
            if limit > 0:
                query += " LIMIT %s"
                params.append(limit)
            cursor.execute(query, tuple(params))
            return list(cursor.fetchall())


def fetch_bills_for_congress_sync(
    settings: Settings,
    *,
    limit: int = 0,
    only_stale: bool = True,
    stale_after_days: int = 7,
    rate_limit_cooldown_hours: int = 12,
) -> list[dict[str, object]]:
    """Load tracked bills that should be refreshed from Congress.gov."""

    ensure_congress_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            query = f"""
                SELECT
                    b.id,
                    b.congress,
                    b.bill_type,
                    b.number,
                    b.title,
                    b.short_title,
                    b.subjects,
                    b.sponsor_id,
                    m.name AS sponsor_name,
                    m.slug AS sponsor_slug,
                    b.committees,
                    b.status,
                    b.introduced_date,
                    b.last_action_date,
                    b.text_url,
                    b.industries,
                    sync.bill_key,
                    sync.last_status,
                    sync.last_error,
                    sync.source_update_date,
                    sync.source_update_date_including_text,
                    sync.synced_at
                FROM bills b
                LEFT JOIN members m ON m.id = b.sponsor_id
                LEFT JOIN {PIPELINE_CONGRESS_BILL_SYNCS_TABLE} sync ON sync.site_bill_id = b.id
                WHERE COALESCE(b.title, '') <> ''
                  AND COALESCE(b.congress, 0) > 0
                  AND COALESCE(b.bill_type, '') <> ''
                  AND COALESCE(CAST(b.number AS TEXT), '') <> ''
                  AND (
                        NOT %s
                        OR sync.site_bill_id IS NULL
                        OR (
                            COALESCE(sync.last_status, '') NOT IN ('ok', 'rate_limited')
                        )
                        OR (
                            COALESCE(sync.last_status, '') = 'ok'
                            AND sync.synced_at < (NOW() - (%s * INTERVAL '1 day'))
                        )
                        OR (
                            COALESCE(sync.last_status, '') = 'rate_limited'
                            AND sync.synced_at < (NOW() - (%s * INTERVAL '1 hour'))
                        )
                  )
                ORDER BY
                    COALESCE(sync.synced_at, TO_TIMESTAMP(0)) ASC,
                    b.last_action_date DESC NULLS LAST,
                    b.introduced_date DESC NULLS LAST,
                    b.id ASC
            """
            params: list[object] = [
                only_stale,
                max(1, stale_after_days),
                max(1, rate_limit_cooldown_hours),
            ]
            if limit > 0:
                query += " LIMIT %s"
                params.append(limit)
            cursor.execute(query, tuple(params))
            return list(cursor.fetchall())


def fetch_alerts_for_search(
    settings: Settings,
    *,
    limit: int = 0,
    only_missing: bool = True,
) -> list[dict[str, object]]:
    """Load active alert records for shared search indexing."""

    ensure_search_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            query = """
                SELECT
                    a.id,
                    a.alert_type,
                    a.severity,
                    a.title,
                    a.summary,
                    a.member_id,
                    m.name AS member_name,
                    m.slug AS member_slug,
                    a.trade_ids,
                    a.vote_ids,
                    a.bill_ids,
                    a.company_ids,
                    a.evidence,
                    a.confidence,
                    a.status,
                    a.created_at,
                    a.updated_at,
                    COALESCE(
                        (
                            SELECT array_agg(DISTINCT UPPER(t.ticker))
                            FROM trades t
                            WHERE t.id = ANY(COALESCE(a.trade_ids, ARRAY[]::text[]))
                              AND COALESCE(t.ticker, '') <> ''
                        ),
                        ARRAY[]::text[]
                    ) AS trade_tickers,
                    COALESCE(
                        (
                            SELECT array_agg(DISTINCT b.title)
                            FROM bills b
                            WHERE b.id = ANY(COALESCE(a.bill_ids, ARRAY[]::text[]))
                              AND COALESCE(b.title, '') <> ''
                        ),
                        ARRAY[]::text[]
                    ) AS bill_titles
                FROM alerts a
                LEFT JOIN members m ON m.id = a.member_id
                WHERE COALESCE(a.title, '') <> ''
                  AND COALESCE(a.status, '') <> 'dismissed'
            """
            if only_missing:
                query += """
                  AND NOT EXISTS (
                        SELECT 1
                        FROM pipeline_search_documents d
                        WHERE d.source_document_id = CONCAT('capitol-alert-', a.id)
                    )
                """
            query += """
                ORDER BY a.updated_at DESC NULLS LAST, a.created_at DESC NULLS LAST
            """
            params: list[object] = []
            if limit > 0:
                query += " LIMIT %s"
                params.append(limit)
            cursor.execute(query, tuple(params))
            return list(cursor.fetchall())


def fetch_search_chunk_embedding_backfill(
    settings: Settings,
    *,
    limit: int = 100,
    source: str | None = None,
) -> list[dict[str, object]]:
    """Load indexed search chunks that still need embeddings."""

    ensure_search_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            query = """
                SELECT c.id, c.document_id, c.content, c.metadata, d.source
                FROM pipeline_search_chunks c
                JOIN pipeline_search_documents d ON d.id = c.document_id
                WHERE c.embedding IS NULL
            """
            params: list[object] = []
            if source:
                query += " AND d.source = %s"
                params.append(source)
            query += " ORDER BY d.updated_at DESC, c.chunk_index ASC LIMIT %s"
            params.append(max(1, limit))
            cursor.execute(query, tuple(params))
            return list(cursor.fetchall())


def update_search_chunk_embeddings(
    settings: Settings,
    rows: list[tuple[str, list[float]]],
) -> dict[str, int]:
    """Write embeddings back into existing search chunks."""

    if not rows:
        return {"updated": 0}
    ensure_search_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                UPDATE pipeline_search_chunks
                SET embedding = %s::vector,
                    updated_at = NOW()
                WHERE id = %s
                """,
                [(vector_literal(embedding), chunk_id) for chunk_id, embedding in rows],
            )
        connection.commit()
    return {"updated": len(rows)}


def fetch_existing_fara_registration_numbers(settings: Settings) -> set[int]:
    """Load already-ingested FARA registrant ids so bulk runs can skip repeats."""

    ensure_fara_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT registration_number FROM {PIPELINE_FARA_REGISTRANTS_TABLE}"
            )
            return {
                int(row["registration_number"])
                for row in cursor.fetchall()
                if row.get("registration_number") is not None
            }


def fetch_pipeline_corpus_status(settings: Settings) -> dict[str, object]:
    """Return a compact status snapshot for pipeline-managed corpora and retrieval."""

    ensure_search_schema(settings)
    ensure_offshore_schema(settings)
    ensure_fara_schema(settings)
    ensure_usaspending_schema(settings)
    ensure_congress_schema(settings)
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                    (SELECT COUNT(*)::int FROM {PIPELINE_OFFSHORE_NODES_TABLE}) AS offshore_nodes,
                    (SELECT COUNT(*)::int FROM {PIPELINE_OFFSHORE_RELATIONSHIPS_TABLE}) AS offshore_relationships,
                    (SELECT COUNT(*)::int FROM {PIPELINE_OFFSHORE_MEMBER_MATCHES_TABLE}) AS offshore_matches,
                    (SELECT COUNT(*)::int FROM {PIPELINE_FARA_REGISTRANTS_TABLE}) AS fara_registrants,
                    (SELECT COUNT(*)::int FROM {PIPELINE_FARA_FOREIGN_PRINCIPALS_TABLE}) AS fara_foreign_principals,
                    (SELECT COUNT(*)::int FROM {PIPELINE_FARA_SHORT_FORMS_TABLE}) AS fara_short_forms,
                    (SELECT COUNT(*)::int FROM {PIPELINE_FARA_DOCUMENTS_TABLE}) AS fara_documents,
                    (SELECT COUNT(*)::int FROM {PIPELINE_FARA_MEMBER_MATCHES_TABLE}) AS fara_matches,
                    (SELECT COUNT(*)::int FROM {PIPELINE_USASPENDING_RECIPIENTS_TABLE}) AS usaspending_recipients,
                    (SELECT COUNT(*)::int FROM {PIPELINE_USASPENDING_COMPANY_MATCHES_TABLE}) AS usaspending_company_matches,
                    (SELECT COUNT(*)::int FROM {PIPELINE_USASPENDING_AWARDS_TABLE}) AS usaspending_awards,
                    (
                        SELECT COUNT(*)::int
                        FROM {PIPELINE_USASPENDING_COMPANY_SYNCS_TABLE}
                        WHERE status = 'matched'
                    ) AS usaspending_matched_syncs,
                    (SELECT COUNT(*)::int FROM {PIPELINE_CONGRESS_BILLS_TABLE}) AS congress_bills,
                    (SELECT COUNT(*)::int FROM {PIPELINE_CONGRESS_BILL_SUMMARIES_TABLE}) AS congress_summaries,
                    (SELECT COUNT(*)::int FROM {PIPELINE_CONGRESS_BILL_ACTIONS_TABLE}) AS congress_actions,
                    (
                        SELECT COUNT(*)::int
                        FROM {PIPELINE_CONGRESS_BILL_SYNCS_TABLE}
                        WHERE last_status = 'ok'
                    ) AS congress_synced_bills,
                    (SELECT COUNT(*)::int FROM {PIPELINE_SEARCH_DOCUMENTS_TABLE}) AS search_documents,
                    (SELECT COUNT(*)::int FROM {PIPELINE_SEARCH_CHUNKS_TABLE}) AS search_chunks,
                    (
                        SELECT COUNT(*)::int
                        FROM {PIPELINE_SEARCH_CHUNKS_TABLE}
                        WHERE embedding IS NOT NULL
                    ) AS embedded_chunks,
                    (
                        SELECT jsonb_object_agg(source, count_value)
                        FROM (
                            SELECT source, COUNT(*)::int AS count_value
                            FROM {PIPELINE_SEARCH_DOCUMENTS_TABLE}
                            GROUP BY source
                            ORDER BY source
                        ) source_counts
                    ) AS search_documents_by_source,
                    (
                        SELECT jsonb_object_agg(source, count_value)
                        FROM (
                            SELECT d.source, COUNT(*)::int AS count_value
                            FROM {PIPELINE_SEARCH_CHUNKS_TABLE} c
                            JOIN {PIPELINE_SEARCH_DOCUMENTS_TABLE} d ON d.id = c.document_id
                            WHERE c.embedding IS NOT NULL
                            GROUP BY d.source
                            ORDER BY d.source
                        ) embedded_counts
                    ) AS embedded_chunks_by_source
                """
            )
            status = cursor.fetchone() or {}

            cursor.execute(
                """
                SELECT status, COUNT(*)::int AS count
                FROM house_filing_stubs
                GROUP BY status
                ORDER BY status
                """
            )
            house_stub_counts = {
                str(row["status"]): int(row["count"])
                for row in cursor.fetchall()
                if row.get("status") is not None
            }

    return {
        "offshore": {
            "nodes": int(status.get("offshore_nodes") or 0),
            "relationships": int(status.get("offshore_relationships") or 0),
            "matches": int(status.get("offshore_matches") or 0),
        },
        "fara": {
            "registrants": int(status.get("fara_registrants") or 0),
            "foreignPrincipals": int(status.get("fara_foreign_principals") or 0),
            "shortForms": int(status.get("fara_short_forms") or 0),
            "documents": int(status.get("fara_documents") or 0),
            "matches": int(status.get("fara_matches") or 0),
        },
        "usaspending": {
            "recipients": int(status.get("usaspending_recipients") or 0),
            "companyMatches": int(status.get("usaspending_company_matches") or 0),
            "awards": int(status.get("usaspending_awards") or 0),
            "matchedSyncs": int(status.get("usaspending_matched_syncs") or 0),
        },
        "congress": {
            "bills": int(status.get("congress_bills") or 0),
            "summaries": int(status.get("congress_summaries") or 0),
            "actions": int(status.get("congress_actions") or 0),
            "syncedBills": int(status.get("congress_synced_bills") or 0),
        },
        "search": {
            "documents": int(status.get("search_documents") or 0),
            "chunks": int(status.get("search_chunks") or 0),
            "embeddedChunks": int(status.get("embedded_chunks") or 0),
            "documentsBySource": status.get("search_documents_by_source") or {},
            "embeddedChunksBySource": status.get("embedded_chunks_by_source") or {},
        },
        "houseStubs": house_stub_counts,
    }


def backfill_crypto_trade_classification(
    settings: Settings,
    *,
    limit: int = 0,
    only_unclassified: bool = True,
) -> dict[str, object]:
    """Normalize existing CapitolExposed trade rows into crypto asset classes."""

    known_tickers = sorted(
        {
            *DIRECT_CRYPTO_SYMBOLS.keys(),
            *CRYPTO_ETF_TICKERS.keys(),
            *CRYPTO_EQUITY_TICKERS.keys(),
        }
    )
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            query = """
                SELECT id, ticker, asset_type, asset_description
                FROM trades
                WHERE (
                    asset_type IN ('Cryptocurrency', 'Crypto ETF', 'Crypto-Adjacent Equity')
                    OR UPPER(COALESCE(ticker, '')) = ANY(%s)
                    OR COALESCE(asset_description, '') ~* %s
                )
            """
            params: list[object] = [known_tickers, CRYPTO_TRADE_SCAN_REGEX]
            if only_unclassified:
                query += " AND COALESCE(asset_type, '') NOT IN ('Cryptocurrency', 'Crypto ETF', 'Crypto-Adjacent Equity')"
            query += " ORDER BY transaction_date DESC NULLS LAST, id ASC"
            if limit > 0:
                query += " LIMIT %s"
                params.append(limit)
            cursor.execute(query, tuple(params))
            rows = list(cursor.fetchall())

            updates: list[tuple[str, str | None, str]] = []
            summary_by_kind = {
                "direct_crypto": 0,
                "crypto_etf": 0,
                "crypto_equity": 0,
            }
            for row in rows:
                classification = classify_crypto_asset(
                    str(row.get("ticker") or "").strip() or None,
                    str(row.get("asset_description") or "").strip() or None,
                )
                if classification.kind == "unrelated":
                    continue
                asset_type = {
                    "direct_crypto": "Cryptocurrency",
                    "crypto_etf": "Crypto ETF",
                    "crypto_equity": "Crypto-Adjacent Equity",
                }[classification.kind]
                normalized_ticker = str(row.get("ticker") or "").strip().upper() or None
                replacement_ticker = normalized_ticker or classification.canonical_symbol
                updates.append((asset_type, replacement_ticker, str(row["id"])))
                summary_by_kind[classification.kind] += 1

            if updates:
                cursor.executemany(
                    """
                    UPDATE trades
                    SET asset_type = %s,
                        ticker = COALESCE(NULLIF(%s, ''), ticker)
                    WHERE id = %s
                    """,
                    updates,
                )
        connection.commit()

    return {
        "scanned": len(rows),
        "updated": len(updates),
        "byKind": summary_by_kind,
    }


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
