"""Pipeline configuration using Pydantic BaseSettings with CAPITOL_ env prefix."""

from __future__ import annotations

from enum import Enum
import os
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings


class OcrBackend(str, Enum):
    """Available OCR backends, ordered by speed (fastest first)."""

    AUTO = "auto"
    PYMUPDF = "pymupdf"
    SURYA = "surya"
    OLMOCR = "olmocr"
    DOCLING = "docling"


class NerBackend(str, Enum):
    """Available NER backends."""

    SPACY = "spacy"
    GLINER = "gliner"
    BOTH = "both"


class DedupMode(str, Enum):
    """Dedup strategies — can be combined via 'all'."""

    EXACT = "exact"  # content hash + title fuzzy + Bates overlap
    MINHASH = "minhash"  # MinHash/LSH near-duplicate
    SEMANTIC = "semantic"  # embedding cosine similarity
    ALL = "all"  # exact → minhash → semantic


class Settings(BaseSettings):
    """Pipeline settings loaded from environment variables prefixed with CAPITOL_."""

    model_config = {"env_prefix": "CAPITOL_"}

    # ── Directory paths ──────────────────────────────────────────────────
    data_dir: Path = Path("./data")
    output_dir: Path = Path("./output")
    cache_dir: Path = Path("./.cache")
    members_registry_path: Path = Path("./data/members-registry.json")
    crypto_asset_map_path: Path = Path("./data/crypto-assets.json")
    offshore_leaks_archive_path: Path = Path("./data/offshore-leaks/full-oldb.LATEST.zip")

    # ── General processing ───────────────────────────────────────────────
    max_workers: int = 4
    user_agent: str = "CapitolExposed Pipeline/0.1"

    # ── OCR settings ─────────────────────────────────────────────────────
    ocr_backend: OcrBackend = OcrBackend.AUTO
    ocr_batch_size: int = 50
    ocr_confidence_threshold: float = 0.7  # flag pages below this
    ocr_fallback_chain: list[str] = ["pymupdf", "surya", "olmocr", "docling"]
    ocr_default_source: str = "manual"
    ocr_default_category: str = "other"

    # ── NER settings ─────────────────────────────────────────────────────
    spacy_model: str = "en_core_web_trf"  # upgraded from en_core_web_sm
    ner_backend: NerBackend = NerBackend.BOTH
    gliner_model: str = "urchade/gliner_multi_pii-v1"
    ner_confidence_threshold: float = 0.5

    # ── Dedup settings ───────────────────────────────────────────────────
    dedup_mode: DedupMode = DedupMode.ALL
    dedup_threshold: float = 0.90  # title fuzzy match threshold
    dedup_jaccard_threshold: float = 0.80  # MinHash Jaccard threshold
    dedup_semantic_threshold: float = 0.95  # embedding cosine similarity
    dedup_shingle_size: int = 5  # n-gram size for MinHash
    dedup_num_perm: int = 128  # MinHash permutation count

    # ── Embedding settings ───────────────────────────────────────────────
    embedding_provider: Literal["none", "openai"] = "none"
    embedding_model: str = "nomic-ai/nomic-embed-text-v2-moe"
    embedding_dimensions: int = 768  # 768 full, 256 Matryoshka
    embedding_chunk_size: int = 3200  # chars (~800 tokens)
    embedding_chunk_overlap: int = 800  # chars (~200 tokens)
    embedding_batch_size: int | None = None  # None = auto-detect
    embedding_device: str | None = None  # None = auto-detect
    openai_api_key: str | None = None
    openai_base_url: str = "https://api.openai.com/v1"
    openai_embedding_model: str = "text-embedding-3-large"
    openai_embedding_dimensions: int = 3072

    # ── Chunker settings ─────────────────────────────────────────────────
    chunker_mode: Literal["fixed", "semantic"] = "semantic"
    chunker_target_tokens: int = 512  # target chunk size in tokens
    chunker_min_tokens: int = 100  # minimum chunk size
    chunker_max_tokens: int = 1024  # maximum chunk size

    # ── Neon Postgres settings ───────────────────────────────────────────
    neon_database_url: str | None = None  # postgresql://...@...neon.tech/...
    neon_pool_size: int = 10
    neon_batch_size: int = 100  # rows per upsert batch

    # ── CapitolExposed source settings ──────────────────────────────────
    house_clerk_feed_base_url: str = "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/"
    house_ptr_pdf_base_url: str = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs"
    senate_watcher_url: str = (
        "https://raw.githubusercontent.com/timothycarambat/"
        "senate-stock-watcher-data/master/aggregate/all_transactions.json"
    )
    quiver_api_token: str | None = None
    offshore_leaks_zip_url: str = (
        "https://offshoreleaks-data.icij.org/offshoreleaks/csv/full-oldb.LATEST.zip"
    )
    fara_base_url: str = "https://efile.fara.gov/api/v1"
    lda_api_token: str | None = None  # LDA.gov lobbying disclosure API token
    fara_bulk_registrants_zip_url: str = "https://efile.fara.gov/bulk/zip/FARA_All_Registrants.csv.zip"
    fara_bulk_foreign_principals_zip_url: str = "https://efile.fara.gov/bulk/zip/FARA_All_ForeignPrincipals.csv.zip"
    fara_bulk_short_forms_zip_url: str = "https://efile.fara.gov/bulk/zip/FARA_All_ShortForms.csv.zip"
    fara_bulk_documents_zip_url: str = "https://efile.fara.gov/bulk/zip/FARA_All_RegistrantDocs.csv.zip"
    fara_request_interval_seconds: float = 2.05
    congress_gov_base_url: str = "https://api.congress.gov/v3"
    congress_gov_request_interval_seconds: float = 0.85
    congress_api_key: str | None = None
    usaspending_base_url: str = "https://api.usaspending.gov/api/v2"
    usaspending_request_interval_seconds: float = 0.35
    site_base_url: str = "https://www.capitolexposed.com"

    # ── Document classifier settings ─────────────────────────────────────
    classifier_model: str = "facebook/bart-large-mnli"
    classifier_confidence_threshold: float = 0.6

    # ── Knowledge graph settings ─────────────────────────────────────────
    kg_llm_provider: str = "openai"  # "openai" or "anthropic"
    kg_llm_model: str = "gpt-4o-mini"
    kg_extract_relationships: bool = False  # LLM extraction is opt-in

    @property
    def resolved_neon_database_url(self) -> str | None:
        """Return the configured Neon URL, falling back to DATABASE_URL for local retrofit work."""

        value = self.neon_database_url or os.getenv("DATABASE_URL")
        return value.strip() if value else None

    @property
    def resolved_openai_api_key(self) -> str | None:
        """Return the configured OpenAI key, falling back to OPENAI_API_KEY for local reuse."""

        value = self.openai_api_key or os.getenv("OPENAI_API_KEY")
        return value.strip() if value else None

    @property
    def resolved_quiver_api_token(self) -> str | None:
        """Return the configured Quiver API token, falling back to QUIVER_API_TOKEN for local reuse."""

        value = self.quiver_api_token or os.getenv("QUIVER_API_TOKEN")
        return value.strip() if value else None

    @property
    def resolved_congress_api_key(self) -> str:
        """Return the configured Congress.gov key, falling back to DEMO_KEY for light usage."""

        value = (
            self.congress_api_key
            or os.getenv("CONGRESS_API_KEY")
            or os.getenv("CONGRESS_GOV_API_KEY")
            or "DEMO_KEY"
        )
        return value.strip()

    @property
    def using_demo_congress_api_key(self) -> bool:
        """Return whether the pipeline is currently falling back to the demo Congress.gov key."""

        return self.resolved_congress_api_key.upper() == "DEMO_KEY"

    def ensure_dirs(self) -> None:
        """Create data, output, and cache directories if they don't exist."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
