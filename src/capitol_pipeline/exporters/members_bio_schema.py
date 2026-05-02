"""Schema additions for Wikipedia-style member infobox + verified headshots.

This extends the CapitolExposed `members` table with biographical infobox fields
(birthplace, religion, education, occupation, family, predecessor, prose bio) and
the headshot verification pipeline columns (URL, perceptual hash, face count,
VLM score, source, verified-at).

Idempotent: every column is added with `IF NOT EXISTS` so re-running is safe.
"""

from __future__ import annotations

from capitol_pipeline.config import Settings
from capitol_pipeline.exporters.neon import advisory_lock, neon_connection


# ── Cross-walk identifier columns ────────────────────────────────────────
# Sourced from unitedstates/congress-legislators YAML (CC0). One ALTER per column
# so a partial run survives — e.g. wikipedia_slug landing without ballotpedia.
_CROSSWALK_COLUMNS: tuple[tuple[str, str], ...] = (
    ("wikidata_qid", "TEXT"),
    ("wikipedia_slug", "TEXT"),
    ("ballotpedia_slug", "TEXT"),
    ("govtrack_id", "TEXT"),
    ("house_history_id", "TEXT"),
    ("votesmart_id", "TEXT"),
    ("icpsr_id", "TEXT"),
    ("cspan_id", "TEXT"),
    ("maplight_id", "TEXT"),
    ("google_entity_id", "TEXT"),
    ("religion_legislator_yaml", "TEXT"),  # legislators-current bio.religion
)

# ── Wikidata-derived infobox columns ─────────────────────────────────────
# Each column corresponds to one or more Wikidata properties resolved by the
# enrichment job. JSONB columns hold {label, qid, qualifiers} arrays so the UI
# can render labelled chips with stable links back to Wikidata.
_BIO_COLUMNS: tuple[tuple[str, str], ...] = (
    ("native_name", "TEXT"),                         # P1559
    ("birthplace", "TEXT"),                          # P19 label
    ("birthplace_qid", "TEXT"),                      # P19 QID
    ("religion", "TEXT"),                            # P140 label
    ("spouse", "TEXT"),                              # P26 label (most recent)
    ("children_count", "INT"),                       # P40 count
    ("alma_mater", "JSONB DEFAULT '[]'::jsonb"),     # P69 [{label, qid, degree, year}]
    ("prior_occupations", "JSONB DEFAULT '[]'::jsonb"),  # P106 [{label, qid}]
    ("military_service", "JSONB DEFAULT '{}'::jsonb"),   # P241/P410 branch+rank
    ("residence", "TEXT"),                           # P551 label
    ("predecessor_qid", "TEXT"),                     # P1365 (replaces) for current office
    ("predecessor_name", "TEXT"),
    ("successor_qid", "TEXT"),                       # P1366 (replaced by)
    ("successor_name", "TEXT"),
    ("wikidata_image_filename", "TEXT"),             # P18 — Commons file name only
    ("wikidata_updated_at", "TIMESTAMPTZ"),
)

# ── Bioguide profile-text columns ────────────────────────────────────────
# Public-domain prose bio — useful for RAG context and as a fallback infobox
# narrative when Wikidata is sparse. Stored as both sanitized text and source
# HTML so the UI can render either.
_BIOGUIDE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("bio_text", "TEXT"),
    ("bio_text_html", "TEXT"),
    ("bio_source_url", "TEXT"),
    ("bio_updated_at", "TIMESTAMPTZ"),
    ("profession_list", "JSONB DEFAULT '[]'::jsonb"),  # bioguide profession array
)

# ── Headshot verification columns ────────────────────────────────────────
# `headshot_url` replaces the legacy `image_url` for the new verified pipeline,
# while keeping `image_url` intact so existing pages render during rollout.
_HEADSHOT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("headshot_url", "TEXT"),
    ("headshot_thumb_url", "TEXT"),
    ("headshot_source", "TEXT"),         # 'unitedstates' | 'congress.gov' | 'wikidata'
    ("headshot_phash", "TEXT"),          # primary source perceptual hash
    ("headshot_alt_phash", "TEXT"),      # cross-source phash for verification audit
    ("headshot_phash_distance", "INT"),  # Hamming distance between primary + alt
    ("headshot_face_count", "INT"),
    ("headshot_content_hash", "TEXT"),   # sha256 of primary bytes
    ("headshot_bytes", "INT"),
    ("headshot_width", "INT"),
    ("headshot_height", "INT"),
    ("headshot_vlm_score", "REAL"),      # null unless VLM check ran
    ("headshot_vlm_decision", "TEXT"),   # 'approved' | 'rejected' | null
    ("headshot_verified_at", "TIMESTAMPTZ"),
    ("headshot_verification_status", "TEXT"),  # 'verified' | 'needs_review' | 'failed'
    ("headshot_verification_notes", "TEXT"),
)


_INDEXES: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_members_wikidata_qid ON members(wikidata_qid)",
    "CREATE INDEX IF NOT EXISTS idx_members_headshot_status "
    "ON members(headshot_verification_status)",
    "CREATE INDEX IF NOT EXISTS idx_members_bio_updated ON members(bio_updated_at DESC)",
)


def ensure_members_bio_schema(settings: Settings) -> dict[str, int]:
    """Add Wikipedia-infobox + verified-headshot columns to the members table.

    Returns a count of columns/indexes touched so callers can log a useful summary.
    """

    all_columns = (
        _CROSSWALK_COLUMNS + _BIO_COLUMNS + _BIOGUIDE_COLUMNS + _HEADSHOT_COLUMNS
    )

    with neon_connection(settings) as connection:
        with advisory_lock(connection, "members-bio-schema"):
            with connection.cursor() as cursor:
                for name, ddl in all_columns:
                    cursor.execute(
                        f"ALTER TABLE members ADD COLUMN IF NOT EXISTS {name} {ddl}"
                    )
                for index_sql in _INDEXES:
                    cursor.execute(index_sql)
            connection.commit()

    return {
        "columns": len(all_columns),
        "indexes": len(_INDEXES),
    }
