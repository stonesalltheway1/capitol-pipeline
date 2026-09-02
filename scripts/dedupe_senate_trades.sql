-- Remove duplicate Senate trade rows written by the Quiver live and bulk providers.
--
-- WHY
--   `build_canonical_senate_trade_id` used to hash `asset_description` and
--   `source_url` alongside the trade facts. The Quiver live feed and the Quiver
--   bulk feed word the same holding differently ("NVIDIA Corporation" vs
--   "NVIDIA Corporation - Common Stock") and the live feed carries no source
--   URL, so the same trade hashed to two different ids and was inserted twice
--   (~2,686 rows). The canonical id now hashes only
--   (member_id, ticker | normalized asset description, transaction_type,
--    transaction_date, amount_min, amount_max, owner), so new ingests collide
--   correctly. This script cleans up what the old scheme already wrote.
--
-- GROUPING
--   (member_id, ticker, transaction_type, transaction_date, amount_min, amount_max).
--   Rows with no ticker (or a literal `--`) fall back to the normalized
--   asset_description instead of grouping every unlisted asset of a member on
--   one date together, which would delete genuinely distinct holdings.
--
-- KEEPER
--   1. a non-null disclosure_date beats a null one
--   2. then the longest asset_description
--   3. then the oldest created_at
--   4. then the lowest id, so the result is deterministic
--
-- USAGE
--   Dry run  : psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/dedupe_senate_trades.sql
--   Apply    : uncomment the COMMIT at the bottom (the script rolls back by default)
--
--   Equivalent CLI, which prints JSON counts and needs no psql:
--     python -m capitol_pipeline dedupe-senate-trades --dry-run
--     python -m capitol_pipeline dedupe-senate-trades --apply

BEGIN;

CREATE TEMP TABLE senate_trade_dedupe_candidates ON COMMIT DROP AS
SELECT
    t.id,
    t.member_id,
    COALESCE(
        NULLIF(NULLIF(UPPER(TRIM(COALESCE(t.ticker, ''))), ''), '--'),
        LOWER(REGEXP_REPLACE(TRIM(COALESCE(t.asset_description, '')), '\s+', ' ', 'g'))
    ) AS asset_key,
    t.transaction_type,
    t.transaction_date,
    t.amount_min,
    t.amount_max,
    ROW_NUMBER() OVER (
        PARTITION BY
            t.member_id,
            COALESCE(
                NULLIF(NULLIF(UPPER(TRIM(COALESCE(t.ticker, ''))), ''), '--'),
                LOWER(REGEXP_REPLACE(TRIM(COALESCE(t.asset_description, '')), '\s+', ' ', 'g'))
            ),
            t.transaction_type,
            t.transaction_date,
            t.amount_min,
            t.amount_max
        ORDER BY
            (t.disclosure_date IS NULL),
            LENGTH(COALESCE(t.asset_description, '')) DESC,
            t.created_at ASC NULLS LAST,
            t.id ASC
    ) AS keep_rank
FROM trades t
WHERE t.source IN ('senate_quiver', 'senate-quiver');

-- What is about to happen.
\echo 'Rows scanned:'
SELECT COUNT(*) AS rows_scanned FROM senate_trade_dedupe_candidates;

\echo 'Duplicate groups:'
SELECT COUNT(*) AS duplicate_groups
FROM (
    SELECT 1
    FROM senate_trade_dedupe_candidates
    GROUP BY member_id, asset_key, transaction_type, transaction_date, amount_min, amount_max
    HAVING COUNT(*) > 1
) AS grouped;

\echo 'Rows that would be deleted:'
SELECT COUNT(*) AS rows_to_delete
FROM senate_trade_dedupe_candidates
WHERE keep_rank > 1;

\echo 'Ten largest duplicate groups:'
SELECT
    member_id,
    asset_key,
    transaction_type,
    transaction_date,
    amount_min,
    amount_max,
    COUNT(*) AS row_count
FROM senate_trade_dedupe_candidates
GROUP BY member_id, asset_key, transaction_type, transaction_date, amount_min, amount_max
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC, member_id
LIMIT 10;

DELETE FROM trades
WHERE id IN (SELECT id FROM senate_trade_dedupe_candidates WHERE keep_rank > 1);

-- Safe by default: inspect the counts above, then swap ROLLBACK for COMMIT.
ROLLBACK;
-- COMMIT;
