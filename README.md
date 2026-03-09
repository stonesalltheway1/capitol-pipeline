# Capitol Pipeline

Capitol Pipeline is the document and disclosure ingestion engine for
[CapitolExposed.com](https://www.capitolexposed.com).

It is being retrofitted from the stronger OCR core in Epstein-Pipeline so the
Congress site stops depending on a thinner, route-local parsing path for House
PTR filings and asset normalization.

## What Exists Now

- Transplanted multi-backend OCR core from Epstein-Pipeline
- Capitol-specific package and settings
- House Clerk XML source adapter
- Senate watcher source adapter
- House PTR parser for text and PDF-backed filings
- CapitolExposed-compatible member registry resolution
- Crypto asset classifier for direct coins, ETFs and trusts, and adjacent equities
- Bridge helpers that emit shapes compatible with CapitolExposed database tables
- Neon exporters for member loading, House stub sync, and parsed trade upserts
- Dedicated pipeline search tables with `tsvector` and optional `pgvector` indexing
- Hybrid retrieval commands for lexical search now and semantic search when embeddings are enabled
- ICIJ Offshore Leaks ingestion into dedicated raw corpus tables plus Congress match extraction
- Official FARA ingestion into dedicated raw corpus tables plus registrant search documents

## Why This Repo Matters

CapitolExposed already has live filing polling and a site-side parser, but that
logic currently lives inside the web app. This repo is the path to:

1. Move filing extraction out of the app layer
2. Reuse the stronger OCR fallback stack already proven on EpsteinExposed
3. Normalize tricky assets, especially crypto, before they reach the site
4. Export site-ready trade and stub payloads with less brittle parsing logic

## Commands

```bash
# Install in editable mode for local development
pip install -e .

# Inspect the House annual disclosure feed
capitol-pipeline house-feed --year 2026

# Sync House filing stubs into CapitolExposed and resolve members first
capitol-pipeline sync-house-feed --year 2026

# Inspect the current Senate watcher aggregate feed
capitol-pipeline senate-feed

# Classify a raw asset
capitol-pipeline classify-crypto --ticker IBIT --description "iShares Bitcoin Trust ETF"

# Backfill crypto-linked trade rows that were previously stored as generic assets or stocks
capitol-pipeline backfill-crypto-trades

# OCR a single PDF through the fallback chain
capitol-pipeline ocr ./sample.pdf

# OCR and parse a House PTR PDF into structured trade rows
capitol-pipeline parse-house-ptr ./sample.pdf \
  --doc-id 20033783 \
  --filing-year 2026 \
  --filing-date 2026-01-15 \
  --member-name "Roger Williams" \
  --member-slug "roger-williams" \
  --member-id "m-20033783" \
  --party R \
  --state TX \
  --upsert

# Fetch a live House PTR from the annual feed, resolve the member, parse it,
# and optionally write the stub and trades back into CapitolExposed
capitol-pipeline process-house-doc \
  --year 2026 \
  --doc-id 20033783 \
  --upsert

# Process a batch of queued House PTR stubs directly from CapitolExposed
capitol-pipeline process-house-backlog --limit 10

# Create the pipeline-managed search schema in Neon
capitol-pipeline ensure-search-schema

# Index one House PTR into pipeline_search_documents and pipeline_search_chunks
capitol-pipeline index-house-doc-search --year 2026 --doc-id 20033783

# Backfill indexed search documents from already-parsed House PTR rows in Neon
capitol-pipeline index-house-search-backfill --only-missing

# Search the indexed filing corpus
capitol-pipeline hybrid-search --query "Roger Williams Chevron"

# Scope a retrieval query to a source and ticker
capitol-pipeline hybrid-search --query "Roger Williams Chevron" --source house-clerk --ticker CVX

# Run the full House ingestion loop: sync the feed, process the queue, and
# optionally index parsed PTRs in one command
capitol-pipeline house-ingest --year 2026 --batch-size 10 --max-batches 5

# Create the dedicated Offshore Leaks corpus tables
capitol-pipeline ensure-offshore-schema

# Ingest the full official ICIJ Offshore Leaks database into raw Neon tables,
# derive exact Congress matches, and index those matches for retrieval
capitol-pipeline ingest-offshore-leaks --with-match-index

# Create the dedicated FARA corpus tables
capitol-pipeline ensure-fara-schema

# Inspect pipeline-managed corpus counts
capitol-pipeline corpus-status

# Ingest the official daily FARA bulk corpus into raw Neon tables and search
capitol-pipeline ingest-fara --mode bulk --with-match-index

# Fallback: ingest via the slower per-registrant API
capitol-pipeline ingest-fara --mode api --limit-registrants 25

# Index CapitolExposed's own published stories and dossiers into the shared corpus
capitol-pipeline index-site-editorial --only-missing

# Backfill missing embeddings on already-indexed search chunks
capitol-pipeline embed-search-backfill --limit 100

# Drain the embedding queue in stable batches, optionally per source
capitol-pipeline embed-search-corpus --source capitol-exposed --batch-size 100
```

## Automation

This repo now includes GitHub Actions for unattended refresh:

- `.github/workflows/house-refresh.yml`
- `.github/workflows/corpus-refresh.yml`
- `.github/workflows/offshore-full-refresh.yml`

Set these repository secrets before enabling the schedules:

- `DATABASE_URL`
- `OPENAI_API_KEY`

Operational details and recovery commands live in
[docs/OPERATIONS.md](docs/OPERATIONS.md).

## Search Layer

Capitol Pipeline now manages its own retrieval tables in Neon instead of
writing into any older app-owned search tables:

- `pipeline_search_documents`
- `pipeline_search_chunks`

That search layer supports:

1. `tsvector` indexes for exact and lexical retrieval
2. `pgvector` indexes for semantic retrieval
3. Hybrid ranking across title, summary, document body, and indexed chunks
4. A shared corpus that can mix CapitolExposed editorial, House PTRs, FARA, and ICIJ cross-references

Embeddings are optional. The lexical path works immediately. To enable OpenAI
embeddings, set:

- `CAPITOL_EMBEDDING_PROVIDER=openai`
- `CAPITOL_OPENAI_API_KEY=...` or reuse `OPENAI_API_KEY`
- `CAPITOL_OPENAI_EMBEDDING_DIMENSIONS=768` if you are writing into the current Neon search schema
- optionally `CAPITOL_OPENAI_EMBEDDING_MODEL`

## Offshore Leaks Layer

The best external cross-reference corpus for CapitolExposed is the official
ICIJ Offshore Leaks structured database. Capitol Pipeline now ingests that
corpus into dedicated raw tables:

- `pipeline_offshore_nodes`
- `pipeline_offshore_relationships`
- `pipeline_offshore_member_matches`

That design keeps the full public corpus available without flooding the main
site retrieval tables with millions of low-signal rows. Congress-facing search
documents are only created for matched records.

## FARA Layer

Capitol Pipeline now ingests the official DOJ FARA daily bulk ZIP exports into dedicated raw tables:

- `pipeline_fara_registrants`
- `pipeline_fara_foreign_principals`
- `pipeline_fara_short_forms`
- `pipeline_fara_documents`
- `pipeline_fara_member_matches`

Each active registrant is also summarized into the shared search corpus so the
Research Desk and future site search can retrieve FARA relationships without
depending on a live API call at request time.

## Retrofit Priorities

1. Replace the current House PTR OCR and extraction path in CapitolExposed
2. Backfill crypto-linked trades already present in the database
3. Replace the site-side House extraction cron path with this package
4. Expand indexed search across filings, stories, and official source documents
5. Add fixture-driven regression tests from real House and Senate disclosures

See [docs/RETROFIT_PLAN.md](docs/RETROFIT_PLAN.md) for the full implementation
plan.
