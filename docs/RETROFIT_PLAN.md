# Capitol Pipeline Retrofit Plan

## What Was Copied

These files were transplanted from the Epstein pipeline because they are
already the strongest reusable spine for CapitolExposed:

- `src/capitol_pipeline/config.py`
- `src/capitol_pipeline/models/document.py`
- `src/capitol_pipeline/processors/ocr.py`

They were chosen because the OCR processor only depends on the config and
document model. That lets CapitolExposed inherit the proven multi-backend OCR
chain without dragging in Epstein-specific downloaders, entity schemas, or
exporters.

## What Was Retrofitted

### Package identity
- New package name: `capitol-pipeline`
- New import root: `capitol_pipeline`
- New environment prefix: `CAPITOL_`

### Capitol-specific sources
- `src/capitol_pipeline/sources/house_clerk.py`
- `src/capitol_pipeline/sources/senate_ethics.py`

### Capitol-specific models
- `src/capitol_pipeline/models/congress.py`

### House PTR parsing
- `src/capitol_pipeline/parsers/house_ptr.py`
- `tests/test_house_ptr_parser.py`

### Member resolution and Neon export
- `src/capitol_pipeline/registries/members.py`
- `src/capitol_pipeline/exporters/neon.py`
- `tests/test_member_registry.py`
- Batch backlog runner via `process-house-backlog`

### Crypto normalization
- `src/capitol_pipeline/normalizers/crypto_assets.py`

## Why This Matters For CapitolExposed

CapitolExposed currently has two weak spots that this pipeline should replace:

1. House PTR extraction is narrower than the OCR stack already proven in the
   Epstein pipeline.
2. Crypto activity is undercounted because the site classifier is shallow even
   though the database already contains direct Bitcoin rows and Bitcoin ETF rows.

## Execution Plan

### Phase 1
- Run House Clerk XML polling in this pipeline
- Use OCR fallback chain for live House PTR PDFs
- Emit normalized filing stubs and parsed rows
- Regression-test the parser against real House PTR text shapes

Status: completed locally

### Phase 2
- Add Capitol member resolution using the site member registry
- Export normalized trades into CapitolExposed database tables
- Replace the site's current House PTR extraction path with this package

Status: completed locally

### Phase 3
- Add direct crypto, crypto ETF/trust, and crypto-equity classification during
  normalization
- Backfill historical CapitolExposed trade rows with the new classifier
- Rewrite the `/crypto` page to use the normalized output

Status: classification completed, site backfill started in CapitolExposed

### Phase 4
- Add dedicated pipeline search tables with `tsvector` and optional `pgvector`
- Index House PTR filings into searchable documents and chunks
- Add hybrid retrieval commands for lexical search now and semantic search later

Status: completed locally

### Phase 5
- Replace Senate watcher dependency with official Senate Ethics ingestion where
  feasible
- Add filings quality scoring and manual-review queue output
- Add regression tests against real Congress PDF fixtures

### Phase 6
- Ingest the official ICIJ Offshore Leaks database into dedicated raw corpus tables
- Derive Congress exact-name matches from the offshore corpus
- Index matched offshore records into the pipeline search layer

Status: in progress

### Phase 7
- Replace CapitolExposed's `lib/ptr-extraction.ts` queue worker with a thin
  wrapper that shells into or directly imports this pipeline
- Move House retry policy and stub status transitions into the pipeline layer
- Add a nightly backfill runner for crypto-linked holdings, ETFs, and trust rows
- Add structured export for parser confidence, OCR backend used, and manual-review notes

## Immediate Next Steps

1. Replace the site's current House PTR cron execution path with this package.
2. Use `house-ingest` as the default runner for feed sync plus backlog processing.
3. Port the remaining House PTR edge-case handling out of the app layer.
4. Backfill indexed search for parsed House PTR documents already in Neon.
5. Finish the full Offshore Leaks corpus import and relationship backfill.
6. Add end-to-end fixture tests from recent filings that previously failed in production.
7. Extend the same pipeline architecture to official Senate ingestion.
