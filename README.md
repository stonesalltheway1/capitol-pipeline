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
```

## Retrofit Priorities

1. Replace the current House PTR OCR and extraction path in CapitolExposed
2. Backfill crypto-linked trades already present in the database
3. Replace the site-side House extraction cron path with this package
4. Add fixture-driven regression tests from real House and Senate disclosures

See [docs/RETROFIT_PLAN.md](docs/RETROFIT_PLAN.md) for the full implementation
plan.
