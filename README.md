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
- Official Senate eFD (efdsearch.senate.gov) scraper for Periodic Transaction Reports
- Senate watcher and Quiver source adapters (both now legacy)
- House PTR parser for text and PDF-backed filings
- Vision transcription for scanned and handwritten PTRs the OCR chain cannot read,
  read twice and reconciled, on Gemini's free tier by default and Claude on request
- CapitolExposed-compatible member registry resolution
- Crypto asset classifier for direct coins, ETFs and trusts, and adjacent equities
- Bridge helpers that emit shapes compatible with CapitolExposed database tables
- Neon exporters for member loading, House stub sync, and parsed trade upserts
- Dedicated pipeline search tables with `tsvector` and optional `pgvector` indexing
- Hybrid retrieval commands for lexical search now and semantic search when embeddings are enabled
- ICIJ Offshore Leaks ingestion into dedicated raw corpus tables plus Congress match extraction
- Official FARA ingestion into dedicated raw corpus tables plus registrant search documents
- Core CapitolExposed entity indexing for members, committees, bills, and alerts

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

# Inspect the official Senate eFD Periodic Transaction Report feed
capitol-pipeline senate-feed --provider efd --limit 10

# ...including each report's parsed rows
capitol-pipeline senate-feed --provider efd --limit 3 --with-transactions

# Scrape efdsearch.senate.gov and write new Senate trades into CapitolExposed
capitol-pipeline senate-ingest --provider efd --with-search-index --no-embeddings

# Backfill a wider eFD window (submitted-date based, capped per run)
capitol-pipeline senate-ingest --provider efd --since 2026-01-01 --max-reports 400

# Collapse Senate trades the old canonical id scheme duplicated
capitol-pipeline dedupe-senate-trades --dry-run
capitol-pipeline dedupe-senate-trades --apply

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

# Read a scanned or handwritten PTR with Claude vision instead of OCR
capitol-pipeline parse-house-ptr ./scan.pdf \
  --doc-id 20033783 \
  --filing-year 2026 \
  --member-name "Roger Williams" \
  --vision-backend claude

# Process a batch of queued House PTR stubs directly from CapitolExposed
capitol-pipeline process-house-backlog --limit 10

# Drain the needs_review queue, handing unreadable scans to Claude vision
capitol-pipeline process-house-review --limit 5 --vision-backend auto

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

# Index core CapitolExposed entities into the shared corpus
capitol-pipeline index-site-core --only-missing

# Backfill missing embeddings on already-indexed search chunks
capitol-pipeline embed-search-backfill --limit 100

# Drain the embedding queue in stable batches, optionally per source
capitol-pipeline embed-search-corpus --source capitol-exposed --batch-size 100
```

## Automation

This repo now includes GitHub Actions for unattended refresh:

- `.github/workflows/house-refresh.yml`
- `.github/workflows/corpus-refresh.yml`
- `.github/workflows/offshore-match-refresh.yml`
- `.github/workflows/offshore-full-refresh.yml`

Set these repository secrets before enabling the schedules:

- `DATABASE_URL`
- `OPENAI_API_KEY`

Operational details and recovery commands live in
[docs/OPERATIONS.md](docs/OPERATIONS.md).

## Senate Trade Sources

Senate PTRs now come from the official disclosure site rather than a paid API:

| Provider | Status |
|---|---|
| `efd` | **Current.** Scrapes `https://efdsearch.senate.gov` directly. Free, no key. |
| `quiver-live` / `quiver-bulk` | Legacy. Needs `QUIVER_API_TOKEN`; the subscription lapsed and returns 403. |
| `watcher` | Dead. The senate-stock-watcher aggregate feed has been frozen since 2020. |

`--provider auto` picks `quiver-live` while a Quiver token is configured and
otherwise falls back to `efd`. It never falls back to the watcher feed.

The eFD scraper accepts the site's prohibition agreement once per session, holds
the resulting cookies, waits at least a second between requests, retries 5xx and
429, and stops after `CAPITOL_SENATE_EFD_MAX_REPORTS_PER_RUN` reports (200 by
default) so a 30-minute timer stays cheap. Electronic filings
(`/search/view/ptr/...`) are parsed from their HTML transaction table with the
standard library HTML parser. Scanned paper filings (`/search/view/paper/...`)
are page-image GIFs that the PDF-only OCR chain cannot read, so they are
recorded in the run summary as `needs_review` with their page-image URLs and
skipped.

Relevant settings (all `CAPITOL_`-prefixed environment variables):

- `CAPITOL_SENATE_EFD_BASE_URL`
- `CAPITOL_SENATE_EFD_USER_AGENT`
- `CAPITOL_SENATE_EFD_REQUEST_INTERVAL_SECONDS`
- `CAPITOL_SENATE_EFD_MAX_REPORTS_PER_RUN`
- `CAPITOL_SENATE_EFD_LOOKBACK_DAYS`
- `CAPITOL_SENATE_EFD_FLOOR_DAYS`

## House PTR Vision Path

About 210 House PTRs sit in `house_filing_stubs.status = 'needs_review'` because
they are photocopies or handwritten forms. The OCR chain returns fragments like
`| 9 984 F 1 | Sale | 1 |`, the regex parser scores 0.0, and the filing is never
turned into trade rows. `--vision-backend` sends the PDF itself to Claude as a
`document` content block and asks for the transaction grid back as
schema-constrained JSON.

| Value | Behavior |
|---|---|
| `off` | Never calls the model. Default on `parse-house-ptr`, `process-house-backlog` and `house-ingest`. |
| `auto` | Calls the model only when the text parser scored under 0.5 or produced no OCR text. **Default on `process-house-review`.** |
| `claude` | Always calls the model for the filing, even when the text parser was confident. Use it to spot-check one document. |

`--ocr-backend` is unchanged and still selects the OCR chain
(`pymupdf` / `surya` / `olmocr` / `docling` / `auto`). The two flags are
independent: the OCR pass always runs first and its text is what decides
whether `auto` escalates.

The escalation order inside `parse_house_ptr_pdf` is:

1. OCR + the regex parser.
2. If the regex found nothing but the OCR text layer is genuinely readable, the
   existing Haiku 4.5 **text** fallback (`ptr_llm_fallback.py`) still runs.
3. If the text is junk or the parse is weak and the vision backend is `auto` or
   `claude`, the PDF goes to the vision model (`ptr_vision.py`).

The vision path (`parser_version` `claude-vision-v2`) renders each page with
pymupdf to a PNG (150 DPI, long edge capped at 1568 px), asks `claude-haiku-4-5`
which way the scan must turn to read upright (falling back to "a portrait page
is a sideways landscape form, rotate 90"), and sends the upright images to
`claude-opus-5` **twice** as independent requests. Rows are matched across the
two reads by asset description; a field is only kept when both reads agree, a
disagreement on the date, type, or amount nulls the field and marks the row
`illegible`, a row only one read saw is kept but marked `illegible`, and a
row-count mismatch or any critical disagreement keeps the stub in review. When
pymupdf is unavailable the PDF is sent as a `document` block instead.

Long filings are read in chunks of 4 pages (paper attachments run 16-18 rows a
page), each chunk getting its own two reads, with rows concatenated and line
numbers continuing across chunks; a read that truncates at `max_tokens` is
retried once with the page group halved. Landscape pages (the paper checkbox
form) also get two close-up strips of the right-hand 58% of the page at a
higher zoom, and the model reports the amount column letter (A-K) alongside
the band; a letter/band mismatch inside a read, or a letter disagreement
between reads, nulls the amount and routes the filing to review. A filing whose
reads both return zero rows and both report `no_transactions_stated` ends
`parsed` with zero rows and `visionParse.noTransactions: true`. A stub
re-processed within 30 days with an unchanged PDF (`visionParse.pdfSha256`)
reuses its previous transcription instead of paying for a new read.

Because two reads of the same model share blind spots (both put every tick on
one Khanna page a column too far left), a classical checkbox detector
(`parsers/ptr_grid.py`, numpy over the rendered page, no model) finds the
amount ladder's column rules and the ticked cell in each row and names the
column A-K. Rows carry `page_number`, so each page's rows are aligned top to
bottom with the detected ticks (the paper form's pre-printed example row is
dropped); a detector letter that agrees confirms the band, a disagreement or
an ambiguous cell nulls the amount, marks the row `partial` and routes the
filing to review. Per-row `detectorLetter`/`detectorStatus` and a per-page
`visionParse.detector` block (`columns`, `rowsAligned`, `agreed`, `disagreed`,
`ambiguous`, `status`) record the outcome.

A vision-parsed filing publishes trade rows only when it resolves to `parsed`;
while it is `needs_review` the transcription stays on the stub
(`parsedTransactions`, `visionParse.rows`) and nothing is written to `trades`
(`visionParse.withheldTrades`). The text path is unchanged.
`process-house-review --doc-id <id>` (repeatable) targets specific stubs.

Vision rows are normalized through exactly the same helpers as text rows
(`clean_asset_description`, `infer_asset_type`, `normalize_date`, the crypto
classifier, member resolution from the stub), so they land with the same
`tr-house-{doc_id}-{line}` ids and are indistinguishable in the `trades` table
apart from `parser_version`.

Settings:

- `CAPITOL_PTR_VISION_DISABLED=1` — kill switch; the path skips with a reason
  and the stub stays `needs_review`.
- `CAPITOL_PTR_VISION_MODEL` — override the read model (default `claude-opus-5`).
- `CAPITOL_PTR_VISION_EFFORT` — reasoning effort (`low`..`max`, default `medium`).
- `CAPITOL_PTR_VISION_CHUNK_PAGES` — pages per read request (default 4).
- `CAPITOL_PTR_VISION_MAX_COST_USD` — per-filing ceiling on the pre-flight cost
  estimate (default 25, sized for a 60-page filing at the measured $0.40 a page); a filing over it is refused
  with the estimate in `visionParse.reason`, and one that overruns 1.5x the
  ceiling while running is abandoned.
- `CAPITOL_PTR_VISION_GRID_ZOOM` — close-up strip zoom relative to the page
  (default 2; 0 disables the strips).
- `CAPITOL_PTR_VISION_PAGE_RANGE` — debug only: `11-13` reads just those pages
  of a filing (labels keep the filing-wide numbering). Unset in production.
- `ANTHROPIC_API_KEY` (or `ANTHROPIC_AUTH_TOKEN`) — required; without it the
  path skips rather than failing the run.

Guardrails: one filing per call, PDFs over 60 pages or 20 MB are skipped with a
reason, the cost ceiling above, one retry on 429/5xx, and `--limit` caps
filings per run.

Every attempt is recorded on the stub under `metadata.visionParse` with the
model, token usage, estimated cost, per-row legibility counts, and the skip
reason when it did not run. A filing leaves the review queue when the model
rated more than half its rows legible; otherwise it stays `needs_review` with
the transcription attached for a human.

## Search Layer

Capitol Pipeline now manages its own retrieval tables in Neon instead of
writing into any older app-owned search tables:

- `pipeline_search_documents`
- `pipeline_search_chunks`

That search layer supports:

1. `tsvector` indexes for exact and lexical retrieval
2. `pgvector` indexes for semantic retrieval
3. Hybrid ranking across title, summary, document body, and indexed chunks
4. A shared corpus that can mix CapitolExposed editorial, members, committees, bills, alerts, House PTRs, FARA, and ICIJ cross-references

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
2. Replace the site-side House and Senate ingest loops with this package
3. Backfill crypto-linked trades already present in the database
4. Expand indexed search across filings, stories, and official source documents
5. Add fixture-driven regression tests from real House and Senate disclosures

See [docs/RETROFIT_PLAN.md](docs/RETROFIT_PLAN.md) for the full implementation
plan.
