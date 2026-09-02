# Pipeline Operations

Capitol Pipeline is designed to run outside the web app and write directly into
CapitolExposed's Neon database. The production path should be:

1. GitHub Actions runs the hourly and daily workflows
2. Workflows call the CLI commands in this repo
3. CapitolExposed reads the refreshed database state and search corpus

## Required GitHub secrets

- `DATABASE_URL`
- `OPENAI_API_KEY`

## Scheduled workflows

### Filings Refresh

File: `.github/workflows/house-refresh.yml`

Runs every 15 minutes and:

1. syncs the current House Clerk XML feed
2. processes queued PTRs
3. normalizes new Senate eFD rows into site-ready trades
4. writes parsed trades and stub state back into Neon
5. indexes newly parsed PTRs and Senate trades into the shared search corpus

This workflow intentionally does not create embeddings. It keeps the live filing
loop fast and cheap.

### Corpus Refresh

File: `.github/workflows/corpus-refresh.yml`

Runs twice per day and:

1. ingests the official FARA bulk corpus
2. indexes new CapitolExposed stories, dossiers, members, committees, bills, and alerts
3. embeds queued search chunks into `pipeline_search_chunks`
4. can optionally refresh the Offshore match index on manual dispatch

### Offshore Match Refresh

File: `.github/workflows/offshore-match-refresh.yml`

Runs weekly and:

1. re-evaluates Congress name matches against the already-ingested Offshore corpus
2. refreshes shared search documents for newly matched records
3. avoids reloading the multi-million-row raw Offshore tables when no upstream change occurred

### Offshore Full Refresh

File: `.github/workflows/offshore-full-refresh.yml`

Manual only. Use this when the upstream ICIJ archive changes or when the raw
Offshore corpus needs a fresh rebuild.

## Recommended operating rhythm

- Filings refresh every 15 minutes
- Corpus refresh twice daily
- Offshore match refresh weekly
- Offshore full raw ingest only when the upstream ICIJ archive changes

## Senate trades

Senate PTRs come from the official disclosure site, `https://efdsearch.senate.gov`.
The Quiver subscription lapsed (403 on every call) and the senate-stock-watcher
aggregate feed has been frozen since 2020, so `--provider efd` is the live path
and `--provider auto` falls back to it whenever `QUIVER_API_TOKEN` is unset.

On the box this runs as a systemd timer every 30 minutes:

```bash
python -u -m capitol_pipeline senate-ingest \
  --provider efd \
  --with-search-index \
  --no-embeddings
```

The window is chosen from the database, not the clock: it starts
`CAPITOL_SENATE_EFD_LOOKBACK_DAYS` (14) before the newest `disclosure_date`
already stored for any Senate source, floored at
`CAPITOL_SENATE_EFD_FLOOR_DAYS` (60) days ago so a scheduled run never sweeps
the whole archive. Override it with `--since YYYY-MM-DD`. Each run opens at most
`--max-reports` reports (200 by default) and waits ≥1 s between requests.

First backfill after deploying the scraper — one wider pass, then let the timer
take over:

```bash
python -u -m capitol_pipeline senate-ingest \
  --provider efd \
  --since 2026-01-01 \
  --max-reports 400 \
  --with-search-index \
  --no-embeddings
```

Reading the summary JSON:

- `reportsListed` — PTRs matched in the submitted-date window
- `electronicParsed` — HTML filings whose transaction table was read
- `paperDeferred` / `paperDeferredReports` — scanned filings recorded as
  `needs_review` with their page-image URLs. The OCR chain only accepts PDFs, so
  these are never parsed automatically; they are a manual follow-up.
- `tradesInserted` / `skipped` — rows written versus already present, unresolved,
  or filed before `--start-date`
- `errors` — per-report failures. A single bad report never aborts the run.

Inspect without writing anything:

```bash
python -u -m capitol_pipeline senate-feed --provider efd --limit 5 --with-transactions
```

### One-off: collapse the duplicated Quiver rows

The canonical Senate trade id used to hash `asset_description` and `source_url`,
which the Quiver live and bulk feeds word differently, so the same trade was
written twice (~2,686 rows). The id now hashes only member, asset, action,
transaction date, amount bounds and owner, so new ingests collide correctly.
Clean up the existing rows once, after deploying:

```bash
python -u -m capitol_pipeline dedupe-senate-trades --dry-run   # counts and sample groups
python -u -m capitol_pipeline dedupe-senate-trades --apply     # one transaction
```

`scripts/dedupe_senate_trades.sql` does the same through `psql` and rolls back
by default. Always read the dry run first; the apply path is transactional, so a
foreign-key conflict rolls the whole sweep back rather than half-deleting.

The TypeScript mirror of the id, `lib/trade-integrity.ts` in the site repo, must
be updated to match or the site and the pipeline will disagree about trade ids.

## Manual recovery commands

```bash
python -u -m capitol_pipeline corpus-status
python -u -m capitol_pipeline house-ingest --year 2026 --batch-size 25 --max-batches 6
python -u -m capitol_pipeline senate-ingest --provider efd --with-search-index --no-embeddings
python -u -m capitol_pipeline dedupe-senate-trades --dry-run
python -u -m capitol_pipeline ingest-fara --mode bulk --skip-existing --with-match-index
python -u -m capitol_pipeline index-site-editorial --only-missing
python -u -m capitol_pipeline index-site-core --only-missing
python -u -m capitol_pipeline embed-search-corpus --batch-size 100 --max-batches 30
```

## Full backfill commands

```bash
python -u -m capitol_pipeline ingest-offshore-leaks --with-match-index
python -u -m capitol_pipeline ingest-fara --mode bulk --with-match-index
python -u -m capitol_pipeline index-house-search-backfill --only-missing
python -u -m capitol_pipeline index-site-editorial --reindex-all
python -u -m capitol_pipeline index-site-core --reindex-all
python -u -m capitol_pipeline ingest-offshore-leaks --skip-nodes --skip-relationships --with-match-index
python -u -m capitol_pipeline embed-search-corpus --batch-size 100 --max-batches 0
```

## Notes

- If `corpus-status` shows embedded chunks stalling while documents continue to
  rise, check `OPENAI_API_KEY` first.
- If `house_filing_stubs` stalls in `pending_extraction`, run the filings workflow
  manually and inspect the summary JSON for deferred or failed documents.
- If Senate trades stop moving while House continues, run
  `senate-ingest --provider efd` manually and inspect `reportsListed`,
  `electronicParsed` and `errors`. `reportsListed: 0` with a sensible
  `windowStartDate` is a real result: Senate PTRs arrive in bursts.
- If every eFD request fails, check the prohibition-agreement handshake first.
  The scraper raises `SenateEfdError` when `POST /search/home/` stops setting a
  session cookie, which is what happens if the site changes that form.
- If the House Clerk feed references a PDF before it is published, the stub is
  deferred and retried later. That is expected behavior.
