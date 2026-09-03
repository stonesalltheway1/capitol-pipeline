# Pipeline Operations

Capitol Pipeline is designed to run outside the web app and write directly into
CapitolExposed's Neon database. The production path should be:

1. GitHub Actions runs the hourly and daily workflows
2. Workflows call the CLI commands in this repo
3. CapitolExposed reads the refreshed database state and search corpus

## Required GitHub secrets

- `DATABASE_URL`
- `OPENAI_API_KEY`
- `ANTHROPIC_API_KEY` (only for the House PTR vision review path; without it the
  vision step skips instead of failing)

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

## House PTR review queue (Claude vision)

Roughly 210 House PTRs are stuck in `house_filing_stubs.status = 'needs_review'`
because they are scanned or handwritten. OCR produces junk, the regex parser
scores 0.0, and re-running OCR with a different backend does not help. Those
filings are readable by a vision model, so `process-house-review` can hand the
PDF straight to Claude.

Drain the queue in small, capped batches:

```bash
python -u -m capitol_pipeline process-house-review \
  --limit 5 \
  --ocr-backend pymupdf \
  --vision-backend auto \
  --with-search-index \
  --no-embeddings
```

`--ocr-backend` still selects the OCR chain and `--vision-backend` is a separate
flag for the Claude transcription path:

- `off` — never call the model (the default everywhere except this command).
- `auto` — call it only when the text parser scored under 0.5 or the OCR text
  was empty. The default for `process-house-review`.
- `claude` — always call it for the filing. Use it to spot-check one document
  with `parse-house-ptr --vision-backend claude`.

`--ocr-backend pymupdf` is the cheap choice here: on a scan the OCR pass has
nothing to find, so there is no point paying for `docling`. Keep
`--ocr-backend docling` when you want one more genuine OCR attempt first.

Required environment on the box:

- `ANTHROPIC_API_KEY` (or `ANTHROPIC_AUTH_TOKEN`). Without it the vision path
  skips with a reason instead of failing the run.
- `CAPITOL_PTR_VISION_MODEL` — optional read-model override; defaults to
  `claude-opus-5`. Orientation detection always uses `claude-haiku-4-5`.
- `CAPITOL_PTR_VISION_DISABLED=1` — kill switch. Set it and every filing is
  skipped with `reason: disabled by CAPITOL_PTR_VISION_DISABLED` and stays in
  the queue. Use this first if spend or output quality looks wrong; you do not
  need to redeploy.

- `CAPITOL_PTR_VISION_EFFORT` — `low`..`max`, default `medium`; use `high` for a
  queue of handwritten forms.
- `CAPITOL_PTR_VISION_CHUNK_PAGES` — pages per read request, default 4.
- `CAPITOL_PTR_VISION_MAX_COST_USD` — per-filing ceiling on the pre-flight
  estimate (pages x two reads x per-page rate + orientation), default 25 so a
  60-page filing fits at the measured ~$0.40 a page (medium effort, strips on;
  `CAPITOL_PTR_VISION_EFFORT=low` is the cost lever). A refused filing records `estimated cost $X ... exceeds
  the $Y ceiling` in `visionParse.reason` with `costEstimateUsd` /
  `costCeilingUsd`; a filing that overruns 1.5x the ceiling mid-way is
  abandoned with what it spent recorded.
- `CAPITOL_PTR_VISION_GRID_ZOOM` — close-up strip zoom (default 2, 0 disables).

Guardrails, in order: the env kill switch, missing credentials, PDFs over 20 MB,
PDFs over 60 pages, the cost ceiling, one filing per call, one retry on 429/5xx
per read (plus one halved retry when a read truncates at `max_tokens`), and
`--limit` as the hard per-run cap. Skipped filings keep `needs_review` and
record why. A filing the model reads as "nothing to report" (both reads, zero
rows, `no_transactions_stated`) ends `parsed` with zero rows and
`visionParse.noTransactions: true`; a stub bounced for another reason (an
unresolved member) is re-processed for free while its PDF hash and
`visionParse.at` are within 30 days.

### Reading the summary JSON

`process-house-review` adds four fields on top of the usual counters:

- `visionBackend` — which mode the run used
- `visionCalls` — filings where a vision attempt was recorded (including skips)
- `visionRowsRecovered` — transactions transcribed
- `visionCostUsd` — estimated spend for the run

Per filing, `processed[].parserVersion` is `claude-vision-v2` (older rows carry
`claude-sonnet-5-vision-v1`; anything starting `claude-` and containing
`vision` is the vision path) when the rows came from vision rather than the
regex or Haiku text paths, and `processed[].visionParse` carries the row count,
legibility counts, cost, and skip reason. The full `metadata.visionParse` record
also has `orientation` (rotation and method per page), `readAgreement`
(`rowsA`, `rowsB`, `matched`, `fieldDisagreements`), and `calls` (usage and cost
per orientation call and per read).

### Where cost is recorded

Cost is estimated in the pipeline, not read back from a bill. Every attempt
writes `house_filing_stubs.metadata.visionParse` with token usage
(`inputTokens`, `cacheReadTokens`, `cacheWriteTokens`, `outputTokens`), the
`costUsd` estimate, and the `pricing` block the estimate used, so an old row
still explains itself if rates change. Claude Opus 5 is $5 / $25 per MTok and
Claude Haiku 4.5 (orientation only) $1 / $5; cache reads bill at 0.1x input and
cache writes at 1.25x input. Every filing costs two Opus reads plus one or two
Haiku calls per page, and `usage` / `costUsd` are the sum across all of them.
The ~2,000-token system prompt is sent with `cache_control: ephemeral`, so after
the first read of a run it should show up as `cacheReadTokens`, not
`inputTokens`.

Query the running total:

```sql
SELECT count(*) AS filings,
       round(sum((metadata->'visionParse'->>'costUsd')::numeric), 4) AS usd
FROM house_filing_stubs
WHERE metadata->'visionParse' IS NOT NULL;
```

### When a filing stays in needs_review

The model rates each row `clear`, `partial`, or `illegible`. More than half the
rows `illegible` keeps the stub `needs_review` with the transcription attached
under `metadata.parsedTransactions` for a human to check; anything better marks
it `parsed`. A stub whose member never resolved can never be marked `parsed`
regardless of legibility — fix the member registry first.

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
python -u -m capitol_pipeline process-house-review --limit 5 --ocr-backend pymupdf --vision-backend auto
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
- If `process-house-review` reports `visionCalls: 0` while filings stay in the
  queue, read `metadata.visionParse.reason` on one of them. The usual causes are
  the `CAPITOL_PTR_VISION_DISABLED` kill switch, a missing `ANTHROPIC_API_KEY`,
  or a filing over the 25-page / 20 MB guardrail.
- If `visionCostUsd` climbs faster than expected, check that
  `visionParse.usage.cacheReadTokens` is non-zero after the first filing in a
  run. Zero cache reads across a batch means the cached system prefix is being
  invalidated and every filing is paying full input price.
