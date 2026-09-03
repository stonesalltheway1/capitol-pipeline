# Pipeline Operations

Capitol Pipeline is designed to run outside the web app and write directly into
CapitolExposed's Neon database. The production path should be:

1. GitHub Actions runs the hourly and daily workflows
2. Workflows call the CLI commands in this repo
3. CapitolExposed reads the refreshed database state and search corpus

## Required GitHub secrets

- `DATABASE_URL`
- `OPENAI_API_KEY`
- `GEMINI_API_KEY` (the House PTR vision review path's default provider, free
  tier; without it the vision step skips instead of failing)
- `ANTHROPIC_API_KEY` (only when `CAPITOL_PTR_VISION_PROVIDER=anthropic`)

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

## House PTR review queue (scanned and handwritten filings)

Roughly 220 House PTRs sit in `house_filing_stubs.status = 'needs_review'`
because they are scanned or handwritten. OCR on the PDF as filed produces junk,
the regex parser scores 0.0, and re-running OCR with a different backend does
not help. Two things fix that, and neither of them costs anything.

### 1. Publish what is already transcribed

Before reading anything, drain the filings that were already read. A PTR parsed
for a filer with no member record keeps its rows in
`metadata.parsedTransactions` and writes nothing to `trades`; loading the
historical members afterwards resolves the filer, but nothing re-runs. As of
2026-09-03 that was 105 filings holding 738 finished rows.

```bash
python -u -m capitol_pipeline repersist-house-stubs --limit 25 --dry-run
python -u -m capitol_pipeline repersist-house-stubs --limit 25 --apply
```

No PDF is downloaded and no model is called. The command is restartable:
publishing a filing gives it trade rows, which takes it out of the queue. It
skips the vision transcriptions that are deliberately withheld pending review
unless you pass `--include-vision`, and `--min-confidence` holds back anything
the original parser was unsure of. Rows whose parser version was never recorded
are published as `house-ptr-text-replay-v1` rather than guessing at `regex-v1`,
and each row's comment says it was published once the member record resolved.

### 2. Read the pages

```bash
python -u -m capitol_pipeline process-house-review \
  --limit 5 \
  --ocr-backend auto \
  --vision-backend auto \
  --with-search-index \
  --no-embeddings
```

`--ocr-backend` selects the OCR chain and `--vision-backend` decides *whether*
the pages are read as images:

- `off` — never call a model (the default everywhere except this command).
- `auto` — call one only when the text parser scored under 0.5 or the OCR text
  was empty. The default for `process-house-review`.
- `on` — always call one for the filing (`claude` and `gemini` are accepted
  spellings, so the box's `HOUSE_REVIEW_VISION_BACKEND` keeps working). Use it
  to spot-check one document with `parse-house-ptr --vision-backend on`.

*Who* answers is `CAPITOL_PTR_VISION_PROVIDER`, and it is a separate decision:

| | |
|---|---|
| `gemini` (default) | Google Generative Language, free tier. Read A is `gemini-3.8-flash`, read B `gemini-3.5-flash`. Needs `GEMINI_API_KEY` (or `GOOGLE_API_KEY`). |
| `anthropic` | The original paid path, `claude-opus-5`. Needs `ANTHROPIC_API_KEY`. |

Neither falls back to the other: a missing key skips the filing with a reason
and leaves it in the queue.

Two things about the free path that were measured, not assumed:

- **Read A and read B are two different model versions on purpose.** Two
  samples of one model agree with themselves. On Wied 9115665 one version read
  Purchase / $1,001-$15,000 and the other Sale / $15,001-$50,000, and the
  second was right; the disagreement is what makes the filing get looked at.
- **Free-tier input may be used to improve Google's models.** Everything sent
  is a published federal disclosure, which is why that trade is acceptable. It
  is a stated decision.

`--ocr-backend auto` is the right choice for a scan: the pages are rendered
upright before OCR runs, which is what turns a sideways scan from noise into
text (87 of 108 House scans are stored rotated 270 degrees). `--ocr-backend
pymupdf` reads only an existing text layer, so on an image-only filing it does
nothing at all — use it only when you deliberately want no OCR.

### Orientation costs nothing

Every page is analysed at 0, 90, 180 and 270 degrees by the checkbox detector
and scored on the form's own asymmetries: a complete A-K ladder, the header
printed above the rows, the ladder at the right margin, and the wide K column
on the right. The filing as a whole then settles pages that are close calls,
because a scan goes through the feeder once. A page with no ladder at any
rotation follows the rest of the filing when the quarter turn agrees, and
otherwise falls back to "a portrait page is a sideways landscape form".

`CAPITOL_PTR_VISION_ORIENTATION` picks the strategy: `grid` (default, free),
`model` (the old paid pick, two calls a page), `heuristic` (page shape only).
`orientation[].method` on the stub records which one decided each page.

### Environment

- `GEMINI_API_KEY` / `GOOGLE_API_KEY` — the free provider's credentials.
- `ANTHROPIC_API_KEY` (or `ANTHROPIC_AUTH_TOKEN`) — only for
  `CAPITOL_PTR_VISION_PROVIDER=anthropic`.
- `CAPITOL_PTR_VISION_PROVIDER` — `gemini` (default) or `anthropic`.
- `CAPITOL_PTR_VISION_MODEL` / `CAPITOL_PTR_VISION_MODEL_B` — read A and read B
  model overrides. Do not set either to `gemini-2.5-flash`: it is retired for
  new API keys and 404s.
- `CAPITOL_PTR_VISION_GEMINI_RPM` — requests per minute per model, default 10.
  Google no longer publishes the free-tier limit; check it in AI Studio before
  sizing a batch. A 429 backs off (honouring the API's own `retryDelay`) and
  retries up to `CAPITOL_PTR_VISION_GEMINI_MAX_ATTEMPTS` (default 4).
- `CAPITOL_PTR_VISION_DISABLED=1` — kill switch. Every filing is skipped with
  `reason: disabled by CAPITOL_PTR_VISION_DISABLED` and stays in the queue. Use
  this first if output quality looks wrong; you do not need to redeploy.
- `CAPITOL_PTR_VISION_EFFORT` — `low`..`max`, default `medium`; use `high` for a
  queue of handwritten forms. On Gemini it maps to `thinkingLevel`.
- `CAPITOL_PTR_VISION_CHUNK_PAGES` — pages per read request, default 4.
- `CAPITOL_PTR_VISION_MAX_COST_USD` — per-filing ceiling on the pre-flight
  estimate, default 25. Every Gemini rate is zero, so on the free provider the
  ceiling never bites and the long typed attachments the paid path had to
  refuse go through. On `anthropic` it still holds: a 60-page filing fits at
  the measured ~$0.40 a page (medium effort, strips on;
  `CAPITOL_PTR_VISION_EFFORT=low` is the cost lever). A refused filing records
  `estimated cost $X ... exceeds the $Y ceiling` in `visionParse.reason` with
  `costEstimateUsd` / `costCeilingUsd`; a filing that overruns 1.5x the ceiling
  mid-way is abandoned with what it spent recorded.
- `CAPITOL_PTR_VISION_GRID_ZOOM` — close-up strip zoom (default 2, 0 disables).
- `CAPITOL_PTR_VISION_PAGE_RANGE` — debug only, e.g. `11-13`: read just those
  pages of a filing at their normal page labels. Never set it on the timer.
- `CAPITOL_PTR_UPRIGHT_OCR=0` — hand OCR the PDF as filed instead of the
  upright render. Only for comparing the two.

### What the detector adds

The checkbox detector (`parsers/ptr_grid.py`) runs on every rendered page
without a model call and cross-checks each row's amount column; its verdicts
are in `visionParse.detector` (per page: `status` one of `ok`, `no-grid`,
`no-rows`, `unaligned`; counts of `agreed`, `disagreed`, `ambiguous`) and per
row in `visionParse.rows` (`det:<letter>/<status>`). A disagreement or an
ambiguous cell nulls that row's amount and sends the filing to review. A
vision filing in `needs_review` publishes nothing to `trades`
(`visionParse.withheldTrades` says how many rows are waiting); once a human
resolves it, `process-house-review --doc-id <id>` re-runs that stub alone and,
if the PDF is unchanged, the previous read is under 30 days old and the same
provider would answer today, reuses the transcription for free.

Guardrails, in order: the env kill switch, missing credentials for the
configured provider, PDFs over 20 MB, PDFs over 60 pages, the cost ceiling, one
filing per call, one retry per read (plus one halved retry when a read
truncates at `max_tokens`), and `--limit` as the hard per-run cap. Skipped
filings keep `needs_review` and record why. A filing both reads agree states
"nothing to report" ends `parsed` with zero rows and
`visionParse.noTransactions: true`.

### Reading the summary JSON

`process-house-review` adds four fields on top of the usual counters:

- `visionBackend` — which mode the run used
- `visionCalls` — filings where a vision attempt was recorded (including skips)
- `visionRowsRecovered` — transactions transcribed
- `visionCostUsd` — estimated spend for the run

Per filing, `processed[].parserVersion` names the reader: `gemini-vision-v2` on
the free provider, `claude-vision-v2` on the paid one (older rows carry
`claude-sonnet-5-vision-v1`; anything starting `claude-` or `gemini-` and
containing `vision` is the vision path) when the rows came from vision rather
than the regex or Haiku text paths, and `processed[].visionParse` carries the row count,
legibility counts, cost, and skip reason. The full `metadata.visionParse` record
also has `orientation` (rotation and method per page), `readAgreement`
(`rowsA`, `rowsB`, `matched`, `fieldDisagreements`), and `calls` (usage and cost
per orientation call and per read).

### Where cost is recorded

Cost is estimated in the pipeline, not read back from a bill. Every attempt
writes `house_filing_stubs.metadata.visionParse` with the provider, both read
models, token usage (`inputTokens`, `cacheReadTokens`, `cacheWriteTokens`,
`outputTokens`), the `costUsd` estimate, and the `pricing` block the estimate
used, so an old row still explains itself if rates change.

On the free provider every rate is 0.0 and `costUsd` is a true zero rather than
an estimate nobody paid. On `anthropic`, Claude Opus 5 is $5 / $25 per MTok and
Claude Haiku 4.5 (orientation only, and only when
`CAPITOL_PTR_VISION_ORIENTATION=model`) $1 / $5; cache reads bill at 0.1x input
and cache writes at 1.25x input.
