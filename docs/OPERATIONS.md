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

### House Refresh

File: `.github/workflows/house-refresh.yml`

Runs every 15 minutes and:

1. syncs the current House Clerk XML feed
2. processes queued PTRs
3. writes parsed trades and stub state back into Neon
4. indexes newly parsed PTRs into the shared search corpus

This workflow intentionally does not create embeddings. It keeps the live filing
loop fast and cheap.

### Corpus Refresh

File: `.github/workflows/corpus-refresh.yml`

Runs twice per day and:

1. ingests the official FARA bulk corpus
2. indexes new CapitolExposed stories, dossiers, members, committees, bills, and alerts
3. embeds queued search chunks into `pipeline_search_chunks`
4. can optionally refresh the Offshore match index on manual dispatch

### Offshore Full Refresh

File: `.github/workflows/offshore-full-refresh.yml`

Manual only. Use this when the upstream ICIJ archive changes or when the raw
Offshore corpus needs a fresh rebuild.

## Recommended operating rhythm

- House refresh every 15 minutes
- Corpus refresh twice daily
- Offshore full raw ingest only when the upstream ICIJ archive changes

## Manual recovery commands

```bash
python -u -m capitol_pipeline corpus-status
python -u -m capitol_pipeline house-ingest --year 2026 --batch-size 25 --max-batches 6
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
python -u -m capitol_pipeline embed-search-corpus --batch-size 100 --max-batches 0
```

## Notes

- If `corpus-status` shows embedded chunks stalling while documents continue to
  rise, check `OPENAI_API_KEY` first.
- If `house_filing_stubs` stalls in `pending_extraction`, run the House workflow
  manually and inspect the summary JSON for deferred or failed documents.
- If the House Clerk feed references a PDF before it is published, the stub is
  deferred and retried later. That is expected behavior.
