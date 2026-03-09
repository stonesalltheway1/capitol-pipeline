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

### Phase 2
- Add Capitol member resolution using the site member registry
- Export normalized trades into CapitolExposed database tables
- Replace the site's current House PTR extraction path with this package

### Phase 3
- Add direct crypto, crypto ETF/trust, and crypto-equity classification during
  normalization
- Backfill historical CapitolExposed trade rows with the new classifier
- Rewrite the `/crypto` page to use the normalized output

### Phase 4
- Replace Senate watcher dependency with official Senate Ethics ingestion where
  feasible
- Add filings quality scoring and manual-review queue output
- Add regression tests against real Congress PDF fixtures

## Immediate Next Steps

1. Connect this package to the CapitolExposed members registry.
2. Add a Neon exporter tailored to CapitolExposed table names.
3. Port the remaining House PTR edge-case handling out of the app layer.
4. Add end-to-end fixture tests from recent filings that previously failed in production.
