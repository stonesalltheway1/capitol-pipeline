# Capitol Pipeline

Open-source data processing pipeline for Congressional financial disclosures, campaign finance, lobbying records, and voting data.

Powers [CapitolGraph.com](https://capitolgraph.com) — a real-time knowledge graph that cross-references Congressional stock trades, lobbying disclosures, campaign donations, and voting records.

## Features

- **Multi-source ingestion**: STOCK Act (House/Senate), FEC, LDA, Congress.gov, SEC EDGAR
- **OCR processing**: Handwritten STOCK Act PDF filings (multi-backend: PyMuPDF, Surya, Docling)
- **Entity extraction**: NER for tickers, trade amounts, committee names, bill numbers
- **Conflict detection**: AI-powered cross-referencing of trades vs. votes vs. lobbying
- **Knowledge graph**: Entity relationship mapping (member → company → committee → bill)
- **Deduplication**: 3-pass dedup for amended/corrected filings
- **Neon Postgres export**: pgvector semantic search + FTS

## From the creators of

- [EpsteinExposed.com](https://epsteinexposed.com) — 2M+ document investigative database
- [Epstein-Pipeline](https://github.com/stonesalltheway1/Epstein-Pipeline) — the open-source pipeline this project is based on

## License

MIT
