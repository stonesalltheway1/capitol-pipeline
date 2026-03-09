from capitol_pipeline.config import Settings
from capitol_pipeline.models.congress import FilingStub, HousePtrParseResult, MemberMatch, NormalizedTradeRow
from capitol_pipeline.models.document import Document
from capitol_pipeline.models.search import build_search_document
from capitol_pipeline.processors.chunking import build_search_chunks
from capitol_pipeline.bridges.search_documents import (
    build_house_ptr_search_document,
    build_house_ptr_search_document_from_stub_row,
)


def test_build_search_document_preserves_core_metadata() -> None:
    document = Document(
        id="doc-1",
        title="Example Filing",
        date="2026-03-01",
        source="house-clerk",
        category="stock-act",
        summary="Example summary",
        memberIds=["m-1"],
        assetTickers=["BTC"],
        tags=["house-ptr"],
        sourceUrl="https://example.test/source",
        pdfUrl="https://example.test/file.pdf",
        ocrText="This is example filing text.",
    )

    search_document = build_search_document(document)
    assert search_document.id == "doc-doc-1"
    assert search_document.source_document_id == "doc-1"
    assert search_document.member_ids == ["m-1"]
    assert search_document.asset_tickers == ["BTC"]
    assert "example filing text" in search_document.content.lower()


def test_build_search_chunks_splits_long_content() -> None:
    settings = Settings(
        embedding_chunk_size=90,
        embedding_chunk_overlap=20,
    )
    document = build_search_document(
        Document(
            id="doc-2",
            title="Long Filing",
            source="house-clerk",
            category="stock-act",
            ocrText="\n\n".join(
                (
                    [
                        "Paragraph one about a trade in Bitcoin and Ethereum and how it intersects with committee oversight and lobbying disclosures."
                    ]
                    + [
                        "Paragraph two about committee oversight, legislation, procurement, and market-moving testimony tied to the same sector."
                    ]
                    + [
                        "Paragraph three about lobbying, votes, donations, and market context that matter for hybrid retrieval and newsroom analysis."
                    ]
                )
                * 4
            ),
        )
    )

    chunks = build_search_chunks(document, settings)
    assert len(chunks) >= 2
    assert chunks[0].document_id == document.id
    assert chunks[0].token_estimate > 0


def test_build_house_ptr_search_document_includes_trade_metadata() -> None:
    stub = FilingStub(
        doc_id="20039999",
        filing_year=2026,
        filing_type="PTR",
        filing_date="2026-01-15",
        member=MemberMatch(
            id="m-1",
            name="Example Member",
            slug="example-member",
            party="R",
            state="TX",
            district="01",
        ),
        source="house-clerk",
        source_url="https://example.test/20039999.pdf",
    )
    parsed = HousePtrParseResult(
        doc_id="20039999",
        member_name="Example Member",
        state="TX",
        parser_confidence=0.91,
        raw_text_preview="Bitcoin (BTC) [ST] P 01/02/2026 01/05/2026 $1,001 - $15,000",
        transactions=[],
    )
    trades = [
        NormalizedTradeRow(
            member=stub.member,
            source="house-clerk",
            disclosure_kind="house-ptr",
            source_id="20039999:1",
            source_url=stub.source_url,
            ticker="BTC",
            asset_description="Bitcoin",
            asset_type="Cryptocurrency",
            transaction_type="purchase",
            transaction_date="2026-01-02",
            disclosure_date="2026-01-15",
            amount_min=1001,
            amount_max=15000,
            owner="self",
        )
    ]

    document = build_house_ptr_search_document(stub, parsed, trades)
    assert document.source_document_id == "house-ptr-20039999"
    assert document.asset_tickers == ["BTC"]
    assert document.metadata["parserConfidence"] == 0.91


def test_build_house_ptr_search_document_from_stub_row_uses_stored_metadata() -> None:
    row = {
        "doc_id": "20033783",
        "filing_year": 2026,
        "source_url": "https://example.test/ptr.pdf",
        "status": "parsed",
        "metadata": {
            "memberId": "m-W000816",
            "memberName": "Roger Williams",
            "filingType": "PTR",
            "filingDate": "2026-01-15",
            "rawTextPreview": "Roger Williams bought CVX.",
            "parsedTransactions": [{"ticker": "CVX"}],
            "parserConfidence": 0.92,
        },
    }

    document = build_house_ptr_search_document_from_stub_row(row)
    assert document.id == "doc-house-ptr-20033783"
    assert document.title == "Roger Williams House PTR 20033783"
    assert document.asset_tickers == ["CVX"]
    assert document.content == "Roger Williams bought CVX."
    assert document.metadata["stubStatus"] == "parsed"
