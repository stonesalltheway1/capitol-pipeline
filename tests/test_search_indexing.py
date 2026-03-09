from capitol_pipeline.config import Settings
from capitol_pipeline.models.congress import FilingStub, HousePtrParseResult, MemberMatch, NormalizedTradeRow
from capitol_pipeline.models.document import Document
from capitol_pipeline.models.search import build_search_document
from capitol_pipeline.processors.chunking import build_search_chunks
from capitol_pipeline.bridges.search_documents import (
    build_alert_search_document,
    build_bill_search_document,
    build_committee_search_document,
    build_dossier_search_document,
    build_house_ptr_search_document,
    build_house_ptr_search_document_from_stub_row,
    build_member_search_document,
    build_news_post_search_document,
)
from capitol_pipeline.registries.members import MemberRegistry


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


def test_build_news_post_search_document_resolves_member_refs() -> None:
    registry = MemberRegistry.from_rows(
        [
            {
                "id": "m-G000000",
                "name": "Josh Gottheimer",
                "slug": "josh-gottheimer",
                "party": "D",
                "state": "NJ",
            }
        ]
    )
    row = {
        "slug": "example-story",
        "title": "Example Story",
        "subtitle": "An accountability reporting sample",
        "excerpt": "This is the summary.",
        "body": "Full story body with a deeper evidence chain.",
        "category": "general",
        "tags": ["oversight", "accountability"],
        "author": "CapitolExposed Research Team",
        "member_refs": [{"name": "Josh Gottheimer", "slug": "josh-gottheimer"}],
        "evidence": [{"type": "trade"}],
        "reading_time": "4 min read",
        "word_count": 900,
        "published_at": "2026-03-08T05:26:06.320Z",
        "updated_at": "2026-03-08T12:00:11.909Z",
    }

    document = build_news_post_search_document(
        row,
        base_url="https://www.capitolexposed.com",
        registry=registry,
    )
    assert document.source == "capitol-exposed"
    assert document.source_url == "https://www.capitolexposed.com/news/example-story"
    assert document.member_ids == ["m-G000000"]
    assert document.metadata["memberSlugs"] == ["josh-gottheimer"]


def test_build_dossier_search_document_includes_findings() -> None:
    row = {
        "member_id": "m-F000459",
        "title": "Investigation Dossier: Example Member",
        "slug": "example-member-2026",
        "summary": "Summary text.",
        "severity": "high",
        "verification_status": "unverified",
        "generated_at": "2026-03-08T12:20:19.767Z",
        "reviewed_at": None,
        "updated_at": "2026-03-08T12:20:19.767Z",
        "executive_summary": "Executive summary text.",
        "methodology": "Methodology text.",
        "finding_count": 2,
        "findings": [
            {
                "category": "trade_conflict",
                "title": "High-conflict trade cluster",
                "narrative": "Narrative text.",
            }
        ],
    }

    document = build_dossier_search_document(
        row,
        base_url="https://www.capitolexposed.com",
    )
    assert document.source == "capitol-exposed"
    assert document.source_url == "https://www.capitolexposed.com/dossier/example-member-2026"
    assert document.member_ids == ["m-F000459"]
    assert "Narrative text." in document.content
    assert "trade_conflict" in document.tags


def test_build_member_search_document_includes_committees_and_tickers() -> None:
    row = {
        "id": "m-M001111",
        "slug": "patty-murray",
        "name": "Patty Murray",
        "party": "D",
        "state": "WA",
        "district": "",
        "chamber": "senate",
        "office": "154 Russell Senate Office Building",
        "website": "https://www.murray.senate.gov",
        "twitter_handle": "PattyMurray",
        "committees": [
            {"id": "cmt-APPROPS", "name": "Senate Appropriations Committee", "role": "Chair"},
        ],
        "top_tickers": [
            {"ticker": "AMGN", "tradeCount": 4},
            {"ticker": "MRK", "tradeCount": 4},
        ],
    }

    document = build_member_search_document(
        row,
        base_url="https://www.capitolexposed.com",
    )
    assert document.source_document_id == "capitol-member-patty-murray"
    assert document.member_ids == ["m-M001111"]
    assert document.committee_ids == ["cmt-APPROPS"]
    assert document.asset_tickers == ["AMGN", "MRK"]
    assert "Senate Appropriations Committee" in document.content


def test_build_committee_search_document_includes_members() -> None:
    row = {
        "id": "cmt-HSPW",
        "name": "House Committee on Transportation and Infrastructure",
        "chamber": "house",
        "code": "HSPW",
        "url": "https://transportation.house.gov/",
        "jurisdiction_industries": ["transportation", "infrastructure"],
        "members": [
            {"id": "m-C000000", "name": "Sample Chair", "party": "R", "role": "Chair"},
            {"id": "m-D000000", "name": "Sample Ranking", "party": "D", "role": "Ranking Member"},
        ],
    }

    document = build_committee_search_document(
        row,
        base_url="https://www.capitolexposed.com",
    )
    assert document.source_document_id == "capitol-committee-cmt-HSPW"
    assert document.committee_ids == ["cmt-HSPW"]
    assert document.member_ids == ["m-C000000", "m-D000000"]
    assert "transportation" in document.tags


def test_build_bill_search_document_includes_sponsor_and_subjects() -> None:
    row = {
        "id": "hr-119-390",
        "bill_type": "hr",
        "number": 390,
        "title": "ACERO Act",
        "short_title": "",
        "subjects": ["Science, Technology, Communications"],
        "sponsor_id": "m-F000480",
        "sponsor_name": "Vince Fong",
        "sponsor_slug": "vince-fong",
        "status": "referred",
        "introduced_date": "2025-01-14",
        "last_action_date": "2026-02-24",
        "industries": ["technology"],
        "committees": [],
    }

    document = build_bill_search_document(
        row,
        base_url="https://www.capitolexposed.com",
    )
    assert document.source_document_id == "capitol-bill-hr-119-390"
    assert document.bill_ids == ["hr-119-390"]
    assert document.member_ids == ["m-F000480"]
    assert "technology" in document.tags
    assert "Sponsor: Vince Fong" in document.content


def test_build_alert_search_document_includes_evidence_and_tickers() -> None:
    row = {
        "id": "alert-123",
        "alert_type": "committee_trade",
        "severity": "critical",
        "title": "ORCL purchase days before related committee vote",
        "summary": "An Oracle trade landed inside a committee vote window.",
        "member_id": "m-K000398",
        "member_name": "Thomas H. Kean, Jr.",
        "member_slug": "thomas-h-kean-jr",
        "bill_ids": ["hr-119-390"],
        "trade_tickers": ["ORCL"],
        "bill_titles": ["ACERO Act"],
        "confidence": 0.83,
        "status": "active",
        "evidence": [
            {"description": "Purchased ORCL two days before the vote."},
            {"description": "Member serves on the relevant committee."},
        ],
        "updated_at": "2026-03-09T02:00:00Z",
    }

    document = build_alert_search_document(
        row,
        base_url="https://www.capitolexposed.com",
    )
    assert document.source_document_id == "capitol-alert-alert-123"
    assert document.asset_tickers == ["ORCL"]
    assert document.bill_ids == ["hr-119-390"]
    assert "Purchased ORCL two days before the vote." in document.content
