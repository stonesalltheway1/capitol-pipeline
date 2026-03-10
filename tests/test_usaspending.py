from capitol_pipeline.models.usaspending import UsaspendingCompanyMatchRecord
from capitol_pipeline.sources.usaspending import (
    build_company_search_queries,
    build_usaspending_award_records,
    score_recipient_profile_match,
)


def test_build_company_search_queries_expands_curated_aliases() -> None:
    queries = build_company_search_queries(
        raw_name="Johnson & Johnson",
        asset_description="Johnson & Johnson common stock",
        ticker="JNJ",
    )

    assert "Johnson & Johnson" in queries
    assert "JANSSEN" in queries
    assert "ETHICON" in queries


def test_score_recipient_profile_match_uses_parent_name() -> None:
    score = score_recipient_profile_match(
        query_name="Broadcom Inc.",
        row={"name": "AVAGO TECHNOLOGIES US INC"},
        profile={"parent_name": "BROADCOM INC.", "alternate_names": ["VMWARE LLC"]},
    )

    assert score >= 95


def test_build_usaspending_award_records_accepts_download_row_strings() -> None:
    match = UsaspendingCompanyMatchRecord(
        match_key="match-1",
        company_id="co-1",
        company_name="Oracle Corporation",
        ticker="ORCL",
        query_name="ORACLE AMERICA",
        canonical_recipient_id="recipient-1",
        recipient_name="ORACLE AMERICA, INC.",
        normalized_recipient_name="oracle america",
        recipient_code=None,
        uei=None,
        match_type="recipient_name",
        match_value="ORACLE AMERICA, INC.",
        total_amount=1250000.0,
        award_count=1,
        top_agencies=["Department of Defense"],
        source_url="https://www.usaspending.gov/recipient/recipient-1/latest",
        summary="Oracle federal spending profile",
        content="Oracle federal spending profile",
        metadata={},
    )

    awards = build_usaspending_award_records(
        match=match,
        awards=[
            {
                "Award ID": "FA1234-26-C-0001",
                "Award Amount": "1250000.50",
                "Action Date": "2026-02-11",
                "Awarding Agency": "Department of Defense",
                "awarding_agency_id": "9700",
                "award_type_code": "A",
                "prime_award_base_transaction_description": "Cloud software support",
                "contract_award_unique_key": "generated-award-key",
            }
        ],
    )

    assert len(awards) == 1
    assert awards[0].award_amount == 1250000.50
    assert awards[0].awarding_agency_id == 9700
    assert awards[0].generated_internal_id == "generated-award-key"
    assert awards[0].award_type == "A"
    assert awards[0].description == "Cloud software support"
