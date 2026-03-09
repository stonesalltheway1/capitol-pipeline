from capitol_pipeline.bridges.search_documents import build_offshore_match_search_document
from capitol_pipeline.models.congress import MemberMatch
from capitol_pipeline.sources.icij_offshore_leaks import (
    build_offshore_node_record,
    build_offshore_relationship_record,
    split_multi_value,
)


def test_split_multi_value_dedupes_semicolon_values() -> None:
    values = split_multi_value("USA; United States; USA")
    assert values == ["USA", "United States"]


def test_build_offshore_node_record_normalizes_core_fields() -> None:
    row = {
        "node_id": "12000001",
        "name": "Patty Murray",
        "countries": "United States",
        "country_codes": "USA",
        "sourceID": "Paradise Papers",
        "note": "Example note",
    }
    node = build_offshore_node_record("officer", row)
    assert node.node_key == "officer:12000001"
    assert node.source_dataset == "Paradise Papers"
    assert node.normalized_name == "patty murray"
    assert node.countries == ["United States"]


def test_build_offshore_relationship_record_builds_stable_key() -> None:
    row = {
        "node_id_start": "1001",
        "node_id_end": "2002",
        "rel_type": "officer_of",
        "link": "officer of",
        "sourceID": "Panama Papers",
    }
    relationship = build_offshore_relationship_record(row)
    assert relationship.start_node_id == "1001"
    assert relationship.end_node_id == "2002"
    assert relationship.source_dataset == "Panama Papers"
    assert relationship.relationship_key


def test_build_offshore_match_search_document_uses_member_and_dataset() -> None:
    node = build_offshore_node_record(
        "officer",
        {
            "node_id": "12000001",
            "name": "Patty Murray",
            "countries": "United States",
            "country_codes": "USA",
            "sourceID": "Paradise Papers",
            "note": "Example note",
        },
    )
    member = MemberMatch(
        id="m-M001111",
        name="Patty Murray",
        slug="patty-murray",
        party="D",
        state="WA",
    )

    document = build_offshore_match_search_document(node, member)
    assert document.source == "icij-offshore-leaks"
    assert document.category == "cross-reference"
    assert document.member_ids == ["m-M001111"]
    assert document.metadata["sourceDataset"] == "Paradise Papers"
