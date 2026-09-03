"""Tests for the historical-members loader and House stub re-resolution."""

from __future__ import annotations

from datetime import date
import json
from pathlib import Path

import pytest

from capitol_pipeline.members_historical import (
    CORE_MEMBER_COLUMNS,
    CROSSWALK_MEMBER_COLUMNS,
    DEFAULT_SINCE,
    RESOLVER_TAG,
    assign_unique_slugs,
    build_fill_null_sql,
    build_insert_sql,
    build_member_row,
    build_member_rows,
    build_stub_metadata_updates,
    iter_recent_legislators,
    map_party,
    resolve_stub_member,
    slugify,
    writable_member_columns,
)
from capitol_pipeline.models.congress import MemberMatch
from capitol_pipeline.registries.members import MemberRegistry


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "legislators"


def load_fixture(name: str) -> list[dict[str, object]]:
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def historical() -> list[dict[str, object]]:
    return load_fixture("legislators-historical-sample.json")


@pytest.fixture(scope="module")
def current() -> list[dict[str, object]]:
    return load_fixture("legislators-current-sample.json")


def by_bioguide(entries: list[dict[str, object]], bioguide: str) -> dict[str, object]:
    for entry in entries:
        if entry["id"]["bioguide"] == bioguide:  # type: ignore[index]
            return entry
    raise KeyError(bioguide)


# ── Row shaping ─────────────────────────────────────────────────────────────


def test_slugify_mirrors_seed_script() -> None:
    assert slugify("Doug Lamborn") == "doug-lamborn"
    assert slugify("Henry C. \"Hank\" Johnson, Jr.") == "henry-c-hank-johnson-jr"
    assert slugify("Angus S. King, Jr.") == "angus-s-king-jr"
    # The seed script strips apostrophes but keeps the diacritic fallout.
    assert slugify("Ben Ray Luján") == "ben-ray-luj-n"
    assert slugify("Beto O'Rourke") == "beto-orourke"


def test_map_party_collapses_to_three_codes() -> None:
    assert map_party("Democrat") == "D"
    assert map_party("Republican") == "R"
    assert map_party("Independent") == "I"
    assert map_party("Libertarian") == "I"
    assert map_party(None) == "I"


def test_iter_recent_legislators_filters_on_last_term_end(historical: list[dict[str, object]]) -> None:
    kept = {entry["id"]["bioguide"] for entry in iter_recent_legislators(historical)}  # type: ignore[index]
    assert "L000564" in kept  # Lamborn, left 2025
    assert "G000554" in kept  # Giffords, resigned 2012-01-25: on the boundary year
    assert "D000549" not in kept  # Jennifer Dunn, left 2005

    strict = {
        entry["id"]["bioguide"]  # type: ignore[index]
        for entry in iter_recent_legislators(historical, since=date(2024, 1, 1))
    }
    assert "L000564" in strict
    assert "G000554" not in strict
    assert "R000572" not in strict  # Mike Rogers (MI), left 2015


def test_build_member_row_house_member_matches_seed_shape(historical: list[dict[str, object]]) -> None:
    row = build_member_row(by_bioguide(historical, "L000564"))
    assert row is not None
    assert row.id == "m-L000564"
    assert row.bioguide_id == "L000564"
    assert row.slug == "doug-lamborn"
    assert row.name == "Doug Lamborn"
    assert (row.first_name, row.last_name) == ("Doug", "Lamborn")
    assert row.party == "R"
    assert row.state == "CO"
    assert row.district == "5"
    assert row.chamber == "house"
    assert row.in_office is False
    assert row.term_start == date(2023, 1, 3)
    assert row.term_end == date(2025, 1, 3)
    assert row.date_of_birth == date(1954, 5, 24)
    assert row.gender == "M"
    assert row.image_url == "https://bioguide.congress.gov/bioguide/photo/L/L000564.jpg"
    assert row.website == "https://lamborn.house.gov"
    assert row.phone == "202-225-4422"
    assert row.office == "2371 Rayburn House Office Building"
    assert row.fec_ids == ["H6CO05159"]
    assert row.opensecrets_id == "N00028133"
    assert row.wikipedia_slug == "Doug_Lamborn"
    assert row.govtrack_id and row.wikidata_qid


def test_build_member_row_senator_has_no_district(historical: list[dict[str, object]]) -> None:
    row = build_member_row(by_bioguide(historical, "M000639"))
    assert row is not None
    assert row.chamber == "senate"
    assert row.district is None
    assert row.state == "NJ"
    assert row.party == "D"
    assert row.term_end == date(2024, 8, 20)
    assert row.fec_ids == ["H2NJ13075", "S6NJ00289"]


def test_build_member_row_falls_back_to_first_last_without_official_full(
    historical: list[dict[str, object]],
) -> None:
    row = build_member_row(by_bioguide(historical, "G000554"))
    assert row is not None
    assert row.name == "Gabrielle Giffords"
    assert row.slug == "gabrielle-giffords"
    assert row.district == "8"


def test_build_member_row_can_mark_current_members_in_office(current: list[dict[str, object]]) -> None:
    row = build_member_row(by_bioguide(current, "D000628"), in_office=True)
    assert row is not None
    assert row.in_office is True
    assert row.name == "Neal P. Dunn"


def test_build_member_row_rejects_entries_without_bioguide_or_terms() -> None:
    assert build_member_row({"id": {}, "name": {"first": "A", "last": "B"}, "terms": [{}]}) is None
    assert build_member_row({"id": {"bioguide": "X000001"}, "name": {}, "terms": []}) is None


def test_build_member_rows_applies_since_filter(historical: list[dict[str, object]]) -> None:
    rows = build_member_rows(historical)
    ids = {row.bioguide_id for row in rows}
    assert "D000549" not in ids
    assert len(rows) == len(historical) - 1
    assert all(row.in_office is False for row in rows)
    assert DEFAULT_SINCE == date(2012, 1, 1)


# ── Slug uniqueness ─────────────────────────────────────────────────────────


def test_assign_unique_slugs_suffixes_collisions_with_existing_rows(
    historical: list[dict[str, object]],
) -> None:
    rows = build_member_rows(historical)
    taken = {"mike-rogers", "robert-menendez", "dianne-feinstein"}
    assign_unique_slugs(rows, taken)
    slugs = {row.bioguide_id: row.slug for row in rows}
    assert slugs["R000572"] == "mike-rogers-mi-8"  # House: state-district
    assert slugs["M000639"] == "robert-menendez-nj"  # Senate: state only
    assert slugs["F000062"] == "dianne-feinstein-ca"
    assert slugs["L000564"] == "doug-lamborn"  # untouched
    assert len(set(slugs.values())) == len(slugs)


def test_assign_unique_slugs_handles_duplicates_within_batch_and_same_seat() -> None:
    first = build_member_row(
        {
            "id": {"bioguide": "A000001"},
            "name": {"first": "Sam", "last": "Example", "official_full": "Sam Example"},
            "terms": [{"type": "rep", "state": "TX", "district": 3, "start": "2019-01-03", "end": "2021-01-03"}],
        }
    )
    second = build_member_row(
        {
            "id": {"bioguide": "A000002"},
            "name": {"first": "Sam", "last": "Example", "official_full": "Sam Example"},
            "terms": [{"type": "rep", "state": "TX", "district": 3, "start": "2021-01-03", "end": "2023-01-03"}],
        }
    )
    assert first and second
    assign_unique_slugs([first, second], set())
    assert first.slug == "sam-example-tx-3"
    assert second.slug == "sam-example-a000002"


# ── SQL shape ───────────────────────────────────────────────────────────────


def test_writable_member_columns_requires_core_and_adds_present_crosswalk() -> None:
    table = set(CORE_MEMBER_COLUMNS) | {"wikidata_qid", "govtrack_id", "unrelated"}
    columns = writable_member_columns(table)
    assert columns[: len(CORE_MEMBER_COLUMNS)] == list(CORE_MEMBER_COLUMNS)
    assert columns[len(CORE_MEMBER_COLUMNS):] == ["wikidata_qid", "govtrack_id"]
    assert "unrelated" not in columns

    with pytest.raises(RuntimeError, match="term_end"):
        writable_member_columns(set(CORE_MEMBER_COLUMNS) - {"term_end"})


def test_insert_sql_never_overwrites_and_fill_sql_only_touches_nulls() -> None:
    insert_sql = build_insert_sql(["id", "bioguide_id", "slug"])
    assert insert_sql.startswith("INSERT INTO members (id, bioguide_id, slug) VALUES (%(id)s, %(bioguide_id)s, %(slug)s)")
    assert insert_sql.endswith("ON CONFLICT DO NOTHING")

    fill_sql = build_fill_null_sql(["term_end", "gender"])
    assert "term_end = COALESCE(members.term_end, %(term_end)s)" in fill_sql
    assert "gender = COALESCE(members.gender, %(gender)s)" in fill_sql
    assert "WHERE bioguide_id = %(bioguide_id)s" in fill_sql
    for identity_column in ("name", "slug", "party", "state", "chamber", "in_office"):
        assert f" {identity_column} =" not in fill_sql


# ── Stub re-resolution ──────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def registry(historical: list[dict[str, object]], current: list[dict[str, object]]) -> MemberRegistry:
    """Registry as it looks after the loader ran: current rows plus historical inserts."""

    rows = [row.as_registry_row() for row in build_member_rows(historical)]
    rows += [row.as_registry_row() for row in build_member_rows(current, in_office=True)]
    return MemberRegistry.from_rows(rows)


@pytest.mark.parametrize(
    ("metadata", "expected_id"),
    [
        (
            {"firstName": "Marjorie Taylor", "lastName": "Greene", "memberName": "Marjorie Taylor Greene", "state": "GA"},
            "m-G000596",
        ),
        (
            {"firstName": "Marjorie Taylor Mrs", "lastName": "Greene", "memberName": "Marjorie Taylor Mrs Greene", "state": "GA"},
            "m-G000596",
        ),
        (
            {"firstName": "Neal Patrick", "lastName": "Dunn, MD, FACS", "memberName": "Neal Patrick Dunn, MD, FACS", "state": "FL"},
            "m-D000628",
        ),
        (
            {"firstName": "Gerald E.", "lastName": "Connolly", "memberName": "Gerald E. Connolly", "state": "VA"},
            "m-C001078",
        ),
        (
            {"firstName": "Kathy", "lastName": "Manning", "memberName": "Kathy Manning", "state": "NC"},
            "m-M001135",
        ),
        (
            {"firstName": "Michael C.", "lastName": "Burgess", "memberName": "Michael C. Burgess", "state": "TX"},
            "m-B001248",
        ),
        (
            {"firstName": "Brandon McDonald", "lastName": "Williams", "memberName": "Brandon McDonald Williams", "state": "NY"},
            "m-W000828",
        ),
        (
            {"firstName": "Earl", "lastName": "Blumenauer", "memberName": "Earl Blumenauer", "rawStateDistrict": "OR03"},
            "m-B000574",
        ),
        # memberName only, no first/last split.
        ({"memberName": "Doug Lamborn", "state": "CO"}, "m-L000564"),
    ],
)
def test_resolve_stub_member_handles_feed_name_forms(
    registry: MemberRegistry, metadata: dict[str, object], expected_id: str
) -> None:
    match = resolve_stub_member(registry, metadata)
    assert match is not None
    assert match.id == expected_id


def test_resolve_stub_member_uses_state_to_split_same_name_members(registry: MemberRegistry) -> None:
    michigan = resolve_stub_member(registry, {"firstName": "Mike", "lastName": "Rogers", "state": "MI"})
    alabama = resolve_stub_member(registry, {"firstName": "Mike", "lastName": "Rogers", "state": "AL"})
    assert michigan is not None and michigan.id == "m-R000572"
    assert alabama is not None and alabama.id == "m-R000575"
    # Without a state the name is ambiguous and must not be guessed.
    assert resolve_stub_member(registry, {"firstName": "Mike", "lastName": "Rogers"}) is None


def test_resolve_stub_member_returns_none_for_non_members_and_blank_names(registry: MemberRegistry) -> None:
    assert resolve_stub_member(registry, {"firstName": "Ada Norah", "lastName": "Henriquez", "state": "PR"}) is None
    assert resolve_stub_member(registry, {}) is None
    assert resolve_stub_member(registry, {"memberName": "", "firstName": "", "lastName": ""}) is None


def test_build_stub_metadata_updates_writes_member_keys_and_keeps_feed_name() -> None:
    match = MemberMatch(
        id="m-G000596",
        bioguide_id="G000596",
        name="Marjorie Taylor Greene",
        slug="marjorie-taylor-greene",
        party="R",
        state="GA",
        district="14",
    )
    metadata = {"memberName": "Marjorie Taylor Mrs Greene", "state": "GA", "district": "14", "retryAfter": "2026-09-01T00:00:00Z"}
    updates = build_stub_metadata_updates(match, metadata, resolved_at="2026-09-02T00:00:00+00:00")
    assert updates["memberId"] == "m-G000596"
    assert updates["memberSlug"] == "marjorie-taylor-greene"
    assert updates["memberName"] == "Marjorie Taylor Greene"
    assert updates["feedMemberName"] == "Marjorie Taylor Mrs Greene"
    assert (updates["party"], updates["state"], updates["district"]) == ("R", "GA", "14")
    assert updates["memberResolvedBy"] == RESOLVER_TAG
    assert updates["memberResolvedAt"] == "2026-09-02T00:00:00+00:00"
    assert "retryAfter" in updates and updates["retryAfter"] is None


def test_build_stub_metadata_updates_omits_unknown_fields_and_respects_no_requeue() -> None:
    match = MemberMatch(id="m-X000001", name="Some Member", slug="some-member")
    updates = build_stub_metadata_updates(match, {"memberName": "Some Member"}, requeue=False)
    assert "party" not in updates and "state" not in updates and "district" not in updates
    assert "feedMemberName" not in updates
    assert "retryAfter" not in updates
