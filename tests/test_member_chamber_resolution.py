"""Chamber- and date-constrained member resolution for trade disclosures.

Every case here is a real failure or a real near-miss from production:

- Sen. James M. Inhofe's 2015-2020 Senate PTRs were attributed to Rep. John
  James (224 rows), on the name "James".
- Sen. Gary Peters' 2015-2020 Senate PTRs were attributed to Rep. Scott Peters
  (111 rows), and Gary Peters filed House PTRs himself until 2015.
- Alan Armstrong, appointed to the Senate on 2026-03-24 and missing from the
  members table, had 707 rows attributed to former Rep. Kelly Armstrong.
- A House PTR by Rep. Mike Collins (GA) was attributed to Sen. Susan Collins.
- Darline Graham took a Senate seat on 2026-07-14, is not in the members
  table, and Lindsey Graham left on 2026-07-11: inside the post-term grace
  window, a surname match would hand her filings to him.
"""

from __future__ import annotations

from datetime import date
import json

from capitol_pipeline.models.congress import MemberMatch
from capitol_pipeline.registries.legislator_service import (
    LegislatorService,
    ServiceTerm,
    parse_legislator_entries,
)
from capitol_pipeline.registries.members import (
    MemberRegistry,
    load_member_registry_from_json,
    normalize_member_lookup_value,
)
from capitol_pipeline.sources.house_clerk import parse_house_feed
from capitol_pipeline.sources.senate_efd import EfdReport, resolve_efd_filer


def _term(chamber: str, start: str, end: str, state: str) -> dict[str, object]:
    return {
        "type": "sen" if chamber == "senate" else "rep",
        "start": start,
        "end": end,
        "state": state,
    }


#: congress-legislators entries, trimmed to the fields the roster reads.
LEGISLATORS: list[dict[str, object]] = [
    {
        "id": {"bioguide": "I000024"},
        "name": {"first": "James", "middle": "M.", "last": "Inhofe", "nickname": "Jim",
                 "official_full": "James M. Inhofe"},
        "terms": [
            _term("house", "1987-01-06", "1994-11-15", "OK"),
            _term("senate", "1994-11-17", "2015-01-03", "OK"),
            _term("senate", "2015-01-06", "2021-01-03", "OK"),
            _term("senate", "2021-01-03", "2023-01-03", "OK"),
        ],
    },
    {
        "id": {"bioguide": "J000307"},
        "name": {"first": "John", "middle": "Edwards", "last": "James", "official_full": "John James"},
        "terms": [
            _term("house", "2023-01-03", "2025-01-03", "MI"),
            _term("house", "2025-01-03", "2027-01-03", "MI"),
        ],
    },
    {
        "id": {"bioguide": "P000595"},
        "name": {"first": "Gary", "middle": "C.", "last": "Peters", "official_full": "Gary C. Peters"},
        "terms": [
            _term("house", "2009-01-06", "2011-01-03", "MI"),
            _term("house", "2011-01-05", "2013-01-03", "MI"),
            _term("house", "2013-01-03", "2015-01-03", "MI"),
            _term("senate", "2015-01-06", "2021-01-03", "MI"),
            _term("senate", "2021-01-03", "2027-01-03", "MI"),
        ],
    },
    {
        "id": {"bioguide": "P000608"},
        "name": {"first": "Scott", "middle": "H.", "last": "Peters", "official_full": "Scott H. Peters"},
        "terms": [
            _term("house", "2013-01-03", "2015-01-03", "CA"),
            _term("house", "2015-01-06", "2027-01-03", "CA"),
        ],
    },
    {
        "id": {"bioguide": "C001035"},
        "name": {"first": "Susan", "middle": "M.", "last": "Collins", "official_full": "Susan M. Collins"},
        "terms": [_term("senate", "1997-01-07", "2027-01-03", "ME")],
    },
    {
        "id": {"bioguide": "C001129"},
        "name": {"first": "Mike", "middle": "Allen", "last": "Collins", "suffix": "Jr.",
                 "official_full": "Mike Collins"},
        "terms": [
            _term("house", "2023-01-03", "2025-01-03", "GA"),
            _term("house", "2025-01-03", "2027-01-03", "GA"),
        ],
    },
    {
        "id": {"bioguide": "C001093"},
        "name": {"first": "Doug", "last": "Collins", "official_full": "Doug Collins"},
        "terms": [_term("house", "2013-01-03", "2021-01-03", "GA")],
    },
    {
        "id": {"bioguide": "A000377"},
        "name": {"first": "Kelly", "last": "Armstrong", "official_full": "Kelly Armstrong"},
        "terms": [_term("house", "2019-01-03", "2024-12-14", "ND")],
    },
    {
        # In the roster, deliberately absent from the members rows below.
        "id": {"bioguide": "A000383"},
        "name": {"first": "Alan", "last": "Armstrong", "official_full": "Alan Armstrong"},
        "terms": [_term("senate", "2026-03-24", "2027-01-03", "OK")],
    },
    {
        "id": {"bioguide": "G000359"},
        "name": {"first": "Lindsey", "middle": "O.", "last": "Graham", "official_full": "Lindsey Graham"},
        "terms": [
            _term("house", "1995-01-04", "2003-01-03", "SC"),
            _term("senate", "2003-01-07", "2026-07-11", "SC"),
        ],
    },
    {
        # In the roster, deliberately absent from the members rows below.
        "id": {"bioguide": "G000608"},
        "name": {"first": "Darline", "last": "Graham", "official_full": "Darline Graham"},
        "terms": [_term("senate", "2026-07-14", "2027-01-03", "SC")],
    },
    {
        "id": {"bioguide": "C001098"},
        "name": {"first": "Ted", "last": "Cruz", "official_full": "Ted Cruz"},
        "terms": [_term("senate", "2013-01-03", "2031-01-03", "TX")],
    },
]


def _row(bioguide: str, name: str, chamber: str, state: str, start: str, end: str,
         *, in_office: bool = True, district: str | None = None) -> dict[str, object]:
    slug = name.lower().replace(".", "").replace(",", "").replace(" ", "-")
    return {
        "id": f"m-{bioguide}",
        "bioguide_id": bioguide,
        "name": name,
        "slug": slug,
        "party": "X",
        "state": state,
        "district": district,
        "chamber": chamber,
        "in_office": in_office,
        "term_start": start,
        "term_end": end,
    }


#: The members table as production had it: current chamber and latest term
#: only, and no row for Alan Armstrong or Darline Graham.
MEMBER_ROWS: list[dict[str, object]] = [
    _row("I000024", "James M. Inhofe", "senate", "OK", "2021-01-03", "2023-01-03", in_office=False),
    _row("J000307", "John James", "house", "MI", "2025-01-03", "2027-01-03", district="10"),
    _row("P000595", "Gary C. Peters", "senate", "MI", "2021-01-03", "2027-01-03"),
    _row("P000608", "Scott H. Peters", "house", "CA", "2025-01-03", "2027-01-03", district="50"),
    _row("C001035", "Susan M. Collins", "senate", "ME", "2021-01-03", "2027-01-03"),
    _row("C001129", "Mike Collins", "house", "GA", "2025-01-03", "2027-01-03", district="10"),
    _row("C001093", "Doug Collins", "house", "GA", "2019-01-03", "2021-01-03", in_office=False, district="9"),
    _row("A000377", "Kelly Armstrong", "house", "ND", "2023-01-03", "2024-12-14", in_office=False, district="0"),
    _row("G000359", "Lindsey Graham", "senate", "SC", "2021-01-03", "2026-07-11", in_office=False),
    _row("C001098", "Ted Cruz", "senate", "TX", "2025-01-03", "2031-01-03"),
]


def _registry(*, with_roster: bool = True) -> MemberRegistry:
    service = parse_legislator_entries(LEGISLATORS) if with_roster else None
    return MemberRegistry.from_rows(MEMBER_ROWS, service=service)


def _member_id(match: MemberMatch | None) -> str | None:
    return match.id if match is not None else None


# ── John James / James M. Inhofe ────────────────────────────────────────────


def test_a_senate_filing_by_inhofe_resolves_to_inhofe_not_john_james() -> None:
    registry = _registry()
    match = registry.resolve(
        name="James M Inhofe", first_name="James M", last_name="Inhofe",
        chamber="senate", as_of="2016-02-01",
    )
    assert _member_id(match) == "m-I000024"


def test_a_senate_filing_never_resolves_to_a_house_member_who_was_never_a_senator() -> None:
    for registry in (_registry(), _registry(with_roster=False)):
        # The surname-only key that used to land on John James.
        assert registry.resolve(name="James", chamber="senate", as_of="2016-02-01") is None
        assert registry.resolve(last_name="James", chamber="senate", as_of="2016-02-01") is None
        assert registry.resolve(name="John James", chamber="senate", as_of="2026-02-01") is None


def test_the_unconstrained_lookup_is_unchanged() -> None:
    # FARA, offshore and search-document linking still use the plain matcher.
    registry = _registry()
    assert _member_id(registry.resolve(name="John James")) == "m-J000307"
    assert _member_id(registry.resolve(name="Gary C. Peters")) == "m-P000595"


# ── Gary Peters / Scott Peters ──────────────────────────────────────────────


def test_peters_senate_filings_go_to_the_senator() -> None:
    registry = _registry()
    for name in ("Gary C Peters", "Peters"):
        assert _member_id(registry.resolve(name=name, chamber="senate", as_of="2016-01-28")) == "m-P000595"
    assert registry.resolve(name="Scott H Peters", chamber="senate", as_of="2016-01-28") is None


def test_peters_house_filings_split_by_state_and_by_year() -> None:
    registry = _registry()
    # Gary Peters filed House PTRs until he moved to the Senate in 2015.
    assert _member_id(registry.resolve_feed_member("Gary", "Peters", "MI", "2014-05-15")) == "m-P000595"
    # After that, a House "Peters" can only be Scott.
    assert _member_id(registry.resolve_feed_member("Scott", "Peters", "CA", "2016-05-15")) == "m-P000608"
    assert registry.resolve_feed_member("Gary", "Peters", "MI", "2017-05-15") is None
    # In 2014 both sat in the House: a bare surname with no state is ambiguous.
    assert registry.resolve(last_name="Peters", chamber="house", as_of="2014-05-15") is None
    assert _member_id(registry.resolve(last_name="Peters", chamber="house", as_of="2018-05-15")) == "m-P000608"


def test_the_members_row_alone_cannot_vouch_for_a_former_chamber() -> None:
    # Without the roster, Gary Peters' row only knows his current Senate term,
    # so his 2014 House filing is left for review rather than guessed at.
    registry = _registry(with_roster=False)
    assert registry.resolve_feed_member("Gary", "Peters", "MI", "2014-05-15") is None
    assert _member_id(registry.resolve(name="Gary C Peters", chamber="senate", as_of="2022-03-01")) == "m-P000595"


# ── Same surname across chambers: the Collinses ─────────────────────────────


def test_a_house_filing_by_mike_collins_does_not_go_to_senator_collins() -> None:
    registry = _registry()
    # House Clerk feed row for doc 20033840, as filed.
    match = registry.resolve_feed_member("Michael A.", "Collins", "GA", "2026-01-20")
    assert _member_id(match) == "m-C001129"
    # Doug Collins (GA) left the House in 2021, so 2026 is not ambiguous...
    assert _member_id(registry.resolve_feed_member(None, "Collins", "GA", "2026-01-20")) == "m-C001129"
    # ...but in 2019 a GA House "Collins" was Doug.
    assert _member_id(registry.resolve_feed_member(None, "Collins", "GA", "2019-06-01")) == "m-C001093"
    # A stated state is required, not preferred: no House Collins sits for ME.
    assert registry.resolve_feed_member("Susan", "Collins", "ME", "2026-01-20") is None


def test_a_senate_collins_is_the_senator() -> None:
    registry = _registry()
    assert _member_id(registry.resolve(name="Susan M Collins", chamber="senate", as_of="2026-01-20")) == "m-C001035"
    assert _member_id(registry.resolve(name="Collins", chamber="senate", as_of="2026-01-20")) == "m-C001035"


# ── A filer missing from the members table ──────────────────────────────────


def test_a_senator_missing_from_members_is_unresolved_not_a_same_surname_member() -> None:
    report = EfdReport(
        report_id="fda235b3-bad7-4637-8fa1-053f354d929c",
        kind="electronic",
        url="https://efdsearch.senate.gov/search/view/ptr/fda235b3-bad7-4637-8fa1-053f354d929c/",
        first_name="Alan",
        last_name="Armstrong",
        submitted_date="2026-07-21",
    )
    for registry in (_registry(), _registry(with_roster=False)):
        assert resolve_efd_filer(report, registry) is None
        assert registry.resolve(last_name="Armstrong", chamber="senate", as_of="2026-07-21") is None


def test_a_successor_missing_from_members_does_not_inherit_the_predecessor() -> None:
    registry = _registry()
    # Lindsey Graham left on 2026-07-11; a late PTR of his is still his.
    assert _member_id(registry.resolve(name="Lindsey O Graham", chamber="senate", as_of="2026-08-15")) == "m-G000359"
    # Darline Graham sat from 2026-07-14 and has no members row: her filing is
    # known to be hers, so it is not handed to the one Graham we do carry.
    assert registry.resolve(name="Darline Graham", chamber="senate", as_of="2026-08-15") is None
    # A bare surname in that window matches both of them: ambiguous.
    assert registry.resolve(name="Graham", chamber="senate", as_of="2026-08-15") is None
    # Well past the grace window, a filing is not Lindsey Graham's either.
    assert registry.resolve(name="Lindsey Graham", chamber="senate", as_of="2028-01-15") is None


# ── Names the eFD uses that the members table does not ──────────────────────


def test_roster_name_variants_resolve_formal_names() -> None:
    registry = _registry()
    assert _member_id(registry.resolve(name="Rafael E Cruz", first_name="Rafael E", last_name="Cruz",
                                       chamber="senate", as_of="2025-11-12")) == "m-C001098"
    assert _member_id(registry.resolve(name="James M Inhofe", chamber="senate", as_of="2020-12-01")) == "m-I000024"


def test_a_bioguide_for_the_wrong_chamber_is_refused() -> None:
    registry = _registry()
    assert registry.resolve(bioguide_id="J000307", chamber="senate", as_of="2016-01-01") is None
    assert _member_id(registry.resolve(bioguide_id="I000024", chamber="senate", as_of="2016-01-01")) == "m-I000024"


# ── Plumbing ────────────────────────────────────────────────────────────────


def test_the_house_feed_passes_the_filing_date_to_the_resolver() -> None:
    seen: list[tuple[str, str, str | None, str | None]] = []

    def resolver(first: str, last: str, state: str | None, filing_date: str | None) -> MemberMatch | None:
        seen.append((first, last, state, filing_date))
        return None

    xml = """
    <FinancialDisclosure><Member>
      <DocID>20033840</DocID><First>Michael A.</First><Last>Collins</Last>
      <StateDst>GA10</StateDst><FilingDate>1/20/2026</FilingDate><FilingType>P</FilingType>
    </Member></FinancialDisclosure>
    """.strip()
    parse_house_feed(xml, year=2026, resolver=resolver)
    assert seen == [("Michael A.", "Collins", "GA", "2026-01-20")]

    registry = _registry()
    stubs = parse_house_feed(xml, year=2026, resolver=registry.resolve_feed_member)
    assert stubs[0].member.id == "m-C001129"


def test_the_json_cache_round_trips_the_roster(tmp_path) -> None:
    registry = _registry()
    path = registry.save_json(tmp_path / "members-registry.json")
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert {"members", "legislators"} <= set(payload)

    reloaded = load_member_registry_from_json(path)
    assert reloaded.resolve(name="Darline Graham", chamber="senate", as_of="2026-08-15") is None
    assert _member_id(reloaded.resolve_feed_member("Gary", "Peters", "MI", "2014-05-15")) == "m-P000595"

    # The bare list the older cache wrote still loads.
    legacy = tmp_path / "legacy.json"
    legacy.write_text(json.dumps(MEMBER_ROWS), encoding="utf-8")
    assert _member_id(load_member_registry_from_json(legacy).resolve(name="John James")) == "m-J000307"


def test_typographic_apostrophes_normalize_like_plain_ones() -> None:
    assert normalize_member_lookup_value("Beto O’Rourke") == normalize_member_lookup_value("Beto O'Rourke")


def test_records_built_directly_can_carry_terms() -> None:
    registry = MemberRegistry.from_records(
        [MemberMatch(id="m-X1", bioguide_id="X000001", name="Pat Example", state="KS")],
        service={
            "X000001": LegislatorService(
                bioguide_id="X000001",
                terms=(ServiceTerm("senate", date(2011, 1, 5), date(2021, 1, 3), "KS"),),
                names=("Pat Example", "Patrick Example"),
                state="KS",
            )
        },
    )
    assert _member_id(registry.resolve(name="Patrick Example", chamber="senate", as_of="2020-06-01")) == "m-X1"
    assert registry.resolve(name="Patrick Example", chamber="house", as_of="2020-06-01") is None
