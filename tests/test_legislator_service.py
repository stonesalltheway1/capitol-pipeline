from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from capitol_pipeline.config import Settings
from capitol_pipeline.registries import legislator_service as ls


ENTRIES = [
    {
        "id": {"bioguide": "P000595"},
        "name": {"first": "Gary", "middle": "C.", "last": "Peters", "official_full": "Gary C. Peters"},
        "terms": [
            {"type": "rep", "start": "2013-01-03", "end": "2015-01-03", "state": "MI"},
            {"type": "sen", "start": "2015-01-06", "end": "2027-01-03", "state": "MI"},
        ],
    },
    {
        # Left long before the STOCK Act: not kept.
        "id": {"bioguide": "X000001"},
        "name": {"first": "Old", "last": "Timer"},
        "terms": [{"type": "rep", "start": "1990-01-03", "end": "1995-01-03", "state": "KS"}],
    },
]


def test_parse_keeps_every_term_and_name_variant() -> None:
    service = ls.parse_legislator_entries(ENTRIES)
    assert set(service) == {"P000595"}
    peters = service["P000595"]
    assert [term.chamber for term in peters.terms] == ["house", "senate"]
    assert peters.terms[0].start == date(2013, 1, 3)
    # Official name first; first+middle+last is the same string and is not repeated.
    assert peters.names == ("Gary C. Peters", "Gary Peters")
    assert peters.state == "MI"


def test_round_trip_through_json() -> None:
    peters = ls.parse_legislator_entries(ENTRIES)["P000595"]
    assert ls.LegislatorService.from_json(peters.to_json()) == peters


def _settings(tmp_path) -> Settings:
    return Settings(cache_dir=tmp_path)


def test_a_fresh_cache_is_used_without_fetching(tmp_path, monkeypatch) -> None:
    service = ls.parse_legislator_entries(ENTRIES)
    ls.write_service_cache(ls.service_cache_path(_settings(tmp_path)), service)

    def boom(_settings: Settings) -> dict[str, ls.LegislatorService]:
        raise AssertionError("must not fetch while the cache is fresh")

    monkeypatch.setattr(ls, "fetch_legislator_service", boom)
    assert ls.load_legislator_service(_settings(tmp_path)) == service


def test_a_failed_refresh_falls_back_to_the_stale_cache(tmp_path, monkeypatch) -> None:
    service = ls.parse_legislator_entries(ENTRIES)
    ls.write_service_cache(ls.service_cache_path(_settings(tmp_path)), service)

    def offline(_settings: Settings) -> dict[str, ls.LegislatorService]:
        raise RuntimeError("network down")

    monkeypatch.setattr(ls, "fetch_legislator_service", offline)
    later = datetime.now(timezone.utc) + timedelta(days=30)
    assert ls.load_legislator_service(_settings(tmp_path), now=later) == service


def test_no_cache_and_no_network_returns_none(tmp_path, monkeypatch) -> None:
    def offline(_settings: Settings) -> dict[str, ls.LegislatorService]:
        raise RuntimeError("network down")

    monkeypatch.setattr(ls, "fetch_legislator_service", offline)
    assert ls.load_legislator_service(_settings(tmp_path)) is None


def test_a_stale_cache_is_refreshed_and_rewritten(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    ls.write_service_cache(ls.service_cache_path(settings), {})
    fresh = ls.parse_legislator_entries(ENTRIES)
    monkeypatch.setattr(ls, "fetch_legislator_service", lambda _settings: fresh)

    assert ls.load_legislator_service(settings) == fresh
    cached = ls.read_service_cache(ls.service_cache_path(settings))
    assert cached is not None and cached[0] == fresh


@pytest.mark.parametrize("value", ["2026-07-21", "2026-07-21T13:00:00+00:00", date(2026, 7, 21)])
def test_parse_iso_date_accepts_dates_and_timestamps(value: object) -> None:
    assert ls.parse_iso_date(value) == date(2026, 7, 21)
