"""Who served in which chamber, and when, from unitedstates/congress-legislators.

``members.chamber`` is the chamber a member sits in *now* (or last sat in), and
``members.term_start``/``term_end`` cover only the latest term. Neither can say
whether a person was a senator on the day a Senate filing was submitted, which
is the only question that matters when a name on a Senate PTR has to be tied to
a member. Without it the name matcher attached Sen. Jim Inhofe's 2015-2020
trades to Rep. John James (last name "James"), Sen. Gary Peters' to Rep. Scott
Peters, and a 2026 appointee's 707 rows to former Rep. Kelly Armstrong.

The congress-legislators dataset (CC0) carries every term of every legislator
with its chamber, state and dates, and every name variant (first, middle,
nickname, official full name). This module turns it into a small per-bioguide
record and caches that on disk, so a 15-minute timer reads a few hundred KB
instead of downloading 15 MB.

The full roster matters as much as the terms. A filer who is missing from the
``members`` table (a senator appointed after the table was last seeded) is
still in this roster, so a lookup can tell "this is someone we do not have"
apart from "this is the only member with that surname".
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import tempfile

import httpx

from capitol_pipeline.config import Settings

logger = logging.getLogger(__name__)

LEGISLATORS_BASE_URL = "https://unitedstates.github.io/congress-legislators/"
LEGISLATORS_CURRENT_URL = f"{LEGISLATORS_BASE_URL}legislators-current.json"
LEGISLATORS_HISTORICAL_URL = f"{LEGISLATORS_BASE_URL}legislators-historical.json"

#: People whose last term ended before this cannot have filed under the STOCK
#: Act (2012) or been confused with someone who did; keeping a few years of
#: margin costs nothing.
SERVICE_SINCE = date(2008, 1, 1)

SERVICE_CACHE_FILENAME = "legislator-service.json"
SERVICE_CACHE_MAX_AGE = timedelta(days=7)
SERVICE_CACHE_VERSION = 1

CHAMBERS = ("house", "senate")


@dataclass(frozen=True, slots=True)
class ServiceTerm:
    """One term in one chamber."""

    chamber: str
    start: date
    end: date
    state: str | None = None

    def covers(self, when: date, *, before: timedelta, after: timedelta) -> bool:
        return self.start - before <= when <= self.end + after


@dataclass(frozen=True, slots=True)
class LegislatorService:
    """Everything the resolver needs to know about one legislator."""

    bioguide_id: str
    terms: tuple[ServiceTerm, ...]
    #: Display names this person is known by, most formal first:
    #: official full name, first + middle + last, first + last, nickname + last.
    names: tuple[str, ...] = ()
    state: str | None = None

    def to_json(self) -> dict[str, object]:
        return {
            "bioguide": self.bioguide_id,
            "state": self.state,
            "names": list(self.names),
            "terms": [
                [term.chamber, term.start.isoformat(), term.end.isoformat(), term.state]
                for term in self.terms
            ],
        }

    @classmethod
    def from_json(cls, payload: Mapping[str, object]) -> "LegislatorService | None":
        bioguide = str(payload.get("bioguide") or "").strip().upper()
        if not bioguide:
            return None
        terms: list[ServiceTerm] = []
        raw_terms = payload.get("terms")
        if isinstance(raw_terms, list):
            for raw in raw_terms:
                if not isinstance(raw, (list, tuple)) or len(raw) < 3:
                    continue
                chamber = str(raw[0])
                start = parse_iso_date(raw[1])
                end = parse_iso_date(raw[2])
                if chamber not in CHAMBERS or start is None or end is None:
                    continue
                state = str(raw[3]).upper() if len(raw) > 3 and raw[3] else None
                terms.append(ServiceTerm(chamber=chamber, start=start, end=end, state=state))
        raw_names = payload.get("names")
        names = tuple(str(name) for name in raw_names if name) if isinstance(raw_names, list) else ()
        state = str(payload.get("state") or "").upper() or None
        return cls(bioguide_id=bioguide, terms=tuple(terms), names=names, state=state)


def parse_iso_date(value: object) -> date | None:
    """Parse ``YYYY-MM-DD`` (or a longer ISO timestamp, or a ``date``) to a date."""

    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _name_variants(name: Mapping[str, object]) -> tuple[str, ...]:
    first = str(name.get("first") or "").strip()
    middle = str(name.get("middle") or "").strip()
    last = str(name.get("last") or "").strip()
    nickname = str(name.get("nickname") or "").strip()
    official = str(name.get("official_full") or "").strip()

    variants: list[str] = []
    for candidate in (
        official,
        " ".join(part for part in (first, middle, last) if part),
        " ".join(part for part in (first, last) if part),
        " ".join(part for part in (nickname, last) if part) if nickname else "",
    ):
        if candidate and candidate not in variants:
            variants.append(candidate)
    return tuple(variants)


def parse_legislator_entries(
    entries: Iterable[Mapping[str, object]],
    *,
    since: date = SERVICE_SINCE,
) -> dict[str, LegislatorService]:
    """Turn congress-legislators entries into ``{bioguide: LegislatorService}``.

    Keeps everyone with at least one term ending on or after ``since``, and all
    of that person's terms (a senator's House years are what tell a 2013 House
    filing from a 2016 Senate one).
    """

    service: dict[str, LegislatorService] = {}
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        ids = entry.get("id")
        if not isinstance(ids, Mapping):
            continue
        bioguide = str(ids.get("bioguide") or "").strip().upper()
        if not bioguide:
            continue
        raw_terms = entry.get("terms")
        if not isinstance(raw_terms, list):
            continue
        terms: list[ServiceTerm] = []
        for raw in raw_terms:
            if not isinstance(raw, Mapping):
                continue
            term_type = raw.get("type")
            chamber = "senate" if term_type == "sen" else "house" if term_type == "rep" else None
            start = parse_iso_date(raw.get("start"))
            end = parse_iso_date(raw.get("end"))
            if chamber is None or start is None or end is None:
                continue
            state = str(raw.get("state") or "").strip().upper() or None
            terms.append(ServiceTerm(chamber=chamber, start=start, end=end, state=state))
        if not terms or max(term.end for term in terms) < since:
            continue
        terms.sort(key=lambda term: term.start)
        name = entry.get("name")
        names = _name_variants(name) if isinstance(name, Mapping) else ()
        service[bioguide] = LegislatorService(
            bioguide_id=bioguide,
            terms=tuple(terms),
            names=names,
            state=terms[-1].state,
        )
    return service


def service_cache_path(settings: Settings) -> Path:
    return Path(settings.cache_dir) / SERVICE_CACHE_FILENAME


def write_service_cache(path: Path, service: Mapping[str, LegislatorService]) -> None:
    """Write the cache atomically; several timers may load the registry at once."""

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": SERVICE_CACHE_VERSION,
        "fetchedAt": datetime.now(timezone.utc).isoformat(),
        "legislators": [record.to_json() for record in service.values()],
    }
    handle, tmp_name = tempfile.mkstemp(prefix=".legislator-service-", dir=str(path.parent))
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as tmp:
            json.dump(payload, tmp)
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def read_service_cache(path: Path) -> tuple[dict[str, LegislatorService], datetime | None] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict) or payload.get("version") != SERVICE_CACHE_VERSION:
        return None
    fetched_at: datetime | None = None
    raw_fetched = payload.get("fetchedAt")
    if isinstance(raw_fetched, str):
        try:
            fetched_at = datetime.fromisoformat(raw_fetched)
        except ValueError:
            fetched_at = None
    service: dict[str, LegislatorService] = {}
    for raw in payload.get("legislators") or []:
        if isinstance(raw, Mapping):
            record = LegislatorService.from_json(raw)
            if record is not None:
                service[record.bioguide_id] = record
    return service, fetched_at


def fetch_legislator_service(settings: Settings) -> dict[str, LegislatorService]:
    """Download current + historical congress-legislators and parse them."""

    entries: list[Mapping[str, object]] = []
    with httpx.Client(
        timeout=120.0,
        follow_redirects=True,
        headers={"User-Agent": settings.user_agent},
    ) as client:
        for url in (LEGISLATORS_CURRENT_URL, LEGISLATORS_HISTORICAL_URL):
            response = client.get(url)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                raise RuntimeError(f"{url} did not return a list; feed format changed.")
            entries.extend(entry for entry in payload if isinstance(entry, Mapping))
    service = parse_legislator_entries(entries)
    if not service:
        raise RuntimeError("congress-legislators returned no usable terms.")
    return service


def load_legislator_service(
    settings: Settings,
    *,
    max_age: timedelta = SERVICE_CACHE_MAX_AGE,
    now: datetime | None = None,
) -> dict[str, LegislatorService] | None:
    """Return the service roster, from cache when fresh, else from the network.

    A failed download falls back to a stale cache. Only when there is neither
    does this return ``None``, and the registry then falls back to the
    ``members`` row (current chamber, latest term), which can refuse a correct
    match but never makes a cross-chamber one.
    """

    path = service_cache_path(settings)
    cached = read_service_cache(path)
    current_time = now or datetime.now(timezone.utc)
    if cached is not None:
        service, fetched_at = cached
        if fetched_at is not None and current_time - fetched_at <= max_age and service:
            return service

    try:
        service = fetch_legislator_service(settings)
    except Exception as error:  # noqa: BLE001 - a stale roster beats no roster
        if cached is not None and cached[0]:
            logger.warning(
                "congress-legislators refresh failed (%s); using the cached roster from %s.",
                error,
                cached[1],
            )
            return cached[0]
        logger.warning(
            "congress-legislators unavailable (%s) and no cache at %s; chamber checks fall back "
            "to the members table, which only knows each member's latest term.",
            error,
            path,
        )
        return None

    try:
        write_service_cache(path, service)
    except OSError as error:
        logger.warning("Could not write %s: %s", path, error)
    return service
