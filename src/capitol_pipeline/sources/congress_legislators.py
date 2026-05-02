"""Cross-walk identifier extractor for unitedstates/congress-legislators.

The `legislators-current.json` feed is the canonical roster maintained by the
@unitedstates community (CC0). Beyond name/party/state it carries every external
identifier we need to join against Wikidata, Wikipedia, Ballotpedia, GovTrack,
VoteSmart, House History, ICPSR, C-SPAN, MapLight, and Google's Knowledge Graph.

This module fetches the JSON build from gh-pages and yields per-bioguide records
shaped for direct upsert into the `members` table. The site-side
``scripts/seed-members.mjs`` already inserts the basic roster columns; this
extractor only writes the new cross-walk + religion columns added by
``ensure_members_bio_schema``.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, fields

import httpx

from capitol_pipeline.config import Settings
from capitol_pipeline.exporters.neon import _chunked_executemany, neon_connection


LEGISLATORS_CURRENT_URL = (
    "https://raw.githubusercontent.com/unitedstates/congress-legislators/"
    "gh-pages/legislators-current.json"
)


@dataclass(slots=True)
class LegislatorCrosswalk:
    """Subset of legislators-current keys we persist on the members table."""

    bioguide_id: str
    wikidata_qid: str | None = None
    wikipedia_slug: str | None = None
    ballotpedia_slug: str | None = None
    govtrack_id: str | None = None
    house_history_id: str | None = None
    votesmart_id: str | None = None
    icpsr_id: str | None = None
    cspan_id: str | None = None
    maplight_id: str | None = None
    google_entity_id: str | None = None
    religion: str | None = None  # bio.religion in the YAML

    def has_any_external_id(self) -> bool:
        """Return True if the row carries at least one non-bioguide external ID.

        Used to skip pure-bioguide rows where we have nothing new to write.
        ``slots=True`` strips ``__dict__``, so we walk dataclass fields instead.
        """

        for field in fields(self):
            if field.name == "bioguide_id":
                continue
            if getattr(self, field.name):
                return True
        return False


def _coerce_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _wikipedia_slug(value: object) -> str | None:
    """Normalize a Wikipedia title to a URL-safe slug.

    The legislators-current YAML stores the human-readable article title with
    spaces (e.g. "Nancy Pelosi"). en.wikipedia.org accepts underscores in URLs.
    """

    text = _coerce_text(value)
    if not text:
        return None
    return text.replace(" ", "_")


def parse_legislator_record(payload: dict[str, object]) -> LegislatorCrosswalk | None:
    """Convert one legislators-current entry into a cross-walk record."""

    ids = payload.get("id") or {}
    if not isinstance(ids, dict):
        return None

    bioguide = _coerce_text(ids.get("bioguide"))
    if not bioguide:
        return None

    bio = payload.get("bio") or {}
    if not isinstance(bio, dict):
        bio = {}

    return LegislatorCrosswalk(
        bioguide_id=bioguide,
        wikidata_qid=_coerce_text(ids.get("wikidata")),
        wikipedia_slug=_wikipedia_slug(ids.get("wikipedia")),
        ballotpedia_slug=_wikipedia_slug(ids.get("ballotpedia")),
        govtrack_id=_coerce_text(ids.get("govtrack")),
        house_history_id=_coerce_text(ids.get("house_history")),
        votesmart_id=_coerce_text(ids.get("votesmart")),
        icpsr_id=_coerce_text(ids.get("icpsr")),
        cspan_id=_coerce_text(ids.get("cspan")),
        maplight_id=_coerce_text(ids.get("maplight")),
        google_entity_id=_coerce_text(ids.get("google_entity_id")),
        religion=_coerce_text(bio.get("religion")),
    )


def fetch_legislator_crosswalk(settings: Settings) -> Iterator[LegislatorCrosswalk]:
    """Stream cross-walk records for every current member of Congress."""

    with httpx.Client(
        timeout=60.0,
        follow_redirects=True,
        headers={"User-Agent": settings.user_agent},
    ) as client:
        response = client.get(LEGISLATORS_CURRENT_URL)
        response.raise_for_status()
        payload = response.json()

    if not isinstance(payload, list):
        raise RuntimeError(
            "legislators-current.json did not return a list — feed format changed."
        )

    for entry in payload:
        if not isinstance(entry, dict):
            continue
        record = parse_legislator_record(entry)
        if record is not None:
            yield record


# ── Persistence ──────────────────────────────────────────────────────────

_UPSERT_SQL = """
UPDATE members SET
    wikidata_qid             = COALESCE(%s, wikidata_qid),
    wikipedia_slug           = COALESCE(%s, wikipedia_slug),
    ballotpedia_slug         = COALESCE(%s, ballotpedia_slug),
    govtrack_id              = COALESCE(%s, govtrack_id),
    house_history_id         = COALESCE(%s, house_history_id),
    votesmart_id             = COALESCE(%s, votesmart_id),
    icpsr_id                 = COALESCE(%s, icpsr_id),
    cspan_id                 = COALESCE(%s, cspan_id),
    maplight_id              = COALESCE(%s, maplight_id),
    google_entity_id         = COALESCE(%s, google_entity_id),
    religion_legislator_yaml = COALESCE(%s, religion_legislator_yaml),
    updated_at               = NOW()
WHERE bioguide_id = %s
"""


def upsert_member_crosswalk(
    settings: Settings,
    records: Iterable[LegislatorCrosswalk],
) -> dict[str, int]:
    """Apply cross-walk IDs to existing members rows. Skips unknown bioguides.

    We deliberately use UPDATE (not INSERT) — seed-members.mjs owns roster
    insertion. If a bioguide doesn't yet exist, the row is silently skipped and
    counted in `missing` so the operator can re-run the seed script.
    """

    rows_to_apply = [r for r in records if r.has_any_external_id()]
    if not rows_to_apply:
        return {"records": 0, "applied": 0, "missing": 0}

    bioguides = [r.bioguide_id for r in rows_to_apply]
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT bioguide_id FROM members WHERE bioguide_id = ANY(%s)",
                (bioguides,),
            )
            known = {row["bioguide_id"] for row in cursor.fetchall()}

    appliable = [r for r in rows_to_apply if r.bioguide_id in known]
    missing = len(rows_to_apply) - len(appliable)

    params = [
        (
            r.wikidata_qid,
            r.wikipedia_slug,
            r.ballotpedia_slug,
            r.govtrack_id,
            r.house_history_id,
            r.votesmart_id,
            r.icpsr_id,
            r.cspan_id,
            r.maplight_id,
            r.google_entity_id,
            r.religion,
            r.bioguide_id,
        )
        for r in appliable
    ]
    applied = _chunked_executemany(settings, _UPSERT_SQL, params)

    return {
        "records": len(rows_to_apply),
        "applied": applied,
        "missing": missing,
    }
