"""Backfill former members of Congress and re-resolve orphaned House PTR stubs.

The ``members`` table is seeded from ``legislators-current`` only (the site's
``scripts/seed-members.mjs``), so a House Periodic Transaction Report filed by
someone who has since left Congress can never resolve its member by name. The
parser refuses to publish trade rows for an unresolved member, so those stubs
sit in ``needs_review`` or ``pending_extraction`` forever.

This module does two things, both idempotent:

1. Loads ``legislators-historical.json`` from unitedstates/congress-legislators
   (CC0, no API key) and inserts everyone whose last term ended on or after
   ``--since`` (default 2012-01-01, the STOCK Act year) who is not already in
   ``members``. Rows are shaped exactly like ``seed-members.mjs`` produces them
   (same id/slug/party/chamber/district conventions) with ``in_office = false``
   and the last term's dates. Existing rows are never overwritten; the only
   change made to them is filling columns that are currently NULL.

2. Re-resolves every ``house_filing_stubs`` row whose metadata has no
   ``memberId`` against the enlarged registry using the pipeline's existing
   name matcher (``MemberRegistry.resolve``), and writes ``memberId`` /
   ``memberSlug`` / ``memberName`` (plus party/state/district when known) into
   the stub metadata so the next queue pass can publish its trades.

``trades.member_id`` is ``NOT NULL`` with a foreign key to ``members``, so there
are never trade rows with an empty member; the repair step exists as a guard
and is expected to report zero.

Run on the box as::

    python -m capitol_pipeline.members_historical --dry-run
    python -m capitol_pipeline.members_historical
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, fields
from datetime import date, datetime, timezone
import json
import re

import click
import httpx

from capitol_pipeline.config import Settings
from capitol_pipeline.exporters.neon import Jsonb, ensure_neon_available, neon_connection
from capitol_pipeline.models.congress import MemberMatch
from capitol_pipeline.registries.members import MemberRegistry
from capitol_pipeline.sources.congress_legislators import _coerce_text, _wikipedia_slug


LEGISLATORS_BASE_URL = "https://unitedstates.github.io/congress-legislators/"
LEGISLATORS_HISTORICAL_URL = f"{LEGISLATORS_BASE_URL}legislators-historical.json"
LEGISLATORS_CURRENT_URL = f"{LEGISLATORS_BASE_URL}legislators-current.json"

#: STOCK Act year. Anyone whose last term ended before this never filed a PTR.
DEFAULT_SINCE = date(2012, 1, 1)

#: Written into stub metadata so a later audit can tell who set ``memberId``.
RESOLVER_TAG = "members_historical"

#: Columns ``seed-members.mjs`` writes. Every one of these must exist.
CORE_MEMBER_COLUMNS: tuple[str, ...] = (
    "id",
    "bioguide_id",
    "slug",
    "name",
    "first_name",
    "last_name",
    "party",
    "state",
    "district",
    "chamber",
    "image_url",
    "website",
    "phone",
    "office",
    "in_office",
    "term_start",
    "term_end",
    "date_of_birth",
    "gender",
    "fec_ids",
    "opensecrets_id",
)

#: Cross-walk columns added by ``ensure_members_bio_schema``. They are written
#: only when the live table has them, so the loader also works on a database
#: where that schema step has not run.
CROSSWALK_MEMBER_COLUMNS: tuple[str, ...] = (
    "wikidata_qid",
    "wikipedia_slug",
    "ballotpedia_slug",
    "govtrack_id",
    "house_history_id",
    "votesmart_id",
    "icpsr_id",
    "cspan_id",
    "maplight_id",
    "google_entity_id",
)

#: Columns that may be filled on an *existing* row when it is NULL. Identity
#: columns (name, slug, party, state, district, chamber, in_office, first/last
#: name) are deliberately absent: an existing row is never rewritten.
FILL_NULL_COLUMNS: tuple[str, ...] = (
    "image_url",
    "website",
    "phone",
    "office",
    "term_start",
    "term_end",
    "date_of_birth",
    "gender",
    "opensecrets_id",
) + CROSSWALK_MEMBER_COLUMNS


@dataclass(slots=True)
class MemberRow:
    """One ``members`` row in the shape ``seed-members.mjs`` produces."""

    id: str
    bioguide_id: str
    slug: str
    name: str
    first_name: str
    last_name: str
    party: str
    state: str
    district: str | None
    chamber: str
    image_url: str | None
    website: str | None
    phone: str | None
    office: str | None
    in_office: bool
    term_start: date | None
    term_end: date | None
    date_of_birth: date | None
    gender: str | None
    fec_ids: list[str]
    opensecrets_id: str | None
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

    def as_params(self) -> dict[str, object]:
        """Return the row as a psycopg parameter mapping (``slots=True`` has no ``__dict__``)."""

        return {field.name: getattr(self, field.name) for field in fields(self)}

    def as_registry_row(self) -> dict[str, object]:
        """Return the subset ``MemberRegistry.from_rows`` needs."""

        return {
            "id": self.id,
            "bioguide_id": self.bioguide_id,
            "name": self.name,
            "slug": self.slug,
            "party": self.party,
            "state": self.state,
            "district": self.district,
        }


# ── Row shaping (mirrors scripts/seed-members.mjs in the site repo) ─────────


def slugify(text: str) -> str:
    """Mirror the site's ``slugify``: lower-case, drop apostrophes, dash the rest.

    Deliberately does *not* strip diacritics, because the seed script does not
    ("André Carson" is ``andr-carson`` in the live table) and slugs must keep
    matching what the site already links to.
    """

    lowered = text.lower()
    lowered = re.sub(r"['‘’]", "", lowered)
    lowered = re.sub(r"[^a-z0-9]+", "-", lowered)
    return lowered.strip("-")


def map_party(party: object) -> str:
    text = _coerce_text(party)
    if not text:
        return "I"
    lowered = text.lower()
    if lowered.startswith("democrat"):
        return "D"
    if lowered.startswith("republican"):
        return "R"
    return "I"


def map_chamber(term_type: object) -> str:
    return "senate" if term_type == "sen" else "house"


def bioguide_image_url(bioguide_id: str | None) -> str | None:
    if not bioguide_id:
        return None
    return f"https://bioguide.congress.gov/bioguide/photo/{bioguide_id[0].upper()}/{bioguide_id}.jpg"


def _parse_date(value: object) -> date | None:
    text = _coerce_text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def last_term(entry: Mapping[str, object]) -> Mapping[str, object] | None:
    terms = entry.get("terms")
    if not isinstance(terms, list) or not terms:
        return None
    term = terms[-1]
    return term if isinstance(term, dict) else None


def iter_recent_legislators(
    entries: Iterable[Mapping[str, object]],
    *,
    since: date = DEFAULT_SINCE,
) -> Iterator[Mapping[str, object]]:
    """Yield legislators whose most recent term ended on or after ``since``."""

    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        term = last_term(entry)
        if term is None:
            continue
        end = _parse_date(term.get("end"))
        if end is not None and end >= since:
            yield entry


def build_member_row(entry: Mapping[str, object], *, in_office: bool = False) -> MemberRow | None:
    """Shape one congress-legislators entry into a ``members`` row.

    Returns ``None`` when the entry has no bioguide id, no terms, or no name,
    matching ``transformLegislator`` in the seed script.
    """

    ids = entry.get("id") or {}
    if not isinstance(ids, Mapping):
        return None
    bioguide = _coerce_text(ids.get("bioguide"))
    if not bioguide:
        return None

    term = last_term(entry)
    if term is None:
        return None

    name = entry.get("name") or {}
    if not isinstance(name, Mapping):
        name = {}
    first = _coerce_text(name.get("first")) or ""
    last = _coerce_text(name.get("last")) or ""
    full_name = _coerce_text(name.get("official_full")) or " ".join(
        part for part in (first, last) if part
    )
    if not full_name:
        return None
    if not first or not last:
        # first_name / last_name are NOT NULL. The dataset always carries both,
        # but fall back to a split of the full name rather than failing the row.
        parts = full_name.split()
        first = first or parts[0]
        last = last or (parts[-1] if len(parts) > 1 else parts[0])

    term_type = term.get("type")
    district: str | None = None
    if term_type == "rep":
        raw_district = term.get("district")
        district = str(raw_district if raw_district is not None else 0)

    bio = entry.get("bio") or {}
    if not isinstance(bio, Mapping):
        bio = {}

    raw_fec = ids.get("fec") or []
    fec_ids = [str(item) for item in raw_fec if item] if isinstance(raw_fec, list) else []

    return MemberRow(
        id=f"m-{bioguide}",
        bioguide_id=bioguide,
        slug=slugify(full_name),
        name=full_name,
        first_name=first,
        last_name=last,
        party=map_party(term.get("party")),
        state=_coerce_text(term.get("state")) or "",
        district=district,
        chamber=map_chamber(term_type),
        image_url=bioguide_image_url(bioguide),
        website=_coerce_text(term.get("url")),
        phone=_coerce_text(term.get("phone")),
        office=_coerce_text(term.get("office")),
        in_office=in_office,
        term_start=_parse_date(term.get("start")),
        term_end=_parse_date(term.get("end")),
        date_of_birth=_parse_date(bio.get("birthday")),
        gender=_coerce_text(bio.get("gender")),
        fec_ids=fec_ids,
        opensecrets_id=_coerce_text(ids.get("opensecrets")),
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
    )


def assign_unique_slugs(rows: Sequence[MemberRow], taken: Iterable[str]) -> list[MemberRow]:
    """Make every slug unique against ``taken`` and within ``rows``.

    Follows the seed script's ``deduplicateSlugs``: a colliding slug gets
    ``-<state>`` (Senate) or ``-<state>-<district>`` (House) appended. If that
    still collides (two people with the same name in the same seat), the
    bioguide id is appended, which is unique by construction.
    """

    reserved = set(taken)
    counts = Counter(row.slug for row in rows)
    for row in rows:
        base = row.slug
        candidates: list[str] = []
        if counts[base] == 1 and base not in reserved:
            candidates.append(base)
        state = row.state.lower()
        suffix = state if row.chamber == "senate" else f"{state}-{row.district}"
        candidates.append(f"{base}-{suffix}")
        candidates.append(f"{base}-{row.bioguide_id.lower()}")
        for candidate in candidates:
            if candidate not in reserved:
                row.slug = candidate
                reserved.add(candidate)
                break
    return list(rows)


def build_member_rows(
    entries: Iterable[Mapping[str, object]],
    *,
    since: date = DEFAULT_SINCE,
    in_office: bool = False,
) -> list[MemberRow]:
    """Shape every legislator with a term ending on/after ``since``."""

    rows: list[MemberRow] = []
    for entry in iter_recent_legislators(entries, since=since):
        row = build_member_row(entry, in_office=in_office)
        if row is not None:
            rows.append(row)
    return rows


# ── Fetch ───────────────────────────────────────────────────────────────────


def fetch_legislators(settings: Settings, url: str) -> list[dict[str, object]]:
    """Download one congress-legislators JSON build."""

    with httpx.Client(
        timeout=120.0,
        follow_redirects=True,
        headers={"User-Agent": settings.user_agent},
    ) as client:
        response = client.get(url)
        response.raise_for_status()
        payload = response.json()
    if not isinstance(payload, list):
        raise RuntimeError(f"{url} did not return a list; feed format changed.")
    return [entry for entry in payload if isinstance(entry, dict)]


# ── Members table ───────────────────────────────────────────────────────────


def fetch_members_table_columns(settings: Settings) -> set[str]:
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'members'
                """
            )
            return {str(row["column_name"]) for row in cursor.fetchall()}


def fetch_existing_members(settings: Settings) -> list[dict[str, object]]:
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, bioguide_id, name, slug, party, state, district, in_office
                FROM members
                ORDER BY in_office DESC NULLS LAST, name ASC
                """
            )
            return list(cursor.fetchall())


def writable_member_columns(table_columns: Iterable[str]) -> list[str]:
    """Return the insert column list: all core columns plus present cross-walk columns."""

    present = set(table_columns)
    missing = [column for column in CORE_MEMBER_COLUMNS if column not in present]
    if missing:
        raise RuntimeError(f"members table is missing required columns: {', '.join(missing)}")
    return list(CORE_MEMBER_COLUMNS) + [
        column for column in CROSSWALK_MEMBER_COLUMNS if column in present
    ]


def build_insert_sql(columns: Sequence[str]) -> str:
    column_list = ", ".join(columns)
    placeholders = ", ".join(f"%({column})s" for column in columns)
    return (
        f"INSERT INTO members ({column_list}) VALUES ({placeholders}) "
        "ON CONFLICT DO NOTHING"
    )


def build_fill_null_sql(columns: Sequence[str]) -> str:
    assignments = ", ".join(f"{column} = COALESCE(members.{column}, %({column})s)" for column in columns)
    return (
        f"UPDATE members SET {assignments}, "
        "fec_ids = CASE WHEN COALESCE(array_length(members.fec_ids, 1), 0) = 0 "
        "THEN %(fec_ids)s ELSE members.fec_ids END, "
        "updated_at = NOW() "
        "WHERE bioguide_id = %(bioguide_id)s"
    )


def insert_member_rows(settings: Settings, rows: Sequence[MemberRow], columns: Sequence[str]) -> int:
    """Insert new members; returns the number of rows actually inserted."""

    if not rows:
        return 0
    sql = build_insert_sql(columns)
    params = [{column: row.as_params()[column] for column in columns} for row in rows]
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(sql, params)
            inserted = cursor.rowcount
        connection.commit()
    return max(int(inserted or 0), 0)


def fill_member_nulls(settings: Settings, rows: Sequence[MemberRow], table_columns: Iterable[str]) -> int:
    """Fill NULL enrichment columns on rows that already exist. Never overwrites."""

    if not rows:
        return 0
    present = set(table_columns)
    columns = [column for column in FILL_NULL_COLUMNS if column in present]
    sql = build_fill_null_sql(columns)
    params = []
    for row in rows:
        values = row.as_params()
        params.append({column: values[column] for column in columns + ["fec_ids", "bioguide_id"]})
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(sql, params)
        connection.commit()
    return len(params)


# ── Stub re-resolution ──────────────────────────────────────────────────────


def fetch_unresolved_stubs(settings: Settings) -> list[dict[str, object]]:
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT doc_id, filing_year, status, extracted_trade_id, metadata
                FROM house_filing_stubs
                WHERE COALESCE(metadata->>'memberId', '') = ''
                ORDER BY filing_year, doc_id
                """
            )
            return list(cursor.fetchall())


def _stub_state(metadata: Mapping[str, object]) -> str | None:
    state = _coerce_text(metadata.get("state"))
    if state:
        return state.upper()
    raw = _coerce_text(metadata.get("rawStateDistrict"))
    if raw and len(raw) >= 2 and raw[:2].isalpha():
        return raw[:2].upper()
    return None


def resolve_stub_member(registry: MemberRegistry, metadata: Mapping[str, object]) -> MemberMatch | None:
    """Resolve a stub's member the same way the House feed sync does.

    First the feed's ``firstName``/``lastName``/state triple (what
    ``resolve_feed_member`` receives), then the full ``memberName``. The state
    is always passed when the stub carries one so a same-name member from
    another state can never be picked.
    """

    first = _coerce_text(metadata.get("firstName"))
    last = _coerce_text(metadata.get("lastName"))
    name = _coerce_text(metadata.get("memberName"))
    state = _stub_state(metadata)

    attempts: list[dict[str, str | None]] = []
    if first or last:
        attempts.append({"first_name": first, "last_name": last, "state": state})
    if name:
        attempts.append({"name": name, "state": state})

    for kwargs in attempts:
        match = registry.resolve(**kwargs)
        if match is not None and match.id:
            return match
    return None


def build_stub_metadata_updates(
    match: MemberMatch,
    metadata: Mapping[str, object],
    *,
    requeue: bool = True,
    resolved_at: str | None = None,
) -> dict[str, object]:
    """Metadata patch that turns an unresolved stub into a resolved one.

    Only keys the resolver actually knows are written, so a stub's feed-derived
    state/district is never nulled out. ``retryAfter`` is cleared when
    ``requeue`` is set so ``house-ingest`` picks the stub up on its next pass.
    """

    updates: dict[str, object] = {
        "memberId": match.id,
        "memberSlug": match.slug,
        "memberName": match.name,
        "memberResolvedBy": RESOLVER_TAG,
        "memberResolvedAt": resolved_at or datetime.now(timezone.utc).isoformat(),
    }
    for key in ("party", "state", "district"):
        value = getattr(match, key)
        if value:
            updates[key] = value
    feed_name = _coerce_text(metadata.get("memberName"))
    if feed_name and feed_name != match.name:
        updates["feedMemberName"] = feed_name
    if requeue:
        updates["retryAfter"] = None
    return updates


def apply_stub_resolutions(settings: Settings, resolutions: Sequence[tuple[str, dict[str, object]]]) -> int:
    """Merge metadata patches into stubs. Status and extracted_trade_id are untouched."""

    if not resolutions:
        return 0
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                UPDATE house_filing_stubs
                SET metadata = COALESCE(metadata, '{}'::jsonb) || %s
                WHERE doc_id = %s
                """,
                [(Jsonb(updates), doc_id) for doc_id, updates in resolutions],  # type: ignore[misc]
            )
            updated = cursor.rowcount
        connection.commit()
    return max(int(updated or 0), 0)


def repair_house_trade_members(settings: Settings, resolutions: Sequence[tuple[str, str]]) -> int:
    """Set ``trades.member_id`` on House trade rows for a doc that lack one.

    House trade ids are ``tr-house-<doc_id>-<line>``. ``trades.member_id`` is
    ``NOT NULL`` with a foreign key, so this is a guard that should report 0;
    it exists so a future schema relaxation cannot leave orphaned rows behind.
    """

    if not resolutions:
        return 0
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                UPDATE trades
                SET member_id = %s
                WHERE id LIKE %s
                  AND (member_id IS NULL OR member_id = '')
                """,
                [(member_id, f"tr-house-{doc_id}-%") for doc_id, member_id in resolutions],
            )
            repaired = cursor.rowcount
        connection.commit()
    return max(int(repaired or 0), 0)


# ── Orchestration ───────────────────────────────────────────────────────────


def run(
    settings: Settings,
    *,
    dry_run: bool = False,
    since: date = DEFAULT_SINCE,
    include_current: bool = False,
    requeue: bool = True,
    historical_url: str = LEGISLATORS_HISTORICAL_URL,
    current_url: str = LEGISLATORS_CURRENT_URL,
) -> dict[str, object]:
    ensure_neon_available()

    table_columns = fetch_members_table_columns(settings)
    insert_columns = writable_member_columns(table_columns)
    existing = fetch_existing_members(settings)
    existing_by_bioguide = {
        str(row.get("bioguide_id") or "").upper(): row for row in existing if row.get("bioguide_id")
    }
    taken_slugs = {str(row.get("slug")) for row in existing if row.get("slug")}

    candidates = build_member_rows(fetch_legislators(settings, historical_url), since=since)
    if include_current:
        candidates += build_member_rows(
            fetch_legislators(settings, current_url), since=since, in_office=True
        )

    new_rows = [row for row in candidates if row.bioguide_id.upper() not in existing_by_bioguide]
    present_rows = [row for row in candidates if row.bioguide_id.upper() in existing_by_bioguide]
    assign_unique_slugs(new_rows, taken_slugs)

    # Rows the historical file says have left but the table still shows in office.
    # Reported only; flipping in_office is the seed script's call, not ours.
    stale_in_office = [
        {"id": row.id, "name": row.name, "termEnd": row.term_end.isoformat() if row.term_end else None}
        for row in present_rows
        if not row.in_office and bool(existing_by_bioguide[row.bioguide_id.upper()].get("in_office"))
    ]

    inserted = 0
    filled = 0
    if not dry_run:
        inserted = insert_member_rows(settings, new_rows, insert_columns)
        filled = fill_member_nulls(settings, present_rows, table_columns)

    registry = MemberRegistry.from_rows(
        [*existing, *(row.as_registry_row() for row in new_rows)]
    )

    stubs = fetch_unresolved_stubs(settings)
    resolved_at = datetime.now(timezone.utc).isoformat()
    metadata_patches: list[tuple[str, dict[str, object]]] = []
    trade_targets: list[tuple[str, str]] = []
    resolved_items: list[dict[str, object]] = []
    unresolved_items: list[dict[str, object]] = []
    for stub in stubs:
        metadata = stub.get("metadata") or {}
        if not isinstance(metadata, dict):
            metadata = {}
        doc_id = str(stub.get("doc_id") or "")
        feed_name = _coerce_text(metadata.get("memberName")) or ""
        match = resolve_stub_member(registry, metadata)
        if match is None or not match.id:
            unresolved_items.append(
                {
                    "docId": doc_id,
                    "filingYear": stub.get("filing_year"),
                    "status": stub.get("status"),
                    "memberName": feed_name,
                    "state": _stub_state(metadata),
                }
            )
            continue
        metadata_patches.append(
            (doc_id, build_stub_metadata_updates(match, metadata, requeue=requeue, resolved_at=resolved_at))
        )
        trade_targets.append((doc_id, str(match.id)))
        resolved_items.append(
            {
                "docId": doc_id,
                "status": stub.get("status"),
                "memberName": feed_name,
                "memberId": match.id,
                "resolvedName": match.name,
            }
        )

    stubs_updated = 0
    trades_repaired = 0
    if not dry_run:
        stubs_updated = apply_stub_resolutions(settings, metadata_patches)
        trades_repaired = repair_house_trade_members(settings, trade_targets)

    resolved_by_member = Counter(
        f"{item['resolvedName']} ({item['memberId']})" for item in resolved_items
    )
    unresolved_by_name = Counter(item["memberName"] or "(blank)" for item in unresolved_items)

    return {
        "dryRun": dry_run,
        "since": since.isoformat(),
        "includeCurrent": include_current,
        "members": {
            "existing": len(existing),
            "candidates": len(candidates),
            "toInsert": len(new_rows),
            "inserted": inserted,
            "alreadyPresent": len(present_rows),
            "nullsFilled": filled,
            "staleInOffice": stale_in_office,
            "insertSample": [
                {
                    "id": row.id,
                    "name": row.name,
                    "slug": row.slug,
                    "party": row.party,
                    "state": row.state,
                    "district": row.district,
                    "chamber": row.chamber,
                    "termEnd": row.term_end.isoformat() if row.term_end else None,
                }
                for row in new_rows[:10]
            ],
        },
        "stubs": {
            "unresolvedBefore": len(stubs),
            "resolvable": len(resolved_items),
            "updated": stubs_updated,
            "stillUnresolved": len(unresolved_items),
            "resolvedByMember": dict(resolved_by_member.most_common()),
            "unresolvedByName": dict(unresolved_by_name.most_common()),
            "unresolved": unresolved_items,
        },
        "trades": {
            "repaired": trades_repaired,
            "note": "trades.member_id is NOT NULL with an FK to members; 0 is the expected value.",
        },
    }


@click.command("sync-members-historical")
@click.option("--dry-run", is_flag=True, default=False, help="Report what would change; write nothing.")
@click.option(
    "--since",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=DEFAULT_SINCE.isoformat(),
    show_default=True,
    help="Keep legislators whose last term ended on or after this date.",
)
@click.option(
    "--include-current/--historical-only",
    default=False,
    show_default=True,
    help="Also insert legislators from legislators-current.json missing from members (in_office = true).",
)
@click.option(
    "--requeue/--no-requeue",
    default=True,
    show_default=True,
    help="Clear retryAfter on resolved stubs so the next house-ingest pass re-parses them.",
)
def sync_members_historical_command(
    dry_run: bool,
    since: datetime,
    include_current: bool,
    requeue: bool,
) -> None:
    """Insert former members from unitedstates/congress-legislators and re-resolve
    House filing stubs whose member never matched."""

    settings = Settings()
    summary = run(
        settings,
        dry_run=dry_run,
        since=since.date(),
        include_current=include_current,
        requeue=requeue,
    )
    click.echo(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":  # pragma: no cover - exercised on the box
    sync_members_historical_command()
