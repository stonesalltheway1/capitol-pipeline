"""Wikidata SPARQL enrichment for the members infobox pipeline.

Reads the cross-walk QIDs already populated by the congress-legislators source
and pulls the structured "infobox" properties Wikipedia exposes for every
sitting member of Congress: birthplace, alma mater, religion, spouse, children,
prior occupations, military service, residence, predecessor / successor, native
name, and the Commons file name for the Wikidata-canonical headshot (P18).

The enrichment is intentionally batched (50 QIDs per SPARQL request) to stay
inside Wikidata's anonymous query budget. Each query carries a polite
``User-Agent`` per the Wikidata access policy, plus a ~1.2s delay between
batches.

License: Wikidata structured data is dedicated to the public domain via CC0.
No attribution is technically required, but we still link back to the QID from
the UI as good practice.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
import time

import httpx

from capitol_pipeline.config import Settings
from capitol_pipeline.exporters.neon import _chunked_executemany, neon_connection


WIKIDATA_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
WIKIDATA_BATCH_SIZE = 50
WIKIDATA_REQUEST_INTERVAL_SECONDS = 1.25


# ── SPARQL templates ─────────────────────────────────────────────────────
# We pull labels via the Wikidata label service rather than a separate
# ``wbgetentities`` round-trip — it's atomic and avoids partial-failure modes.
# OPTIONAL clauses keep the result row alive even when a property is missing.

_INFOBOX_SPARQL = """
SELECT ?person ?personLabel ?nativeName ?image
       ?birthPlace ?birthPlaceLabel
       ?religion ?religionLabel
       ?spouse ?spouseLabel
       ?residence ?residenceLabel
       ?predecessor ?predecessorLabel
       ?successor ?successorLabel
       (GROUP_CONCAT(DISTINCT CONCAT(STR(?almaMater), "|", ?almaMaterLabelStr); separator="||") AS ?almaMatersJoined)
       (GROUP_CONCAT(DISTINCT CONCAT(STR(?occupation), "|", ?occupationLabelStr); separator="||") AS ?occupationsJoined)
       (GROUP_CONCAT(DISTINCT CONCAT(STR(?militaryBranch), "|", ?militaryBranchLabelStr); separator="||") AS ?militaryJoined)
       (COUNT(DISTINCT ?child) AS ?childrenCount)
WHERE {
  VALUES ?person { %(values)s }

  OPTIONAL { ?person wdt:P1559 ?nativeName . }
  OPTIONAL { ?person wdt:P18  ?image . }
  OPTIONAL {
    ?person wdt:P19 ?birthPlace .
    ?birthPlace rdfs:label ?birthPlaceLabel . FILTER(LANG(?birthPlaceLabel) = "en")
  }
  OPTIONAL {
    ?person wdt:P140 ?religion .
    ?religion rdfs:label ?religionLabel . FILTER(LANG(?religionLabel) = "en")
  }
  OPTIONAL {
    ?person wdt:P26 ?spouse .
    ?spouse rdfs:label ?spouseLabel . FILTER(LANG(?spouseLabel) = "en")
  }
  OPTIONAL {
    ?person wdt:P551 ?residence .
    ?residence rdfs:label ?residenceLabel . FILTER(LANG(?residenceLabel) = "en")
  }
  OPTIONAL {
    ?person wdt:P1365 ?predecessor .
    ?predecessor rdfs:label ?predecessorLabel . FILTER(LANG(?predecessorLabel) = "en")
  }
  OPTIONAL {
    ?person wdt:P1366 ?successor .
    ?successor rdfs:label ?successorLabel . FILTER(LANG(?successorLabel) = "en")
  }
  OPTIONAL {
    ?person wdt:P69 ?almaMater .
    ?almaMater rdfs:label ?almaMaterLabel . FILTER(LANG(?almaMaterLabel) = "en")
    BIND(STR(?almaMaterLabel) AS ?almaMaterLabelStr)
  }
  OPTIONAL {
    ?person wdt:P106 ?occupation .
    ?occupation rdfs:label ?occupationLabel . FILTER(LANG(?occupationLabel) = "en")
    BIND(STR(?occupationLabel) AS ?occupationLabelStr)
  }
  OPTIONAL {
    ?person wdt:P241 ?militaryBranch .
    ?militaryBranch rdfs:label ?militaryBranchLabel . FILTER(LANG(?militaryBranchLabel) = "en")
    BIND(STR(?militaryBranchLabel) AS ?militaryBranchLabelStr)
  }
  OPTIONAL { ?person wdt:P40 ?child . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
GROUP BY ?person ?personLabel ?nativeName ?image
         ?birthPlace ?birthPlaceLabel
         ?religion ?religionLabel
         ?spouse ?spouseLabel
         ?residence ?residenceLabel
         ?predecessor ?predecessorLabel
         ?successor ?successorLabel
"""


@dataclass(slots=True)
class WikidataInfobox:
    """One row of structured infobox facts pulled from Wikidata for a person."""

    qid: str
    native_name: str | None = None
    birthplace: str | None = None
    birthplace_qid: str | None = None
    religion: str | None = None
    spouse: str | None = None
    children_count: int | None = None
    residence: str | None = None
    predecessor_qid: str | None = None
    predecessor_name: str | None = None
    successor_qid: str | None = None
    successor_name: str | None = None
    image_filename: str | None = None
    alma_mater: list[dict[str, str]] = field(default_factory=list)
    prior_occupations: list[dict[str, str]] = field(default_factory=list)
    military_branches: list[dict[str, str]] = field(default_factory=list)


def _qid_from_uri(uri: str | None) -> str | None:
    if not uri:
        return None
    if "/entity/" in uri:
        return uri.rsplit("/entity/", 1)[1]
    return uri


def _filename_from_image_uri(uri: str | None) -> str | None:
    """Wikidata returns ``http://commons.wikimedia.org/wiki/Special:FilePath/<file>``.

    We strip to the bare file name so the UI can build any size variant via
    ``Special:FilePath/<name>?width=600``.
    """

    if not uri:
        return None
    marker = "Special:FilePath/"
    if marker in uri:
        return uri.rsplit(marker, 1)[1]
    return uri.rsplit("/", 1)[-1]


def _parse_label_pairs(joined: str | None) -> list[dict[str, str]]:
    """Parse the GROUP_CONCAT'd ``<uri>|<label>`` pairs from a SPARQL response."""

    if not joined:
        return []
    items: list[dict[str, str]] = []
    seen_qids: set[str] = set()
    for chunk in joined.split("||"):
        chunk = chunk.strip()
        if not chunk or "|" not in chunk:
            continue
        uri, _, label = chunk.partition("|")
        qid = _qid_from_uri(uri.strip())
        label = label.strip()
        if not qid or qid in seen_qids:
            continue
        seen_qids.add(qid)
        items.append({"qid": qid, "label": label or qid})
    return items


def _parse_row(row: dict[str, dict[str, str]]) -> WikidataInfobox | None:
    person_qid = _qid_from_uri(row.get("person", {}).get("value"))
    if not person_qid:
        return None

    def cell(name: str) -> str | None:
        node = row.get(name)
        if not node:
            return None
        value = node.get("value")
        return value.strip() if value else None

    children_raw = cell("childrenCount")
    children_count: int | None
    try:
        children_count = int(children_raw) if children_raw is not None else None
    except ValueError:
        children_count = None
    if children_count == 0:
        children_count = None

    return WikidataInfobox(
        qid=person_qid,
        native_name=cell("nativeName"),
        birthplace=cell("birthPlaceLabel"),
        birthplace_qid=_qid_from_uri(cell("birthPlace")),
        religion=cell("religionLabel"),
        spouse=cell("spouseLabel"),
        children_count=children_count,
        residence=cell("residenceLabel"),
        predecessor_qid=_qid_from_uri(cell("predecessor")),
        predecessor_name=cell("predecessorLabel"),
        successor_qid=_qid_from_uri(cell("successor")),
        successor_name=cell("successorLabel"),
        image_filename=_filename_from_image_uri(cell("image")),
        alma_mater=_parse_label_pairs(cell("almaMatersJoined")),
        prior_occupations=_parse_label_pairs(cell("occupationsJoined")),
        military_branches=_parse_label_pairs(cell("militaryJoined")),
    )


def _build_values_clause(qids: Iterable[str]) -> str:
    return " ".join(f"wd:{qid}" for qid in qids if qid)


def fetch_wikidata_infoboxes(
    settings: Settings,
    qids: Iterable[str],
) -> list[WikidataInfobox]:
    """Run the infobox SPARQL query in batches and return parsed rows."""

    deduped_qids = sorted({qid.strip() for qid in qids if qid and qid.strip()})
    if not deduped_qids:
        return []

    results: list[WikidataInfobox] = []
    headers = {
        "User-Agent": (
            f"{settings.user_agent} (capitolexposed.com; contact@capitolexposed.com)"
        ),
        "Accept": "application/sparql-results+json",
    }
    with httpx.Client(timeout=120.0, follow_redirects=True, headers=headers) as client:
        for start in range(0, len(deduped_qids), WIKIDATA_BATCH_SIZE):
            batch = deduped_qids[start : start + WIKIDATA_BATCH_SIZE]
            query = _INFOBOX_SPARQL % {"values": _build_values_clause(batch)}
            response = client.post(
                WIKIDATA_SPARQL_ENDPOINT,
                data={"query": query, "format": "json"},
            )
            response.raise_for_status()
            payload = response.json()
            for row in payload.get("results", {}).get("bindings", []):
                parsed = _parse_row(row)
                if parsed is not None:
                    results.append(parsed)
            time.sleep(WIKIDATA_REQUEST_INTERVAL_SECONDS)
    return results


# ── Persistence ──────────────────────────────────────────────────────────

_UPDATE_SQL = """
UPDATE members SET
    native_name              = COALESCE(%s, native_name),
    birthplace               = COALESCE(%s, birthplace),
    birthplace_qid           = COALESCE(%s, birthplace_qid),
    religion                 = COALESCE(%s, religion),
    spouse                   = COALESCE(%s, spouse),
    children_count           = COALESCE(%s, children_count),
    residence                = COALESCE(%s, residence),
    predecessor_qid          = COALESCE(%s, predecessor_qid),
    predecessor_name         = COALESCE(%s, predecessor_name),
    successor_qid            = COALESCE(%s, successor_qid),
    successor_name           = COALESCE(%s, successor_name),
    wikidata_image_filename  = COALESCE(%s, wikidata_image_filename),
    alma_mater               = CASE WHEN %s::jsonb <> '[]'::jsonb THEN %s::jsonb ELSE alma_mater END,
    prior_occupations        = CASE WHEN %s::jsonb <> '[]'::jsonb THEN %s::jsonb ELSE prior_occupations END,
    military_service         = CASE WHEN %s::jsonb <> '{}'::jsonb THEN %s::jsonb ELSE military_service END,
    wikidata_updated_at      = %s,
    updated_at               = NOW()
WHERE wikidata_qid = %s
"""


def fetch_member_qids(settings: Settings) -> list[tuple[str, str]]:
    """Return ``(bioguide_id, wikidata_qid)`` for every member with a known QID."""

    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT bioguide_id, wikidata_qid
                FROM members
                WHERE wikidata_qid IS NOT NULL AND wikidata_qid <> ''
                ORDER BY bioguide_id
                """
            )
            return [(row["bioguide_id"], row["wikidata_qid"]) for row in cursor.fetchall()]


def upsert_wikidata_infoboxes(
    settings: Settings,
    infoboxes: Iterable[WikidataInfobox],
) -> int:
    """Persist Wikidata infobox rows back to the members table by QID."""

    import json as _json

    now = datetime.now(timezone.utc)
    params: list[tuple] = []
    for ib in infoboxes:
        military_payload = (
            {"branches": ib.military_branches} if ib.military_branches else {}
        )
        alma_json = _json.dumps(ib.alma_mater)
        occ_json = _json.dumps(ib.prior_occupations)
        military_json = _json.dumps(military_payload)
        params.append(
            (
                ib.native_name,
                ib.birthplace,
                ib.birthplace_qid,
                ib.religion,
                ib.spouse,
                ib.children_count,
                ib.residence,
                ib.predecessor_qid,
                ib.predecessor_name,
                ib.successor_qid,
                ib.successor_name,
                ib.image_filename,
                alma_json,
                alma_json,
                occ_json,
                occ_json,
                military_json,
                military_json,
                now,
                ib.qid,
            )
        )
    if not params:
        return 0
    return _chunked_executemany(settings, _UPDATE_SQL, params)
