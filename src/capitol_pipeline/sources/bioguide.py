"""Bioguide profileText importer.

The Library of Congress Bioguide JSON feed (`bioguide.congress.gov/search/bio/
{ID}.json`) carries the official prose biography for every member ever to serve
in the U.S. Congress. The text is U.S. government work — public domain, no
attribution requirement — which makes it safe to ingest, store, and embed for
RAG context (Wikipedia article prose is CC BY-SA and would compel share-alike).

Bioguide is hostile to bots: the endpoint returns 403 to default httpx UAs, and
it rate-limits aggressively. We send a realistic browser UA, throttle to one
request every two seconds, and cache the raw JSON on disk so re-runs are cheap.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
import json
from pathlib import Path
import time
from typing import Any

import httpx

from capitol_pipeline.config import Settings
from capitol_pipeline.exporters.neon import _chunked_executemany, neon_connection


BIOGUIDE_BIO_URL = "https://bioguide.congress.gov/search/bio/{bioguide_id}.json"
BIOGUIDE_REQUEST_INTERVAL_SECONDS = 2.0
BIOGUIDE_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.5 Safari/605.1.15 "
    "(+https://capitolexposed.com; contact@capitolexposed.com)"
)


@dataclass(slots=True)
class BioguideRecord:
    """Parsed slice of the Bioguide JSON response we persist."""

    bioguide_id: str
    bio_text: str | None
    bio_text_html: str | None
    profession_list: list[str]
    source_url: str


class _PlainTextExtractor(HTMLParser):
    """Strip HTML to whitespace-collapsed plain text for storage + embedding."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._chunks: list[str] = []

    def handle_data(self, data: str) -> None:
        text = data.strip()
        if text:
            self._chunks.append(text)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"p", "br", "li", "div"}:
            self._chunks.append("\n")

    def text(self) -> str:
        joined = " ".join(self._chunks)
        return " ".join(joined.split())


def _strip_html(html: str | None) -> str | None:
    if not html:
        return None
    parser = _PlainTextExtractor()
    parser.feed(html)
    return parser.text() or None


def _coerce_profession_list(payload: Any) -> list[str]:
    """Bioguide's `profession` field is sometimes a list, sometimes a delimited string."""

    if not payload:
        return []
    if isinstance(payload, list):
        return [str(item).strip() for item in payload if str(item).strip()]
    if isinstance(payload, str):
        return [chunk.strip() for chunk in payload.split(";") if chunk.strip()]
    return []


def _bioguide_cache_path(settings: Settings, bioguide_id: str) -> Path:
    cache_dir = settings.cache_dir / "bioguide"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"{bioguide_id}.json"


def fetch_bioguide_record(
    client: httpx.Client,
    settings: Settings,
    bioguide_id: str,
    *,
    use_cache: bool = True,
) -> BioguideRecord | None:
    """Fetch one Bioguide JSON record (cached on disk by default)."""

    cache_path = _bioguide_cache_path(settings, bioguide_id)
    payload: dict[str, Any] | None = None

    if use_cache and cache_path.exists() and cache_path.stat().st_size > 0:
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            payload = None

    if payload is None:
        url = BIOGUIDE_BIO_URL.format(bioguide_id=bioguide_id)
        response = client.get(url)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()
        cache_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if not isinstance(payload, dict):
        return None

    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    if not isinstance(data, dict):
        return None

    profile_html = (
        data.get("profileText")
        or data.get("biography")
        or data.get("biographicalText")
    )
    profession = _coerce_profession_list(data.get("profession"))

    return BioguideRecord(
        bioguide_id=bioguide_id.upper(),
        bio_text=_strip_html(profile_html if isinstance(profile_html, str) else None),
        bio_text_html=profile_html if isinstance(profile_html, str) else None,
        profession_list=profession,
        source_url=f"https://bioguide.congress.gov/search/bio/{bioguide_id}",
    )


def fetch_bioguide_records(
    settings: Settings,
    bioguide_ids: Iterable[str],
    *,
    use_cache: bool = True,
) -> Iterable[BioguideRecord]:
    """Iterate Bioguide records with throttling between non-cached calls."""

    headers = {
        "User-Agent": BIOGUIDE_USER_AGENT,
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    with httpx.Client(timeout=30.0, follow_redirects=True, headers=headers) as client:
        for bioguide_id in bioguide_ids:
            cache_hit = (
                use_cache and _bioguide_cache_path(settings, bioguide_id).exists()
            )
            try:
                record = fetch_bioguide_record(
                    client, settings, bioguide_id, use_cache=use_cache
                )
            except httpx.HTTPError:
                record = None
            if record is not None:
                yield record
            if not cache_hit:
                time.sleep(BIOGUIDE_REQUEST_INTERVAL_SECONDS)


# ── Persistence ──────────────────────────────────────────────────────────

_UPDATE_SQL = """
UPDATE members SET
    bio_text         = COALESCE(%s, bio_text),
    bio_text_html    = COALESCE(%s, bio_text_html),
    bio_source_url   = %s,
    bio_updated_at   = %s,
    profession_list  = CASE WHEN %s::jsonb <> '[]'::jsonb THEN %s::jsonb ELSE profession_list END,
    updated_at       = NOW()
WHERE bioguide_id = %s
"""


def fetch_member_bioguides(
    settings: Settings,
    *,
    only_missing: bool = True,
    limit: int | None = None,
) -> list[str]:
    """Return bioguide IDs to enrich. By default only members without bio_text."""

    where = "WHERE bio_text IS NULL OR bio_text = ''" if only_missing else ""
    limit_clause = f"LIMIT {int(limit)}" if limit and limit > 0 else ""
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT bioguide_id FROM members
                {where}
                ORDER BY bioguide_id
                {limit_clause}
                """
            )
            return [row["bioguide_id"] for row in cursor.fetchall()]


def upsert_bioguide_records(
    settings: Settings,
    records: Iterable[BioguideRecord],
) -> int:
    now = datetime.now(timezone.utc)
    params: list[tuple] = []
    for record in records:
        profession_json = json.dumps(record.profession_list)
        params.append(
            (
                record.bio_text,
                record.bio_text_html,
                record.source_url,
                now,
                profession_json,
                profession_json,
                record.bioguide_id,
            )
        )
    if not params:
        return 0
    return _chunked_executemany(settings, _UPDATE_SQL, params)
