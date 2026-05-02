"""Wikipedia REST API summary importer.

Replaces the Bioguide profileText importer for the prose-bio column. The
Library of Congress Bioguide endpoint (`bioguide.congress.gov/search/bio/...`)
returns 403 to every non-browser request behind an Akamai bot gate, so it's
unusable for unattended ingestion.

Wikipedia's REST API is the practical alternative: it returns a 2-3 sentence
plain-text ``extract`` plus ``extract_html`` for any article title, served
under CC BY-SA 4.0. We cite the Wikipedia article URL as the source so the
attribution requirement is satisfied via UI link-back.

Endpoint: ``https://en.wikipedia.org/api/rest_v1/page/summary/{title}``
Rate limit: 200 req/s anonymous; we politely throttle to ~5 req/s.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import time
from urllib.parse import quote

import httpx

from capitol_pipeline.config import Settings
from capitol_pipeline.exporters.neon import _chunked_executemany, neon_connection


WIKIPEDIA_SUMMARY_URL = "https://en.wikipedia.org/api/rest_v1/page/summary/{title}"
WIKIPEDIA_REQUEST_INTERVAL_SECONDS = 0.2


@dataclass(slots=True)
class WikipediaSummary:
    """One member's lead-section extract from Wikipedia."""

    bioguide_id: str
    bio_text: str | None
    bio_text_html: str | None
    source_url: str


def fetch_wikipedia_summaries(
    settings: Settings,
    members: Iterable[tuple[str, str]],  # (bioguide_id, wikipedia_slug)
) -> Iterable[WikipediaSummary]:
    """Stream Wikipedia summaries for the given (bioguide_id, slug) pairs."""

    headers = {
        "User-Agent": (
            f"{settings.user_agent} (capitolexposed.com; contact@capitolexposed.com)"
        ),
        "Accept": "application/json",
        "Api-User-Agent": (
            f"{settings.user_agent} (capitolexposed.com; contact@capitolexposed.com)"
        ),
    }
    with httpx.Client(timeout=30.0, follow_redirects=True, headers=headers) as client:
        for bioguide_id, wikipedia_slug in members:
            if not wikipedia_slug:
                continue
            url = WIKIPEDIA_SUMMARY_URL.format(title=quote(wikipedia_slug, safe="_()"))
            try:
                response = client.get(url)
            except httpx.HTTPError:
                continue
            if response.status_code != 200:
                # 404 = redirected/disambiguation, skip silently
                continue
            payload = response.json()
            extract = payload.get("extract") if isinstance(payload, dict) else None
            extract_html = (
                payload.get("extract_html") if isinstance(payload, dict) else None
            )
            article_url = (
                payload.get("content_urls", {})
                .get("desktop", {})
                .get("page")
                if isinstance(payload, dict)
                else None
            ) or f"https://en.wikipedia.org/wiki/{wikipedia_slug}"
            if extract:
                yield WikipediaSummary(
                    bioguide_id=bioguide_id.upper(),
                    bio_text=extract,
                    bio_text_html=extract_html,
                    source_url=article_url,
                )
            time.sleep(WIKIPEDIA_REQUEST_INTERVAL_SECONDS)


# ── Persistence ──────────────────────────────────────────────────────────

_UPDATE_SQL = """
UPDATE members SET
    bio_text       = COALESCE(%s, bio_text),
    bio_text_html  = COALESCE(%s, bio_text_html),
    bio_source_url = %s,
    bio_updated_at = %s,
    updated_at     = NOW()
WHERE bioguide_id = %s
"""


def fetch_member_wiki_pairs(
    settings: Settings,
    *,
    only_missing: bool = True,
) -> list[tuple[str, str]]:
    """Return ``(bioguide_id, wikipedia_slug)`` pairs for members to enrich."""

    where = "AND (bio_text IS NULL OR bio_text = '')" if only_missing else ""
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT bioguide_id, wikipedia_slug
                FROM members
                WHERE wikipedia_slug IS NOT NULL AND wikipedia_slug <> ''
                {where}
                ORDER BY bioguide_id
                """
            )
            return [
                (row["bioguide_id"], row["wikipedia_slug"])
                for row in cursor.fetchall()
            ]


def upsert_wikipedia_summaries(
    settings: Settings,
    summaries: Iterable[WikipediaSummary],
) -> int:
    now = datetime.now(timezone.utc)
    params: list[tuple] = []
    for s in summaries:
        params.append(
            (
                s.bio_text,
                s.bio_text_html,
                s.source_url,
                now,
                s.bioguide_id,
            )
        )
    if not params:
        return 0
    return _chunked_executemany(settings, _UPDATE_SQL, params)
