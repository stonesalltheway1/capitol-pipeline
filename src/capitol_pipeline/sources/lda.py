"""LDA.gov Lobbying Disclosure Act API adapter.

Ingests LD-1/LD-2 filings and LD-203 contribution reports from the
official Senate Lobbying Disclosure API.

Endpoints:
  - /api/v1/filings/         LD-1 (registrations) + LD-2 (quarterly reports)
  - /api/v1/contributions/   LD-203 (lobbyist contribution reports)
  - /api/v1/registrants/     Lobbying firm details
  - /api/v1/clients/         Client (company) details

Auth: Token-based, header: Authorization: Token <key>
Rate limit: 120 requests/min with API key
Pagination: 25 results/page max, requires filter param for pages > 1
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Any

import httpx

from capitol_pipeline.config import Settings

logger = logging.getLogger(__name__)

LDA_BASE = "https://lda.gov/api/v1"
PAGE_SIZE = 25
RATE_DELAY = 0.55  # ~109 req/min (under 120 limit)


def _build_client(settings: Settings, timeout: float = 30.0) -> httpx.Client:
    token = settings.lda_api_token
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Token {token}"
    return httpx.Client(
        base_url=LDA_BASE,
        headers=headers,
        timeout=timeout,
        follow_redirects=True,
    )


def _paginate(
    client: httpx.Client,
    path: str,
    params: dict[str, Any],
    *,
    max_pages: int = 100,
) -> list[dict[str, Any]]:
    """Fetch all pages from a paginated LDA endpoint."""
    all_results: list[dict[str, Any]] = []
    params = {**params, "page_size": PAGE_SIZE, "page": 1}

    for page_num in range(1, max_pages + 1):
        params["page"] = page_num
        time.sleep(RATE_DELAY)

        resp = client.get(path, params=params)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            logger.warning("Rate limited, waiting %ds", retry_after)
            time.sleep(retry_after)
            resp = client.get(path, params=params)

        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        all_results.extend(results)

        total = data.get("count", 0)
        logger.info(
            "  Page %d: %d results (total: %d, fetched: %d)",
            page_num, len(results), total, len(all_results),
        )

        if not data.get("next") or len(all_results) >= total:
            break

    return all_results


# ── Filings (LD-1 / LD-2) ───────────────────────────────────────────────


def fetch_filings(
    settings: Settings,
    *,
    filing_year: int,
    filing_period: str | None = None,
    max_pages: int = 100,
) -> list[dict[str, Any]]:
    """Fetch LD-1/LD-2 lobbying filings for a given year.

    filing_period: 'first_quarter', 'second_quarter', 'third_quarter', 'fourth_quarter', 'mid_year', 'year_end'
    """
    params: dict[str, Any] = {"filing_year": filing_year}
    if filing_period:
        params["filing_period"] = filing_period

    with _build_client(settings) as client:
        return _paginate(client, "/filings/", params, max_pages=max_pages)


def normalize_filing(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize an LDA filing into our lobbying_filings schema."""
    registrant = raw.get("registrant") or {}
    client_obj = raw.get("client") or {}

    # Extract lobbying activities
    activities = raw.get("lobbying_activities", [])
    issue_codes = []
    specific_issues = []
    lobbyist_names = []
    gov_entities = []

    for act in activities:
        if act.get("general_issue_code"):
            issue_codes.append(act["general_issue_code"])
        if act.get("description"):
            specific_issues.append(act["description"])
        for lob in act.get("lobbyists", []):
            name = f"{lob.get('first_name', '')} {lob.get('last_name', '')}".strip()
            if name:
                lobbyist_names.append(name)
        for entity in act.get("government_entities", []):
            if entity.get("name"):
                gov_entities.append(entity["name"])

    filing_uuid = raw.get("filing_uuid", "")
    filing_id = f"lda-{filing_uuid[:36]}" if filing_uuid else f"lda-{hashlib.md5(str(raw).encode()).hexdigest()[:24]}"

    income = raw.get("income")
    expenses = raw.get("expenses")
    amount = int(income or expenses or 0)

    filing_type = raw.get("filing_type_display", raw.get("filing_type", ""))
    period = raw.get("filing_period_display", raw.get("filing_period", ""))
    year = raw.get("filing_year")
    filing_period_str = f"{year}_{raw.get('filing_period', '')}" if year else period

    return {
        "id": filing_id,
        "registrant_name": registrant.get("name", ""),
        "registrant_id": str(registrant.get("id", "")),
        "client_name": client_obj.get("name", ""),
        "client_id": str(client_obj.get("id", "")),
        "company_id": None,  # resolved later via company matching
        "amount": amount,
        "issue_codes": list(set(issue_codes)),
        "specific_bills": specific_issues[:10],  # Cap to avoid bloat
        "lobbyists": list(set(lobbyist_names)),
        "filing_period": filing_period_str,
        "filing_date": (raw.get("dt_posted") or "")[:10] or None,
        "filing_type": filing_type,
        "gov_entities": list(set(gov_entities)),
    }


# ── Contributions (LD-203) ───────────────────────────────────────────────


def fetch_contributions(
    settings: Settings,
    *,
    filing_year: int,
    filing_period: str | None = None,
    max_pages: int = 200,
) -> list[dict[str, Any]]:
    """Fetch LD-203 contribution reports for a given year."""
    params: dict[str, Any] = {"filing_year": filing_year}
    if filing_period:
        params["filing_period"] = filing_period

    with _build_client(settings) as client:
        return _paginate(client, "/contributions/", params, max_pages=max_pages)


def normalize_contribution(raw: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize an LD-203 contribution report.

    Returns None if the report has no actual contributions (no_contributions=true).
    """
    if raw.get("no_contributions"):
        return None

    registrant = raw.get("registrant") or {}
    lobbyist = raw.get("lobbyist") or {}
    filing_uuid = raw.get("filing_uuid", "")

    lobbyist_name = " ".join(filter(None, [
        lobbyist.get("first_name", ""),
        lobbyist.get("last_name", ""),
    ])).strip()

    items = []
    for item in raw.get("contribution_items", []):
        items.append({
            "amount": item.get("amount"),
            "recipient_name": item.get("recipient_name", ""),
            "contribution_type": item.get("contribution_type_display", item.get("contribution_type", "")),
            "date": item.get("date"),
        })

    for pac in raw.get("pacs", []):
        if isinstance(pac, dict):
            items.append({
                "amount": pac.get("amount"),
                "recipient_name": pac.get("name", ""),
                "contribution_type": "PAC",
                "date": None,
            })
        elif isinstance(pac, str) and pac.strip():
            items.append({
                "amount": None,
                "recipient_name": pac.strip(),
                "contribution_type": "PAC",
                "date": None,
            })

    if not items:
        return None

    return {
        "id": f"lda-contrib-{filing_uuid[:36]}",
        "registrant_name": registrant.get("name", ""),
        "registrant_id": str(registrant.get("id", "")),
        "lobbyist_name": lobbyist_name,
        "filing_year": raw.get("filing_year"),
        "filing_period": raw.get("filing_period_display", ""),
        "filing_date": (raw.get("dt_posted") or "")[:10] or None,
        "contribution_items": items,
    }
