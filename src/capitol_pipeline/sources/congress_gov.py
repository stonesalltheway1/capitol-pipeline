"""Official Congress.gov API adapter for legislative context."""

from __future__ import annotations

from datetime import datetime
import hashlib
import html
import re
import time
from typing import Any

import httpx

from capitol_pipeline.config import Settings
from capitol_pipeline.models.legislation import (
    CongressBillActionRecord,
    CongressBillRecord,
    CongressBillSummaryRecord,
)


HTML_TAG_RE = re.compile(r"<[^>]+>")
CONTROL_WHITESPACE_RE = re.compile(r"[\r\n\t]+")


def normalize_bill_type(value: str | None) -> str:
    """Normalize a bill type into the lowercase format Congress.gov expects."""

    return (value or "").strip().lower()


def strip_congress_html(value: str | None) -> str:
    """Convert Congress.gov HTML fragments into plain text."""

    if not value:
        return ""
    text = html.unescape(value.replace("&nbsp;", " "))
    text = HTML_TAG_RE.sub(" ", text)
    text = CONTROL_WHITESPACE_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_date(value: Any) -> str | None:
    """Convert Congress.gov timestamps into YYYY-MM-DD."""

    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    text = str(value).strip()
    if not text:
        return None
    if "T" in text:
        return text.split("T", 1)[0]
    if " " in text:
        return text.split(" ", 1)[0]
    return text[:10]


def normalize_timestamp(value: Any) -> str | None:
    """Keep Congress.gov update timestamps in a stable string form."""

    if value is None:
        return None
    text = str(value).strip()
    return text or None


def make_bill_key(congress: int, bill_type: str, bill_number: str) -> str:
    """Return the stable bill key used across pipeline tables."""

    return f"{int(congress)}-{normalize_bill_type(bill_type)}-{str(bill_number).strip()}"


class CongressGovApiClient:
    """Rate-limited HTTP client for the official Congress.gov API."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._last_request_at = 0.0

    def _throttle(self) -> None:
        delay = max(0.0, float(self.settings.congress_gov_request_interval_seconds))
        if delay <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        remaining = delay - elapsed
        if remaining > 0:
            time.sleep(remaining)

    def _request_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        max_attempts: int = 4,
    ) -> dict[str, Any]:
        """Execute one Congress.gov request with polite retry behavior."""

        request_params = {
            "format": "json",
            "api_key": self.settings.resolved_congress_api_key,
            **(params or {}),
        }
        last_error: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            self._throttle()
            try:
                with httpx.Client(
                    timeout=60.0,
                    follow_redirects=True,
                    headers={"User-Agent": self.settings.user_agent},
                ) as client:
                    response = client.get(
                        f"{self.settings.congress_gov_base_url.rstrip('/')}/{path.lstrip('/')}",
                        params=request_params,
                    )
                    self._last_request_at = time.monotonic()
                    response.raise_for_status()
                    body = response.json()
                    return body if isinstance(body, dict) else {}
            except httpx.HTTPStatusError as error:
                last_error = error
                status_code = error.response.status_code if error.response is not None else None
                if status_code == 429 and error.response is not None:
                    retry_after = error.response.headers.get("Retry-After")
                    retry_delay = float(retry_after) if retry_after and retry_after.isdigit() else 0.0
                    if retry_delay > 0 and retry_delay <= 20 and attempt < max_attempts:
                        time.sleep(retry_delay)
                        continue
                if status_code not in {429, 500, 502, 503, 504} or attempt >= max_attempts:
                    raise
            except httpx.HTTPError as error:
                last_error = error
                if attempt >= max_attempts:
                    raise
            time.sleep(min(8, attempt * 2))
        if last_error:
            raise last_error
        return {}

    def fetch_bill_detail(self, congress: int, bill_type: str, bill_number: str) -> dict[str, Any]:
        """Fetch one bill detail record."""

        return self._request_json(f"bill/{int(congress)}/{normalize_bill_type(bill_type)}/{bill_number}")

    def fetch_bill_summaries(
        self,
        congress: int,
        bill_type: str,
        bill_number: str,
        *,
        page_size: int = 100,
    ) -> list[dict[str, Any]]:
        """Fetch every available official bill summary version."""

        return self._fetch_paginated(
            f"bill/{int(congress)}/{normalize_bill_type(bill_type)}/{bill_number}/summaries",
            item_key="summaries",
            page_size=page_size,
        )

    def fetch_bill_actions(
        self,
        congress: int,
        bill_type: str,
        bill_number: str,
        *,
        page_size: int = 250,
    ) -> list[dict[str, Any]]:
        """Fetch bill actions, newest first."""

        return self._fetch_paginated(
            f"bill/{int(congress)}/{normalize_bill_type(bill_type)}/{bill_number}/actions",
            item_key="actions",
            page_size=page_size,
        )

    def _fetch_paginated(
        self,
        path: str,
        *,
        item_key: str,
        page_size: int,
    ) -> list[dict[str, Any]]:
        """Collect all rows from one paginated Congress.gov endpoint."""

        items: list[dict[str, Any]] = []
        offset = 0
        total = None
        while total is None or offset < total:
            payload = self._request_json(
                path,
                params={
                    "limit": max(1, page_size),
                    "offset": offset,
                },
            )
            page_items = payload.get(item_key)
            if not isinstance(page_items, list) or not page_items:
                break
            items.extend(item for item in page_items if isinstance(item, dict))
            pagination = payload.get("pagination") or {}
            total = int(pagination.get("count") or len(items))
            offset += len(page_items)
            if len(page_items) < page_size:
                break
        return items


def build_congress_bill_record(
    *,
    site_bill_row: dict[str, Any],
    bill_payload: dict[str, Any],
    summaries: list[dict[str, Any]],
    actions: list[dict[str, Any]],
) -> CongressBillRecord:
    """Build one canonical Congress.gov bill record."""

    bill = bill_payload.get("bill") if isinstance(bill_payload.get("bill"), dict) else {}
    congress = int(bill.get("congress") or site_bill_row.get("congress") or 0)
    bill_type = normalize_bill_type(str(bill.get("type") or site_bill_row.get("bill_type") or ""))
    bill_number = str(bill.get("number") or site_bill_row.get("number") or "").strip()
    bill_key = make_bill_key(congress, bill_type, bill_number)
    summary_records = build_congress_bill_summary_records(
        site_bill_id=str(site_bill_row.get("id") or "").strip(),
        congress=congress,
        bill_type=bill_type,
        bill_number=bill_number,
        bill_key=bill_key,
        source_url=str(bill.get("legislationUrl") or "").strip() or None,
        items=summaries,
    )
    action_records = build_congress_bill_action_records(
        site_bill_id=str(site_bill_row.get("id") or "").strip(),
        congress=congress,
        bill_type=bill_type,
        bill_number=bill_number,
        bill_key=bill_key,
        source_url=str(bill.get("legislationUrl") or "").strip() or None,
        items=actions,
    )
    latest_summary_text = summary_records[0].text if summary_records else None
    latest_action = action_records[0] if action_records else None
    committee_names = list(
        dict.fromkeys(
            committee
            for action in action_records
            for committee in action.committee_names
            if committee
        )
    )
    committee_codes = list(
        dict.fromkeys(
            code
            for action in action_records
            for code in action.committee_codes
            if code
        )
    )
    subjects = [str(subject).strip() for subject in (site_bill_row.get("subjects") or []) if str(subject).strip()]
    content_parts = [
        latest_summary_text,
        latest_action.text if latest_action else None,
        f"Policy area: {bill.get('policyArea', {}).get('name')}" if isinstance(bill.get("policyArea"), dict) else None,
        f"Committees: {', '.join(committee_names)}" if committee_names else None,
        f"Subjects: {', '.join(subjects)}" if subjects else None,
    ]
    sponsors = bill.get("sponsors") if isinstance(bill.get("sponsors"), list) else []
    primary_sponsor = sponsors[0] if sponsors and isinstance(sponsors[0], dict) else {}
    return CongressBillRecord(
        bill_key=bill_key,
        site_bill_id=str(site_bill_row.get("id") or "").strip(),
        congress=congress,
        bill_type=bill_type,
        bill_number=bill_number,
        title=str(bill.get("title") or site_bill_row.get("title") or "").strip(),
        short_title=str(site_bill_row.get("short_title") or "").strip() or None,
        origin_chamber=str(bill.get("originChamber") or "").strip() or None,
        policy_area=str((bill.get("policyArea") or {}).get("name") or "").strip() or None,
        sponsor_bioguide_id=str(primary_sponsor.get("bioguideId") or "").strip() or None,
        sponsor_full_name=str(primary_sponsor.get("fullName") or "").strip() or None,
        sponsor_party=str(primary_sponsor.get("party") or "").strip() or None,
        sponsor_state=str(primary_sponsor.get("state") or "").strip() or None,
        sponsor_district=str(primary_sponsor.get("district") or "").strip() or None,
        introduced_date=normalize_date(bill.get("introducedDate") or site_bill_row.get("introduced_date")),
        latest_action_date=normalize_date((bill.get("latestAction") or {}).get("actionDate")),
        latest_action_text=str((bill.get("latestAction") or {}).get("text") or "").strip() or None,
        update_date=normalize_timestamp(bill.get("updateDate")),
        update_date_including_text=normalize_timestamp(bill.get("updateDateIncludingText")),
        legislation_url=str(bill.get("legislationUrl") or "").strip() or None,
        api_url=(
            f"https://api.congress.gov/v3/bill/{congress}/{bill_type}/{bill_number}?format=json"
            if congress and bill_type and bill_number
            else None
        ),
        summaries_count=len(summary_records),
        actions_count=len(action_records),
        committees_count=int(((bill.get("committees") or {}).get("count")) or len(committee_names)),
        cosponsors_count=int(((bill.get("cosponsors") or {}).get("count")) or 0),
        subject_count=int(((bill.get("subjects") or {}).get("count")) or len(subjects)),
        text_version_count=int(((bill.get("textVersions") or {}).get("count")) or 0),
        summary=latest_summary_text,
        content="\n\n".join(part for part in content_parts if part),
        committee_names=committee_names,
        committee_codes=committee_codes,
        subjects=subjects,
        metadata={
            "latestAction": bill.get("latestAction") or {},
            "constitutionalAuthorityStatementText": strip_congress_html(
                str(bill.get("constitutionalAuthorityStatementText") or "")
            )
            or None,
            "committeeReports": bill.get("committeeReports") or [],
            "cboCostEstimates": bill.get("cboCostEstimates") or [],
            "sponsors": sponsors,
        },
    )


def build_congress_bill_summary_records(
    *,
    site_bill_id: str,
    congress: int,
    bill_type: str,
    bill_number: str,
    bill_key: str,
    source_url: str | None,
    items: list[dict[str, Any]],
) -> list[CongressBillSummaryRecord]:
    """Normalize official bill summary versions."""

    records: list[CongressBillSummaryRecord] = []
    for item in items:
        text = strip_congress_html(str(item.get("text") or ""))
        if not text:
            continue
        version_code = str(item.get("versionCode") or "").strip() or None
        action_date = normalize_date(item.get("actionDate"))
        update_date = normalize_timestamp(item.get("updateDate"))
        stable_bits = [bill_key, version_code or "", action_date or "", update_date or "", text[:120]]
        summary_key = hashlib.sha1("|".join(stable_bits).encode("utf-8")).hexdigest()
        records.append(
            CongressBillSummaryRecord(
                summary_key=summary_key,
                bill_key=bill_key,
                site_bill_id=site_bill_id,
                congress=congress,
                bill_type=bill_type,
                bill_number=bill_number,
                version_code=version_code,
                action_date=action_date,
                action_desc=str(item.get("actionDesc") or "").strip() or None,
                update_date=update_date,
                text=text,
                source_url=source_url,
                metadata={key: value for key, value in item.items() if key != "text"},
            )
        )
    records.sort(
        key=lambda record: (
            record.action_date or "",
            record.update_date or "",
            record.version_code or "",
        ),
        reverse=True,
    )
    return records


def build_congress_bill_action_records(
    *,
    site_bill_id: str,
    congress: int,
    bill_type: str,
    bill_number: str,
    bill_key: str,
    source_url: str | None,
    items: list[dict[str, Any]],
) -> list[CongressBillActionRecord]:
    """Normalize official bill actions."""

    records: list[CongressBillActionRecord] = []
    for item in items:
        text = strip_congress_html(str(item.get("text") or ""))
        if not text:
            continue
        committees = item.get("committees") if isinstance(item.get("committees"), list) else []
        committee_names = [
            str(committee.get("name") or "").strip()
            for committee in committees
            if isinstance(committee, dict) and str(committee.get("name") or "").strip()
        ]
        committee_codes = [
            str(committee.get("systemCode") or "").strip()
            for committee in committees
            if isinstance(committee, dict) and str(committee.get("systemCode") or "").strip()
        ]
        action_date = normalize_date(item.get("actionDate"))
        action_time = str(item.get("actionTime") or "").strip() or None
        action_code = str(item.get("actionCode") or "").strip() or None
        action_type = str(item.get("type") or "").strip() or None
        stable_bits = [bill_key, action_date or "", action_time or "", action_code or "", text[:160]]
        action_key = hashlib.sha1("|".join(stable_bits).encode("utf-8")).hexdigest()
        source_system = item.get("sourceSystem") if isinstance(item.get("sourceSystem"), dict) else {}
        source_system_code = source_system.get("code")
        records.append(
            CongressBillActionRecord(
                action_key=action_key,
                bill_key=bill_key,
                site_bill_id=site_bill_id,
                congress=congress,
                bill_type=bill_type,
                bill_number=bill_number,
                action_date=action_date,
                action_time=action_time,
                action_code=action_code,
                action_type=action_type,
                text=text,
                source_system_name=str(source_system.get("name") or "").strip() or None,
                source_system_code=int(source_system_code) if isinstance(source_system_code, int) else None,
                committee_names=committee_names,
                committee_codes=committee_codes,
                source_url=source_url,
                metadata={
                    key: value
                    for key, value in item.items()
                    if key not in {"text", "committees", "sourceSystem"}
                },
            )
        )
    records.sort(
        key=lambda record: (record.action_date or "", record.action_time or ""),
        reverse=True,
    )
    return records
