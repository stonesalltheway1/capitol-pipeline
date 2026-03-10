"""Official USAspending API adapter."""

from __future__ import annotations

from collections import Counter
import csv
from datetime import date
import html
import hashlib
import io
import re
import time
from typing import Any
import zipfile

import httpx

from capitol_pipeline.config import Settings
from capitol_pipeline.models.usaspending import (
    UsaspendingAwardRecord,
    UsaspendingCompanyMatchRecord,
    UsaspendingRecipientRecord,
)
from capitol_pipeline.registries.members import normalize_member_lookup_value


DEFAULT_USASPENDING_START_DATE = "2020-01-01"
DEFAULT_USASPENDING_END_DATE = date.today().isoformat()
DEFAULT_USASPENDING_CONTRACT_AWARD_TYPE_CODES = ["A", "B", "C", "D"]
DEFAULT_USASPENDING_IDV_AWARD_TYPE_CODES = [
    "IDV_A",
    "IDV_B",
    "IDV_B_A",
    "IDV_B_B",
    "IDV_B_C",
    "IDV_C",
    "IDV_D",
    "IDV_E",
]


CORPORATE_SUFFIX_RE = re.compile(
    r"\b("
    r"incorporated|inc|corp|corporation|co|company|limited|ltd|llc|lp|plc|holdings|holding|group"
    r")\.?\b",
    re.IGNORECASE,
)
TRAILING_SECURITY_RE = re.compile(
    r"\b("
    r"common stock|common|ordinary shares|class [a-z0-9-]+|series [a-z0-9-]+"
    r")\b.*$",
    re.IGNORECASE,
)
CONTROL_WHITESPACE_RE = re.compile(r"[\r\n\t]+")
LEADING_AMOUNT_RE = re.compile(r"^\$[\d,\s.-]+\s+")
TRAILING_PARENS_RE = re.compile(r"\s*\((?:NASDAQ|NYSE|AMEX|OTC|CRYPTO:[A-Z]+|[A-Z.]{1,10})\)\s*$")
GENERIC_COMPANY_TOKENS = {
    "and",
    "the",
    "holdings",
    "holding",
    "group",
    "technologies",
    "technology",
    "systems",
    "system",
}

USASPENDING_COMPANY_ALIASES_BY_TICKER: dict[str, tuple[str, ...]] = {
    "AVGO": ("AVAGO TECHNOLOGIES", "CA, INC.", "VMWARE"),
    "CVS": ("CVS PHARMACY", "CAREMARK", "MINUTECLINIC", "CORAM"),
    "JNJ": ("JANSSEN", "ETHICON", "DEPUY"),
    "JPM": ("JPMORGAN CHASE BANK", "J. P. MORGAN", "JPMORGAN"),
    "MRK": ("MERCK SHARP", "MERCK SHARP & DOHME", "MSD"),
    "ORCL": ("ORACLE AMERICA", "ORACLE USA", "ORACLE CORPORATION"),
    "T": ("AT&T", "AT&T CORP", "AT&T SERVICES"),
    "UNH": ("OPTUM", "UNITED HEALTHCARE", "OPTUMSERVE", "LEWIN GROUP"),
    "VZ": ("VERIZON BUSINESS", "CELLCO PARTNERSHIP", "VERIZON WIRELESS"),
}

USASPENDING_COMPANY_ALIASES_BY_NAME: dict[str, tuple[str, ...]] = {
    "AT T": ("AT&T", "AT&T CORP", "AT&T SERVICES"),
    "BROADCOM": ("AVAGO TECHNOLOGIES", "CA, INC.", "VMWARE"),
    "CVS HEALTH": ("CVS PHARMACY", "CAREMARK", "MINUTECLINIC", "CORAM"),
    "JOHNSON JOHNSON": ("JANSSEN", "ETHICON", "DEPUY"),
    "JPMORGAN CHASE": ("JPMORGAN CHASE BANK", "J. P. MORGAN", "JPMORGAN"),
    "MERCK": ("MERCK SHARP", "MERCK SHARP & DOHME", "MSD"),
    "ORACLE": ("ORACLE AMERICA", "ORACLE USA", "ORACLE CORPORATION"),
    "UNITEDHEALTH": ("OPTUM", "UNITED HEALTHCARE", "OPTUMSERVE", "LEWIN GROUP"),
    "VERIZON": ("VERIZON BUSINESS", "CELLCO PARTNERSHIP", "VERIZON WIRELESS"),
}


def normalize_usaspending_value(value: object) -> str | None:
    """Normalize a USAspending field into trimmed text."""

    if value is None:
        return None
    text = str(value).strip()
    return text or None


def coerce_optional_float(value: object) -> float | None:
    """Convert a mixed USAspending amount value into float when possible."""

    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def coerce_optional_int(value: object) -> int | None:
    """Convert a mixed USAspending id value into int when possible."""

    if value in (None, ""):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def normalize_company_query_name(raw_name: str | None, *, ticker: str | None = None) -> str | None:
    """Clean a noisy company or asset label into a usable USAspending search query."""

    normalized = normalize_usaspending_value(raw_name)
    if not normalized:
        return None

    text = CONTROL_WHITESPACE_RE.sub(" ", html.unescape(normalized))
    text = text.replace("\u00a0", " ").strip()
    text = LEADING_AMOUNT_RE.sub("", text).strip()
    if ">" in text:
        text = text.split(">")[-1].strip()
    if re.search(r"\b(trust|brokerage|account|401\(k\)|ira|retirement)\b", text, re.IGNORECASE) and ") " in text:
        text = text.rsplit(") ", 1)[-1].strip()

    if ticker:
        ticker_pattern = re.compile(
            rf"(.+?)\s+\((?:{re.escape(ticker.upper())}|NASDAQ|NYSE|AMEX|OTC)\)\s*$",
            re.IGNORECASE,
        )
        ticker_match = ticker_pattern.search(text)
        if ticker_match:
            text = ticker_match.group(1).strip()

    parts = [part.strip() for part in re.split(r"\s{2,}", text) if part.strip()]
    if len(parts) > 1:
        text = parts[-1]

    if "\n" in normalized or "\r" in normalized:
        line_candidates = [
            line.strip()
            for line in normalized.splitlines()
            if line and line.strip() and len(line.strip()) > 2
        ]
        for line in reversed(line_candidates):
            cleaned_line = CONTROL_WHITESPACE_RE.sub(" ", line).strip()
            if ticker and f"({ticker.upper()})" in cleaned_line.upper():
                cleaned_line = cleaned_line.rsplit("(", 1)[0].strip()
            cleaned_line = TRAILING_SECURITY_RE.sub("", cleaned_line).strip(" -")
            cleaned_line = TRAILING_PARENS_RE.sub("", cleaned_line).strip()
            if cleaned_line and not cleaned_line.lower().startswith("direction.") and "portfolio rebalance" not in cleaned_line.lower():
                text = cleaned_line
                break

    text = TRAILING_SECURITY_RE.sub("", text).strip(" -")
    text = TRAILING_PARENS_RE.sub("", text).strip()
    text = re.sub(r"\s+", " ", text).strip(" ,.-")
    return text or None


def strip_corporate_suffixes(name: str | None) -> str | None:
    """Remove common corporate suffixes to create a looser search variant."""

    normalized = normalize_usaspending_value(name)
    if not normalized:
        return None
    stripped = CORPORATE_SUFFIX_RE.sub(" ", normalized)
    stripped = re.sub(r"\s+", " ", stripped).strip(" ,.-")
    return stripped or None


def normalize_alias_lookup_key(raw_name: str | None) -> str | None:
    """Create a stable alias-map lookup key for one company name."""

    normalized = strip_corporate_suffixes(raw_name) or normalize_usaspending_value(raw_name)
    if not normalized:
        return None
    cleaned = normalize_member_lookup_value(normalized)
    cleaned = cleaned.replace("&", " ")
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned.upper() or None


def expand_company_query_variants(name: str) -> list[str]:
    """Generate a few conservative text variants for one query."""

    variants = [name]
    if "&" in name:
        variants.append(name.replace("&", "AND"))
        variants.append(name.replace("&", " "))
    if "." in name:
        variants.append(name.replace(".", " "))
    if "," in name:
        variants.append(name.replace(",", " "))

    deduped: list[str] = []
    seen: set[str] = set()
    for variant in variants:
        cleaned = re.sub(r"\s+", " ", variant).strip(" ,.-")
        if not cleaned:
            continue
        lowered = cleaned.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(cleaned)
    return deduped


def build_company_search_queries(
    *,
    raw_name: str | None,
    asset_description: str | None = None,
    ticker: str | None = None,
) -> list[str]:
    """Build a small, stable query set for one site company."""

    candidates: list[str] = []
    for source in (asset_description, raw_name):
        cleaned = normalize_company_query_name(source, ticker=ticker)
        if cleaned:
            candidates.extend(expand_company_query_variants(cleaned))
            stripped = strip_corporate_suffixes(cleaned)
            if stripped and stripped.lower() != cleaned.lower():
                candidates.extend(expand_company_query_variants(stripped))

    if ticker:
        candidates.extend(USASPENDING_COMPANY_ALIASES_BY_TICKER.get(ticker.upper(), ()))
    alias_key = normalize_alias_lookup_key(raw_name)
    if alias_key:
        candidates.extend(USASPENDING_COMPANY_ALIASES_BY_NAME.get(alias_key, ()))

    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        lowered = candidate.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(candidate)
    return deduped


def tokenize_company_name(raw_name: str | None) -> list[str]:
    """Turn a company-like name into meaningful matching tokens."""

    normalized = strip_corporate_suffixes(raw_name) or normalize_usaspending_value(raw_name)
    if not normalized:
        return []
    return [
        token
        for token in normalize_member_lookup_value(normalized).split()
        if token and token not in GENERIC_COMPANY_TOKENS and len(token) > 1
    ]


def score_recipient_match(query_name: str, recipient_name: str) -> int:
    """Return a conservative match score for one query and recipient candidate."""

    normalized_query = normalize_member_lookup_value(query_name)
    normalized_recipient = normalize_member_lookup_value(recipient_name)
    stripped_query = normalize_member_lookup_value(strip_corporate_suffixes(query_name) or query_name)
    stripped_recipient = normalize_member_lookup_value(
        strip_corporate_suffixes(recipient_name) or recipient_name
    )
    if not normalized_query or not normalized_recipient:
        return 0
    if stripped_query and stripped_query == stripped_recipient:
        return 100
    if normalized_query == normalized_recipient:
        return 95

    query_tokens = tokenize_company_name(query_name)
    recipient_tokens = tokenize_company_name(recipient_name)
    if not query_tokens or not recipient_tokens:
        return 0

    overlap = [token for token in query_tokens if token in recipient_tokens]
    if len(query_tokens) >= 2 and len(overlap) == len(query_tokens):
        return 85
    if len(overlap) >= 2:
        overlap_ratio = len(overlap) / len(query_tokens)
        if overlap_ratio >= 0.8:
            return 75
        if overlap_ratio >= 0.6:
            return 65
    if len(overlap) == 1 and len(query_tokens) == 1:
        return 55
    return 0


def candidate_profile_names(
    row: dict[str, Any],
    profile: dict[str, Any] | None = None,
) -> list[str]:
    """Collect usable recipient and parent names for profile-aware matching."""

    names: list[str] = []

    def append_name(value: object) -> None:
        normalized = normalize_usaspending_value(value)
        if normalized and normalized not in names:
            names.append(normalized)

    append_name(row.get("name"))
    if profile:
        append_name(profile.get("name"))
        append_name(profile.get("parent_name"))
        for value in profile.get("alternate_names") or []:
            append_name(value)
        for parent in profile.get("parents") or []:
            if isinstance(parent, dict):
                append_name(parent.get("parent_name"))
    return names


def score_recipient_profile_match(
    *,
    query_name: str,
    row: dict[str, Any],
    profile: dict[str, Any] | None = None,
) -> int:
    """Score one recipient against its own name plus parent and alternate names."""

    return max(
        (score_recipient_match(query_name, candidate) for candidate in candidate_profile_names(row, profile)),
        default=0,
    )


def select_recipient_matches(
    *,
    query_name: str,
    rows: list[dict[str, Any]],
    limit: int = 3,
) -> list[dict[str, Any]]:
    """Keep only the strongest recipient matches for one query."""

    scored_rows: list[tuple[int, float, dict[str, Any]]] = []
    for row in rows:
        recipient_name = normalize_usaspending_value(row.get("name"))
        if not recipient_name:
            continue
        score = score_recipient_match(query_name, recipient_name)
        if score < 55:
            continue
        amount_value = row.get("amount")
        scored_rows.append(
            (
                score,
                float(amount_value) if isinstance(amount_value, (int, float)) else 0.0,
                row,
            )
        )
    scored_rows.sort(key=lambda item: (-item[0], -item[1], str(item[2].get("name") or "")))
    deduped: list[dict[str, Any]] = []
    seen_recipient_ids: set[str] = set()
    for _score, _amount, row in scored_rows:
        recipient_id = normalize_usaspending_value(row.get("recipient_id")) or ""
        if recipient_id in seen_recipient_ids:
            continue
        seen_recipient_ids.add(recipient_id)
        deduped.append(row)
        if len(deduped) >= max(1, limit):
            break
    return deduped


class UsaspendingApiClient:
    """Rate-limited HTTP client for official USAspending endpoints."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._last_request_at = 0.0

    def _throttle(self) -> None:
        delay = max(0.0, float(self.settings.usaspending_request_interval_seconds))
        if delay <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        remaining = delay - elapsed
        if remaining > 0:
            time.sleep(remaining)

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        payload: dict[str, Any] | None = None,
        max_attempts: int = 4,
    ) -> dict[str, Any]:
        """Execute one JSON request with polite pacing and transient retry."""

        last_error: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            self._throttle()
            try:
                with httpx.Client(
                    timeout=120.0,
                    follow_redirects=True,
                    headers={"User-Agent": self.settings.user_agent},
                ) as client:
                    response = client.request(
                        method.upper(),
                        f"{self.settings.usaspending_base_url.rstrip('/')}/{path.lstrip('/')}",
                        json=payload,
                    )
                    self._last_request_at = time.monotonic()
                    response.raise_for_status()
                    body = response.json()
                    return body if isinstance(body, dict) else {}
            except httpx.HTTPStatusError as error:
                last_error = error
                status_code = error.response.status_code if error.response is not None else None
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

    def post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        """POST one JSON endpoint with polite pacing."""

        return self._request_json("POST", path, payload=payload)

    def get_json(self, path: str) -> dict[str, Any]:
        """GET one JSON endpoint with polite pacing."""

        return self._request_json("GET", path)


def build_usaspending_time_period(
    *,
    start_date: str = DEFAULT_USASPENDING_START_DATE,
    end_date: str = DEFAULT_USASPENDING_END_DATE,
) -> list[dict[str, str]]:
    """Return the USAspending time_period filter payload."""

    return [{"start_date": start_date, "end_date": end_date}]


def search_recipient_summaries(
    settings: Settings,
    *,
    query_name: str,
    start_date: str = DEFAULT_USASPENDING_START_DATE,
    end_date: str = DEFAULT_USASPENDING_END_DATE,
    limit: int = 10,
    client: UsaspendingApiClient | None = None,
) -> list[dict[str, Any]]:
    """Search USAspending recipient rollups for one company-like name."""

    api_client = client or UsaspendingApiClient(settings)
    payload = {
        "filters": {
            "recipient_search_text": [query_name],
            "time_period": build_usaspending_time_period(
                start_date=start_date,
                end_date=end_date,
            ),
            "award_type_codes": DEFAULT_USASPENDING_CONTRACT_AWARD_TYPE_CODES,
        },
        "page": 1,
        "limit": max(1, limit),
    }
    response = api_client.post_json("search/spending_by_category/recipient", payload)
    results = response.get("results")
    if not isinstance(results, list):
        return []
    return [row for row in results if isinstance(row, dict)]


def fetch_recipient_profile(
    settings: Settings,
    *,
    recipient_id: str,
    client: UsaspendingApiClient | None = None,
) -> dict[str, Any]:
    """Fetch one official USAspending recipient profile by hashed id."""

    api_client = client or UsaspendingApiClient(settings)
    return api_client.get_json(f"recipient/{recipient_id}/")


def search_awards_for_recipient_name(
    settings: Settings,
    *,
    recipient_name: str,
    start_date: str = DEFAULT_USASPENDING_START_DATE,
    end_date: str = DEFAULT_USASPENDING_END_DATE,
    limit: int = 10,
    client: UsaspendingApiClient | None = None,
) -> list[dict[str, Any]]:
    """Fetch top prime-award rows for a given recipient using the official download flow."""

    api_client = client or UsaspendingApiClient(settings)
    payload = {
        "filters": {
            "recipient_search_text": [recipient_name],
            "time_period": build_usaspending_time_period(
                start_date=start_date,
                end_date=end_date,
            ),
            "award_type_codes": [
                *DEFAULT_USASPENDING_CONTRACT_AWARD_TYPE_CODES,
                *DEFAULT_USASPENDING_IDV_AWARD_TYPE_CODES,
            ],
        },
        "file_format": "csv",
    }
    response = api_client.post_json("download/awards/", payload)
    status_url = str(response.get("status_url") or "").replace(
        "https://api.usaspending.gov/api/v2/",
        "",
    )
    if not status_url:
        return []

    status_body: dict[str, Any] = {}
    for _attempt in range(24):
        status_body = api_client.get_json(status_url)
        status = str(status_body.get("status") or "").lower()
        if status == "finished":
            break
        if status == "failed":
            raise RuntimeError(str(status_body.get("message") or "USAspending award download failed"))
        time.sleep(5)

    if str(status_body.get("status") or "").lower() != "finished":
        raise RuntimeError(f"USAspending award download did not finish for {recipient_name}")

    file_url = normalize_usaspending_value(status_body.get("file_url"))
    if not file_url:
        return []

    download_response: httpx.Response | None = None
    last_download_error: Exception | None = None
    for attempt in range(1, 5):
        try:
            with httpx.Client(
                timeout=180.0,
                follow_redirects=True,
                headers={"User-Agent": settings.user_agent},
            ) as download_client:
                download_response = download_client.get(file_url)
                download_response.raise_for_status()
                break
        except httpx.HTTPError as error:
            last_download_error = error
            if attempt >= 4:
                raise
            time.sleep(min(10, attempt * 2))

    if download_response is None:
        if last_download_error:
            raise last_download_error
        return []

    normalized_name = normalize_member_lookup_value(recipient_name)
    results: list[dict[str, Any]] = []
    with zipfile.ZipFile(io.BytesIO(download_response.content)) as archive:
        prime_files = [
            name
            for name in archive.namelist()
            if "PrimeAwardSummaries" in name and "Subawards" not in name
        ]
        seen_award_keys: set[str] = set()
        for file_name in prime_files:
            with archive.open(file_name) as handle:
                reader = csv.DictReader(io.TextIOWrapper(handle, encoding="utf-8-sig"))
                for row in reader:
                    csv_recipient_name = normalize_usaspending_value(row.get("recipient_name"))
                    if not csv_recipient_name:
                        continue
                    if normalize_member_lookup_value(csv_recipient_name) != normalized_name:
                        continue

                    award_key = (
                        normalize_usaspending_value(row.get("contract_award_unique_key"))
                        or normalize_usaspending_value(row.get("award_id_piid"))
                        or normalize_usaspending_value(row.get("award_id_fain"))
                        or ""
                    )
                    if not award_key or award_key in seen_award_keys:
                        continue
                    seen_award_keys.add(award_key)

                    amount_value = None
                    for candidate in (
                        row.get("current_total_value_of_award"),
                        row.get("total_obligated_amount"),
                    ):
                        try:
                            if candidate not in (None, ""):
                                amount_value = float(candidate)
                                break
                        except (TypeError, ValueError):
                            continue

                    results.append(
                        {
                            "Award ID": normalize_usaspending_value(row.get("award_id_piid"))
                            or normalize_usaspending_value(row.get("award_id_fain"))
                            or award_key,
                            "Recipient Name": csv_recipient_name,
                            "Action Date": normalize_usaspending_value(row.get("award_latest_action_date"))
                            or normalize_usaspending_value(row.get("award_base_action_date")),
                            "Award Amount": amount_value,
                            "Awarding Agency": normalize_usaspending_value(row.get("awarding_agency_name")),
                            "Award Type": normalize_usaspending_value(row.get("award_type")),
                            "Description": normalize_usaspending_value(
                                row.get("prime_award_base_transaction_description")
                            ),
                            "generated_internal_id": award_key,
                            "internal_id": None,
                            "awarding_agency_id": normalize_usaspending_value(
                                row.get("awarding_agency_code")
                            ),
                            "agency_slug": None,
                            "recipient_parent_name": normalize_usaspending_value(
                                row.get("recipient_parent_name")
                            ),
                            "recipient_uei": normalize_usaspending_value(row.get("recipient_uei")),
                            "recipient_duns": normalize_usaspending_value(row.get("recipient_duns")),
                            "award_type_code": normalize_usaspending_value(row.get("award_type_code")),
                            "contract_award_unique_key": award_key,
                        }
                    )

    deduped: list[dict[str, Any]] = []
    for row in sorted(
        results,
        key=lambda item: float(item.get("Award Amount") or 0),
        reverse=True,
    ):
        deduped.append(row)
        if len(deduped) >= max(1, limit):
            break
    return deduped


def search_awarding_agencies_for_recipient_name(
    settings: Settings,
    *,
    recipient_name: str,
    start_date: str = DEFAULT_USASPENDING_START_DATE,
    end_date: str = DEFAULT_USASPENDING_END_DATE,
    limit: int = 10,
    client: UsaspendingApiClient | None = None,
) -> list[dict[str, Any]]:
    """Fetch top awarding agencies for a recipient across contracts and IDVs."""

    api_client = client or UsaspendingApiClient(settings)
    payload_template = {
        "filters": {
            "recipient_search_text": [recipient_name],
            "time_period": build_usaspending_time_period(
                start_date=start_date,
                end_date=end_date,
            ),
        },
        "page": 1,
        "limit": max(1, limit),
    }
    results: list[dict[str, Any]] = []
    for award_type_codes in (
        DEFAULT_USASPENDING_CONTRACT_AWARD_TYPE_CODES,
        DEFAULT_USASPENDING_IDV_AWARD_TYPE_CODES,
    ):
        payload = {
            **payload_template,
            "filters": {
                **payload_template["filters"],
                "award_type_codes": award_type_codes,
            },
        }
        response = api_client.post_json("search/spending_by_category/awarding_agency/", payload)
        group_results = response.get("results")
        if isinstance(group_results, list):
            results.extend(row for row in group_results if isinstance(row, dict))

    deduped: list[dict[str, Any]] = []
    seen_agency_ids: set[str] = set()
    for row in sorted(
        results,
        key=lambda item: float(item.get("amount") or 0),
        reverse=True,
    ):
        agency_id = normalize_usaspending_value(row.get("id")) or normalize_usaspending_value(row.get("name")) or ""
        if agency_id in seen_agency_ids:
            continue
        seen_agency_ids.add(agency_id)
        deduped.append(row)
        if len(deduped) >= max(1, limit):
            break
    return deduped


def build_recipient_source_url(recipient_id: str) -> str:
    """Return the public USAspending recipient profile URL."""

    return f"https://www.usaspending.gov/recipient/{recipient_id}/latest"


def build_usaspending_recipient_record(
    row: dict[str, Any],
    *,
    query_name: str,
) -> UsaspendingRecipientRecord:
    """Normalize one USAspending recipient summary row."""

    recipient_id = normalize_usaspending_value(row.get("recipient_id")) or ""
    name = normalize_usaspending_value(row.get("name")) or f"Recipient {recipient_id}"
    total_amount = row.get("amount")
    total_outlays = row.get("total_outlays")
    summary = (
        f"{name} received approximately ${float(total_amount):,.0f} in tracked federal spending during the queried window."
        if isinstance(total_amount, (int, float))
        else f"{name} appears in USAspending recipient search results for the queried window."
    )
    content = "\n".join(
        line
        for line in [
            f"Recipient: {name}",
            f"Recipient ID: {recipient_id}",
            f"UEI: {normalize_usaspending_value(row.get('uei'))}",
            f"Recipient code: {normalize_usaspending_value(row.get('code'))}",
            f"Queried as: {query_name}",
            f"Federal spending: ${float(total_amount):,.2f}" if isinstance(total_amount, (int, float)) else None,
            f"Total outlays: ${float(total_outlays):,.2f}" if isinstance(total_outlays, (int, float)) else None,
        ]
        if line
    )
    return UsaspendingRecipientRecord(
        recipient_id=recipient_id,
        name=name,
        normalized_name=normalize_member_lookup_value(name),
        query_name=query_name,
        recipient_code=normalize_usaspending_value(row.get("code")),
        uei=normalize_usaspending_value(row.get("uei")),
        total_amount=float(total_amount) if isinstance(total_amount, (int, float)) else None,
        total_outlays=float(total_outlays) if isinstance(total_outlays, (int, float)) else None,
        source_url=build_recipient_source_url(recipient_id) if recipient_id else None,
        summary=summary,
        content=content,
        metadata={key: value for key, value in row.items() if value not in (None, "")},
    )


def build_usaspending_company_match_record(
    *,
    company_id: str,
    company_name: str,
    ticker: str | None,
    query_name: str,
    recipient: UsaspendingRecipientRecord,
    awards: list[dict[str, Any]] | None = None,
    agencies: list[dict[str, Any]] | None = None,
) -> UsaspendingCompanyMatchRecord:
    """Build one site-company to USAspending recipient match."""

    match_key = hashlib.sha1(
        f"{company_id}|{recipient.recipient_id}".encode("utf-8")
    ).hexdigest()
    awards = awards or []
    agencies = agencies or []
    awarding_agencies = [
        str(row.get("name") or "").strip()
        for row in agencies
        if str(row.get("name") or "").strip()
    ] or [
        str(row.get("Awarding Agency") or "").strip()
        for row in awards
        if str(row.get("Awarding Agency") or "").strip()
    ]
    top_agencies = [
        agency
        for agency, _count in Counter(awarding_agencies).most_common(5)
    ]
    total_transactions = recipient.metadata.get("totalTransactions")
    award_count = int(total_transactions) if isinstance(total_transactions, int) else len(awards)
    summary = (
        f"{company_name} matches USAspending recipient {recipient.name}, with approximately "
        f"${recipient.total_amount:,.0f} in tracked federal spending during the queried window."
        if recipient.total_amount is not None
        else f"{company_name} matches USAspending recipient {recipient.name} in federal spending records."
    )
    content_lines = [
        summary,
        f"Ticker: {ticker}" if ticker else None,
        f"Queried as: {query_name}",
        f"Recipient code: {recipient.recipient_code}" if recipient.recipient_code else None,
        f"UEI: {recipient.uei}" if recipient.uei else None,
        f"Top agencies: {', '.join(top_agencies)}" if top_agencies else None,
        (
            "Agency rollup: "
            + "; ".join(
                f"{str(row.get('name') or '').strip()} (${float(row.get('amount')):,.0f})"
                for row in agencies[:6]
                if row.get("name") and isinstance(row.get("amount"), (int, float))
            )
        )
        if agencies
        else None,
        (
            "Top awards: "
            + "; ".join(
                f"{str(row.get('Award ID') or '').strip()} "
                f"({str(row.get('Awarding Agency') or '').strip()}, "
                f"${float(row.get('Award Amount')):,.0f})"
                for row in awards[:6]
                if row.get("Award ID") and isinstance(row.get("Award Amount"), (int, float))
            )
        )
        if awards
        else None,
    ]
    return UsaspendingCompanyMatchRecord(
        match_key=match_key,
        company_id=company_id,
        company_name=company_name,
        ticker=ticker,
        query_name=query_name,
        canonical_recipient_id=recipient.recipient_id,
        recipient_name=recipient.name,
        normalized_recipient_name=recipient.normalized_name,
        recipient_code=recipient.recipient_code,
        uei=recipient.uei,
        match_type="recipient_name",
        match_value=recipient.name,
        total_amount=recipient.total_amount,
        award_count=award_count,
        top_agencies=top_agencies,
        source_url=recipient.source_url,
        summary=summary,
        content="\n".join(line for line in content_lines if line),
        metadata={
            "queryName": query_name,
            "recipientMetadata": recipient.metadata,
            "agencyRollup": agencies,
            "awardSampleCount": len(awards),
        },
    )


def build_usaspending_award_records(
    *,
    match: UsaspendingCompanyMatchRecord,
    awards: list[dict[str, Any]],
) -> list[UsaspendingAwardRecord]:
    """Normalize a set of award rows for one company match."""

    records: list[UsaspendingAwardRecord] = []
    for row in awards:
        award_id = normalize_usaspending_value(row.get("Award ID")) or ""
        generated_internal_id = (
            normalize_usaspending_value(row.get("generated_internal_id"))
            or normalize_usaspending_value(row.get("contract_award_unique_key"))
        )
        internal_id = row.get("internal_id")
        award_amount = coerce_optional_float(row.get("Award Amount"))
        awarding_agency_id = coerce_optional_int(row.get("awarding_agency_id"))
        action_date = (
            normalize_usaspending_value(row.get("Action Date"))
            or normalize_usaspending_value(row.get("award_latest_action_date"))
            or normalize_usaspending_value(row.get("award_base_action_date"))
        )
        award_type = (
            normalize_usaspending_value(row.get("Award Type"))
            or normalize_usaspending_value(row.get("award_type_code"))
        )
        description = (
            normalize_usaspending_value(row.get("Description"))
            or normalize_usaspending_value(row.get("prime_award_base_transaction_description"))
        )
        stable_key = hashlib.sha1(
            "|".join(
                [
                    match.match_key,
                    award_id,
                    str(internal_id or ""),
                    generated_internal_id or "",
                ]
            ).encode("utf-8")
        ).hexdigest()
        records.append(
            UsaspendingAwardRecord(
                award_key=stable_key,
                match_key=match.match_key,
                canonical_recipient_id=match.canonical_recipient_id,
                recipient_name=match.recipient_name,
                award_id=award_id or f"award-{stable_key[:12]}",
                internal_id=coerce_optional_int(internal_id),
                generated_internal_id=generated_internal_id,
                action_date=action_date,
                award_amount=award_amount,
                award_type=award_type,
                awarding_agency=normalize_usaspending_value(row.get("Awarding Agency")),
                awarding_agency_id=awarding_agency_id,
                agency_slug=normalize_usaspending_value(row.get("agency_slug")),
                description=description,
                source_url=match.source_url,
                metadata={key: value for key, value in row.items() if value not in (None, "")},
            )
        )
    return records
