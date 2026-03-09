"""Official FARA API adapter."""

from __future__ import annotations

from datetime import datetime
import hashlib
import time
from typing import Any

import httpx

from capitol_pipeline.config import Settings
from capitol_pipeline.models.fara import (
    FaraDocumentRecord,
    FaraForeignPrincipalRecord,
    FaraRegistrantRecord,
    FaraShortFormRecord,
)
from capitol_pipeline.registries.members import normalize_member_lookup_value


def normalize_fara_value(value: object) -> str | None:
    """Normalize a FARA API field into trimmed text."""

    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalize_fara_date(value: object) -> str | None:
    """Normalize FARA dates to YYYY-MM-DD."""

    text = normalize_fara_value(value)
    if not text:
        return None

    for fmt in ("%m/%d/%Y", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except ValueError:
            continue
    return text


def build_fara_address(*parts: object) -> str | None:
    """Build a compact address line from sparse FARA fields."""

    rendered = [normalize_fara_value(part) for part in parts]
    compact = [part for part in rendered if part]
    if not compact:
        return None
    return ", ".join(compact)


def unwrap_fara_rows(payload: dict[str, Any], *path: str) -> list[dict[str, Any]]:
    """Extract row arrays from the inconsistent FARA JSON envelopes."""

    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return []
        current = current.get(key)
    if current is None:
        return []
    if isinstance(current, list):
        return [row for row in current if isinstance(row, dict)]
    if isinstance(current, dict):
        row = current.get("ROW")
        if isinstance(row, list):
            return [item for item in row if isinstance(item, dict)]
        if isinstance(row, dict):
            return [row]
    return []


class FaraApiClient:
    """Rate-limited HTTP client for the official FARA API."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._last_request_at = 0.0

    def _throttle(self) -> None:
        delay = max(0.0, float(self.settings.fara_request_interval_seconds))
        if delay <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        remaining = delay - elapsed
        if remaining > 0:
            time.sleep(remaining)

    def get_json(self, path: str) -> dict[str, Any]:
        """GET one JSON endpoint with polite pacing."""

        self._throttle()
        with httpx.Client(
            timeout=120.0,
            follow_redirects=True,
            headers={"User-Agent": self.settings.user_agent},
        ) as client:
            response = client.get(f"{self.settings.fara_base_url.rstrip('/')}/{path.lstrip('/')}")
            response.raise_for_status()
            self._last_request_at = time.monotonic()
            return response.json()


def build_fara_registrant_record(row: dict[str, Any]) -> FaraRegistrantRecord:
    """Normalize an active registrant row."""

    registration_number = int(row["Registration_Number"])
    name = normalize_fara_value(row.get("Name")) or f"FARA registrant {registration_number}"
    summary = f"{name} is an active FARA registrant under registration number {registration_number}."
    content = "\n".join(
        part
        for part in [
            f"Registrant: {name}",
            f"Registration number: {registration_number}",
            f"Registration date: {normalize_fara_date(row.get('Registration_Date')) or 'Unknown'}",
            f"Address: {build_fara_address(row.get('Address_1'), row.get('Address_2'), row.get('City'), row.get('State'), row.get('Zip')) or 'Unknown'}",
        ]
        if part
    )
    return FaraRegistrantRecord(
        registration_number=registration_number,
        name=name,
        normalized_name=normalize_member_lookup_value(name),
        registration_date=normalize_fara_date(row.get("Registration_Date")),
        address_1=normalize_fara_value(row.get("Address_1")),
        address_2=normalize_fara_value(row.get("Address_2")),
        city=normalize_fara_value(row.get("City")),
        state=normalize_fara_value(row.get("State")),
        zip_code=normalize_fara_value(row.get("Zip")),
        summary=summary,
        content=content,
        metadata={key: value for key, value in row.items() if value not in (None, "")},
    )


def build_fara_foreign_principal_record(row: dict[str, Any]) -> FaraForeignPrincipalRecord:
    """Normalize a foreign principal row."""

    registration_number = int(row["REG_NUMBER"])
    name = normalize_fara_value(row.get("FP_NAME")) or f"Foreign principal {registration_number}"
    stable_key = hashlib.sha1(
        "|".join(
            [
                str(registration_number),
                name,
                normalize_fara_value(row.get("COUNTRY_NAME")) or "",
                normalize_fara_date(row.get("FP_REG_DATE")) or "",
            ]
        ).encode("utf-8")
    ).hexdigest()
    return FaraForeignPrincipalRecord(
        principal_key=stable_key,
        registration_number=registration_number,
        foreign_principal_name=name,
        normalized_name=normalize_member_lookup_value(name),
        registrant_name=normalize_fara_value(row.get("REGISTRANT_NAME")) or f"Registrant {registration_number}",
        country_name=normalize_fara_value(row.get("COUNTRY_NAME")),
        registration_date=normalize_fara_date(row.get("REG_DATE")),
        foreign_principal_registration_date=normalize_fara_date(row.get("FP_REG_DATE")),
        address_1=normalize_fara_value(row.get("ADDRESS_1")),
        address_2=normalize_fara_value(row.get("ADDRESS_2")),
        city=normalize_fara_value(row.get("CITY")),
        state=normalize_fara_value(row.get("STATE")),
        zip_code=normalize_fara_value(row.get("ZIP")),
        metadata={key: value for key, value in row.items() if value not in (None, "")},
    )


def build_fara_short_form_record(row: dict[str, Any]) -> FaraShortFormRecord:
    """Normalize a short-form registrant row."""

    registration_number = int(row["REG_NUMBER"])
    first_name = normalize_fara_value(row.get("SF_FIRST_NAME")) or ""
    last_name = normalize_fara_value(row.get("SF_LAST_NAME")) or ""
    full_name = " ".join(part for part in [first_name, last_name] if part) or f"Short form {registration_number}"
    stable_key = hashlib.sha1(
        "|".join(
            [
                str(registration_number),
                full_name,
                normalize_fara_date(row.get("SHORTFORM_DATE")) or "",
            ]
        ).encode("utf-8")
    ).hexdigest()
    return FaraShortFormRecord(
        short_form_key=stable_key,
        registration_number=registration_number,
        registrant_name=normalize_fara_value(row.get("REGISTRANT_NAME")) or f"Registrant {registration_number}",
        first_name=first_name,
        last_name=last_name,
        full_name=full_name,
        normalized_name=normalize_member_lookup_value(full_name),
        short_form_date=normalize_fara_date(row.get("SHORTFORM_DATE")),
        registration_date=normalize_fara_date(row.get("REG_DATE")),
        address_1=normalize_fara_value(row.get("ADDRESS_1")),
        address_2=normalize_fara_value(row.get("ADDRESS_2")),
        city=normalize_fara_value(row.get("CITY")),
        state=normalize_fara_value(row.get("STATE")),
        zip_code=normalize_fara_value(row.get("ZIP")),
        metadata={key: value for key, value in row.items() if value not in (None, "")},
    )


def build_fara_document_record(row: dict[str, Any]) -> FaraDocumentRecord:
    """Normalize a FARA document metadata row."""

    registration_number = int(row["REGISTRATION_NUMBER"])
    url = normalize_fara_value(row.get("URL")) or ""
    stable_key = hashlib.sha1(
        "|".join(
            [
                str(registration_number),
                normalize_fara_value(row.get("DOCUMENT_TYPE")) or "",
                normalize_fara_date(row.get("DATE_STAMPED")) or "",
                url,
            ]
        ).encode("utf-8")
    ).hexdigest()
    return FaraDocumentRecord(
        document_key=stable_key,
        registration_number=registration_number,
        registrant_name=normalize_fara_value(row.get("REGISTRANT_NAME")) or f"Registrant {registration_number}",
        document_type=normalize_fara_value(row.get("DOCUMENT_TYPE")) or "FARA filing",
        date_stamped=normalize_fara_date(row.get("DATE_STAMPED")),
        url=url,
        short_form_name=normalize_fara_value(row.get("SHORT_FORM_NAME")),
        foreign_principal_name=normalize_fara_value(row.get("FOREIGN_PRINCIPAL_NAME")),
        foreign_principal_country=normalize_fara_value(row.get("FOREIGN_PRINCIPAL_COUNTRY")),
        metadata={key: value for key, value in row.items() if value not in (None, "")},
    )


def fetch_active_registrants(settings: Settings, client: FaraApiClient | None = None) -> list[FaraRegistrantRecord]:
    """Fetch all active FARA registrants."""

    fara_client = client or FaraApiClient(settings)
    payload = fara_client.get_json("Registrants/json/Active")
    rows = unwrap_fara_rows(payload, "REGISTRANTS_ACTIVE")
    return [build_fara_registrant_record(row) for row in rows]


def fetch_foreign_principals(
    settings: Settings,
    registration_number: int,
    client: FaraApiClient | None = None,
) -> list[FaraForeignPrincipalRecord]:
    """Fetch active foreign principals for one registrant."""

    fara_client = client or FaraApiClient(settings)
    payload = fara_client.get_json(f"ForeignPrincipals/json/Active/{registration_number}")
    rows = unwrap_fara_rows(payload, "ROWSET")
    return [build_fara_foreign_principal_record(row) for row in rows]


def fetch_short_forms(
    settings: Settings,
    registration_number: int,
    client: FaraApiClient | None = None,
) -> list[FaraShortFormRecord]:
    """Fetch active short-form registrants for one registrant."""

    fara_client = client or FaraApiClient(settings)
    payload = fara_client.get_json(f"ShortFormRegistrants/json/Active/{registration_number}")
    rows = unwrap_fara_rows(payload, "ROWSET")
    return [build_fara_short_form_record(row) for row in rows]


def fetch_reg_documents(
    settings: Settings,
    registration_number: int,
    client: FaraApiClient | None = None,
) -> list[FaraDocumentRecord]:
    """Fetch document metadata for one registrant."""

    fara_client = client or FaraApiClient(settings)
    payload = fara_client.get_json(f"RegDocs/json/{registration_number}")
    rows = unwrap_fara_rows(payload, "ROWSET")
    return [build_fara_document_record(row) for row in rows]
