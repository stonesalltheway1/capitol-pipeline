"""Official FARA API adapter."""

from __future__ import annotations

import csv
from datetime import datetime
import hashlib
import io
from pathlib import Path
import re
import time
from typing import Any
import zipfile

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


def pick_fara_value(row: dict[str, Any], *keys: str) -> str | None:
    """Return the first populated key from a mixed API and bulk-data row."""

    for key in keys:
        if key in row:
            normalized = normalize_fara_value(row.get(key))
            if normalized:
                return normalized
    return None


def is_digit_string(value: str | None) -> bool:
    """Return whether the provided value contains only digits after trimming."""

    return bool(value and value.strip().isdigit())


def looks_like_date(value: str | None) -> bool:
    """Return whether the provided text resembles a supported FARA date."""

    normalized = normalize_fara_value(value)
    if not normalized:
        return False
    return bool(re.fullmatch(r"\d{2}/\d{2}/\d{4}", normalized) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", normalized))


def merge_broken_text(*parts: str | None) -> str | None:
    """Repair split text fragments from malformed quoted bulk CSV rows."""

    cleaned_parts = []
    for part in parts:
        normalized = normalize_fara_value(part)
        if normalized:
            cleaned_parts.append(normalized.strip(' "'))
    if not cleaned_parts:
        return None
    return " ".join(cleaned_parts).replace(' ,"', ",").strip()


def split_embedded_date(value: str | None) -> tuple[str | None, str | None]:
    """Split a value that accidentally absorbed a trailing date due to bad quoting."""

    normalized = normalize_fara_value(value)
    if not normalized:
        return None, None
    match = re.match(r"^(.*?)[,\s]+(\d{2}/\d{2}/\d{4})\"?$", normalized)
    if not match:
        return normalized.strip(' "'), None
    return match.group(1).strip(' "'), match.group(2)


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


def fara_bulk_cache_dir(settings: Settings) -> Path:
    """Return the on-disk cache directory for official FARA bulk files."""

    cache_dir = settings.cache_dir / "fara"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def download_fara_bulk_archive(settings: Settings, url: str) -> Path:
    """Download one official FARA bulk archive into the local cache if needed."""

    destination = fara_bulk_cache_dir(settings) / Path(url).name
    if destination.exists() and destination.stat().st_size > 0:
        return destination

    with httpx.Client(
        timeout=300.0,
        follow_redirects=True,
        headers={"User-Agent": settings.user_agent},
    ) as client:
        response = client.get(url)
        response.raise_for_status()
        destination.write_bytes(response.content)
    return destination


def iter_bulk_csv_rows(archive_path: Path) -> list[dict[str, str]]:
    """Read one zipped CSV archive from the official FARA bulk feed."""

    with zipfile.ZipFile(archive_path) as bundle:
        members = [name for name in bundle.namelist() if name.lower().endswith(".csv")]
        if not members:
            return []
        with bundle.open(members[0]) as handle:
            text = io.TextIOWrapper(handle, encoding="iso-8859-1", newline="")
            reader = csv.DictReader(text)
            return [
                {
                    str(key).strip(): (value or "")
                    for key, value in row.items()
                    if key is not None and str(key).strip()
                }
                for row in reader
            ]


def repair_bulk_foreign_principal_row(row: dict[str, Any]) -> dict[str, Any]:
    """Repair the small set of malformed official bulk foreign-principal rows."""

    registration_number = pick_fara_value(row, "Registration Number")
    if is_digit_string(registration_number):
        return row

    repaired = dict(row)
    embedded_name, embedded_date = split_embedded_date(pick_fara_value(row, "Foreign Principal"))
    if embedded_date:
        repaired["Foreign Principal"] = embedded_name or repaired.get("Foreign Principal")
        repaired["Foreign Principal Registration Date"] = embedded_date

    country_or_registration = pick_fara_value(row, "Country/Location Represented")
    registrant_date = pick_fara_value(row, "Registrant Date")
    registrant_name = pick_fara_value(row, "Registrant Name")

    if is_digit_string(country_or_registration) and looks_like_date(registration_number):
        repaired["Country/Location Represented"] = pick_fara_value(row, "Foreign Principal Registration Date") or ""
        repaired["Registration Number"] = country_or_registration or ""
        repaired["Registrant Date"] = registration_number or ""
        repaired["Registrant Name"] = registrant_date or ""
        repaired["Address 1"] = registrant_name or ""
        repaired["Address 2"] = pick_fara_value(row, "Address 1") or ""
        repaired["City"] = pick_fara_value(row, "Address 2") or ""
        repaired["State"] = pick_fara_value(row, "City") or ""
        repaired["Zip"] = pick_fara_value(row, "State") or ""
        return repaired

    if is_digit_string(registrant_date) and looks_like_date(registrant_name):
        repaired["Foreign Principal"] = merge_broken_text(
            pick_fara_value(row, "Foreign Principal"),
            pick_fara_value(row, "Foreign Principal Registration Date"),
        ) or repaired.get("Foreign Principal")
        repaired["Foreign Principal Registration Date"] = country_or_registration or ""
        repaired["Country/Location Represented"] = registration_number or ""
        repaired["Registration Number"] = registrant_date or ""
        repaired["Registrant Date"] = registrant_name or ""
        repaired["Registrant Name"] = pick_fara_value(row, "Address 1") or ""
        repaired["Address 1"] = pick_fara_value(row, "Address 2") or ""
        repaired["Address 2"] = pick_fara_value(row, "City") or ""
        repaired["City"] = pick_fara_value(row, "State") or ""
        repaired["State"] = pick_fara_value(row, "Zip") or ""
        repaired["Zip"] = ""
        return repaired

    return repaired


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

    registration_number = int(
        pick_fara_value(row, "Registration_Number", "Registration Number") or "0"
    )
    name = pick_fara_value(row, "Name", "Business Name") or f"FARA registrant {registration_number}"
    summary = f"{name} is an active FARA registrant under registration number {registration_number}."
    content = "\n".join(
        part
        for part in [
            f"Registrant: {name}",
            f"Registration number: {registration_number}",
            f"Registration date: {normalize_fara_date(pick_fara_value(row, 'Registration_Date', 'Registration Date')) or 'Unknown'}",
            f"Address: {build_fara_address(row.get('Address_1'), row.get('Address 1'), row.get('Address_2'), row.get('Address 2'), row.get('City'), row.get('State'), row.get('Zip')) or 'Unknown'}",
        ]
        if part
    )
    return FaraRegistrantRecord(
        registration_number=registration_number,
        name=name,
        normalized_name=normalize_member_lookup_value(name),
        registration_date=normalize_fara_date(
            pick_fara_value(row, "Registration_Date", "Registration Date")
        ),
        address_1=pick_fara_value(row, "Address_1", "Address 1"),
        address_2=pick_fara_value(row, "Address_2", "Address 2"),
        city=normalize_fara_value(row.get("City")),
        state=normalize_fara_value(row.get("State")),
        zip_code=pick_fara_value(row, "Zip", "ZIP"),
        summary=summary,
        content=content,
        metadata={key: value for key, value in row.items() if value not in (None, "")},
    )


def build_fara_foreign_principal_record(row: dict[str, Any]) -> FaraForeignPrincipalRecord:
    """Normalize a foreign principal row."""

    registration_number = int(
        pick_fara_value(row, "REG_NUMBER", "Registration Number") or "0"
    )
    name = pick_fara_value(row, "FP_NAME", "Foreign Principal") or f"Foreign principal {registration_number}"
    stable_key = hashlib.sha1(
        "|".join(
            [
                str(registration_number),
                name,
                pick_fara_value(row, "COUNTRY_NAME", "Country/Location Represented") or "",
                normalize_fara_date(
                    pick_fara_value(row, "FP_REG_DATE", "Foreign Principal Registration Date")
                )
                or "",
            ]
        ).encode("utf-8")
    ).hexdigest()
    return FaraForeignPrincipalRecord(
        principal_key=stable_key,
        registration_number=registration_number,
        foreign_principal_name=name,
        normalized_name=normalize_member_lookup_value(name),
        registrant_name=pick_fara_value(row, "REGISTRANT_NAME", "Registrant Name")
        or f"Registrant {registration_number}",
        country_name=pick_fara_value(row, "COUNTRY_NAME", "Country/Location Represented"),
        registration_date=normalize_fara_date(pick_fara_value(row, "REG_DATE", "Registrant Date")),
        foreign_principal_registration_date=normalize_fara_date(
            pick_fara_value(row, "FP_REG_DATE", "Foreign Principal Registration Date")
        ),
        address_1=pick_fara_value(row, "ADDRESS_1", "Address 1"),
        address_2=pick_fara_value(row, "ADDRESS_2", "Address 2"),
        city=pick_fara_value(row, "CITY", "City"),
        state=pick_fara_value(row, "STATE", "State"),
        zip_code=pick_fara_value(row, "ZIP", "Zip"),
        metadata={key: value for key, value in row.items() if value not in (None, "")},
    )


def build_fara_short_form_record(row: dict[str, Any]) -> FaraShortFormRecord:
    """Normalize a short-form registrant row."""

    registration_number = int(
        pick_fara_value(row, "REG_NUMBER", "Registration Number") or "0"
    )
    first_name = pick_fara_value(row, "SF_FIRST_NAME", "Short Form First Name") or ""
    last_name = pick_fara_value(row, "SF_LAST_NAME", "Short Form Last Name") or ""
    full_name = " ".join(part for part in [first_name, last_name] if part) or f"Short form {registration_number}"
    stable_key = hashlib.sha1(
        "|".join(
            [
                str(registration_number),
                full_name,
                normalize_fara_date(pick_fara_value(row, "SHORTFORM_DATE", "Short Form Date")) or "",
            ]
        ).encode("utf-8")
    ).hexdigest()
    return FaraShortFormRecord(
        short_form_key=stable_key,
        registration_number=registration_number,
        registrant_name=pick_fara_value(row, "REGISTRANT_NAME", "Registrant Name")
        or f"Registrant {registration_number}",
        first_name=first_name,
        last_name=last_name,
        full_name=full_name,
        normalized_name=normalize_member_lookup_value(full_name),
        short_form_date=normalize_fara_date(pick_fara_value(row, "SHORTFORM_DATE", "Short Form Date")),
        registration_date=normalize_fara_date(
            pick_fara_value(row, "REG_DATE", "Registration Date")
        ),
        address_1=pick_fara_value(row, "ADDRESS_1", "Address 1"),
        address_2=pick_fara_value(row, "ADDRESS_2", "Address 2"),
        city=pick_fara_value(row, "CITY", "City"),
        state=pick_fara_value(row, "STATE", "State"),
        zip_code=pick_fara_value(row, "ZIP", "Zip"),
        metadata={key: value for key, value in row.items() if value not in (None, "")},
    )


def build_fara_document_record(row: dict[str, Any]) -> FaraDocumentRecord:
    """Normalize a FARA document metadata row."""

    registration_number = int(
        pick_fara_value(row, "REGISTRATION_NUMBER", "Registration Number") or "0"
    )
    url = pick_fara_value(row, "URL") or ""
    stable_key = hashlib.sha1(
        "|".join(
            [
                str(registration_number),
                pick_fara_value(row, "DOCUMENT_TYPE", "Document Type") or "",
                normalize_fara_date(pick_fara_value(row, "DATE_STAMPED", "Date Stamped")) or "",
                url,
            ]
        ).encode("utf-8")
    ).hexdigest()
    return FaraDocumentRecord(
        document_key=stable_key,
        registration_number=registration_number,
        registrant_name=pick_fara_value(row, "REGISTRANT_NAME", "Registrant Name")
        or f"Registrant {registration_number}",
        document_type=pick_fara_value(row, "DOCUMENT_TYPE", "Document Type") or "FARA filing",
        date_stamped=normalize_fara_date(pick_fara_value(row, "DATE_STAMPED", "Date Stamped")),
        url=url,
        short_form_name=pick_fara_value(row, "SHORT_FORM_NAME", "Short Form Name"),
        foreign_principal_name=pick_fara_value(
            row,
            "FOREIGN_PRINCIPAL_NAME",
            "Foreign Principal Name",
        ),
        foreign_principal_country=pick_fara_value(
            row,
            "FOREIGN_PRINCIPAL_COUNTRY",
            "Foreign Principal Country",
        ),
        metadata={key: value for key, value in row.items() if value not in (None, "")},
    )


def fetch_active_registrants(settings: Settings, client: FaraApiClient | None = None) -> list[FaraRegistrantRecord]:
    """Fetch all active FARA registrants."""

    fara_client = client or FaraApiClient(settings)
    payload = fara_client.get_json("Registrants/json/Active")
    rows = unwrap_fara_rows(payload, "REGISTRANTS_ACTIVE")
    return [build_fara_registrant_record(row) for row in rows]


def fetch_bulk_registrants(settings: Settings) -> list[FaraRegistrantRecord]:
    """Fetch the official bulk registrant export."""

    archive_path = download_fara_bulk_archive(settings, settings.fara_bulk_registrants_zip_url)
    return [build_fara_registrant_record(row) for row in iter_bulk_csv_rows(archive_path)]


def fetch_bulk_foreign_principals(settings: Settings) -> list[FaraForeignPrincipalRecord]:
    """Fetch the official bulk foreign principal export."""

    archive_path = download_fara_bulk_archive(settings, settings.fara_bulk_foreign_principals_zip_url)
    records: list[FaraForeignPrincipalRecord] = []
    for row in iter_bulk_csv_rows(archive_path):
        repaired = repair_bulk_foreign_principal_row(row)
        registration_number = pick_fara_value(repaired, "REG_NUMBER", "Registration Number")
        if not is_digit_string(registration_number):
            continue
        records.append(build_fara_foreign_principal_record(repaired))
    return records


def fetch_bulk_short_forms(settings: Settings) -> list[FaraShortFormRecord]:
    """Fetch the official bulk short-form export."""

    archive_path = download_fara_bulk_archive(settings, settings.fara_bulk_short_forms_zip_url)
    return [build_fara_short_form_record(row) for row in iter_bulk_csv_rows(archive_path)]


def fetch_bulk_reg_documents(settings: Settings) -> list[FaraDocumentRecord]:
    """Fetch the official bulk document export."""

    archive_path = download_fara_bulk_archive(settings, settings.fara_bulk_documents_zip_url)
    return [build_fara_document_record(row) for row in iter_bulk_csv_rows(archive_path)]


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
