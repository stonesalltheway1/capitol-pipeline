"""ICIJ Offshore Leaks structured export adapter."""

from __future__ import annotations

import csv
import hashlib
from pathlib import Path
from typing import Iterable
import urllib.request
import zipfile

from capitol_pipeline.config import Settings
from capitol_pipeline.models.offshore import OffshoreNodeRecord, OffshoreRelationshipRecord
from capitol_pipeline.registries.members import normalize_member_lookup_value


OFFSHORE_NODE_FILES: dict[str, str] = {
    "entity": "nodes-entities.csv",
    "officer": "nodes-officers.csv",
    "address": "nodes-addresses.csv",
    "intermediary": "nodes-intermediaries.csv",
    "other": "nodes-others.csv",
}


def ensure_offshore_archive(settings: Settings, *, force_download: bool = False) -> Path:
    """Download the official ICIJ Offshore Leaks archive if it is not cached locally."""

    archive_path = settings.offshore_leaks_archive_path
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    if archive_path.exists() and not force_download:
        return archive_path

    request = urllib.request.Request(
        settings.offshore_leaks_zip_url,
        headers={"User-Agent": settings.user_agent},
    )
    with urllib.request.urlopen(request, timeout=180) as response:
        with archive_path.open("wb") as output_file:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                output_file.write(chunk)
    return archive_path


def split_multi_value(raw: str | None) -> list[str]:
    """Split semicolon-delimited or comma-delimited metadata into stable lists."""

    if not raw:
        return []
    parts: list[str] = []
    for chunk in raw.replace("|", ";").split(";"):
        item = chunk.strip()
        if not item:
            continue
        if "," in item and len(item) > 3:
            subparts = [part.strip() for part in item.split(",") if part.strip()]
            if len(subparts) > 1:
                parts.extend(subparts)
                continue
        parts.append(item)
    seen: set[str] = set()
    deduped: list[str] = []
    for item in parts:
        lowered = item.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(item)
    return deduped


def build_offshore_node_summary(node_type: str, row: dict[str, str]) -> str:
    """Build a concise summary string for a node."""

    dataset = row.get("sourceID") or "Offshore Leaks"
    countries = ", ".join(split_multi_value(row.get("countries")))
    jurisdiction = row.get("jurisdiction_description") or row.get("jurisdiction") or ""
    company_type = row.get("company_type") or ""
    parts = [part for part in [dataset, countries, jurisdiction, company_type] if part]
    descriptor = " | ".join(parts)
    base = row.get("name") or row.get("original_name") or row.get("address") or row.get("node_id") or "Unnamed node"
    return f"{base} | {descriptor}" if descriptor else str(base)


def build_offshore_node_content(node_type: str, row: dict[str, str]) -> str:
    """Build searchable free text for a node."""

    ordered_fields = [
        row.get("name"),
        row.get("original_name"),
        row.get("former_name"),
        row.get("jurisdiction_description"),
        row.get("company_type"),
        row.get("address"),
        row.get("countries"),
        row.get("sourceID"),
        row.get("service_provider"),
        row.get("note"),
    ]
    if node_type == "officer":
        ordered_fields.append("Officer or beneficial owner record")
    if node_type == "intermediary":
        ordered_fields.append("Intermediary or service-provider record")
    return "\n".join(part.strip() for part in ordered_fields if part and part.strip())


def build_offshore_node_record(node_type: str, row: dict[str, str]) -> OffshoreNodeRecord:
    """Normalize one Offshore Leaks node row."""

    node_id = (row.get("node_id") or "").strip()
    if not node_id:
        raise ValueError("Offshore node row is missing node_id")
    name = (
        (row.get("name") or "").strip()
        or (row.get("original_name") or "").strip()
        or (row.get("address") or "").strip()
        or f"Offshore node {node_id}"
    )
    dataset = (row.get("sourceID") or "Offshore Leaks").strip()
    return OffshoreNodeRecord(
        node_key=f"{node_type}:{node_id}",
        node_id=node_id,
        node_type=node_type,  # type: ignore[arg-type]
        name=name,
        normalized_name=normalize_member_lookup_value(name),
        source_dataset=dataset,
        summary=build_offshore_node_summary(node_type, row),
        content=build_offshore_node_content(node_type, row),
        countries=split_multi_value(row.get("countries")),
        country_codes=split_multi_value(row.get("country_codes") or row.get("country_codes")),
        jurisdiction=(row.get("jurisdiction") or "").strip() or None,
        jurisdiction_description=(row.get("jurisdiction_description") or "").strip() or None,
        company_type=(row.get("company_type") or "").strip() or None,
        address=(row.get("address") or "").strip() or None,
        status=(row.get("status") or "").strip() or None,
        service_provider=(row.get("service_provider") or "").strip() or None,
        note=(row.get("note") or "").strip() or None,
        valid_until=(row.get("valid_until") or "").strip() or None,
        metadata={key: value for key, value in row.items() if value not in (None, "")},
    )


def build_offshore_relationship_record(row: dict[str, str]) -> OffshoreRelationshipRecord:
    """Normalize one Offshore Leaks relationship row."""

    start_node_id = (row.get("node_id_start") or "").strip()
    end_node_id = (row.get("node_id_end") or "").strip()
    rel_type = (row.get("rel_type") or "").strip()
    dataset = (row.get("sourceID") or "Offshore Leaks").strip()
    raw_key = "|".join(
        [
            start_node_id,
            end_node_id,
            rel_type,
            (row.get("link") or "").strip(),
            dataset,
        ]
    )
    relationship_key = hashlib.sha1(raw_key.encode("utf-8")).hexdigest()
    return OffshoreRelationshipRecord(
        relationship_key=relationship_key,
        start_node_id=start_node_id,
        end_node_id=end_node_id,
        rel_type=rel_type,
        link=(row.get("link") or "").strip() or None,
        status=(row.get("status") or "").strip() or None,
        start_date=(row.get("start_date") or "").strip() or None,
        end_date=(row.get("end_date") or "").strip() or None,
        source_dataset=dataset,
        metadata={key: value for key, value in row.items() if value not in (None, "")},
    )


def iter_offshore_nodes(
    settings: Settings,
    *,
    node_types: Iterable[str] | None = None,
    limit_per_type: int = 0,
) -> Iterable[OffshoreNodeRecord]:
    """Yield normalized Offshore Leaks nodes from the cached archive."""

    archive_path = ensure_offshore_archive(settings)
    selected = list(node_types) if node_types else list(OFFSHORE_NODE_FILES.keys())
    with zipfile.ZipFile(archive_path) as archive:
        for node_type in selected:
            filename = OFFSHORE_NODE_FILES[node_type]
            count = 0
            with archive.open(filename) as handle:
                text = (line.decode("utf-8", errors="ignore") for line in handle)
                reader = csv.DictReader(text)
                for row in reader:
                    yield build_offshore_node_record(node_type, row)
                    count += 1
                    if limit_per_type > 0 and count >= limit_per_type:
                        break


def iter_offshore_relationships(
    settings: Settings,
    *,
    limit: int = 0,
) -> Iterable[OffshoreRelationshipRecord]:
    """Yield normalized Offshore Leaks relationships from the cached archive."""

    archive_path = ensure_offshore_archive(settings)
    with zipfile.ZipFile(archive_path) as archive:
        with archive.open("relationships.csv") as handle:
            text = (line.decode("utf-8", errors="ignore") for line in handle)
            reader = csv.DictReader(text)
            count = 0
            for row in reader:
                yield build_offshore_relationship_record(row)
                count += 1
                if limit > 0 and count >= limit:
                    break
