"""House Clerk feed adapter for Capitol Pipeline."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Callable

import httpx

from capitol_pipeline.config import Settings
from capitol_pipeline.models.congress import FilingStub, MemberMatch

MemberResolver = Callable[[str, str, str | None], MemberMatch | None]


def normalize_date(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    if len(value) == 10 and value[4] == "-":
        return value
    parts = value.split("/")
    if len(parts) == 3:
        month, day, year = parts
        return f"{year.zfill(4)}-{month.zfill(2)}-{day.zfill(2)}"
    return value


def parse_state_district(value: str | None) -> tuple[str | None, str | None]:
    raw = (value or "").strip().upper()
    if not raw:
        return None, None
    return raw[:2], raw[2:] or None


def build_house_feed_url(year: int, settings: Settings | None = None) -> str:
    settings = settings or Settings()
    return f"{settings.house_clerk_feed_base_url}{year}FD.xml"


def build_house_ptr_pdf_url(year: int, doc_id: str, settings: Settings | None = None) -> str:
    settings = settings or Settings()
    return f"{settings.house_ptr_pdf_base_url}/{year}/{doc_id}.pdf"


def parse_house_feed(
    xml_text: str,
    year: int,
    resolver: MemberResolver | None = None,
    settings: Settings | None = None,
) -> list[FilingStub]:
    """Parse the House annual financial disclosure feed into filing stubs."""

    settings = settings or Settings()
    root = ET.fromstring(xml_text)
    filings: list[FilingStub] = []

    for member_node in root.findall(".//Member"):
        doc_id = (member_node.findtext("DocID") or "").strip()
        if not doc_id:
            continue
        filing_type = (member_node.findtext("FilingType") or "PTR").strip().upper()
        if filing_type not in {"P", "PTR"}:
            continue
        first_name = (member_node.findtext("First") or "").strip() or None
        last_name = (member_node.findtext("Last") or "").strip() or None
        state_district = (member_node.findtext("StateDst") or "").strip() or None
        state, district = parse_state_district(state_district)
        matched = (
            resolver(first_name or "", last_name or "", state)
            if resolver and (first_name or last_name)
            else None
        )
        member_name = matched.name if matched else " ".join(
            part for part in [first_name, last_name] if part
        ).strip()
        filings.append(
            FilingStub(
                doc_id=doc_id,
                filing_year=year,
                filing_type=filing_type or "PTR",
                filing_date=normalize_date(member_node.findtext("FilingDate")),
                first_name=first_name,
                last_name=last_name,
                member=matched
                or MemberMatch(
                    name=member_name or f"House PTR {doc_id}",
                    state=state,
                    district=district,
                ),
                source="house-clerk",
                source_url=build_house_ptr_pdf_url(year, doc_id, settings),
                raw_state_district=state_district,
            )
        )

    return filings


def fetch_house_feed(
    year: int,
    resolver: MemberResolver | None = None,
    settings: Settings | None = None,
    timeout_seconds: float = 20.0,
) -> list[FilingStub]:
    """Fetch and parse the House Clerk XML feed for a disclosure year."""

    settings = settings or Settings()
    url = build_house_feed_url(year, settings)
    with httpx.Client(
        headers={"User-Agent": settings.user_agent},
        follow_redirects=True,
        timeout=timeout_seconds,
    ) as client:
        response = client.get(url)
        response.raise_for_status()
    return parse_house_feed(response.text, year=year, resolver=resolver, settings=settings)
