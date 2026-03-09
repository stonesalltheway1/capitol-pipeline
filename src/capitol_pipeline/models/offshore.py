"""Models for the ICIJ Offshore Leaks corpus."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


OffshoreNodeType = Literal["entity", "officer", "address", "intermediary", "other"]


class OffshoreNodeRecord(BaseModel):
    """A single normalized node from the Offshore Leaks corpus."""

    node_key: str
    node_id: str
    node_type: OffshoreNodeType
    name: str
    normalized_name: str
    source_dataset: str
    summary: str
    content: str
    countries: list[str] = Field(default_factory=list)
    country_codes: list[str] = Field(default_factory=list)
    jurisdiction: str | None = None
    jurisdiction_description: str | None = None
    company_type: str | None = None
    address: str | None = None
    status: str | None = None
    service_provider: str | None = None
    note: str | None = None
    valid_until: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


class OffshoreRelationshipRecord(BaseModel):
    """A single normalized relationship from the Offshore Leaks corpus."""

    relationship_key: str
    start_node_id: str
    end_node_id: str
    rel_type: str
    link: str | None = None
    status: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    source_dataset: str
    metadata: dict[str, object] = Field(default_factory=dict)


class OffshoreMemberMatchRecord(BaseModel):
    """An exact-name Congress match against an Offshore Leaks node."""

    match_key: str
    member_id: str
    member_name: str
    member_slug: str | None = None
    node_key: str
    node_type: OffshoreNodeType
    source_dataset: str
    match_type: str = "exact_name"
    match_value: str
    metadata: dict[str, object] = Field(default_factory=dict)
