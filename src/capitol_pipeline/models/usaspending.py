"""Models for USAspending recipient and award corpora."""

from __future__ import annotations

from pydantic import BaseModel, Field


class UsaspendingRecipientRecord(BaseModel):
    """One normalized USAspending recipient summary row."""

    recipient_id: str
    name: str
    normalized_name: str
    query_name: str
    recipient_code: str | None = None
    uei: str | None = None
    total_amount: float | None = None
    total_outlays: float | None = None
    source_url: str | None = None
    summary: str
    content: str
    metadata: dict[str, object] = Field(default_factory=dict)


class UsaspendingCompanyMatchRecord(BaseModel):
    """One company-to-recipient match built from USAspending search results."""

    match_key: str
    company_id: str
    company_name: str
    ticker: str | None = None
    query_name: str
    canonical_recipient_id: str
    recipient_name: str
    normalized_recipient_name: str
    recipient_code: str | None = None
    uei: str | None = None
    match_type: str
    match_value: str
    total_amount: float | None = None
    award_count: int = 0
    top_agencies: list[str] = Field(default_factory=list)
    source_url: str | None = None
    summary: str
    content: str
    metadata: dict[str, object] = Field(default_factory=dict)


class UsaspendingAwardRecord(BaseModel):
    """One normalized USAspending award row tied to a company match."""

    award_key: str
    match_key: str
    canonical_recipient_id: str
    recipient_name: str
    award_id: str
    internal_id: int | None = None
    generated_internal_id: str | None = None
    action_date: str | None = None
    award_amount: float | None = None
    award_type: str | None = None
    awarding_agency: str | None = None
    awarding_agency_id: int | None = None
    agency_slug: str | None = None
    description: str | None = None
    source_url: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)
