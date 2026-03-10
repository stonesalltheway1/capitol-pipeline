"""Official Congress.gov legislative context models."""

from __future__ import annotations

from pydantic import BaseModel, Field


class CongressBillRecord(BaseModel):
    """Canonical official metadata for one bill from Congress.gov."""

    bill_key: str
    site_bill_id: str
    congress: int
    bill_type: str
    bill_number: str
    title: str
    short_title: str | None = None
    origin_chamber: str | None = None
    policy_area: str | None = None
    sponsor_bioguide_id: str | None = None
    sponsor_full_name: str | None = None
    sponsor_party: str | None = None
    sponsor_state: str | None = None
    sponsor_district: str | None = None
    introduced_date: str | None = None
    latest_action_date: str | None = None
    latest_action_text: str | None = None
    update_date: str | None = None
    update_date_including_text: str | None = None
    legislation_url: str | None = None
    api_url: str | None = None
    summaries_count: int = 0
    actions_count: int = 0
    committees_count: int = 0
    cosponsors_count: int = 0
    subject_count: int = 0
    text_version_count: int = 0
    summary: str | None = None
    content: str = ""
    committee_names: list[str] = Field(default_factory=list)
    committee_codes: list[str] = Field(default_factory=list)
    subjects: list[str] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)


class CongressBillSummaryRecord(BaseModel):
    """One official bill summary version from Congress.gov."""

    summary_key: str
    bill_key: str
    site_bill_id: str
    congress: int
    bill_type: str
    bill_number: str
    version_code: str | None = None
    action_date: str | None = None
    action_desc: str | None = None
    update_date: str | None = None
    text: str
    source_url: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


class CongressBillActionRecord(BaseModel):
    """One official bill action from Congress.gov."""

    action_key: str
    bill_key: str
    site_bill_id: str
    congress: int
    bill_type: str
    bill_number: str
    action_date: str | None = None
    action_time: str | None = None
    action_code: str | None = None
    action_type: str | None = None
    text: str
    source_system_name: str | None = None
    source_system_code: int | None = None
    committee_names: list[str] = Field(default_factory=list)
    committee_codes: list[str] = Field(default_factory=list)
    source_url: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)
