"""FARA corpus models."""

from __future__ import annotations

from pydantic import BaseModel, Field


class FaraRegistrantRecord(BaseModel):
    """One active FARA registrant."""

    registration_number: int
    name: str
    normalized_name: str
    registration_date: str | None = None
    address_1: str | None = None
    address_2: str | None = None
    city: str | None = None
    state: str | None = None
    zip_code: str | None = None
    summary: str
    content: str
    metadata: dict[str, object] = Field(default_factory=dict)


class FaraForeignPrincipalRecord(BaseModel):
    """One foreign principal attached to a FARA registrant."""

    principal_key: str
    registration_number: int
    foreign_principal_name: str
    normalized_name: str
    registrant_name: str
    country_name: str | None = None
    registration_date: str | None = None
    foreign_principal_registration_date: str | None = None
    address_1: str | None = None
    address_2: str | None = None
    city: str | None = None
    state: str | None = None
    zip_code: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


class FaraShortFormRecord(BaseModel):
    """One active short-form registrant tied to a FARA registrant."""

    short_form_key: str
    registration_number: int
    registrant_name: str
    first_name: str
    last_name: str
    full_name: str
    normalized_name: str
    short_form_date: str | None = None
    registration_date: str | None = None
    address_1: str | None = None
    address_2: str | None = None
    city: str | None = None
    state: str | None = None
    zip_code: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


class FaraDocumentRecord(BaseModel):
    """One document metadata row tied to a FARA registrant."""

    document_key: str
    registration_number: int
    registrant_name: str
    document_type: str
    date_stamped: str | None = None
    url: str
    short_form_name: str | None = None
    foreign_principal_name: str | None = None
    foreign_principal_country: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


class FaraMemberMatchRecord(BaseModel):
    """One exact-name Congress match against a FARA entity."""

    match_key: str
    member_id: str
    member_name: str
    member_slug: str | None = None
    registration_number: int
    entity_kind: str
    entity_key: str
    registrant_name: str
    match_type: str = "exact_name"
    match_value: str
    metadata: dict[str, object] = Field(default_factory=dict)


class FaraRegistrantBundle(BaseModel):
    """All FARA rows tied to one registrant."""

    registrant: FaraRegistrantRecord
    foreign_principals: list[FaraForeignPrincipalRecord] = Field(default_factory=list)
    short_forms: list[FaraShortFormRecord] = Field(default_factory=list)
    documents: list[FaraDocumentRecord] = Field(default_factory=list)
