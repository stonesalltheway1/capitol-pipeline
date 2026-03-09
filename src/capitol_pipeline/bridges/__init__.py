"""Bridge helpers for exporting Capitol Pipeline output into site-facing shapes."""

from .capitol_exposed import build_house_stub_payload, build_trade_payload
from .search_documents import (
    build_alert_search_document,
    build_bill_search_document,
    build_committee_search_document,
    build_dossier_search_document,
    build_fara_member_match_search_document,
    build_fara_registrant_search_document,
    build_house_ptr_search_document,
    build_house_ptr_search_document_from_stub_row,
    build_member_search_document,
    build_news_post_search_document,
    build_offshore_match_search_document,
    build_senate_trade_search_document,
)

__all__ = [
    "build_alert_search_document",
    "build_bill_search_document",
    "build_committee_search_document",
    "build_dossier_search_document",
    "build_fara_member_match_search_document",
    "build_fara_registrant_search_document",
    "build_house_ptr_search_document",
    "build_house_ptr_search_document_from_stub_row",
    "build_member_search_document",
    "build_news_post_search_document",
    "build_offshore_match_search_document",
    "build_senate_trade_search_document",
    "build_house_stub_payload",
    "build_trade_payload",
]
