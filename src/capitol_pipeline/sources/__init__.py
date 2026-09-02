"""Source adapters for Capitol Pipeline."""

from .fara import (
    FaraApiClient,
    fetch_active_registrants,
    fetch_foreign_principals,
    fetch_reg_documents,
    fetch_short_forms,
)
from .house_clerk import build_house_feed_url, build_house_ptr_pdf_url, fetch_house_feed
from .icij_offshore_leaks import (
    ensure_offshore_archive,
    iter_offshore_nodes,
    iter_offshore_relationships,
)
from .senate_efd import (
    EfdReport,
    EfdTransaction,
    SenateEfdClient,
    fetch_electronic_ptr,
    list_ptr_reports,
)
from .senate_ethics import fetch_quiver_bulk_congress_feed, fetch_senate_watcher_feed

__all__ = [
    "build_house_feed_url",
    "build_house_ptr_pdf_url",
    "EfdReport",
    "EfdTransaction",
    "FaraApiClient",
    "SenateEfdClient",
    "ensure_offshore_archive",
    "fetch_active_registrants",
    "fetch_foreign_principals",
    "fetch_electronic_ptr",
    "fetch_house_feed",
    "fetch_reg_documents",
    "fetch_quiver_bulk_congress_feed",
    "fetch_senate_watcher_feed",
    "fetch_short_forms",
    "list_ptr_reports",
    "iter_offshore_nodes",
    "iter_offshore_relationships",
]
