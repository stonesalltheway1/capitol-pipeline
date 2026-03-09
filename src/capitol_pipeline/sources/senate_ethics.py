"""Senate trade feed adapter for Capitol Pipeline.

This starts with the Senate watcher aggregate feed because it is the same
interim source the site already uses. The long-term plan is to replace or
augment this with official Senate Ethics disclosure ingestion.
"""

from __future__ import annotations

import httpx
from pydantic import BaseModel

from capitol_pipeline.config import Settings


class SenateWatcherTrade(BaseModel):
    senator: str | None = None
    transaction_date: str | None = None
    ticker: str | None = None
    amount: str | None = None
    type: str | None = None
    asset_description: str | None = None
    comment: str | None = None
    ptr_link: str | None = None
    owner: str | None = None
    asset_type: str | None = None


def fetch_senate_watcher_feed(
    settings: Settings | None = None,
    timeout_seconds: float = 20.0,
) -> list[SenateWatcherTrade]:
    """Fetch the current Senate watcher aggregate JSON feed."""

    settings = settings or Settings()
    with httpx.Client(
        headers={"User-Agent": settings.user_agent},
        follow_redirects=True,
        timeout=timeout_seconds,
    ) as client:
        response = client.get(settings.senate_watcher_url)
        response.raise_for_status()
    payload = response.json()
    return [SenateWatcherTrade.model_validate(row) for row in payload]
