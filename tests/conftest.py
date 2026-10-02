"""Shared test setup."""

from __future__ import annotations

import pytest

from capitol_pipeline.parsers import ptr_vision_provider


@pytest.fixture(autouse=True)
def _fresh_vision_run() -> None:
    """Every test starts with an unlimited, unstopped vision call budget.

    The budget is module state (one per CLI run); a test that spends or stops
    it must not leave the next one unable to make a call.
    """

    ptr_vision_provider.start_run(None)
