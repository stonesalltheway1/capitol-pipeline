"""A Senate re-read updates the row that already holds the trade.

A row keeps the id it was inserted with when a correction later rewrites the
facts that id hashes (the member-attribution repair, the amount-band
migration). Re-reading the filing computes a fresh id, misses the row under
``ON CONFLICT (id)``, and the INSERT then trips idx_trades_unique_senate_natural_v2.
senate-refresh and senate-reconcile failed on that from 2026-10-02.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest

from capitol_pipeline.config import Settings
from capitol_pipeline.exporters import neon
from capitol_pipeline.models.congress import MemberMatch, NormalizedTradeRow

URL = "https://efdsearch.senate.gov/search/view/ptr/b999bc0e-3eb0-4ca9-ab07-8e8f2e04b41f/"


def _senate_row(**overrides: Any) -> NormalizedTradeRow:
    fields: dict[str, Any] = dict(
        member=MemberMatch(id="m-A000383", name="Example Senator"),
        source="senate-efd",
        disclosure_kind="senate-trade",
        source_id="b999bc0e:1",
        source_url=URL,
        ticker="WMB",
        asset_description="Williams Companies, Inc. (The) Common Stock",
        asset_type="Option",
        transaction_type="purchase",
        transaction_date="2026-08-20",
        disclosure_date="2026-09-17",
        amount_min=15001,
        amount_max=50000,
        owner="joint",
    )
    fields.update(overrides)
    return NormalizedTradeRow(**fields)


class _Cursor:
    def __init__(self, existing: dict[str, str]) -> None:
        self.existing = existing
        self.lookups: list[dict[str, Any]] = []
        self.upserted: list[dict[str, Any]] = []
        self._next: dict[str, str] | None = None

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: dict[str, Any]) -> None:
        assert sql is neon.SENATE_NATURAL_KEY_LOOKUP_SQL
        self.lookups.append(params)
        found = self.existing.get(params["source_url"])
        self._next = {"id": found} if found else None

    def fetchone(self) -> dict[str, str] | None:
        return self._next

    def executemany(self, sql: str, params: list[dict[str, Any]]) -> None:
        assert sql is neon.TRADE_UPSERT_SQL
        self.upserted.extend(params)


def _run(monkeypatch: pytest.MonkeyPatch, rows: list[NormalizedTradeRow], existing: dict[str, str]) -> tuple[_Cursor, dict[str, object]]:
    cursor = _Cursor(existing)

    class _Connection:
        def cursor(self) -> _Cursor:
            return cursor

        def commit(self) -> None:
            return None

    @contextmanager
    def _connection(_settings: Settings):  # type: ignore[no-untyped-def]
        yield _Connection()

    monkeypatch.setattr(neon, "neon_connection", _connection)
    return cursor, neon.upsert_trade_rows_to_neon(Settings(), rows)


def test_a_row_held_under_an_older_id_is_updated_in_place(monkeypatch: pytest.MonkeyPatch) -> None:
    stale = "tr-senate-9dba6cce9021c944"  # hashed under m-A000377 before re-attribution
    cursor, result = _run(monkeypatch, [_senate_row()], existing={URL: stale})
    assert cursor.upserted[0]["id"] == stale
    assert result["trade_ids"] == [stale]


def test_a_new_trade_keeps_its_canonical_id(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor, result = _run(monkeypatch, [_senate_row()], existing={})
    assert cursor.upserted[0]["id"] == "tr-senate-a47a4fd26cd9ca0f"
    assert result["trade_ids"] == ["tr-senate-a47a4fd26cd9ca0f"]


def test_batch_twins_share_one_id_and_one_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    # Same natural key, but the canonical ids differ because the owner text
    # differs only in case/whitespace, which the index folds and the hash
    # (after its own normalization) may not.
    first = _senate_row()
    twin = _senate_row(owner=" Joint ", source_id="b999bc0e:2")
    cursor, _ = _run(monkeypatch, [first, twin], existing={})
    assert len(cursor.lookups) == 1
    assert cursor.upserted[0]["id"] == cursor.upserted[1]["id"]


def test_house_rows_never_look_up_a_natural_key(monkeypatch: pytest.MonkeyPatch) -> None:
    house = _senate_row(source="house-clerk", disclosure_kind="house-ptr", source_id="20000037:9")
    cursor, result = _run(monkeypatch, [house], existing={})
    assert cursor.lookups == []
    assert result["trade_ids"] == ["tr-house-20000037-9"]


def test_a_null_in_an_uncoalesced_column_is_outside_the_index() -> None:
    payload = {"source": "senate_efd", "member_id": "m-A000383", "asset_description": None,
               "transaction_type": "purchase", "transaction_date": "2026-08-20"}
    assert neon._senate_natural_key(payload) is None
    assert neon._senate_natural_key({**payload, "asset_description": "X", "source": "house_clerk"}) is None


def test_the_lookup_mirrors_every_column_of_the_index() -> None:
    sql = " ".join(neon.SENATE_NATURAL_KEY_LOOKUP_SQL.split())
    for fragment in (
        "member_id = %(member_id)s::text",
        "COALESCE(NULLIF(upper(btrim(ticker)), ''), '')",
        "lower(btrim(asset_description))",
        "lower(btrim(transaction_type))",
        "transaction_date = %(transaction_date)s::date",
        "COALESCE(disclosure_date, '0001-01-01'::date)",
        "COALESCE(amount_min, 0)",
        "COALESCE(amount_max, 0)",
        "lower(COALESCE(NULLIF(btrim(owner), ''), 'self'))",
        "lower(COALESCE(source_url, ''))",
        "source = ANY(%(sources)s)",
    ):
        assert fragment in sql
