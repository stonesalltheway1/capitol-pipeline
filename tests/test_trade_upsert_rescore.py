"""A re-read that changes a trade sends it back to the conflict scorer.

``upsert_trade_rows_to_neon`` used to overwrite ``conflict_score`` with 0 and
``conflict_flags`` with [] on every re-read and leave ``conflict_scored_at``
alone. The site's nightly scorer (lib/daily-pipeline.ts) selects rows by that
watermark, so a re-read zeroed a trade's score for good. The statement now
resets the score *and* clears the watermark when anything the score is
computed from changed, and keeps the score when nothing did.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest

from capitol_pipeline.config import Settings
from capitol_pipeline.exporters import neon
from capitol_pipeline.models.congress import MemberMatch, NormalizedTradeRow


def _squash(sql: str) -> str:
    return " ".join(sql.split())


def test_every_scored_field_is_compared() -> None:
    sql = _squash(neon.TRADE_UPSERT_SQL)
    for field in neon.TRADE_SCORED_FIELDS:
        assert f"trades.{field}" in sql
        assert f"EXCLUDED.{field}" in sql
    # The row's id and its provenance are not inputs to the score.
    assert "trades.comment" not in sql
    assert "trades.parser_version" not in sql


def test_the_watermark_is_cleared_only_when_the_score_is_reset() -> None:
    sql = _squash(neon.TRADE_UPSERT_SQL)
    changed = _squash(neon._SCORED_CHANGED)
    assert f"conflict_score = CASE WHEN {changed} THEN EXCLUDED.conflict_score ELSE trades.conflict_score END" in sql
    assert f"conflict_flags = CASE WHEN {changed} THEN EXCLUDED.conflict_flags ELSE trades.conflict_flags END" in sql
    assert f"conflict_scored_at = CASE WHEN {changed} THEN NULL ELSE trades.conflict_scored_at END" in sql
    # Nothing else may write the score unconditionally.
    assert "conflict_score = EXCLUDED.conflict_score," not in sql
    assert "conflict_flags = EXCLUDED.conflict_flags," not in sql


def test_the_upsert_runs_that_statement(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    class _Cursor:
        def __enter__(self) -> "_Cursor":
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        def executemany(self, sql: str, params: list[dict[str, Any]]) -> None:
            seen["sql"], seen["params"] = sql, params

    class _Connection:
        def cursor(self) -> _Cursor:
            return _Cursor()

        def commit(self) -> None:
            seen["committed"] = True

    @contextmanager
    def _connection(_settings: Settings):  # type: ignore[no-untyped-def]
        yield _Connection()

    monkeypatch.setattr(neon, "neon_connection", _connection)
    row = NormalizedTradeRow(
        member=MemberMatch(id="m-X000001", name="Example Member"),
        source="house-clerk",
        disclosure_kind="house-ptr",
        source_id="20000037:9",
        source_url="https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2014/20000037.pdf",
        ticker="GMCR",
        asset_description="Green Mountain Coffee Roasters, Inc.",
        asset_type="Stock",
        transaction_type="sale",
        transaction_date="2013-12-20",
        disclosure_date="2014-01-08",
        amount_min=1001,
        amount_max=15000,
        owner="spouse",
    )
    result = neon.upsert_trade_rows_to_neon(Settings(), [row])
    assert result["trade_ids"] == ["tr-house-20000037-9"]
    assert seen["sql"] is neon.TRADE_UPSERT_SQL
    assert seen["committed"] is True
    assert seen["params"][0]["conflict_score"] == 0.0


def test_an_amendment_fold_sends_the_row_back_to_the_scorer(monkeypatch: pytest.MonkeyPatch) -> None:
    executed: list[tuple[str, tuple[object, ...]]] = []

    class _Cursor:
        rowcount = 1

        def __enter__(self) -> "_Cursor":
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[object, ...]) -> None:
            executed.append((sql, params))

    class _Connection:
        def cursor(self) -> _Cursor:
            return _Cursor()

        def commit(self) -> None:
            return None

    @contextmanager
    def _connection(_settings: Settings):  # type: ignore[no-untyped-def]
        yield _Connection()

    monkeypatch.setattr(neon, "neon_connection", _connection)
    neon.apply_house_amendment_changes(
        Settings(),
        updates={
            "tr-house-1-1": {"amount_min": 15001, "amount_max": 50000},
            "tr-house-1-2": {"comment_note": "Amended by House PTR 2"},
        },
        deletes=[],
    )
    by_id = {params[-1]: sql for sql, params in executed}
    assert "conflict_scored_at = NULL" in by_id["tr-house-1-1"]
    # A note alone changes nothing the score reads.
    assert "conflict_scored_at" not in by_id["tr-house-1-2"]
