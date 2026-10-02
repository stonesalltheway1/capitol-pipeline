"""The House review queue gives every stuck filing a turn.

``pipeline@house-review`` runs ``process-house-review --limit 12`` every six
hours. It used to take the twelve most recently *detected* filings from the
review queue with no regard to when they were last tried, so the same twelve
came back ``needs_review`` run after run (most seen 7-9 times in 50 hours,
one 109 times) while the other ~620 were never reached.

Now the queue is served least recently attempted first, and a filing whose
attempt comes back the same as last time waits twice as long before the next
one (12 h, 24 h, 48 h ... up to a week). A change of OCR or vision
configuration makes every reviewed filing eligible again at once.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from capitol_pipeline import cli
from capitol_pipeline.config import Settings
from capitol_pipeline.models.congress import HousePtrParseResult


# ── Selection ────────────────────────────────────────────────────────────────


class _Recorder:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    def connection(self, _settings: Settings) -> Any:
        recorder = self

        class _Cursor:
            def __enter__(self) -> "_Cursor":
                return self

            def __exit__(self, *_exc: object) -> bool:
                return False

            def execute(self, sql: str, params: tuple[Any, ...]) -> None:
                recorder.executed.append((sql, params))

            def fetchall(self) -> list[dict[str, Any]]:
                return []

        class _Connection:
            def __enter__(self) -> "_Connection":
                return self

            def __exit__(self, *_exc: object) -> bool:
                return False

            def cursor(self) -> _Cursor:
                return _Cursor()

        return _Connection()


@pytest.fixture()
def recorder(monkeypatch: pytest.MonkeyPatch) -> _Recorder:
    from capitol_pipeline.exporters import neon

    rec = _Recorder()
    monkeypatch.setattr(neon, "neon_connection", rec.connection)
    return rec


def _order_by(sql: str) -> str:
    return " ".join(sql.split("ORDER BY", 1)[1].split("LIMIT", 1)[0].split())


def test_the_scheduled_review_serves_least_recently_reviewed_first_and_backs_off(recorder: _Recorder) -> None:
    from capitol_pipeline.exporters import neon

    config = cli.review_config_signature("auto", "off")
    neon.fetch_house_stub_queue(Settings(), limit=12, only_needs_review=True, review_config=config)

    sql, params = recorder.executed[0]
    # Least recently attempted first; among equals, the newest filing year.
    assert _order_by(sql).startswith(
        "NULLIF(metadata->>'extractionStartedAt', '')::timestamptz ASC NULLS FIRST, filing_year DESC"
    )
    # Back-off is honoured, unless the filing was last reviewed another way.
    assert "metadata->>'retryAfter'" in sql
    assert "(metadata ? 'reviewLastConfig' AND metadata->>'reviewLastConfig' <> %s)" in sql
    assert params == (config, 12)


def test_a_targeted_review_ignores_back_off(recorder: _Recorder) -> None:
    from capitol_pipeline.exporters import neon

    neon.fetch_house_stub_queue(
        Settings(),
        limit=1,
        only_needs_review=True,
        doc_ids=["8219444"],
        review_config=cli.review_config_signature("auto", "on"),
    )
    sql, params = recorder.executed[0]
    assert "reviewLastConfig" not in sql
    assert params == (["8219444"], 1)


def test_the_ingest_queue_order_is_unchanged(recorder: _Recorder) -> None:
    from capitol_pipeline.exporters import neon

    neon.fetch_house_stub_queue(Settings(), limit=25)
    sql, params = recorder.executed[0]
    assert _order_by(sql).startswith("CASE WHEN status = 'pending_extraction' THEN 0")
    assert "extractionStartedAt" not in sql
    assert params == (25,)


# ── Back-off ────────────────────────────────────────────────────────────────


def test_identical_outcomes_double_the_wait_up_to_a_week() -> None:
    outcome = cli.review_outcome_signature("needs_review", parser_version="regex-v1")
    metadata: dict[str, object] = {}
    waits = []
    for _ in range(7):
        streak, hours = cli.next_review_backoff(metadata, outcome, 12)
        waits.append(hours)
        metadata = {"reviewLastOutcome": outcome, "reviewOutcomeStreak": streak}
    assert waits == [12, 24, 48, 96, 168, 168, 168]


def test_a_different_outcome_starts_over() -> None:
    first = cli.review_outcome_signature("needs_review", trade_rows=0, parser_version="regex-v1")
    second = cli.review_outcome_signature("needs_review", trade_rows=3, parser_version="gemini-vision-v1")
    metadata = {"reviewLastOutcome": first, "reviewOutcomeStreak": 4}
    assert cli.next_review_backoff(metadata, second, 12) == (1, 12)
    assert cli.next_review_backoff(metadata, first, 12) == (5, 168)


class _StubState:
    def __init__(self) -> None:
        self.updates: list[dict[str, Any]] = []

    def __call__(self, _settings: Settings, *, doc_id: str, status: str, extracted_trade_id: Any,
                 metadata_updates: dict[str, Any]) -> None:
        self.updates.append({"doc_id": doc_id, "status": status, **metadata_updates})


def _queue_row(metadata: dict[str, Any]) -> dict[str, Any]:
    return {
        "doc_id": "9116331",
        "filing_year": 2026,
        "source": "house-clerk",
        "source_url": "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/9116331.pdf",
        "status": "needs_review",
        "metadata": {
            "memberId": "m-X000001",
            "memberName": "Pat Example",
            "filingDate": "2026-09-15",
            **metadata,
        },
    }


def _run_review(monkeypatch: pytest.MonkeyPatch, row: dict[str, Any]) -> _StubState:
    state = _StubState()
    monkeypatch.setattr(cli, "update_house_stub_state", state)
    monkeypatch.setattr(
        cli,
        "parse_live_house_stub",
        lambda stub, settings, ocr, vision: (
            HousePtrParseResult(doc_id=stub.doc_id, parser_confidence=0.0, parser_version="regex-v1"),
            [],
        ),
    )
    monkeypatch.setattr(
        cli,
        "persist_parsed_house_stub",
        lambda settings, stub, parsed, trades: {"stubStatus": "needs_review", "trades": {"upserted": 0}},
    )
    summary = cli.process_house_queue_rows(
        Settings(), [row], ocr_backend="auto", vision_backend="off", review_mode=True
    )
    assert summary["needsReview"] == 1
    return state


def _hours_until(iso: str) -> float:
    return (datetime.fromisoformat(iso) - datetime.now(timezone.utc)) / timedelta(hours=1)


def test_a_review_attempt_records_its_configuration_and_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    state = _run_review(monkeypatch, _queue_row({}))
    started, finished = state.updates[0], state.updates[-1]

    assert started["status"] == "extracting"
    assert started["reviewLastConfig"] == "ocr=auto;vision=off"
    assert started["reviewAttempts"] == 1

    assert finished["status"] == "needs_review"
    assert finished["reviewOutcomeStreak"] == 1
    assert finished["reviewLastOutcome"] == "needs_review|rows=0|withheld=0|parser=regex-v1"
    assert 11.9 < _hours_until(finished["retryAfter"]) <= 12


def test_the_same_answer_again_waits_longer(monkeypatch: pytest.MonkeyPatch) -> None:
    row = _queue_row(
        {
            "reviewAttempts": 3,
            "reviewLastOutcome": "needs_review|rows=0|withheld=0|parser=regex-v1",
            "reviewOutcomeStreak": 3,
            "reviewLastConfig": "ocr=auto;vision=off",
        }
    )
    finished = _run_review(monkeypatch, row).updates[-1]
    assert finished["reviewOutcomeStreak"] == 4
    assert 95.9 < _hours_until(finished["retryAfter"]) <= 96


def test_the_ingest_path_keeps_its_fixed_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    state = _StubState()
    monkeypatch.setattr(cli, "update_house_stub_state", state)
    monkeypatch.setattr(
        cli,
        "parse_live_house_stub",
        lambda stub, settings, ocr, vision: (
            HousePtrParseResult(doc_id=stub.doc_id, parser_confidence=0.0, parser_version="regex-v1"),
            [],
        ),
    )
    monkeypatch.setattr(
        cli,
        "persist_parsed_house_stub",
        lambda settings, stub, parsed, trades: {"stubStatus": "needs_review", "trades": {"upserted": 0}},
    )
    row = dict(_queue_row({"reviewOutcomeStreak": 5}), status="pending_extraction")
    cli.process_house_queue_rows(Settings(), [row], ocr_backend="auto", review_retry_hours=12)
    finished = state.updates[-1]
    assert "reviewOutcomeStreak" not in finished
    assert "reviewLastConfig" not in state.updates[0]
    assert 11.9 < _hours_until(finished["retryAfter"]) <= 12
