"""Amended and deleted House PTR rows are not new trades.

Every row on an electronically filed House PTR carries a "Filing Status":

* ``New`` -- the transaction is disclosed for the first time;
* ``Amended`` -- it restates a transaction an earlier PTR already disclosed,
  usually to correct a field (amount, type, owner, date, even the asset);
* ``Deleted`` -- it withdraws a transaction an earlier PTR disclosed.

Publishing an amended row as a trade counts the transaction twice and, worse,
measures its lateness from the amendment: Nancy Pelosi's page showed "10 Late
Filings" that were all amendment lines restating trades she had disclosed on
time. So at ingest:

* an amended row whose original is already a row in ``trades`` is folded into
  that row -- the original keeps its id and its disclosure date and takes the
  amended values -- and is not inserted;
* an amended row whose original is not in ``trades`` but was transcribed from
  an earlier filing is inserted with that filing's date as its disclosure
  date, so lateness runs from the first disclosure;
* an amended row whose original cannot be found is inserted as it is; its
  comment still says "Filing Status: Amended", which the site reads as
  "lateness not established";
* a deleted row withdraws its original (the row is deleted) and is never
  inserted itself.

The matching is the method used for the 2026-10-02 production repair
(``trades_houseamend2_actions_20261002``), checked there against every
amended and deleted line in 5,939 House PTRs: same member, an earlier filing,
the same asset and transaction date -- or, because correcting them is what
amendments are for, a date off by a typo (a year, a month, a few days) or an
asset whose ticker changed while everything else held.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date
import re

from capitol_pipeline.models.congress import NormalizedTradeRow

AMENDED = "amended"
DELETED = "deleted"

_STOP_TOKENS = frozenset({
    "inc", "corp", "corporation", "co", "company", "the", "common", "stock", "class", "a", "b", "c",
    "shares", "ltd", "plc", "llc", "lp", "l", "p", "of", "and", "com", "sa", "nv", "ag", "ordinary", "group",
})

#: Words too common in security names to say two names are the same issuer.
_GENERIC_TOKENS = frozenset({
    "holdings", "holding", "financial", "companies", "adr", "ads", "unsp", "spon", "sponsored", "fund", "funds",
    "large", "cap", "growth", "value", "international", "intl", "trust", "capital", "bank", "bancorp", "energy",
    "partners", "global", "technologies", "technology", "systems", "services", "industries", "resources",
    "income", "index", "etf", "series", "new", "york", "note", "notes", "bond", "bonds", "call", "make",
    "whole", "american", "united", "states", "national", "first", "pharmaceuticals", "therapeutics", "health",
    "healthcare", "investment", "investors", "realty", "properties", "acquisition", "depositary", "units",
    "limited", "incorporated", "brands", "ordinary", "class", "common", "preferred", "treasury",
})


@dataclass(frozen=True)
class PriorLine:
    """A transaction an earlier filing by the same member disclosed.

    ``trade_id`` is set when the line is a row in ``trades``; a line known only
    from an earlier stub's stored transcription has none.
    """

    key: str
    doc_id: str
    filing_date: str | None
    ticker: str | None
    asset_description: str
    transaction_type: str
    transaction_date: str | None
    amount_min: int = 0
    amount_max: int = 0
    owner: str = "self"
    description: str | None = None
    trade_id: str | None = None
    filing_status: str | None = None


@dataclass
class AmendmentPlan:
    """What persisting one filing should do to ``trades``."""

    rows: list[NormalizedTradeRow] = field(default_factory=list)
    updates: dict[str, dict[str, object]] = field(default_factory=dict)
    deletes: list[str] = field(default_factory=list)
    actions: list[dict[str, object]] = field(default_factory=list)

    def summary(self) -> dict[str, object]:
        return {
            "counts": dict(Counter(str(action["action"]) for action in self.actions)),
            "actions": self.actions,
        }


def _tokens(value: str | None) -> set[str]:
    text = re.sub(r"\([^)]*\)|\[[^\]]*\]", " ", (value or "").lower())
    return {token for token in re.findall(r"[a-z0-9]+", text) if token not in _STOP_TOKENS}


def _distinctive(value: str | None) -> set[str]:
    return {t for t in _tokens(value) if len(t) >= 3 and t not in _GENERIC_TOKENS and not t.isdigit()}


#: A ticker in parentheses inside a name. Not one straight after a digit: the
#: "(k)" of "401(k)" and the "(b)" of "403(b)" in an account name are not
#: tickers (see the same helper in house_line_ids).
_TICKER_IN_NAME = re.compile(r"(?<!\d)\(\s*([A-Za-z][A-Za-z.\-]{0,7})\s*\)")


def _ticker_in(value: str | None) -> str | None:
    found = _TICKER_IN_NAME.findall(value or "")
    return found[-1].upper() if found else None


def asset_similarity(a_ticker: str | None, a_asset: str | None, b_ticker: str | None, b_asset: str | None) -> float:
    """1.0 for the same ticker, 0.0 for different ones, name overlap otherwise."""

    a_ticker = (a_ticker or _ticker_in(a_asset) or "").upper() or None
    b_ticker = (b_ticker or _ticker_in(b_asset) or "").upper() or None
    if a_ticker and b_ticker:
        return 1.0 if a_ticker == b_ticker else 0.0
    ta, tb = _tokens(a_asset), _tokens(b_asset)
    if a_ticker and a_ticker.lower() in tb:
        return 0.9
    if b_ticker and b_ticker.lower() in ta:
        return 0.9
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _text_similarity(a: str | None, b: str | None) -> float:
    ta = set(re.findall(r"[a-z0-9$.,]+", (a or "").lower()))
    tb = set(re.findall(r"[a-z0-9$.,]+", (b or "").lower()))
    if not ta and not tb:
        return 0.5
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _description(comment: str | None) -> str | None:
    match = re.search(r"Description\s*:\s*(.+?)(?=\s*\|\s*(?:Comments?|Subholding|Location|Filing|Parsed)\b|$)",
                      comment or "", re.I)
    return match.group(1).strip() if match else None


def _is_band(low: int, high: int) -> bool:
    return 0 < low < high


def _parse_date(value: str | None) -> date | None:
    try:
        return date.fromisoformat(value) if value else None
    except ValueError:
        return None


def score_prior(row: NormalizedTradeRow, prior: PriorLine, *, filing_date: str | None) -> tuple[float, str] | None:
    """How well ``prior`` explains ``row`` as the transaction it restates.

    Returns (score, tier) or None. Tiers, strongest first: ``exact_date``,
    ``year_typo`` (same month and day, a year apart), ``month_typo`` (same year
    and day), ``near_date`` (within 60 days; the type may change only within
    five), and ``asset_corrected`` (same date, type, amount and owner, a
    different ticker, but a distinctive name word or the description shared).
    """

    row_date, prior_date = _parse_date(row.transaction_date), _parse_date(prior.transaction_date)
    if row_date is None or prior_date is None:
        return None
    days = abs((row_date - prior_date).days)
    same_type = (prior.transaction_type or "").lower() == (row.transaction_type or "").lower()
    same_amount = (prior.amount_min, prior.amount_max) == (row.amount_min, row.amount_max)
    same_owner = (prior.owner or "self") == (row.owner or "self")
    asset = asset_similarity(row.ticker, row.asset_description, prior.ticker, prior.asset_description)
    row_description = _description(row.comment)
    if asset < 0.34:
        shared_name = bool(_distinctive(row.asset_description) & _distinctive(prior.asset_description))
        shared_description = bool(row_description and prior.description) and (
            _text_similarity(row_description, prior.description) >= 0.6
        )
        if not (days == 0 and same_type and same_amount and same_owner and (shared_name or shared_description)):
            return None
        tier, score = "asset_corrected", 5.0
    elif days == 0:
        tier, score = "exact_date", 10.0
    elif (row_date.month, row_date.day) == (prior_date.month, prior_date.day) and abs(row_date.year - prior_date.year) == 1:
        tier, score = "year_typo", 8.0
    elif (row_date.year, row_date.day) == (prior_date.year, prior_date.day):
        tier, score = "month_typo", 6.0
    elif days <= 60:
        tier, score = "near_date", 4.0 - days / 6
    else:
        return None
    if not same_type and not (tier == "exact_date" or (tier == "near_date" and days <= 5)):
        return None
    score += 2 * asset
    score += 1.5 if same_type else 0
    score += 1.0 if same_amount else 0
    score += 0.5 if same_owner else 0
    score += 2 * _text_similarity(row_description, prior.description)
    # An original report cannot predate the trade it reports.
    if prior.filing_date and row.transaction_date and prior.filing_date < row.transaction_date:
        score -= 3
    # A report filed the same day is rarely the one being corrected when an
    # earlier one carries the same line; an amendment restates a live
    # transaction, not one already withdrawn.
    if filing_date and prior.filing_date == filing_date:
        score -= 0.5
    if (row.filing_status or "").lower() == AMENDED and (prior.filing_status or "").lower() == DELETED:
        score -= 1
    return score, tier


def _assign(
    rows: list[NormalizedTradeRow],
    priors: list[PriorLine],
    *,
    filing_date: str | None,
    doc_id: str,
) -> dict[int, tuple[PriorLine, str]]:
    """One-to-one assignment of this filing's rows to earlier lines, best first.

    Two passes: an amendment usually restates one earlier report, so the
    second pass favours lines from the report that explained most rows in the
    first.
    """

    eligible = [
        prior for prior in priors
        if prior.doc_id != doc_id
        and (not filing_date or not prior.filing_date or (prior.filing_date, prior.doc_id) < (filing_date, doc_id))
    ]
    order = {prior.key: index for index, prior in enumerate(
        sorted(eligible, key=lambda p: (p.filing_date or "", p.doc_id, p.key)))}

    def pairs(dominant: str | None) -> list[tuple[float, int, str, PriorLine, str]]:
        found = []
        for index, row in enumerate(rows):
            for prior in eligible:
                scored = score_prior(row, prior, filing_date=filing_date)
                if scored is None:
                    continue
                score, tier = scored
                if dominant and prior.doc_id == dominant:
                    score += 2
                # Prefer the most recent earlier version (a direct predecessor).
                score += 0.001 * order[prior.key] / max(len(order), 1)
                found.append((score, index, prior.key, prior, tier))
        return found

    def greedy(found):
        found.sort(key=lambda item: -item[0])
        used_rows: set[int] = set()
        used_priors: set[str] = set()
        chosen: dict[int, tuple[PriorLine, str]] = {}
        for _score, index, key, prior, tier in found:
            if index in used_rows or key in used_priors:
                continue
            used_rows.add(index)
            used_priors.add(key)
            chosen[index] = (prior, tier)
        return chosen

    first = greedy(pairs(None))
    votes = Counter(prior.doc_id for prior, _tier in first.values())
    dominant = None
    if votes:
        top, count = votes.most_common(1)[0]
        if count * 2 >= len(first):
            dominant = top
    return greedy(pairs(dominant)) if dominant else first


def amended_changes(row: NormalizedTradeRow, prior: PriorLine) -> dict[str, object]:
    """The trades fields the amendment changed relative to the original."""

    changes: dict[str, object] = {}
    if (row.amount_min, row.amount_max) != (prior.amount_min, prior.amount_max) and row.amount_max > 0:
        # A degenerate amount ("$15,001") is a parse artefact, not a correction.
        if _is_band(row.amount_min, row.amount_max) or not _is_band(prior.amount_min, prior.amount_max):
            changes["amount_min"] = row.amount_min
            changes["amount_max"] = row.amount_max
    if row.transaction_type and row.transaction_type != prior.transaction_type:
        changes["transaction_type"] = row.transaction_type
    if row.owner and row.owner != prior.owner:
        changes["owner"] = row.owner
    if row.transaction_date and row.transaction_date != prior.transaction_date:
        changes["transaction_date"] = row.transaction_date
    return changes


def plan_house_amendments(
    rows: list[NormalizedTradeRow],
    *,
    doc_id: str,
    filing_date: str | None,
    trade_priors: list[PriorLine],
    transcribed_priors: list[PriorLine] = (),  # type: ignore[assignment]
) -> AmendmentPlan:
    """Decide what one filing's rows do to ``trades``. Pure; no database.

    ``trade_priors`` are the member's rows in ``trades`` from other filings;
    ``transcribed_priors`` are lines from the member's earlier stubs' stored
    transcriptions, used only to date an amendment whose original never became
    a row.
    """

    plan = AmendmentPlan()
    restating = [row for row in rows if (row.filing_status or "").lower() in (AMENDED, DELETED)]
    plan.rows = [row for row in rows if (row.filing_status or "").lower() not in (AMENDED, DELETED)]
    if not restating:
        return plan

    matched = _assign(restating, trade_priors, filing_date=filing_date, doc_id=doc_id)
    amended_unmatched = [
        row for index, row in enumerate(restating)
        if index not in matched and (row.filing_status or "").lower() == AMENDED
    ]
    dated = _assign(amended_unmatched, list(transcribed_priors), filing_date=filing_date, doc_id=doc_id)
    dated_by_row = {id(row): dated[index] for index, row in enumerate(amended_unmatched) if index in dated}

    for index, row in enumerate(restating):
        status = (row.filing_status or "").lower()
        own_id = f"tr-house-{row.source_id.replace(':', '-')}"
        action: dict[str, object] = {
            "line": row.source_id,
            "status": status,
            "transactionDate": row.transaction_date,
            "ticker": row.ticker,
            "asset": row.asset_description[:80],
        }
        found = matched.get(index)
        if found is not None:
            prior, tier = found
            action.update({"original": prior.trade_id, "originalDoc": prior.doc_id,
                           "originalFilingDate": prior.filing_date, "tier": tier})
            # A row this filing published on an earlier run goes either way.
            plan.deletes.append(own_id)
            if status == DELETED:
                plan.deletes.append(str(prior.trade_id))
                action["action"] = "withdrew_original"
            else:
                changes = amended_changes(row, prior)
                changes["comment_note"] = f"Amended by House PTR {doc_id} filed {filing_date}"
                plan.updates[str(prior.trade_id)] = changes
                action["action"] = "applied_to_original"
                action["changes"] = {k: v for k, v in changes.items() if k != "comment_note"}
            plan.actions.append(action)
            continue
        if status == DELETED:
            # Nothing on file to withdraw; a withdrawal is not a trade.
            plan.deletes.append(own_id)
            action["action"] = "dropped_deleted_line"
            plan.actions.append(action)
            continue
        dated_found = dated_by_row.get(id(row))
        if dated_found is not None:
            prior, tier = dated_found
            note = (f"First disclosed in House PTR {prior.doc_id} filed {prior.filing_date}; this row is that "
                    f"transaction as restated in House PTR {doc_id}, so it carries the first filing's disclosure date")
            plan.rows.append(row.model_copy(update={
                "disclosure_date": prior.filing_date,
                "comment": " | ".join(part for part in ((row.comment or "").strip(), note) if part),
            }))
            action.update({"action": "inserted_with_original_date", "originalDoc": prior.doc_id,
                           "originalFilingDate": prior.filing_date, "tier": tier})
        else:
            plan.rows.append(row)
            action["action"] = "inserted_original_not_located"
        plan.actions.append(action)
    plan.deletes = list(dict.fromkeys(plan.deletes))
    return plan


def prior_from_trade(row: dict[str, object]) -> PriorLine:
    """A ``trades`` row (as fetched by fetch_house_amendment_priors) as a prior line."""

    trade_id = str(row["id"])
    doc_match = re.match(r"^tr-house-(\d+)", trade_id) or re.search(r"/(\d+)\.pdf", str(row.get("source_url") or ""))
    comment = str(row.get("comment") or "") or None
    status = re.search(r"F?iling\s+Status\s*:\s*(New|Amended|Deleted)\b", comment or "", re.I)
    return PriorLine(
        key=trade_id,
        doc_id=doc_match.group(1) if doc_match else "",
        filing_date=str(row["disclosure_date"]) if row.get("disclosure_date") else None,
        ticker=(str(row["ticker"]) if row.get("ticker") else None),
        asset_description=str(row.get("asset_description") or ""),
        transaction_type=str(row.get("transaction_type") or ""),
        transaction_date=str(row["transaction_date"]) if row.get("transaction_date") else None,
        amount_min=int(row.get("amount_min") or 0),
        amount_max=int(row.get("amount_max") or 0),
        owner=str(row.get("owner") or "self"),
        description=_description(comment),
        trade_id=trade_id,
        filing_status=status.group(1).lower() if status else None,
    )


def priors_from_transcription(doc_id: str, filing_date: str | None, transactions: list[dict]) -> list[PriorLine]:
    """A stub's stored ``parsedTransactions`` as prior lines (no trade ids)."""

    lines = []
    for entry in transactions or []:
        if not isinstance(entry, dict):
            continue
        comment = str(entry.get("comment") or "") or None
        status = entry.get("filing_status")
        if not status:
            found = re.search(r"F?iling\s+Status\s*:\s*(New|Amended|Deleted)\b", comment or "", re.I)
            status = found.group(1).lower() if found else None
        if status in (AMENDED, DELETED):
            # Only a first disclosure can date a restatement.
            continue
        lines.append(PriorLine(
            key=f"{doc_id}:{entry.get('line_number')}",
            doc_id=doc_id,
            filing_date=filing_date,
            ticker=(str(entry["ticker"]) if entry.get("ticker") else None),
            asset_description=str(entry.get("asset_description") or ""),
            transaction_type=str(entry.get("transaction_type") or ""),
            transaction_date=str(entry["transaction_date"]) if entry.get("transaction_date") else None,
            amount_min=int(entry.get("amount_min") or 0),
            amount_max=int(entry.get("amount_max") or 0),
            owner=str(entry.get("owner") or "self"),
            description=_description(comment),
            filing_status=status,
        ))
    return lines
