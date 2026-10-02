"""A House PTR row keeps its trade id when its filing is read again.

House trade ids are ``tr-house-<doc>-<line>``, and ``line`` was the row's
position in whatever parse published it. Position is not identity. When a
parser fix finds rows an earlier parse missed -- 2,856 sale rows whose "S" the
Clerk's small-capital fonts print as "s", recovered on 2026-10-02 -- every row
after the first recovered one moves down a place, and an upsert keyed on
position overwrites row N with a different transaction and leaves the old
last rows behind as stale duplicates. ``house_amendments`` makes it worse: it
deletes ``tr-house-<doc>-<line>`` for every amended or deleted row, so a shifted
number deletes somebody else's trade.

So a filing that has been published or transcribed before keeps its
numbering. Before its rows are written, each one is matched by content to
what the filing carried last time:

* the stub's stored ``parsedTransactions`` -- the transcription the ids were
  assigned from, which still holds a row's values as printed even after an
  amendment changed the published row; and
* the filing's live ``tr-house-<doc>-<n>`` rows in ``trades``.

A row that matches keeps that line number. The owner, ticker and asset name
may differ -- correcting them is what a parser fix is for -- but the
transaction date must agree, and two different tickers never match. A row
that matches nothing is new: it takes the next number above every number the
filing has used and above its own position, so it can never land on an id
that once meant something else. A previously published row the new parse no
longer finds is reported and left alone; nothing here deletes. A filing with
no history is numbered by position, exactly as before.

Some rows a filing prints are deliberately not in ``trades``, and a later
read must not put them back:

* the amendment logic publishes an amendment whose original was transcribed
  but never published under the original filing's date, with "First
  disclosed in House PTR <doc> filed <date>" in its comment. That row *is* the
  original transaction; publishing the original as well would count it twice;
* a row whose id was withdrawn (a later filing deleted it, or a repair took it
  out) is not brought back by reading its filing again;
* a stored row marked ``withheld`` (set by this step, or by a repair, with the
  reason) stays withheld.

Such rows stay in the stored transcription, marked, and are not written.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from capitol_pipeline.house_amendments import PriorLine, prior_from_trade, score_prior
from capitol_pipeline.models.congress import (
    HousePtrParseResult,
    HousePtrTransaction,
    MemberMatch,
    NormalizedTradeRow,
)

#: A pair below this score is not the same row (see :func:`score_line`).
MATCH_THRESHOLD = 7.5

_STOP_TOKENS = frozenset({
    "inc", "corp", "corporation", "co", "company", "the", "common", "stock", "class", "a", "b", "c",
    "shares", "ltd", "plc", "llc", "lp", "l", "p", "of", "and", "com", "sa", "nv", "ag", "ordinary", "group",
})


@dataclass(frozen=True)
class LineReference:
    """One row a filing carried before, under the line number its id uses."""

    line_number: int
    transaction_date: str | None
    transaction_type: str
    amount_min: int
    amount_max: int
    ticker: str | None
    asset_description: str
    owner: str
    origin: str  # "stored" (the stub's transcription), "trade" (live) or "withdrawn"
    withheld: str | None = None  # a stored row deliberately kept out of trades, and why


@dataclass
class LineAssignment:
    """New line number for each row of a fresh parse, and what was decided."""

    numbers: dict[int, int] = field(default_factory=dict)
    matched: int = 0
    renumbered: int = 0
    fresh: list[int] = field(default_factory=list)
    unmatched_references: list[int] = field(default_factory=list)

    def summary(self) -> dict[str, object]:
        return {
            "matched": self.matched,
            "renumbered": self.renumbered,
            "fresh": self.fresh,
            "unmatchedReferences": self.unmatched_references,
        }


def _tokens(value: str | None) -> set[str]:
    text = re.sub(r"\([^)]*\)|\[[^\]]*\]", " ", (value or "").lower())
    return {token for token in re.findall(r"[a-z0-9]+", text) if token not in _STOP_TOKENS}


#: A ticker in parentheses inside a name. Not one straight after a digit: the
#: "(k)" of "401(k)" and the "(b)" of "403(b)" in an account name are not
#: tickers, and reading "(k)" as K kept doc 20033916's JPM row from matching
#: its own earlier transcription ("... 401(k) - Dave JP Morgan Chase & Co.").
_TICKER_IN_NAME = re.compile(r"(?<!\d)\(\s*([A-Za-z][A-Za-z.\-]{0,7})\s*\)")


def _ticker_in(value: str | None) -> str | None:
    found = _TICKER_IN_NAME.findall(value or "")
    return found[-1].upper() if found else None


#: Names an earlier parse wrote when it could not read one.
_PLACEHOLDER_NAME = re.compile(r"^\s*(?:Pending House PTR extraction|[\d\s]*)\s*$", re.I)

#: An asset this similar is the same asset whatever else differs.
STRONG_ASSET = 0.8


def asset_similarity(a_ticker: str | None, a_asset: str | None, b_ticker: str | None, b_asset: str | None) -> float:
    """1.0 for one ticker, 0.0 for two different ones, name overlap otherwise.

    A ticker still sitting in a name ("apple Inc. (aaPl)", as the case-blind
    parser left it) counts as that row's ticker. One name's words all inside
    the other's is 0.8: a parser fix that completes a name ("093024" becoming
    "ALLY FINANCIAL INC B/E 05.125% 093024") does not change the asset. A name
    an old parse could not read at all ("Pending House PTR extraction", a bare
    number) says nothing either way and scores 0.3.
    """

    a_ticker = (a_ticker or _ticker_in(a_asset) or "").upper() or None
    b_ticker = (b_ticker or _ticker_in(b_asset) or "").upper() or None
    if a_ticker and b_ticker:
        return 1.0 if a_ticker == b_ticker else 0.0
    ta, tb = _tokens(a_asset), _tokens(b_asset)
    if ta == tb and ta:
        return 1.0
    if ta and tb and (ta <= tb or tb <= ta):
        return STRONG_ASSET
    if not ta or not tb or _PLACEHOLDER_NAME.match(a_asset or "") or _PLACEHOLDER_NAME.match(b_asset or ""):
        return 0.3
    return len(ta & tb) / len(ta | tb)


def score_line(row: HousePtrTransaction, reference: LineReference, *, position: int) -> float | None:
    """How well ``reference`` explains ``row`` as the same printed row, or None.

    Content decides, not position: when a parser fix inserts rows, every later
    row's position is wrong, so the old position is only a tie-break (0.25)
    between rows that are otherwise identical. The transaction date must
    agree; two different tickers never match; a different type or amount band
    is accepted only for the same asset. Then type and amount (2 each), the
    asset (up to 3) and the owner (0.5) add up, and a pair needs 7.5.
    """

    if not row.transaction_date or row.transaction_date != reference.transaction_date:
        return None
    row_ticker = (row.ticker or _ticker_in(row.asset_description) or "").upper()
    ref_ticker = (reference.ticker or _ticker_in(reference.asset_description) or "").upper()
    if row_ticker and ref_ticker and row_ticker != ref_ticker:
        return None
    asset = asset_similarity(row.ticker, row.asset_description, reference.ticker, reference.asset_description)
    same_type = row.transaction_type == reference.transaction_type
    same_amount = (row.amount_min, row.amount_max) == (reference.amount_min, reference.amount_max)
    if (not same_type or not same_amount) and asset < STRONG_ASSET:
        return None
    score = 3.0
    score += 2.0 if same_type else 0.0
    score += 2.0 if same_amount else 0.0
    score += 3.0 * asset
    score += 0.5 if (row.owner or "self") == (reference.owner or "self") else 0.0
    score += 0.25 if reference.line_number == position else 0.0
    return score


def references_from(
    doc_id: str,
    stored: list[dict[str, object]] | None,
    live: list[dict[str, object]] | None,
    withdrawn: list[dict[str, object]] | None = None,
) -> list[LineReference]:
    """The rows a filing carried before: its stored transcription, its live
    trades, and the last snapshot of each of its withdrawn ids."""

    references: list[LineReference] = []
    for entry in stored or []:
        if not isinstance(entry, dict):
            continue
        try:
            line_number = int(entry.get("line_number"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        references.append(LineReference(
            line_number=line_number,
            transaction_date=str(entry["transaction_date"]) if entry.get("transaction_date") else None,
            transaction_type=str(entry.get("transaction_type") or ""),
            amount_min=int(entry.get("amount_min") or 0),
            amount_max=int(entry.get("amount_max") or 0),
            ticker=str(entry["ticker"]) if entry.get("ticker") else None,
            asset_description=str(entry.get("asset_description") or ""),
            owner=str(entry.get("owner") or "self"),
            origin="stored",
            withheld=str(entry["withheld"]) if entry.get("withheld") else None,
        ))
    pattern = re.compile(rf"^tr-house-{re.escape(doc_id)}-(\d+)$")
    rows = [(row, "trade") for row in live or []] + [(row, "withdrawn") for row in withdrawn or []]
    for row, origin in rows:
        found = pattern.match(str(row.get("id") or ""))
        if not found:
            continue
        references.append(LineReference(
            line_number=int(found.group(1)),
            transaction_date=str(row["transaction_date"]) if row.get("transaction_date") else None,
            transaction_type=str(row.get("transaction_type") or ""),
            amount_min=int(row.get("amount_min") or 0),
            amount_max=int(row.get("amount_max") or 0),
            ticker=str(row["ticker"]) if row.get("ticker") else None,
            asset_description=str(row.get("asset_description") or ""),
            owner=str(row.get("owner") or "self"),
            origin=origin,
        ))
    return references


def assign_line_numbers(
    transactions: list[HousePtrTransaction],
    references: list[LineReference],
) -> LineAssignment:
    """Give each row of a fresh parse the line number its id already uses.

    ``transactions`` carry their position in the fresh parse as
    ``line_number``. With no references every row keeps its position.
    Matching is one-to-one, best pair first; a stored row and a live trade
    under the same number are one reference, scored on whichever content
    agrees better.
    """

    assignment = LineAssignment()
    if not references:
        assignment.numbers = {row.line_number: row.line_number for row in transactions}
        return assignment
    by_number: dict[int, list[LineReference]] = {}
    for reference in references:
        by_number.setdefault(reference.line_number, []).append(reference)
    pairs: list[tuple[float, int, int]] = []
    for row in transactions:
        for number, group in by_number.items():
            scores = [score_line(row, reference, position=row.line_number) for reference in group]
            best = max((score for score in scores if score is not None), default=None)
            if best is not None and best >= MATCH_THRESHOLD:
                pairs.append((best, row.line_number, number))
    # Highest score first; ties go to the nearest number, then document order.
    pairs.sort(key=lambda pair: (-pair[0], abs(pair[1] - pair[2]), pair[1], pair[2]))
    used_rows: set[int] = set()
    used_numbers: set[int] = set()
    for _score, position, number in pairs:
        if position in used_rows or number in used_numbers:
            continue
        used_rows.add(position)
        used_numbers.add(number)
        assignment.numbers[position] = number
    assignment.matched = len(assignment.numbers)
    assignment.renumbered = sum(1 for position, number in assignment.numbers.items() if position != number)
    next_number = max([*by_number, *(row.line_number for row in transactions), 0]) + 1
    for row in transactions:
        if row.line_number in assignment.numbers:
            continue
        assignment.numbers[row.line_number] = next_number
        assignment.fresh.append(next_number)
        next_number += 1
    assignment.unmatched_references = sorted(set(by_number) - used_numbers)
    return assignment


def apply_line_numbers(
    parsed: HousePtrParseResult,
    trades: list[NormalizedTradeRow],
    numbers: dict[int, int],
) -> tuple[HousePtrParseResult, list[NormalizedTradeRow]]:
    """Renumber a parse and its trade rows (``source_id`` is ``<doc>:<line>``)."""

    if all(position == number for position, number in numbers.items()):
        return parsed, trades
    transactions = [
        row.model_copy(update={"line_number": numbers.get(row.line_number, row.line_number)})
        for row in parsed.transactions
    ]
    renumbered: list[NormalizedTradeRow] = []
    for trade in trades:
        doc_id, _, line = trade.source_id.rpartition(":")
        try:
            position = int(line)
        except ValueError:
            renumbered.append(trade)
            continue
        renumbered.append(trade.model_copy(update={"source_id": f"{doc_id}:{numbers.get(position, position)}"}))
    return parsed.model_copy(update={"transactions": transactions}), renumbered


def _as_prior(trade: NormalizedTradeRow, doc_id: str, filing_date: str | None) -> PriorLine:
    return PriorLine(
        key=trade.source_id,
        doc_id=doc_id,
        filing_date=filing_date,
        ticker=trade.ticker,
        asset_description=trade.asset_description,
        transaction_type=trade.transaction_type,
        transaction_date=trade.transaction_date,
        amount_min=trade.amount_min,
        amount_max=trade.amount_max,
        owner=trade.owner or "self",
        filing_status=trade.filing_status,
    )


def _standin_row(row: dict[str, object]) -> NormalizedTradeRow:
    prior = prior_from_trade(row)
    return NormalizedTradeRow(
        member=MemberMatch(name=""),
        source="house-clerk",
        disclosure_kind="house-ptr",
        source_id=prior.key,
        ticker=prior.ticker,
        asset_description=prior.asset_description,
        asset_type="Asset",
        transaction_type=prior.transaction_type,
        transaction_date=prior.transaction_date,
        amount_min=prior.amount_min,
        amount_max=prior.amount_max,
        owner=prior.owner,
        comment=str(row.get("comment") or "") or None,
        filing_status="amended",
    )


def _represented_by(
    trades: list[NormalizedTradeRow],
    standins: list[dict[str, object]],
    *,
    doc_id: str,
    filing_date: str | None,
) -> dict[str, str]:
    """``source_id`` -> id of the amendment row that already publishes it.

    ``standins`` are trades rows from other filings whose comment says they
    were "First disclosed in House PTR <doc_id>". Matching is the amendment
    matcher's, read the other way round: the stand-in restates the row.
    """

    marker = f"First disclosed in House PTR {doc_id} "
    eligible = [trade for trade in trades if (trade.filing_status or "new").lower() not in ("amended", "deleted")]
    pairs: list[tuple[float, str, str]] = []
    for standin in standins:
        if marker not in str(standin.get("comment") or ""):
            continue
        restated = _standin_row(standin)
        for trade in eligible:
            scored = score_prior(restated, _as_prior(trade, doc_id, filing_date), filing_date=None)
            if scored is not None:
                pairs.append((scored[0], str(standin["id"]), trade.source_id))
    pairs.sort(key=lambda pair: -pair[0])
    used: set[str] = set()
    held: dict[str, str] = {}
    for _score, standin_id, source_id in pairs:
        if standin_id in used or source_id in held:
            continue
        used.add(standin_id)
        held[source_id] = standin_id
    return held


def withhold_rows(
    parsed: HousePtrParseResult,
    trades: list[NormalizedTradeRow],
    references: list[LineReference],
    *,
    doc_id: str,
    filing_date: str | None,
    standins: list[dict[str, object]] | None = None,
) -> tuple[HousePtrParseResult, list[NormalizedTradeRow], list[dict[str, object]]]:
    """Keep out of ``trades`` the rows of a renumbered parse that must not be published.

    Only a row with no live ``trades`` row under its number is a candidate: a
    published row is never withdrawn here. A candidate is held back when its
    stored row is marked ``withheld``, when its number is an id that was
    withdrawn (a ``withdrawn`` reference, matched by content like any other),
    or when an amendment row from another filing already publishes it. Held
    rows keep their number and are marked in the transcription, so the next
    read holds them back too. Returns the parse, the rows to write, a report.
    """

    live = {reference.line_number for reference in references if reference.origin == "trade"}
    withdrawn = {reference.line_number for reference in references if reference.origin == "withdrawn"}
    marked = {
        reference.line_number: reference.withheld
        for reference in references
        if reference.origin == "stored" and reference.withheld
    }
    holds: dict[int, str] = {}
    for transaction in parsed.transactions:
        number = transaction.line_number
        if number in live:
            continue
        if number in marked:
            holds[number] = str(marked[number])
        elif number in withdrawn:
            holds[number] = f"withdrawn earlier: tr-house-{doc_id}-{number} is in the trade change log"
    # Every row competes for a stand-in, published ones included, so a
    # stand-in for a row this filing already published cannot settle on an
    # identical-looking neighbour; only an unpublished row is then held.
    for source_id, standin_id in _represented_by(
        trades, standins or [], doc_id=doc_id, filing_date=filing_date
    ).items():
        number = int(source_id.rpartition(":")[2])
        if number not in live and number not in holds:
            holds[number] = f"represented by {standin_id}"
    if not holds and not any(t.withheld for t in parsed.transactions):
        return parsed, trades, []
    transactions = [
        transaction.model_copy(update={"withheld": holds.get(transaction.line_number)})
        for transaction in parsed.transactions
    ]
    kept = [trade for trade in trades if _number(trade) not in holds]
    report = [{"line": number, "reason": reason} for number, reason in sorted(holds.items())]
    return parsed.model_copy(update={"transactions": transactions}), kept, report


def _number(trade: NormalizedTradeRow) -> int | None:
    try:
        return int(trade.source_id.rpartition(":")[2])
    except ValueError:
        return None
