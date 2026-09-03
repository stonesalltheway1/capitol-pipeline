"""Claude vision extraction for scanned and handwritten House PTR PDFs.

Roughly 210 House periodic transaction reports sit in
``house_filing_stubs.status = 'needs_review'`` because they are photocopies or
handwritten forms. The OCR chain (``pymupdf`` -> ``surya`` -> ``docling``)
returns fragments like ``| 9 984 F 1 | Sale | 1 |``, the regex parser scores
0.0, and the filing never becomes trade rows. A vision-capable model reads
those pages directly.

How a filing is read (v2)
-------------------------
1. Every page is rendered with ``pymupdf`` to a PNG at about 150 DPI with the
   long edge capped at :data:`MAX_IMAGE_LONG_EDGE` pixels.
2. Each page is also rendered small at 0, 90, 180 and 270 degrees, and a cheap
   ``claude-haiku-4-5`` call is shown all four and asked which one reads
   upright; a second call confirms the pick. (Asking how far a single sideways
   scan is turned was routinely answered "0".) If that fails, a portrait page
   is assumed to be a sideways landscape form and rotated 90 degrees.
3. The upright page images are sent to the read model (``claude-opus-5`` by
   default) **twice**, as two independent requests, asking for the transaction
   grid back as schema-constrained JSON. The model is not deterministic, so a
   field is only trusted when both reads agree on it. Disagreements are nulled
   and the row is marked ``illegible``; rows only one read saw are kept but
   marked ``illegible``; a row-count mismatch forces manual review.
4. When ``pymupdf`` is not importable the PDF itself is sent as a ``document``
   block instead, and orientation is left to the model.

This module is a peer of :mod:`capitol_pipeline.parsers.ptr_llm_fallback`
(the Haiku text fallback, still used when the OCR text layer is decent), not a
replacement.

Guardrails
----------
* ``CAPITOL_PTR_VISION_DISABLED=1`` kills the path at runtime.
* PDFs over :data:`MAX_VISION_PDF_PAGES` pages or :data:`MAX_VISION_PDF_BYTES`
  bytes are skipped with a reason; the stub stays ``needs_review``.
* One filing per call, one retry on 429/5xx per read, and the caller caps
  filings per run with ``--limit``.

Nothing here touches the database. The caller records
``usage`` / ``cost_usd`` / ``reason`` into the stub's ``visionParse`` metadata.
"""

from __future__ import annotations

import base64
import difflib
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# -- Model + version --------------------------------------------------------

#: Default read model. Override per-run with ``CAPITOL_PTR_VISION_MODEL``.
MODEL_ID = "claude-opus-5"

#: Cheap model used only to decide page orientation.
ORIENTATION_MODEL_ID = "claude-haiku-4-5"

#: Recorded as ``parser_version`` on every row this path produces. Named
#: generically so a model swap does not need a new literal everywhere; use
#: :func:`is_vision_parser_version` rather than comparing against it.
VISION_PARSER_VERSION = "claude-vision-v2"

#: Used when the installed SDK rejects ``output_config`` and we fall back to
#: a single strict tool.
VISION_TOOL_NAME = "record_ptr_transactions"


def is_vision_parser_version(version: object) -> bool:
    """Return whether a ``parser_version`` string came from this module.

    Matches every version this path has ever written (``claude-sonnet-5-vision-v1``
    and ``claude-vision-v2`` alike) so status decisions keyed on the literal
    keep working across model changes.
    """

    text = str(version or "").strip().lower()
    return text.startswith("claude-") and "vision" in text


# -- Guardrails -------------------------------------------------------------

MAX_VISION_PDF_PAGES = 25
MAX_VISION_PDF_BYTES = 20 * 1024 * 1024
MAX_OUTPUT_TOKENS = 16000
EFFORT = "high"
RETRY_SLEEP_SECONDS = 5.0

#: Independent reads of the same page images per filing.
READS_PER_FILING = 2

# -- Page rendering ---------------------------------------------------------

RENDER_DPI = 150
MAX_IMAGE_LONG_EDGE = 1568
ROTATIONS: tuple[int, ...] = (0, 90, 180, 270)
ORIENTATION_MAX_TOKENS = 8

#: The four candidate renderings shown to the orientation model are smaller
#: than the read images: legible enough to tell upright from sideways, cheap
#: enough that four of them cost less than one read image.
ORIENTATION_CANDIDATE_LONG_EDGE = 1000

# -- Pricing (USD per MTok: input, output) ----------------------------------

MODEL_PRICING: dict[str, tuple[float, float]] = {
    "claude-opus-5": (5.0, 25.0),
    "claude-opus-4-8": (5.0, 25.0),
    "claude-opus-4-7": (5.0, 25.0),
    "claude-opus-4-6": (5.0, 25.0),
    "claude-sonnet-5": (2.0, 10.0),
    "claude-sonnet-4-6": (3.0, 15.0),
    "claude-haiku-4-5": (1.0, 5.0),
}

#: Family fallbacks for dated or future ids, checked by prefix in this order.
_FAMILY_PRICING: tuple[tuple[str, tuple[float, float]], ...] = (
    ("claude-opus", (5.0, 25.0)),
    ("claude-sonnet-5", (2.0, 10.0)),
    ("claude-sonnet", (3.0, 15.0)),
    ("claude-haiku", (1.0, 5.0)),
)

#: Unknown model: assume the Opus tier so estimates err high.
DEFAULT_PRICING: tuple[float, float] = (5.0, 25.0)

CACHE_READ_MULTIPLIER = 0.1
CACHE_WRITE_MULTIPLIER = 1.25

#: The default read model's rates, kept as module constants for callers that
#: only need the headline numbers.
PRICE_INPUT_PER_MTOK, PRICE_OUTPUT_PER_MTOK = MODEL_PRICING[MODEL_ID]

_EMPTY_USAGE: dict[str, int] = {
    "input": 0,
    "cache_read": 0,
    "cache_write": 0,
    "output": 0,
}

LEGIBILITY_WEIGHTS: dict[str, float] = {
    "clear": 1.0,
    "partial": 0.6,
    "illegible": 0.0,
}

_LEGIBILITY_RANK: dict[str, int] = {"clear": 0, "partial": 1, "illegible": 2}
_LEGIBILITY_BY_RANK: tuple[str, ...] = ("clear", "partial", "illegible")

# -- Two-read agreement -----------------------------------------------------

#: Minimum ``difflib`` ratio between normalized asset descriptions for two
#: rows from different reads to be treated as the same transaction.
SIMILARITY_THRESHOLD = 0.8

#: Disagreement on any of these nulls the field and marks the row illegible.
CRITICAL_FIELDS: tuple[str, ...] = ("transaction_date", "transaction_type", "amount")

#: Disagreement on these nulls the field and marks the row at least partial.
SOFT_FIELDS: tuple[str, ...] = (
    "notification_date",
    "owner",
    "ticker",
    "asset_type_code",
    "cap_gains_over_200",
)


# -- Output schema ----------------------------------------------------------
# Structured outputs reject ``maxItems`` / ``minItems`` / ``minLength`` /
# ``pattern``. ``additionalProperties: false`` and ``required`` are accepted
# and used here.

TRANSACTION_ITEM_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "owner": {
            "anyOf": [
                {"type": "string", "enum": ["self", "spouse", "dependent", "joint"]},
                {"type": "null"},
            ],
            "description": (
                "Owner column. Blank means the filer -> 'self'. SP -> 'spouse'. "
                "DC -> 'dependent'. JT -> 'joint'. Null only when the column is "
                "present but unreadable."
            ),
        },
        "asset_description": {
            "type": "string",
            "description": (
                "The asset exactly as written, including issuer, instrument, "
                "coupon and maturity for bonds. Do not expand abbreviations."
            ),
        },
        "ticker": {
            "type": ["string", "null"],
            "description": "Bare ticker printed in parentheses, or null. Never guess one.",
        },
        "asset_type_code": {
            "type": ["string", "null"],
            "description": (
                "Bracketed asset-type code such as ST, OP, MF, ETF, GS or CS, or null."
            ),
        },
        "transaction_type": {
            "type": "string",
            "enum": ["purchase", "sale", "sale_partial", "exchange"],
            "description": "P, S, S (partial), or E as printed in the Type column.",
        },
        "transaction_date": {
            "type": ["string", "null"],
            "description": "Transaction Date column as YYYY-MM-DD, or null when illegible.",
        },
        "notification_date": {
            "type": ["string", "null"],
            "description": (
                "Notification Date column as YYYY-MM-DD, or null when blank or illegible."
            ),
        },
        "amount_min": {
            "type": "integer",
            "description": (
                "Lower bound of the checked amount bucket in whole dollars. 0 if unreadable."
            ),
        },
        "amount_max": {
            "type": "integer",
            "description": (
                "Upper bound of the checked amount bucket in whole dollars. 0 if unreadable."
            ),
        },
        "cap_gains_over_200": {
            "type": ["boolean", "null"],
            "description": (
                "The 'Cap. Gains > $200?' checkbox. Null when the column is blank "
                "or unreadable."
            ),
        },
        "comment": {
            "type": ["string", "null"],
            "description": (
                "Filer annotations for this row: subholding-of text, account "
                "nicknames, 'Filing Status: New', handwritten margin notes."
            ),
        },
        "legibility": {
            "type": "string",
            "enum": ["clear", "partial", "illegible"],
            "description": (
                "'clear' when every field on the row was readable, 'partial' when "
                "one or more fields were guessed or left null, 'illegible' when "
                "the row could barely be made out at all."
            ),
        },
    },
    "required": [
        "owner",
        "asset_description",
        "ticker",
        "asset_type_code",
        "transaction_type",
        "transaction_date",
        "notification_date",
        "amount_min",
        "amount_max",
        "cap_gains_over_200",
        "comment",
        "legibility",
    ],
}

PTR_VISION_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "filer_name": {
            "type": ["string", "null"],
            "description": "Member name from the form header, without the 'Hon.' honorific.",
        },
        "filing_date": {
            "type": ["string", "null"],
            "description": "Date the report was filed or signed, YYYY-MM-DD, or null.",
        },
        "page_count": {
            "type": ["integer", "null"],
            "description": "Number of pages you were shown.",
        },
        "notes": {
            "type": ["string", "null"],
            "description": (
                "Short operator note: what was hard to read, which rows you skipped, "
                "whether the scan is rotated or cropped."
            ),
        },
        "transactions": {
            "type": "array",
            "description": "One entry per transaction row on the form, in printed order.",
            "items": TRANSACTION_ITEM_SCHEMA,
        },
    },
    "required": ["filer_name", "filing_date", "page_count", "notes", "transactions"],
}


# -- System prompt ----------------------------------------------------------
# Kept stable and cached with ``cache_control: ephemeral``. Opus 5 caches
# prefixes from 512 tokens; Sonnet 5 from 1,024. This is deliberately
# substantive so it clears both floors, and every filing in the review queue
# reuses it verbatim.

# The blank House PTR form carries a pre-printed example row in the grid
# ("Example: Mega Corp. Common Stock", Sale, 02/05/20, 03/07/20,
# $15,001-$50,000). Vision models copy its dates onto real rows whose
# handwriting they cannot read. The prompt warns about it; this is the belt.
EXAMPLE_ROW_TRANSACTION_DATE = "2020-02-05"
EXAMPLE_ROW_NOTIFICATION_DATE = "2020-03-07"
_EXAMPLE_ROW_DESCRIPTION = re.compile(r"\bmega\s+corp", re.IGNORECASE)


def scrub_example_row_values(
    rows: list[dict],
    filing_year: int | None,
) -> tuple[list[dict], int]:
    """Remove the form's example row and neutralise its dates on other rows.

    Returns ``(rows, scrubbed)`` where ``scrubbed`` counts rows that were
    dropped or had a field nulled. A nulled date downgrades the row's
    legibility to ``illegible`` so the stub is routed back to review instead of
    being published with a date that came off the blank form. Filings from 2020
    and 2021 legitimately contain early-2020 trades, so dates are only nulled
    when the filing year is unknown or 2022 or later.
    """

    kept: list[dict] = []
    scrubbed = 0
    guard_dates = filing_year is None or filing_year >= 2022
    for row in rows:
        if not isinstance(row, dict):
            continue
        description = str(row.get("asset_description") or "")
        if _EXAMPLE_ROW_DESCRIPTION.search(description):
            scrubbed += 1
            continue
        if guard_dates:
            touched = False
            for field in ("transaction_date", "notification_date"):
                value = row.get(field)
                if value in (EXAMPLE_ROW_TRANSACTION_DATE, EXAMPLE_ROW_NOTIFICATION_DATE):
                    row[field] = None
                    touched = True
            if touched:
                row["legibility"] = "illegible"
                scrubbed += 1
        kept.append(row)
    return kept, scrubbed


SYSTEM_PROMPT = """You are a careful transcriptionist for United States House of Representatives Periodic Transaction Reports (PTRs). You are given the pages of a single filing, usually as one image per page that has already been turned upright, occasionally as the PDF itself. Most of the filings you will see are photocopies, faxes, phone photographs, or forms completed by hand, which is exactly why they reached you: automated text extraction already failed on them. Your job is to read the transaction grid off the page and return it as structured data, transcribing exactly what is written and never improving on it.

## The form

A House PTR is filed under the STOCK Act by a Member, officer, or employee of the House. It discloses purchases, sales, and exchanges of stocks, bonds, commodity futures, and other securities held by the filer, the filer's spouse, or the filer's dependent children, when the transaction exceeded $1,000. It is a transaction report, not a holdings report: each row is one event, not a position.

The header block, usually on page one, carries the filer's name (often prefixed "Hon."), their status (Member, Officer, Employee, Candidate), their state and district written together as a two letter state code plus a district number such as TX25 or MO02, the reporting period or calendar year, and a filing identification number typically written as "Filing ID #" followed by eight digits. There is frequently a signature and a date near the end of the document, and sometimes a separate "Date Received" stamp applied by the Legislative Resource Center. Handwritten filings often place the name on a ruled line rather than in a printed field.

Beneath the header is the transaction table. On the standard printed form the columns run left to right in this order:

1. **Owner** - who holds the asset.
2. **Asset** - the security description, sometimes with a ticker in parentheses and an asset-type code in square brackets.
3. **Transaction Type** - a single letter code, or on the paper form three tick-box columns headed P, S and S (partial), with a fourth for E.
4. **Date** - the date of the transaction itself.
5. **Notification Date** - the date the filer was notified of the transaction.
6. **Amount** - a checked or circled dollar range bucket; on the paper form one tick-box column per bucket.
7. **Cap. Gains > $200?** - a yes/no checkbox for capital gains over two hundred dollars.

Handwritten and older filings often collapse, reorder, or omit columns, and may run the amount bucket into the margin. Read the column headers on the page you are actually given rather than assuming this order, and map what you find onto the fields of the schema. Follow each row with a ruler's discipline: the tick mark that belongs to a row sits on that row's horizontal line, and the column it belongs to is the one whose header is directly above it. Count columns from the left edge of the amount ladder rather than estimating.

## Owner codes

The Owner column is a short code, and it is blank far more often than not.

- Blank, or "F", or "Filer", or "Self" means the filing Member holds the asset: report `self`.
- "SP", "S", or "Spouse" means the filer's spouse: report `spouse`.
- "DC", "Dependent", "Dep. Child", or "Child" means a dependent child: report `dependent`.
- "JT", "Joint", or "Spouse/DC" means jointly held: report `joint`.

If a code is present but you genuinely cannot make it out, report null rather than defaulting to `self`. A blank column is not the same thing as an unreadable one; blank is `self`.

## Transaction type codes

- **P** - purchase. Report `purchase`.
- **S** - sale. Report `sale`.
- **S (partial)** - a partial sale, sometimes written "S(partial)", "S - partial", or "SP" in the type column rather than the owner column. On the paper form it is its own tick-box column to the right of S. Report `sale_partial`.
- **E** - exchange. Report `exchange`.

Be careful with "SP": in the Owner column it means spouse, in the Type column it means a partial sale. Decide from which column the mark sits in, not from the letters alone. When the type is a tick in one of several columns, report the column whose header is directly above the tick; a tick under "S (partial)" is `sale_partial`, not `sale`.

## Amount buckets

The Amount column is a checkbox or a circle against a printed ladder of ranges. Transcribe the bucket bounds as integers, exactly as the ranges are printed on the form:

- $1,001 - $15,000 -> amount_min 1001, amount_max 15000
- $15,001 - $50,000 -> 15001, 50000
- $50,001 - $100,000 -> 50001, 100000
- $100,001 - $250,000 -> 100001, 250000
- $250,001 - $500,000 -> 250001, 500000
- $500,001 - $1,000,000 -> 500001, 1000000
- $1,000,001 - $5,000,000 -> 1000001, 5000000
- $5,000,001 - $25,000,000 -> 5000001, 25000000
- $25,000,001 - $50,000,000 -> 25000001, 50000000
- Over $50,000,000 -> 50000001, 100000000

The top three brackets are only available to a spouse or dependent child, so seeing one on a filer-owned row is a signal you may have misread the check mark. If no bucket is marked, or you cannot tell which of two adjacent boxes carries the mark, report 0 for both bounds and say so in that row's comment. Never average two buckets, and never invent a precise dollar figure: the form does not carry one.

## Dates

The paper form itself carries a PRE-PRINTED EXAMPLE ROW in the transaction grid, typeset in the same place as a real entry: asset "Example: Mega Corp. Common Stock", an x under Sale, transaction date 02/05/20, notification date 03/07/20, and an x in the $15,001-$50,000 column. It is part of the blank form, not a transaction. Never return it as a row, and never let its values leak into the rows above or below it: if you find yourself writing 2020-02-05, 2020-03-07 or the $15,001-$50,000 band for a row whose own handwritten date or check mark you cannot actually read, you have copied the example - report null for that field and mark the row partial or illegible instead. Every real row has its own handwritten date; read each one on its own line, and read the check mark in the amount column on that same line.

Scans are often rotated ninety degrees or upside down; read them in the orientation of the printed text.

Dates are printed on the form as MM/DD/YYYY, and handwritten as M/D/YY more often than not. Convert every date to YYYY-MM-DD. Two-digit years belong to the reporting period; use the calendar year in the header, or the filing date you are told, to resolve them. The Transaction Date is when the trade happened; the Notification Date is when the filer learned of it, and for a self-directed account the two are often identical. The two dates sit in two separate columns: never read the notification column and report it as the transaction date, and never copy one column into the other. The Notification Date is frequently blank on handwritten forms - report null, not a copy of the transaction date. No date on the form can be later than the date the report was filed. If a date is smudged, cropped, or overwritten so that you cannot read it, report null and mark the row's legibility accordingly. A date you cannot read is never worth guessing: a wrong date silently corrupts the disclosure timeline that this data feeds.

## Cap. Gains > $200?

This is a single checkbox per row. A tick, an X, or a "Y" means true; an explicit "N" or "No" means false. A blank column, a column that does not exist on this form, or a mark you cannot resolve is null. Do not infer it from the amount bucket.

## Asset descriptions and subholdings

Transcribe the asset description as written. Keep issuer names, share-class wording such as "Common Stock" or "Class A", bond coupon rates and maturity dates, fund names, and municipal issuer names intact. Do not expand abbreviations, do not correct spelling, and do not normalize punctuation or capitalization.

Handwriting is the exception to the spelling rule. A handwritten asset is almost always the name of a real, usually well-known, issuer or fund: "AT&T", "General Electric", "Apple", "Vanguard 500". When the strokes could be read either as a nonsense string or as a real issuer name, report the real issuer name, rate the row `partial`, and put your literal reading in the comment. An ampersand in handwriting often looks like a plus sign, a capital T like a lower-case t, and a capital G like a 6.

A ticker appears in parentheses immediately after the description on printed forms. Report only the bare symbol, with no parentheses. Municipal bonds, private funds, limited partnerships, real estate, and directly held cryptocurrency usually have no ticker: report null. **Never supply a ticker that is not printed on the page**, even when the issuer obviously has one - a downstream classifier depends on the distinction between a disclosed ticker and an inferred one.

An asset-type code may appear in square brackets after the description: ST for stock, OP for option, MF for mutual fund, ETF for exchange traded fund, GS for government security, CS for corporate security, and others. Report the bare code without brackets, or null when there is none.

Filings routinely carry per-row annotations under or beside the asset: "Filing Status: New", "Subholding Of:" naming a parent account or trust, a brokerage name with a truncated account number, "Description:" text, or a handwritten margin note. These belong in the row's comment field, not glued onto the asset description. Where a row is a subholding, keep the child asset in asset_description and put the parent in the comment.

## Legibility

Every row carries a legibility rating, and it decides whether a human is asked to look at this filing:

- `clear` - you read every field on the row without straining.
- `partial` - the row is real and usable, but at least one field was hard to read, ambiguous, or reported as null.
- `illegible` - you can tell a row is there but most of it cannot be made out.

Rate honestly and per row. Over-reporting `clear` puts unverified data on a public accountability site; over-reporting `illegible` sends work to a human that did not need to go there.

## Rules

Transcribe, do not interpret. Return one entry per transaction row in printed order. Skip page headers, column headers, subtotals, continuation markers, instructional text, and the certification block: those are not transactions. Do not merge two rows, and do not split one row into two. If the same transaction is listed twice because the table continues across a page break, report it once. If the filing genuinely contains no transaction rows - a cover sheet, an amendment notice, a "no reportable transactions" statement - return an empty transactions array and explain in notes. Never invent a transaction to fill an empty report, and use null wherever the page does not tell you the answer."""


USER_INSTRUCTION = (
    "Transcribe every transaction row from this House Periodic Transaction "
    "Report. Read the pages as images; the text layer is unreliable or absent. "
    "Return the filing header fields and one entry per transaction row, in "
    "printed order, using null wherever the page is illegible."
)

ORIENTATION_SYSTEM_PROMPT = (
    "You judge the orientation of scanned document pages. Reply with a single "
    "number and nothing else."
)

ORIENTATION_QUESTION = (
    "These are four renderings of the same scanned page, each rotated clockwise by "
    "the number of degrees in its label. Exactly one of them shows the printed and "
    "handwritten text upright, reading normally left to right and top to bottom. "
    "Answer with that rendering's label only: 0, 90, 180, or 270."
)

ORIENTATION_CONFIRM_QUESTION = (
    "Is the text on this scanned page upright, reading normally left to right and "
    "top to bottom? Answer YES or NO."
)


# -- Client -----------------------------------------------------------------

_client: Any = None


def _client_once() -> Any:
    """Module-level client factory. Tests patch this."""

    global _client
    if _client is None:
        import anthropic

        _client = anthropic.Anthropic()
    return _client


def resolve_model_id() -> str:
    """Return the configured vision model id."""

    return os.environ.get("CAPITOL_PTR_VISION_MODEL", "").strip() or MODEL_ID


def vision_disabled() -> bool:
    """Return whether the env kill switch is engaged."""

    raw = os.environ.get("CAPITOL_PTR_VISION_DISABLED", "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _has_credentials() -> bool:
    return bool(
        (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
        or (os.environ.get("ANTHROPIC_AUTH_TOKEN") or "").strip()
    )


# -- Helpers ----------------------------------------------------------------


def _pdf_module() -> Any | None:
    """Return the importable pymupdf module, or None."""

    for module_name in ("pymupdf", "fitz"):
        try:
            return __import__(module_name)
        except Exception:  # pragma: no cover - depends on optional extras
            continue
    return None


def count_pdf_pages(pdf_path: Path) -> int | None:
    """Return the page count, or None when no PDF reader is importable."""

    module = _pdf_module()
    if module is not None:
        try:
            with module.open(str(pdf_path)) as document:
                return int(document.page_count)
        except Exception:  # pragma: no cover - depends on optional extras
            pass
    try:  # pragma: no cover - depends on optional extras
        from pypdf import PdfReader

        return len(PdfReader(str(pdf_path)).pages)
    except Exception:
        return None


def pricing_for_model(model: str | None) -> tuple[float, float]:
    """Return ``(input, output)`` USD per MTok for a model id."""

    key = (model or "").strip().lower()
    if key in MODEL_PRICING:
        return MODEL_PRICING[key]
    for family, price in _FAMILY_PRICING:
        if key.startswith(family):
            return price
    return DEFAULT_PRICING


def estimate_cost_usd(usage: dict[str, int], model: str | None = None) -> float:
    """Estimate request cost in USD from a normalized usage dict.

    ``model`` defaults to the configured read model; pass
    :data:`ORIENTATION_MODEL_ID` for the orientation calls.
    """

    price_in, price_out = pricing_for_model(model or resolve_model_id())
    dollars = (
        int(usage.get("input") or 0) * price_in
        + int(usage.get("cache_read") or 0) * price_in * CACHE_READ_MULTIPLIER
        + int(usage.get("cache_write") or 0) * price_in * CACHE_WRITE_MULTIPLIER
        + int(usage.get("output") or 0) * price_out
    ) / 1_000_000
    return round(dollars, 6)


def _normalize_usage(usage: Any) -> dict[str, int]:
    if usage is None:
        return dict(_EMPTY_USAGE)
    return {
        "input": int(getattr(usage, "input_tokens", 0) or 0),
        "cache_read": int(getattr(usage, "cache_read_input_tokens", 0) or 0),
        "cache_write": int(getattr(usage, "cache_creation_input_tokens", 0) or 0),
        "output": int(getattr(usage, "output_tokens", 0) or 0),
    }


def sum_usage(*usages: dict[str, int] | None) -> dict[str, int]:
    """Add normalized usage dicts field by field."""

    total = dict(_EMPTY_USAGE)
    for usage in usages:
        if not usage:
            continue
        for key in total:
            total[key] += int(usage.get(key) or 0)
    return total


def summarize_legibility(transactions: list[dict[str, Any]]) -> dict[str, int]:
    """Count rows by legibility rating."""

    counts = {"clear": 0, "partial": 0, "illegible": 0, "total": 0}
    for row in transactions:
        if not isinstance(row, dict):
            continue
        rating = str(row.get("legibility") or "partial").strip().lower()
        if rating not in LEGIBILITY_WEIGHTS:
            rating = "partial"
        counts[rating] += 1
        counts["total"] += 1
    return counts


def legibility_confidence(counts: dict[str, int]) -> float:
    """Derive a parser confidence from the model's per-row legibility ratings."""

    total = int(counts.get("total") or 0)
    if total <= 0:
        return 0.0
    weighted = sum(
        weight * int(counts.get(key) or 0) for key, weight in LEGIBILITY_WEIGHTS.items()
    )
    return max(0.0, min(0.95, round(weighted / total, 2)))


def majority_illegible(counts: dict[str, int]) -> bool:
    """Return whether more than half the rows were rated illegible."""

    total = int(counts.get("total") or 0)
    if total <= 0:
        return True
    return int(counts.get("illegible") or 0) * 2 > total


def _skip(reason: str, **extra: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": False,
        "skipped": True,
        "reason": reason,
        "model": resolve_model_id(),
        "orientation_model": ORIENTATION_MODEL_ID,
        "parser_version": VISION_PARSER_VERSION,
        "filer_name": None,
        "filing_date": None,
        "page_count": None,
        "notes": None,
        "transactions": [],
        "legibility": {"clear": 0, "partial": 0, "illegible": 0, "total": 0},
        "confidence": 0.0,
        "needs_review": True,
        "usage": dict(_EMPTY_USAGE),
        "cost_usd": 0.0,
        "stop_reason": None,
        "attempts": 0,
        "orientation": None,
        "read_agreement": None,
        "calls": [],
    }
    payload.update(extra)
    return payload


def _is_retryable(error: Exception) -> bool:
    """Return whether an SDK error is worth exactly one more attempt."""

    status = getattr(error, "status_code", None)
    if status is None:
        status = getattr(error, "status", None)
    try:
        status_int = int(status)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        status_int = 0
    if status_int == 429 or 500 <= status_int <= 599:
        return True
    return type(error).__name__ in {
        "RateLimitError",
        "InternalServerError",
        "APIConnectionError",
        "APITimeoutError",
        "OverloadedError",
    }


def _rejected_structured_output(error: Exception) -> bool:
    """Return whether a 400 looks like the API refusing ``output_config.format``."""

    status = getattr(error, "status_code", None)
    if status is None:
        status = getattr(error, "status", None)
    if status not in (400, 422):
        return False
    message = str(error).lower()
    return any(token in message for token in ("output_config", "json_schema", "output_format"))


# -- Page images + orientation ---------------------------------------------


def _image_block(png: bytes) -> dict[str, Any]:
    return {
        "type": "image",
        "source": {
            "type": "base64",
            "media_type": "image/png",
            "data": base64.standard_b64encode(png).decode("ascii"),
        },
    }


def _render_page(
    page: Any,
    module: Any,
    base_rotation: int,
    rotation: int,
    max_long_edge: int = MAX_IMAGE_LONG_EDGE,
) -> tuple[bytes, int, int]:
    """Render one page as PNG bytes with ``rotation`` degrees clockwise applied.

    ``page.set_rotation`` follows the PDF ``/Rotate`` convention (clockwise) and
    only changes the in-memory document. The zoom is chosen so the page renders
    at :data:`RENDER_DPI` unless that would push the long edge past
    ``max_long_edge``.
    """

    page.set_rotation((base_rotation + rotation) % 360)
    rect = page.rect
    long_edge = float(max(rect.width, rect.height)) or 1.0
    zoom = min(RENDER_DPI / 72.0, max_long_edge / long_edge)
    pixmap = page.get_pixmap(matrix=module.Matrix(zoom, zoom), alpha=False)
    return pixmap.tobytes("png"), int(pixmap.width), int(pixmap.height)


def orientation_heuristic(width: int, height: int) -> int:
    """Rotation to apply when the model cannot be asked.

    The House PTR paper form is landscape, so a portrait page is almost always a
    sideways scan of it. Which way it was turned is a coin toss; 90 is the
    convention here and the model confirms it when it can.
    """

    return 90 if height > width else 0


def _first_text(message: Any) -> str:
    for block in list(getattr(message, "content", None) or []):
        if getattr(block, "type", None) == "text":
            return str(getattr(block, "text", "") or "")
    return ""


def _ask_orientation(client: Any, content: list[dict[str, Any]]) -> tuple[str, dict[str, int]]:
    response = client.messages.create(
        model=ORIENTATION_MODEL_ID,
        max_tokens=ORIENTATION_MAX_TOKENS,
        system=ORIENTATION_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": content}],
    )
    return _first_text(response), _normalize_usage(getattr(response, "usage", None))


def _said_no(answer: str) -> bool:
    return bool(re.search(r"\bno\b", answer, re.IGNORECASE)) and not re.search(
        r"\byes\b", answer, re.IGNORECASE
    )


def detect_orientation(
    client: Any,
    render: Any,
    *,
    width: int,
    height: int,
) -> tuple[int, str, dict[str, int]]:
    """Return ``(rotation, method, usage)`` for one page.

    ``render`` is a callable ``(rotation, max_long_edge) -> png``. All four
    rotations are rendered small and shown together to
    :data:`ORIENTATION_MODEL_ID`, which picks the upright one; judging
    candidates side by side is far more reliable than asking how far a single
    image is turned (a lone sideways scan was routinely called "0"). The chosen
    rendering is then shown once more to confirm; a "no" falls back to
    :func:`orientation_heuristic`, or to the opposite turn when the heuristic
    is the rejected answer. Any failure falls back to the heuristic.
    """

    usage = dict(_EMPTY_USAGE)
    try:
        candidates = {
            rotation: render(rotation, ORIENTATION_CANDIDATE_LONG_EDGE) for rotation in ROTATIONS
        }
        content: list[dict[str, Any]] = []
        for rotation in ROTATIONS:
            content.append({"type": "text", "text": f"Label {rotation}:"})
            content.append(_image_block(candidates[rotation]))
        content.append({"type": "text", "text": ORIENTATION_QUESTION})
        answer, call_usage = _ask_orientation(client, content)
        usage = sum_usage(usage, call_usage)
        match = re.search(r"\b(0|90|180|270)\b", answer)
        if match is None:
            logger.warning("ptr_vision: orientation model answered %r; using heuristic", answer)
            return orientation_heuristic(width, height), "heuristic", usage
        rotation = int(match.group(1))

        confirmation, call_usage = _ask_orientation(
            client,
            [_image_block(candidates[rotation]), {"type": "text", "text": ORIENTATION_CONFIRM_QUESTION}],
        )
        usage = sum_usage(usage, call_usage)
        if _said_no(confirmation):
            fallback = orientation_heuristic(width, height)
            if fallback == rotation:
                fallback = {0: 180, 90: 270, 180: 0, 270: 90}[rotation]
            logger.info(
                "ptr_vision: orientation model retracted %d degrees; using %d", rotation, fallback
            )
            return fallback, "model-corrected", usage
        return rotation, "model-confirmed", usage
    except Exception as error:  # noqa: BLE001 - orientation must never fail the read
        logger.warning(
            "ptr_vision: orientation call failed (%s: %s); using heuristic",
            type(error).__name__,
            error,
        )
    return orientation_heuristic(width, height), "heuristic", usage


def prepare_page_images(
    pdf_path: Path,
    client: Any,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]] | None:
    """Render every page upright.

    Returns ``(pages, orientation, usage)`` or None when pymupdf is unavailable
    or cannot open the file. Each page is ``{"index", "png", "width", "height"}``
    and each orientation entry ``{"page", "rotation", "method", "width",
    "height"}``. ``usage`` is the orientation model's summed token usage.
    """

    module = _pdf_module()
    if module is None:
        return None
    try:
        document = module.open(str(pdf_path))
    except Exception as error:  # noqa: BLE001 - fall back to the document block
        logger.warning("ptr_vision: pymupdf could not open %s: %s", pdf_path.name, error)
        return None

    pages: list[dict[str, Any]] = []
    orientation: list[dict[str, Any]] = []
    usage = dict(_EMPTY_USAGE)
    try:
        with document:
            for position in range(int(document.page_count)):
                page = document[position]
                base_rotation = int(getattr(page, "rotation", 0) or 0)
                rect = page.rect
                natural_width, natural_height = float(rect.width), float(rect.height)

                def _render(
                    rotation: int,
                    max_long_edge: int = MAX_IMAGE_LONG_EDGE,
                    _page: Any = page,
                    _base: int = base_rotation,
                ) -> bytes:
                    return _render_page(_page, module, _base, rotation, max_long_edge)[0]

                rotation, method, call_usage = detect_orientation(
                    client, _render, width=int(natural_width), height=int(natural_height)
                )
                usage = sum_usage(usage, call_usage)
                png, width, height = _render_page(page, module, base_rotation, rotation)
                pages.append({"index": position + 1, "png": png, "width": width, "height": height})
                orientation.append(
                    {
                        "page": position + 1,
                        "rotation": rotation,
                        "method": method,
                        "width": width,
                        "height": height,
                    }
                )
    except Exception as error:  # noqa: BLE001 - fall back to the document block
        logger.warning("ptr_vision: page rendering failed for %s: %s", pdf_path.name, error)
        return None
    if not pages:
        return None
    return pages, orientation, usage


def filing_context(filing_year: int | None, filing_date: str | None) -> str:
    """Per-filing hint appended to the user turn (after the cached prefix)."""

    parts: list[str] = []
    if filing_date:
        parts.append(f"This report was filed on {filing_date}.")
    elif filing_year:
        parts.append(f"This report was filed in {filing_year}.")
    if parts:
        parts.append(
            "No transaction or notification date on it can be later than that; "
            "use it to resolve two-digit years, never to fill in a date you cannot read."
        )
    return " ".join(parts)


def build_image_content(
    pages: list[dict[str, Any]],
    filename: str,
    context: str = "",
) -> list[dict[str, Any]]:
    """User content: a label and image per page, then the instruction."""

    total = len(pages)
    blocks: list[dict[str, Any]] = []
    for page in pages:
        blocks.append({"type": "text", "text": f"Page {page['index']} of {total}:"})
        blocks.append(_image_block(page["png"]))
    text = f"{USER_INSTRUCTION}\n\n{context}".rstrip() + f"\n\nFile: {filename}"
    blocks.append({"type": "text", "text": text})
    return blocks


def build_document_content(
    pdf_b64: str,
    filename: str,
    context: str = "",
) -> list[dict[str, Any]]:
    """User content when pymupdf is unavailable: the PDF as a document block."""

    text = f"{USER_INSTRUCTION}\n\n{context}".rstrip() + f"\n\nFile: {filename}"
    return [
        {
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": "application/pdf",
                "data": pdf_b64,
            },
        },
        {"type": "text", "text": text},
    ]


# -- Request ----------------------------------------------------------------


def _request_kwargs(
    content: list[dict[str, Any]],
    *,
    structured: bool,
    with_output_config: bool = True,
) -> dict[str, Any]:
    """Build the Messages request.

    ``structured`` picks schema-constrained text output over a single strict
    tool. ``with_output_config`` is dropped only when the installed SDK does not
    accept the parameter at all.
    """

    kwargs: dict[str, Any] = {
        "model": resolve_model_id(),
        "max_tokens": MAX_OUTPUT_TOKENS,
        "system": [
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        "thinking": {"type": "adaptive"},
        "messages": [{"role": "user", "content": content}],
    }
    if structured:
        kwargs["output_config"] = {
            "effort": EFFORT,
            "format": {"type": "json_schema", "schema": PTR_VISION_SCHEMA},
        }
    else:
        if with_output_config:
            kwargs["output_config"] = {"effort": EFFORT}
        kwargs["tools"] = [
            {
                "name": VISION_TOOL_NAME,
                "description": "Record every transaction row transcribed from the PTR pages.",
                "strict": True,
                "input_schema": PTR_VISION_SCHEMA,
            }
        ]
    return kwargs


def _invoke(client: Any, kwargs: dict[str, Any]) -> Any:
    """Prefer streaming (multi-page PTR reads are slow) and fall back to create."""

    stream = getattr(getattr(client, "messages", None), "stream", None)
    if stream is not None:
        with stream(**kwargs) as active:
            return active.get_final_message()
    return client.messages.create(**kwargs)


def _payload_from_message(message: Any) -> dict[str, Any] | None:
    """Read the JSON payload out of a structured-output or tool-use response."""

    blocks = list(getattr(message, "content", None) or [])
    for block in blocks:
        if getattr(block, "type", None) == "tool_use":
            candidate = getattr(block, "input", None)
            if isinstance(candidate, dict):
                return candidate
    for block in blocks:
        if getattr(block, "type", None) != "text":
            continue
        text = (getattr(block, "text", "") or "").strip()
        if not text:
            continue
        try:
            candidate = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict):
            return candidate
    return None


def _coerce_transactions(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    rows: list[dict[str, Any]] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        if not str(entry.get("asset_description") or "").strip():
            continue
        rows.append(entry)
    return rows


def _clean_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


class _ReadState:
    """Structured-output downgrade state shared by the reads of one filing."""

    def __init__(self) -> None:
        self.structured = True
        self.with_output_config = True
        self.downgraded = False


def _read_once(
    client: Any,
    content: list[dict[str, Any]],
    state: _ReadState,
    *,
    label: str,
    filename: str,
) -> dict[str, Any]:
    """One transcription request with the retry / downgrade ladder.

    Returns ``{"ok", "reason", "payload", "usage", "cost_usd", "attempts",
    "stop_reason", "structured"}``. Never raises.
    """

    attempts = 0
    message: Any = None
    last_error: Exception | None = None

    while attempts < 2:
        attempts += 1
        kwargs = _request_kwargs(
            content,
            structured=state.structured,
            with_output_config=state.with_output_config,
        )
        try:
            message = _invoke(client, kwargs)
            break
        except TypeError as error:
            # The installed SDK does not accept output_config at all -> drop it
            # and use a single strict tool instead. Does not consume a retry.
            if not state.downgraded and "output_config" in str(error):
                logger.warning(
                    "ptr_vision: SDK rejected output_config; retrying with strict tool use"
                )
                state.structured = False
                state.with_output_config = False
                state.downgraded = True
                attempts -= 1
                continue
            last_error = error
            break
        except Exception as error:  # noqa: BLE001 - classified by _is_retryable
            last_error = error
            if not state.downgraded and _rejected_structured_output(error):
                # The API refused the json_schema format; fall back to the tool
                # form once before giving up. Does not consume a retry.
                logger.warning(
                    "ptr_vision: API rejected structured output (%s); retrying with strict tool use",
                    error,
                )
                state.structured = False
                state.downgraded = True
                attempts -= 1
                continue
            if attempts >= 2 or not _is_retryable(error):
                break
            logger.warning(
                "ptr_vision: retryable error on %s %s (%s); retrying once",
                filename,
                label,
                type(error).__name__,
            )
            time.sleep(RETRY_SLEEP_SECONDS)

    outcome: dict[str, Any] = {
        "label": label,
        "ok": False,
        "reason": None,
        "payload": None,
        "usage": dict(_EMPTY_USAGE),
        "cost_usd": 0.0,
        "attempts": attempts,
        "stop_reason": None,
        "structured": state.structured,
    }
    if message is None:
        logger.error("ptr_vision: %s failed for %s: %s", label, filename, last_error)
        outcome["reason"] = f"api error: {last_error}"
        return outcome

    usage = _normalize_usage(getattr(message, "usage", None))
    outcome["usage"] = usage
    outcome["cost_usd"] = estimate_cost_usd(usage)
    stop_reason = getattr(message, "stop_reason", None)
    outcome["stop_reason"] = stop_reason

    if stop_reason == "refusal":
        details = getattr(message, "stop_details", None)
        category = (
            details.get("category")
            if isinstance(details, dict)
            else getattr(details, "category", None)
        )
        outcome["reason"] = f"model refused (category={category})"
        return outcome
    if stop_reason == "max_tokens":
        outcome["reason"] = "response truncated at max_tokens"
        return outcome

    payload = _payload_from_message(message)
    if payload is None:
        outcome["reason"] = "no structured payload in response"
        return outcome

    outcome["ok"] = True
    outcome["payload"] = payload
    return outcome


# -- Two-read agreement -----------------------------------------------------


def normalize_description(text: Any) -> str:
    """Lower-case, strip punctuation, collapse whitespace."""

    return " ".join(re.sub(r"[^0-9a-z]+", " ", str(text or "").lower()).split())


def description_similarity(a: Any, b: Any) -> float:
    """Similarity between two normalized asset descriptions, 0..1.

    The ``difflib`` character ratio, lifted by token containment so that
    "AT&T" and "AT&T Inc" (ratio 0.67 on "at t" / "at t inc") still pair up
    when one read added or dropped a suffix. Containment only counts when the
    shorter string is at least four characters, so a stray "Inc" cannot match
    everything.
    """

    left, right = normalize_description(a), normalize_description(b)
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    ratio = difflib.SequenceMatcher(None, left, right).ratio()
    shorter, longer = sorted((left, right), key=len)
    if len(shorter) >= 4:
        short_tokens = set(shorter.split())
        long_tokens = set(longer.split())
        if short_tokens and short_tokens <= long_tokens:
            return 1.0
        containment = len(short_tokens & long_tokens) / max(1, len(short_tokens))
        ratio = max(ratio, containment)
    return ratio


def _comparable(row: dict[str, Any], field: str) -> Any:
    """Field value in the form used for agreement checks."""

    if field == "amount":
        try:
            low = int(row.get("amount_min") or 0)
        except (TypeError, ValueError):
            low = 0
        try:
            high = int(row.get("amount_max") or 0)
        except (TypeError, ValueError):
            high = 0
        return (low, high)
    value = row.get(field)
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text or None


def match_rows(
    rows_a: list[dict[str, Any]],
    rows_b: list[dict[str, Any]],
) -> list[tuple[int, int, float]]:
    """Pair rows across two reads.

    Candidates need a description similarity of at least
    :data:`SIMILARITY_THRESHOLD`. Among candidates, a matching transaction type
    wins, then the higher similarity, then the closer printed position, so two
    rows for the same asset (a purchase and a sale) pair up with their own
    counterparts. Greedy, one-to-one.
    """

    candidates: list[tuple[int, float, int, int, int]] = []
    for i, row_a in enumerate(rows_a):
        for j, row_b in enumerate(rows_b):
            ratio = description_similarity(
                row_a.get("asset_description"), row_b.get("asset_description")
            )
            if ratio < SIMILARITY_THRESHOLD:
                continue
            type_a = _comparable(row_a, "transaction_type")
            type_b = _comparable(row_b, "transaction_type")
            same_type = 1 if (type_a and type_b and type_a == type_b) else 0
            candidates.append((same_type, ratio, -abs(i - j), i, j))
    candidates.sort(reverse=True)

    used_a: set[int] = set()
    used_b: set[int] = set()
    pairs: list[tuple[int, int, float]] = []
    for _same_type, ratio, _distance, i, j in candidates:
        if i in used_a or j in used_b:
            continue
        used_a.add(i)
        used_b.add(j)
        pairs.append((i, j, ratio))
    pairs.sort()
    return pairs


def _worst_legibility(*ratings: Any) -> str:
    rank = 0
    for rating in ratings:
        text = str(rating or "partial").strip().lower()
        rank = max(rank, _LEGIBILITY_RANK.get(text, 1))
    return _LEGIBILITY_BY_RANK[rank]


def _downgrade(legibility: str, floor: str) -> str:
    return _LEGIBILITY_BY_RANK[max(_LEGIBILITY_RANK[legibility], _LEGIBILITY_RANK[floor])]


def _merge_comment(*comments: Any) -> str | None:
    texts = [str(c).strip() for c in comments if c is not None and str(c).strip()]
    if not texts:
        return None
    return max(texts, key=len)


def merge_matched_rows(row_a: dict[str, Any], row_b: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Merge one matched pair, keeping only fields both reads agree on.

    Returns ``(merged_row, disagreements)``. A disagreement on a
    :data:`CRITICAL_FIELDS` entry nulls the field and marks the row
    ``illegible``; on a :data:`SOFT_FIELDS` entry it nulls the field and marks
    the row at least ``partial``. The asset description keeps the reading the
    model was more confident about (``clear`` over ``partial``), and the longer
    of the two when it rated both the same.
    """

    merged: dict[str, Any] = dict(row_a)
    disagreements: list[str] = []

    desc_a = str(row_a.get("asset_description") or "").strip()
    desc_b = str(row_b.get("asset_description") or "").strip()
    rank_a = _LEGIBILITY_RANK.get(str(row_a.get("legibility") or "partial").lower(), 1)
    rank_b = _LEGIBILITY_RANK.get(str(row_b.get("legibility") or "partial").lower(), 1)
    if rank_b < rank_a or (rank_b == rank_a and len(desc_b) > len(desc_a)):
        merged["asset_description"] = desc_b
    else:
        merged["asset_description"] = desc_a

    for field in CRITICAL_FIELDS + SOFT_FIELDS:
        value_a = _comparable(row_a, field)
        value_b = _comparable(row_b, field)
        if field == "amount":
            if value_a != value_b:
                merged["amount_min"] = None
                merged["amount_max"] = None
                disagreements.append("amount")
            continue
        if value_a == value_b:
            merged[field] = row_a.get(field) if row_a.get(field) is not None else row_b.get(field)
        else:
            merged[field] = None
            disagreements.append(field)

    legibility = _worst_legibility(row_a.get("legibility"), row_b.get("legibility"))
    if any(field in CRITICAL_FIELDS for field in disagreements):
        legibility = "illegible"
    elif disagreements:
        legibility = _downgrade(legibility, "partial")
    merged["legibility"] = legibility

    comment = _merge_comment(row_a.get("comment"), row_b.get("comment"))
    if disagreements:
        note = "two reads disagreed on: " + ", ".join(disagreements)
        comment = f"{comment}; {note}" if comment else note
    merged["comment"] = comment
    return merged, disagreements


def _unmatched(row: dict[str, Any], label: str) -> dict[str, Any]:
    copy = dict(row)
    copy["legibility"] = "illegible"
    note = f"seen by only one of two reads ({label})"
    existing = _merge_comment(copy.get("comment"))
    copy["comment"] = f"{existing}; {note}" if existing else note
    return copy


def reconcile_reads(
    rows_a: list[dict[str, Any]],
    rows_b: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Combine two independent reads of the same pages.

    Returns ``(rows, agreement)``. Rows follow read A's printed order with read
    B's unmatched rows appended. ``agreement`` is the ``readAgreement``
    metadata block: ``rowsA``, ``rowsB``, ``matched``, ``unmatchedA``,
    ``unmatchedB``, ``rowCountsAgree`` and per-field disagreement counts.
    """

    pairs = match_rows(rows_a, rows_b)
    by_a = {i: (j, ratio) for i, j, ratio in pairs}
    matched_b = {j for _i, j, _ratio in pairs}

    merged_rows: list[dict[str, Any]] = []
    disagreement_counts: dict[str, int] = {}
    for i, row_a in enumerate(rows_a):
        if i in by_a:
            j, _ratio = by_a[i]
            merged, disagreements = merge_matched_rows(row_a, rows_b[j])
            for field in disagreements:
                disagreement_counts[field] = disagreement_counts.get(field, 0) + 1
            merged_rows.append(merged)
        else:
            merged_rows.append(_unmatched(row_a, "read A"))
    for j, row_b in enumerate(rows_b):
        if j not in matched_b:
            merged_rows.append(_unmatched(row_b, "read B"))

    agreement: dict[str, Any] = {
        "rowsA": len(rows_a),
        "rowsB": len(rows_b),
        "matched": len(pairs),
        "unmatchedA": len(rows_a) - len(pairs),
        "unmatchedB": len(rows_b) - len(pairs),
        "rowCountsAgree": len(rows_a) == len(rows_b),
        "fieldDisagreements": disagreement_counts,
    }
    return merged_rows, agreement


# -- Metadata ---------------------------------------------------------------


def build_vision_metadata(report: dict[str, Any]) -> dict[str, Any]:
    """Compact a vision result for ``house_filing_stubs.metadata.visionParse``.

    The raw transcription is deliberately dropped: the normalized rows already
    land in ``metadata.parsedTransactions``, and this record only has to explain
    what the model was asked, what it cost, and why a filing did or did not
    leave the review queue.
    """

    usage = report.get("usage") or {}
    model = report.get("model")
    price_in, price_out = pricing_for_model(model)
    orientation_model = report.get("orientation_model") or ORIENTATION_MODEL_ID
    orient_in, orient_out = pricing_for_model(orientation_model)
    metadata: dict[str, Any] = {
        "model": model,
        "orientationModel": orientation_model,
        "parserVersion": report.get("parser_version"),
        "ok": bool(report.get("ok")),
        "skipped": bool(report.get("skipped")),
        "reason": report.get("reason"),
        "stopReason": report.get("stop_reason"),
        "attempts": report.get("attempts"),
        "structuredOutput": report.get("structuredOutput"),
        "rowCount": len(report.get("transactions") or []),
        "legibility": report.get("legibility"),
        "confidence": report.get("confidence"),
        "needsReview": bool(report.get("needs_review", True)),
        "pageCount": report.get("page_count"),
        "filerName": report.get("filer_name"),
        "filingDate": report.get("filing_date"),
        "notes": (report.get("notes") or None),
        "orientation": report.get("orientation"),
        "readAgreement": report.get("read_agreement"),
        "calls": report.get("calls") or [],
        "usage": {
            "inputTokens": int(usage.get("input") or 0),
            "cacheReadTokens": int(usage.get("cache_read") or 0),
            "cacheWriteTokens": int(usage.get("cache_write") or 0),
            "outputTokens": int(usage.get("output") or 0),
        },
        "costUsd": report.get("cost_usd", 0.0),
        "pricing": {
            "inputPerMTok": price_in,
            "outputPerMTok": price_out,
            "cacheReadMultiplier": CACHE_READ_MULTIPLIER,
            "cacheWriteMultiplier": CACHE_WRITE_MULTIPLIER,
            "orientation": {"inputPerMTok": orient_in, "outputPerMTok": orient_out},
        },
    }
    scrubs = int(report.get("example_row_scrubs") or 0)
    if scrubs:
        metadata["exampleRowScrubs"] = scrubs
    return metadata


# -- Entry point ------------------------------------------------------------


def _call_record(label: str, model: str, usage: dict[str, int], cost: float, **extra: Any) -> dict[str, Any]:
    record: dict[str, Any] = {
        "label": label,
        "model": model,
        "usage": dict(usage),
        "costUsd": round(float(cost), 6),
    }
    record.update(extra)
    return record


def row_summary(row: dict[str, Any]) -> str:
    """One-line rendering of a raw read row for the stub metadata.

    Lets a reviewer see what each read said before reconciliation without
    storing the full transcription twice.
    """

    amount = f"{row.get('amount_min')}-{row.get('amount_max')}"
    parts = (
        str(row.get("asset_description") or "").strip(),
        str(row.get("transaction_type") or "?"),
        str(row.get("transaction_date") or "?"),
        str(row.get("notification_date") or "-"),
        amount,
        str(row.get("owner") or "-"),
        str(row.get("legibility") or "?"),
    )
    return " | ".join(parts)


def extract_via_vision(
    pdf_path: Path,
    *,
    filing_year: int | None = None,
    filing_date: str | None = None,
) -> dict[str, Any]:
    """Transcribe a House PTR PDF with a Claude vision model, twice, and agree.

    Always returns a dict. ``ok`` is False and ``reason`` is set whenever the
    filing was skipped or the call failed; the caller then leaves the stub in
    ``needs_review`` and records ``reason`` in its metadata. ``filing_year``
    drives the example-row scrub; ``filing_date`` (YYYY-MM-DD) is passed to the
    model as a hint for two-digit years.
    """

    if vision_disabled():
        logger.warning("ptr_vision: disabled by CAPITOL_PTR_VISION_DISABLED")
        return _skip("disabled by CAPITOL_PTR_VISION_DISABLED")

    if not _has_credentials():
        logger.warning("ptr_vision: no ANTHROPIC_API_KEY / ANTHROPIC_AUTH_TOKEN; skipping")
        return _skip("anthropic credentials not configured")

    if not pdf_path.exists():
        return _skip(f"pdf not found: {pdf_path}")

    pdf_bytes = pdf_path.read_bytes()
    size = len(pdf_bytes)
    if size > MAX_VISION_PDF_BYTES:
        logger.warning(
            "ptr_vision: %s is %d bytes (> %d)", pdf_path.name, size, MAX_VISION_PDF_BYTES
        )
        return _skip(f"pdf too large: {size} bytes (limit {MAX_VISION_PDF_BYTES})")

    page_count = count_pdf_pages(pdf_path)
    if page_count is not None and page_count > MAX_VISION_PDF_PAGES:
        logger.warning(
            "ptr_vision: %s has %d pages (> %d)",
            pdf_path.name,
            page_count,
            MAX_VISION_PDF_PAGES,
        )
        return _skip(
            f"pdf too long: {page_count} pages (limit {MAX_VISION_PDF_PAGES})",
            page_count=page_count,
        )
    if page_count is None:
        logger.debug("ptr_vision: no PDF reader available; skipping the page-count guardrail")

    client = _client_once()
    model = resolve_model_id()
    context = filing_context(filing_year, filing_date)
    calls: list[dict[str, Any]] = []

    # Upright page images when pymupdf can render them, the raw PDF otherwise.
    orientation: list[dict[str, Any]] | None = None
    prepared = prepare_page_images(pdf_path, client)
    if prepared is not None:
        pages, orientation, orient_usage = prepared
        orient_cost = estimate_cost_usd(orient_usage, ORIENTATION_MODEL_ID)
        calls.append(
            _call_record(
                "orientation",
                ORIENTATION_MODEL_ID,
                orient_usage,
                orient_cost,
                pages=len(pages),
            )
        )
        content = build_image_content(pages, pdf_path.name, context)
        if page_count is None:
            page_count = len(pages)
        logger.info(
            "ptr_vision: %s -> %d upright page image(s) %s",
            pdf_path.name,
            len(pages),
            [(entry["rotation"], entry["method"]) for entry in orientation],
        )
    else:
        # Base64 with no newlines, as the document content block requires.
        pdf_b64 = base64.standard_b64encode(pdf_bytes).decode("ascii")
        content = build_document_content(pdf_b64, pdf_path.name, context)
        logger.info("ptr_vision: %s -> sending the PDF as a document block", pdf_path.name)

    state = _ReadState()
    reads: list[dict[str, Any]] = []
    for position in range(READS_PER_FILING):
        label = f"read {'AB'[position] if position < 2 else position + 1}"
        outcome = _read_once(client, content, state, label=label, filename=pdf_path.name)
        raw_rows = _coerce_transactions((outcome["payload"] or {}).get("transactions"))
        calls.append(
            _call_record(
                label,
                model,
                outcome["usage"],
                outcome["cost_usd"],
                ok=outcome["ok"],
                reason=outcome["reason"],
                attempts=outcome["attempts"],
                stopReason=outcome["stop_reason"],
                rows=[row_summary(row) for row in raw_rows] if outcome["ok"] else None,
            )
        )
        reads.append(outcome)
        if position == 0 and not outcome["ok"]:
            # A first read that refused, truncated, or errored would fail the
            # same way again; do not pay for the second.
            break

    usage = sum_usage(*(call["usage"] for call in calls))
    cost = round(sum(float(call["costUsd"]) for call in calls), 6)
    attempts = sum(int(read["attempts"]) for read in reads)
    read_a = reads[0]
    if not read_a["ok"]:
        return _skip(
            str(read_a["reason"]),
            usage=usage,
            cost_usd=cost,
            stop_reason=read_a["stop_reason"],
            attempts=attempts,
            orientation=orientation,
            calls=calls,
            page_count=page_count,
        )

    payload_a = read_a["payload"] or {}
    rows_a, scrubs_a = scrub_example_row_values(
        _coerce_transactions(payload_a.get("transactions")), filing_year
    )

    read_b = reads[1] if len(reads) > 1 else None
    payload_b: dict[str, Any] = {}
    if read_b is not None and read_b["ok"]:
        payload_b = read_b["payload"] or {}
        rows_b, scrubs_b = scrub_example_row_values(
            _coerce_transactions(payload_b.get("transactions")), filing_year
        )
        transactions, agreement = reconcile_reads(rows_a, rows_b)
    else:
        # Only one usable read: nothing can be confirmed, so every row goes to
        # a human. Its values are kept so the reviewer has something to check.
        scrubs_b = 0
        transactions = [_unmatched(row, "read A") for row in rows_a]
        agreement = {
            "rowsA": len(rows_a),
            "rowsB": None,
            "matched": 0,
            "unmatchedA": len(rows_a),
            "unmatchedB": 0,
            "rowCountsAgree": False,
            "fieldDisagreements": {},
            "readBFailed": (read_b or {}).get("reason") or "second read not attempted",
        }

    counts = summarize_legibility(transactions)
    confidence = legibility_confidence(counts)
    review_reasons: list[str] = []
    if majority_illegible(counts):
        review_reasons.append("majority illegible")
    if not agreement.get("rowCountsAgree"):
        review_reasons.append("reads disagree on row count")
    critical = {
        field: count
        for field, count in (agreement.get("fieldDisagreements") or {}).items()
        if field in CRITICAL_FIELDS
    }
    if critical:
        review_reasons.append("reads disagree on " + ", ".join(sorted(critical)))
    needs_review = bool(review_reasons)

    def _header(field: str) -> Any:
        value = payload_a.get(field)
        return value if value is not None else payload_b.get(field)

    notes = [
        _clean_optional_text(payload_a.get("notes")),
        _clean_optional_text(payload_b.get("notes")),
    ]
    unique_notes = [note for index, note in enumerate(notes) if note and note not in notes[:index]]

    reported_pages = _header("page_count")
    result: dict[str, Any] = {
        "ok": bool(transactions),
        "skipped": False,
        "reason": None if transactions else "model returned no transaction rows",
        "model": model,
        "orientation_model": ORIENTATION_MODEL_ID,
        "parser_version": VISION_PARSER_VERSION,
        "filer_name": _clean_optional_text(_header("filer_name")),
        "filing_date": _clean_optional_text(_header("filing_date")),
        "page_count": reported_pages if reported_pages is not None else page_count,
        "notes": " | ".join(unique_notes) or None,
        "transactions": transactions,
        "legibility": counts,
        "confidence": confidence,
        "needs_review": needs_review,
        "needs_review_reasons": review_reasons,
        "usage": usage,
        "cost_usd": cost,
        "stop_reason": read_a["stop_reason"],
        "attempts": attempts,
        "structuredOutput": state.structured,
        "orientation": orientation,
        "read_agreement": agreement,
        "example_row_scrubs": scrubs_a + scrubs_b,
        "calls": calls,
    }

    logger.info(
        "ptr_vision: %s -> %d rows (clear=%d partial=%d illegible=%d) agreement=%s "
        "confidence=%.2f needsReview=%s cost=$%.4f usage=%s",
        pdf_path.name,
        len(transactions),
        counts["clear"],
        counts["partial"],
        counts["illegible"],
        {k: agreement.get(k) for k in ("rowsA", "rowsB", "matched")},
        confidence,
        needs_review,
        cost,
        usage,
    )
    return result
