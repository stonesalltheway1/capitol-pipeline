"""Vision extraction for scanned and handwritten House PTR PDFs.

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
2. The page is analysed at 0, 90, 180 and 270 degrees by the checkbox
   detector and the rotation whose analysis scores highest is taken as
   upright: a complete A-K ladder, the header printed above the rows, the
   ladder at the right margin and the wide K column on the right are all
   asymmetries of the form itself, so the half turn is settled without a
   model. A page with no ladder at any rotation falls back to the heuristic
   (a portrait page is a sideways landscape form, so rotate 90). Setting
   ``CAPITOL_PTR_VISION_ORIENTATION=model`` restores the old paid pick.
3. Landscape pages (the paper checkbox form) also get two close-up strips of
   the right-hand 58% of the page rendered at twice the zoom, so the amount
   ladder's tick boxes are unambiguous; the model reports the column letter
   (A-K) and the band must agree with it.
3b. A classical checkbox detector (:mod:`capitol_pipeline.parsers.ptr_grid`)
   reads the same upright page without a model: it finds the ladder's column
   rules and the ticked cell per row, and its letter must agree with the
   model's or the amount is nulled and the filing reviewed.
4. Pages are grouped into chunks of :func:`resolve_chunk_pages` (default 4)
   and each chunk is read **twice**, as two independent requests, asking for
   the transaction grid back as schema-constrained JSON. On the default
   (Gemini) provider the two reads are two different model versions, which
   makes the disagreements real rather than sampling noise. A field is only
   trusted when both reads agree on it: disagreements are nulled and the row
   is marked ``illegible``; rows only one read saw are kept but marked
   ``illegible``; a row-count mismatch forces manual review. A read that
   truncates at ``max_tokens`` is retried once with the page group halved.
5. When ``pymupdf`` is not importable the PDF itself is sent as a ``document``
   block instead, and orientation is left to the model. Only the Anthropic
   provider accepts that: Gemini rasterises a PDF at its own resolution and
   reads every two-digit year one low, so it refuses the part.

Which vendor answers is :mod:`capitol_pipeline.parsers.ptr_vision_provider`
(``CAPITOL_PTR_VISION_PROVIDER``, default ``gemini``, free of charge). This
module builds the pages, the prompt and the schema and owns the agreement
rules; it never branches on the provider.

A filing whose reads both return zero rows and both report that the form
states there is nothing to report is a terminal ``no_transactions`` result,
not a review item.

This module is a peer of :mod:`capitol_pipeline.parsers.ptr_llm_fallback`
(the Haiku text fallback, still used when the OCR text layer is decent), not a
replacement.

Guardrails
----------
* ``CAPITOL_PTR_VISION_DISABLED=1`` kills the path at runtime.
* Missing credentials for the configured provider skip the filing rather than
  falling through to another vendor.
* PDFs over :data:`MAX_VISION_PDF_PAGES` pages or :data:`MAX_VISION_PDF_BYTES`
  bytes are skipped with a reason; the stub stays ``needs_review``.
* The estimated cost (pages x two reads x per-page rate, plus orientation) must
  stay under :func:`resolve_max_filing_cost_usd` (``CAPITOL_PTR_VISION_MAX_COST_USD``,
  default $25); a filing over it is refused with the estimate in the reason, and
  a filing that overruns 1.5x the ceiling while running is abandoned. On the
  free tier every rate is zero, so the ceiling never bites and the long typed
  attachments the paid path had to refuse go through.
* One filing per call, one retry on 429/5xx per read, and the caller caps
  filings per run with ``--limit``.

Nothing here touches the database. The caller records
``usage`` / ``cost_usd`` / ``reason`` into the stub's ``visionParse`` metadata.
"""

from __future__ import annotations

import difflib
import hashlib
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from capitol_pipeline.parsers import ptr_grid, ptr_vision_provider

logger = logging.getLogger(__name__)

# -- Model + version --------------------------------------------------------

#: Default Anthropic read model. Override per-run with
#: ``CAPITOL_PTR_VISION_MODEL``; which vendor answers is
#: ``CAPITOL_PTR_VISION_PROVIDER`` (see :mod:`ptr_vision_provider`).
MODEL_ID = ptr_vision_provider.ANTHROPIC_MODEL_ID

#: Anthropic model used only to decide page orientation, and only when
#: ``CAPITOL_PTR_VISION_ORIENTATION=model``. The default orientation pick is
#: free and deterministic (:func:`detect_orientation_from_grid`).
ORIENTATION_MODEL_ID = ptr_vision_provider.ANTHROPIC_ORIENTATION_MODEL_ID

#: Recorded as ``parser_version`` on every row this path produces, one per
#: vendor so a published row says who read the page. The Anthropic literal is
#: the one already stored on thousands of rows and does not change. Use
#: :func:`is_vision_parser_version` rather than comparing against either.
VISION_PARSER_VERSIONS: dict[str, str] = {
    "anthropic": "claude-vision-v2",
    "gemini": "gemini-vision-v2",
}
VISION_PARSER_VERSION = VISION_PARSER_VERSIONS["anthropic"]

#: Vendor prefixes :func:`is_vision_parser_version` recognises.
_VISION_VERSION_PREFIXES: tuple[str, ...] = ("claude-", "gemini-")


def vision_parser_version(provider_name: str) -> str:
    """The ``parser_version`` a given provider's transcriptions carry."""

    return VISION_PARSER_VERSIONS.get(provider_name, f"{provider_name}-vision-v2")


def current_vision_parser_version() -> str:
    """The ``parser_version`` a read started right now would carry."""

    return vision_parser_version(ptr_vision_provider.resolve_provider_name())

#: Used when the installed SDK rejects ``output_config`` and we fall back to
#: a single strict tool.
VISION_TOOL_NAME = ptr_vision_provider.VISION_TOOL_NAME


def is_vision_parser_version(version: object) -> bool:
    """Return whether a ``parser_version`` string came from this module.

    Matches every version this path has ever written (``claude-sonnet-5-vision-v1``,
    ``claude-vision-v2``, ``gemini-vision-v2``) so status decisions keyed on the
    literal keep working across model and vendor changes.
    """

    text = str(version or "").strip().lower()
    return text.startswith(_VISION_VERSION_PREFIXES) and "vision" in text


# -- Guardrails -------------------------------------------------------------

MAX_VISION_PDF_PAGES = 60
MAX_VISION_PDF_BYTES = 20 * 1024 * 1024
#: Streaming, so this is room rather than a target: a four-page chunk of a
#: dense attachment is ~90 rows (~7K tokens of JSON) plus adaptive thinking,
#: which at medium effort ran to ~25K tokens on such a chunk (a 32K cap
#: truncated one and cost a halved retry).
MAX_OUTPUT_TOKENS = 64000
RETRY_SLEEP_SECONDS = 5.0

#: Independent reads of the same page images per filing.
READS_PER_FILING = 2

#: Reasoning effort for the read model. ``medium`` is enough for typed pages;
#: set ``CAPITOL_PTR_VISION_EFFORT=high`` for a queue of handwritten forms.
DEFAULT_EFFORT = "medium"
EFFORT_LEVELS: tuple[str, ...] = ("low", "medium", "high", "xhigh", "max")
#: Kept for callers that only need the default; :func:`resolve_effort` is what
#: requests use.
EFFORT = DEFAULT_EFFORT

#: Pages per request (paper attachments run 16-18 rows a page, so four pages
#: is ~70 rows of JSON). Override with ``CAPITOL_PTR_VISION_CHUNK_PAGES``.
DEFAULT_CHUNK_PAGES = 4
MAX_CHUNK_PAGES = 12

#: Cost ceiling per filing in USD, compared against the pre-flight estimate;
#: sized so a 60-page filing (two reads, close-up strips, ~$0.40 a page at
#: medium effort) still fits. Override with ``CAPITOL_PTR_VISION_MAX_COST_USD``;
#: ``CAPITOL_PTR_VISION_EFFORT=low`` is the lever that cuts the thinking that
#: dominates the bill.
DEFAULT_MAX_FILING_COST_USD = 25.0
#: A filing whose running cost passes this multiple of the ceiling is abandoned.
COST_OVERRUN_FACTOR = 1.5

#: Days a stub's previous vision result stays reusable when the PDF is unchanged.
VISION_REUSE_MAX_AGE_DAYS = 30

# -- Cost estimate inputs (tokens per page, calibrated on live runs) ---------
# Measured on a 23-page typed attachment (8219444) at medium effort with the
# close-up strips on: 503K input tokens over 14 reads (~9K per page) and 285K
# output tokens (~6.2K per page-read, of which ~1.6K is the JSON and the rest
# adaptive thinking), $9.23 for the filing, $0.40 a page.

EST_TOKENS_FULL_PAGE = 2600
EST_TOKENS_GRID_STRIP = 3200
EST_OUTPUT_TOKENS_PER_PAGE = 6200
EST_CACHED_PROMPT_TOKENS = 5300
EST_ORIENTATION_COST_PER_PAGE_USD = 0.006

# -- Page rendering ---------------------------------------------------------

RENDER_DPI = 150
MAX_IMAGE_LONG_EDGE = 1568
ROTATIONS: tuple[int, ...] = (0, 90, 180, 270)
ORIENTATION_MAX_TOKENS = 8

#: The four candidate renderings shown to the orientation model are smaller
#: than the read images: legible enough to tell upright from sideways, cheap
#: enough that four of them cost less than one read image.
ORIENTATION_CANDIDATE_LONG_EDGE = 1000

# -- Transaction-grid close-ups --------------------------------------------
# The paper form is landscape; its amount ladder sits at the right edge and a
# tick one column off is the one field the two reads routinely agree on
# wrongly. Landscape pages therefore also get close-up strips of the
# right-hand part of the page rendered at a higher zoom.

#: Fraction of the page width where the close-up starts (right-hand 58%).
GRID_CROP_X_FRACTION = 0.42
#: Vertical strips per page; each covers (1/strips + overlap) of the height.
GRID_CROP_STRIPS = 2
GRID_CROP_OVERLAP = 0.10
#: Zoom relative to the full-page render, capped by MAX_IMAGE_LONG_EDGE.
#: Override with ``CAPITOL_PTR_VISION_GRID_ZOOM``; 0 disables the close-ups.
DEFAULT_GRID_CROP_ZOOM = 2.0

# -- Pricing (USD per MTok: input, output) ----------------------------------

MODEL_PRICING: dict[str, tuple[float, float]] = ptr_vision_provider.ANTHROPIC_PRICING

#: Family fallbacks for dated or future ids, checked by prefix in this order.
_FAMILY_PRICING: tuple[tuple[str, tuple[float, float]], ...] = (
    ptr_vision_provider.ANTHROPIC_FAMILY_PRICING
)

#: Unknown Anthropic model: assume the Opus tier so estimates err high.
DEFAULT_PRICING: tuple[float, float] = ptr_vision_provider.ANTHROPIC_DEFAULT_PRICING

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

#: Amount ladder as lettered on the paper form. K is a flag column ("Transaction
#: in a Spouse or Dependent Child Asset over $1,000,000"), not a band.
AMOUNT_LETTER_BANDS: dict[str, tuple[int, int]] = {
    "A": (1001, 15000),
    "B": (15001, 50000),
    "C": (50001, 100000),
    "D": (100001, 250000),
    "E": (250001, 500000),
    "F": (500001, 1000000),
    "G": (1000001, 5000000),
    "H": (5000001, 25000000),
    "I": (25000001, 50000000),
    "J": (50000001, 100000000),
}
AMOUNT_LETTERS: tuple[str, ...] = tuple(AMOUNT_LETTER_BANDS) + ("K",)

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

#: The Type column as the schema names it.
CANONICAL_TRANSACTION_TYPES: tuple[str, ...] = (
    "purchase",
    "sale",
    "sale_partial",
    "exchange",
)

#: Every spelling of the Type column that means one of the canonical four.
#:
#: The structured-output enum normally does this for us -- measured across the
#: nine filings read on 2026-09-03, ``gemini-3.8-flash`` and
#: ``gemini-3.5-flash`` between them emitted only ``purchase``, ``sale``,
#: ``sale_partial`` and ``exchange``, so nothing here fires on that data. It is
#: the belt for the paths where no enum is enforced: ``_ReadState.structured``
#: goes False when the API rejects the schema, and a provider added later may
#: honour the enum loosely. Two spellings of the same fact must agree rather
#: than null each other.
TRANSACTION_TYPE_ALIASES: dict[str, str] = {
    "p": "purchase",
    "purchase": "purchase",
    "purchased": "purchase",
    "buy": "purchase",
    "bought": "purchase",
    "s": "sale",
    "sale": "sale",
    "sold": "sale",
    "sell": "sale",
    "s partial": "sale_partial",
    "sale partial": "sale_partial",
    "partial sale": "sale_partial",
    "partial": "sale_partial",
    "sale_partial": "sale_partial",
    "e": "exchange",
    "exchange": "exchange",
    "exchanged": "exchange",
}

_TRANSACTION_TYPE_PUNCTUATION = re.compile(r"[^a-z0-9]+")


def canonical_transaction_type(value: Any) -> str | None:
    """The canonical name of a Type column reading, or None when it is blank.

    Punctuation and case are dropped, so ``"S (partial)"``, ``"S-Partial"`` and
    ``"sale_partial"`` are one value. A string that is not a spelling of any of
    :data:`CANONICAL_TRANSACTION_TYPES` comes back lower-cased and squeezed
    rather than as None: two reads of the same unrecognised text still agree,
    and two different ones still disagree.
    """

    if value is None:
        return None
    text = " ".join(_TRANSACTION_TYPE_PUNCTUATION.sub(" ", str(value).strip().lower()).split())
    if not text:
        return None
    return TRANSACTION_TYPE_ALIASES.get(text, text)


def _type_for_agreement(value: Any) -> str | None:
    """The Type column reading as the two reads are compared on it.

    The attachment form ticks "Sale" and "Partial Sale" together, and the site
    collapses both to "sale": the two readings agree on the trade.
    """

    canonical = canonical_transaction_type(value)
    return "sale" if canonical == "sale_partial" else canonical


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
        "amount_column_letter": {
            "anyOf": [
                {"type": "string", "enum": list(AMOUNT_LETTERS)},
                {"type": "null"},
            ],
            "description": (
                "On the paper form, the letter printed above the ticked amount "
                "column (A = $1,001-$15,000 ... J = Over $50,000,000, K = the "
                "spouse/dependent over-$1,000,000 flag), found by counting boxes "
                "from column A. Null on typed forms that print the dollar band, "
                "or when no box is ticked."
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
        "page_number": {
            "type": ["integer", "null"],
            "description": (
                "The N from the 'Page N of M' label shown above the image this row "
                "was read from. Null only when the row cannot be placed on a page."
            ),
        },
    },
    "required": [
        "page_number",
        "owner",
        "asset_description",
        "ticker",
        "asset_type_code",
        "transaction_type",
        "transaction_date",
        "notification_date",
        "amount_min",
        "amount_max",
        "amount_column_letter",
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
        "no_transactions_stated": {
            "type": "boolean",
            "description": (
                "True only when the pages you were shown carry an explicit statement "
                "that there is nothing to report (for example 'Nothing to report', "
                "'No reportable transactions', 'None'). False when the grid simply "
                "has rows, or is blank without such a statement."
            ),
        },
        "transactions": {
            "type": "array",
            "description": "One entry per transaction row on the form, in printed order.",
            "items": TRANSACTION_ITEM_SCHEMA,
        },
    },
    "required": [
        "filer_name",
        "filing_date",
        "page_count",
        "notes",
        "no_transactions_stated",
        "transactions",
    ],
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

On the paper form the amount columns are lettered A through K across the top of the ladder: A is $1,001-$15,000, B $15,001-$50,000, C $50,001-$100,000, D $100,001-$250,000, E $250,001-$500,000, F $500,001-$1,000,000, G $1,000,001-$5,000,000, H $5,000,001-$25,000,000, I $25,000,001-$50,000,000, J Over $50,000,000, and K is a separate flag column for a transaction in a spouse's or dependent child's asset over $1,000,000. For every ticked row report `amount_column_letter`: find the ticked box, then count boxes leftward to column A (the first, leftmost box) and name the letter you land on. Then set amount_min and amount_max to that letter's band; the letter and the band must agree, and if they do not, you have miscounted - recount. Adjacent columns are the usual mistake, so use the close-up strips when they are provided. On typed electronic forms that print the dollar band as text there are no letters: report null.

## Close-up strips

For each landscape page you may also be given one or more close-up strips of its right-hand part, labelled as such, rendered at a higher zoom. They show the same rows as the full page: use them to resolve which box carries a tick and to read dates, and use the full page for the asset names and the owner column. Do not report rows twice because they appear in both.

## Dates

The paper form itself carries a PRE-PRINTED EXAMPLE ROW in the transaction grid, typeset in the same place as a real entry: asset "Example: Mega Corp. Common Stock", an x under Sale, transaction date 02/05/20, notification date 03/07/20, and an x in the $15,001-$50,000 column. It is part of the blank form, not a transaction. Never return it as a row, and never let its values leak into the rows above or below it: if you find yourself writing 2020-02-05, 2020-03-07 or the $15,001-$50,000 band for a row whose own handwritten date or check mark you cannot actually read, you have copied the example - report null for that field and mark the row partial or illegible instead. Every real row has its own handwritten date; read each one on its own line, and read the check mark in the amount column on that same line.

Scans are often rotated ninety degrees or upside down; read them in the orientation of the printed text.

## Nothing to report

Some filings state in the grid, or in place of it, that there is nothing to report: "NOTHING TO REPORT FOR JANUARY 2023", "No reportable transactions", "None". Return an empty transactions array and set `no_transactions_stated` to true. Set it to false whenever the pages carry rows, and also when the grid is merely blank with no such statement - a blank grid may be a continuation page or a bad scan, and it is for a human to decide.

## Long filings

Long filings are sent in groups of pages. When you are told you are looking at pages 6 to 10 of 23, transcribe only the rows on those pages, do not invent the header from memory (report null for filer_name and filing_date unless they are on the pages you can see), and keep the rows in the printed order of those pages. Every image is preceded by a "Page N of M:" label: report that N as `page_number` on each row so the rows can be checked against the page they came from.

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
    """Return the read model id for the configured provider."""

    override = os.environ.get("CAPITOL_PTR_VISION_MODEL", "").strip()
    if override:
        return override
    if ptr_vision_provider.resolve_provider_name() == "gemini":
        return ptr_vision_provider.GEMINI_MODEL_ID
    return MODEL_ID


#: How a page's upright rotation is chosen. ``grid`` is free and deterministic
#: (the checkbox detector's own ladder, scored at all four rotations);
#: ``model`` asks :data:`ORIENTATION_MODEL_ID`, which is what this path did
#: before and what the Anthropic tests exercise; ``heuristic`` skips both.
ORIENTATION_MODES: tuple[str, ...] = ("grid", "model", "heuristic")
DEFAULT_ORIENTATION_MODE = "grid"


def resolve_orientation_mode() -> str:
    """Return the configured orientation strategy (``CAPITOL_PTR_VISION_ORIENTATION``)."""

    raw = os.environ.get("CAPITOL_PTR_VISION_ORIENTATION", "").strip().lower()
    if raw and raw not in ORIENTATION_MODES:
        logger.warning(
            "ptr_vision: unknown CAPITOL_PTR_VISION_ORIENTATION=%r; using %s",
            raw,
            DEFAULT_ORIENTATION_MODE,
        )
        return DEFAULT_ORIENTATION_MODE
    return raw or DEFAULT_ORIENTATION_MODE


def resolve_effort() -> str:
    """Return the configured reasoning effort for the read model."""

    raw = os.environ.get("CAPITOL_PTR_VISION_EFFORT", "").strip().lower()
    return raw if raw in EFFORT_LEVELS else DEFAULT_EFFORT


def _env_number(name: str, default: float, *, low: float, high: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        logger.warning("ptr_vision: ignoring non-numeric %s=%r", name, raw)
        return default
    return min(high, max(low, value))


def resolve_chunk_pages() -> int:
    """Pages per read request."""

    return int(
        _env_number("CAPITOL_PTR_VISION_CHUNK_PAGES", DEFAULT_CHUNK_PAGES, low=1, high=MAX_CHUNK_PAGES)
    )


def resolve_max_filing_cost_usd() -> float:
    """Per-filing cost ceiling in USD."""

    return _env_number(
        "CAPITOL_PTR_VISION_MAX_COST_USD", DEFAULT_MAX_FILING_COST_USD, low=0.01, high=1000.0
    )


def resolve_grid_zoom() -> float:
    """Close-up zoom relative to the full page; 0 disables the strips."""

    return _env_number("CAPITOL_PTR_VISION_GRID_ZOOM", DEFAULT_GRID_CROP_ZOOM, low=0.0, high=4.0)


def resolve_page_range() -> tuple[int, int] | None:
    """Debug knob: ``CAPITOL_PTR_VISION_PAGE_RANGE=11-13`` (or ``12``) reads only those pages.

    Page numbers are 1-based and keep their filing-wide labels, so a targeted
    run of a long filing costs only the pages named. Unset in production.
    """

    raw = os.environ.get("CAPITOL_PTR_VISION_PAGE_RANGE", "").strip()
    if not raw:
        return None
    try:
        if "-" in raw:
            first, last = (int(part) for part in raw.split("-", 1))
        else:
            first = last = int(raw)
    except ValueError:
        logger.warning("ptr_vision: ignoring malformed CAPITOL_PTR_VISION_PAGE_RANGE=%r", raw)
        return None
    if first < 1 or last < first:
        return None
    return first, last


def vision_disabled() -> bool:
    """Return whether the env kill switch is engaged."""

    raw = os.environ.get("CAPITOL_PTR_VISION_DISABLED", "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


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
    """Return ``(input, output)`` USD per MTok for a model id.

    Gemini's flash tiers are free of charge on the free tier, so every cost in
    the record is a true zero rather than an estimate nobody paid.
    """

    key = (model or "").strip().lower()
    if key.startswith("gemini"):
        return ptr_vision_provider.GEMINI_PRICING
    if key in MODEL_PRICING:
        return MODEL_PRICING[key]
    for family, price in _FAMILY_PRICING:
        if key.startswith(family):
            return price
    return DEFAULT_PRICING


def estimate_cost_usd(usage: dict[str, int], model: str | None = None) -> float:
    """Estimate request cost in USD from a normalized usage dict.

    ``model`` names the model that was billed; it defaults to the Anthropic
    read model so the pricing arithmetic stays checkable on its own. Every
    call site inside this module passes the model that actually answered.
    """

    price_in, price_out = pricing_for_model(model or MODEL_ID)
    dollars = (
        int(usage.get("input") or 0) * price_in
        + int(usage.get("cache_read") or 0) * price_in * CACHE_READ_MULTIPLIER
        + int(usage.get("cache_write") or 0) * price_in * CACHE_WRITE_MULTIPLIER
        + int(usage.get("output") or 0) * price_out
    ) / 1_000_000
    return round(dollars, 6)


def estimate_filing_cost_usd(
    page_count: int,
    *,
    model: str | None = None,
    strips_per_page: float = 0.0,
    chunk_pages: int | None = None,
    orientation_cost_per_page: float | None = None,
) -> float:
    """Pre-flight estimate: pages x two reads x per-page rate, plus orientation.

    ``strips_per_page`` is the average number of close-up strips sent per page
    (0 before orientation is known, when every page might be portrait).
    ``orientation_cost_per_page`` is 0 when the rotation is decided by the free
    detector rather than by a model.
    """

    pages = max(0, int(page_count))
    if pages == 0:
        return 0.0
    price_in, price_out = pricing_for_model(model or MODEL_ID)
    requests = -(-pages // max(1, chunk_pages or resolve_chunk_pages()))
    input_tokens = pages * (EST_TOKENS_FULL_PAGE + strips_per_page * EST_TOKENS_GRID_STRIP)
    cached_tokens = requests * EST_CACHED_PROMPT_TOKENS * CACHE_READ_MULTIPLIER
    output_tokens = pages * EST_OUTPUT_TOKENS_PER_PAGE
    per_read = ((input_tokens + cached_tokens) * price_in + output_tokens * price_out) / 1_000_000
    orientation = (
        EST_ORIENTATION_COST_PER_PAGE_USD
        if orientation_cost_per_page is None
        else max(0.0, float(orientation_cost_per_page))
    )
    return round(per_read * READS_PER_FILING + pages * orientation, 4)


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


def _skip(reason: str, *, provider: Any = None, **extra: Any) -> dict[str, Any]:
    provider_name = getattr(provider, "name", None) or ptr_vision_provider.resolve_provider_name()
    payload: dict[str, Any] = {
        "ok": False,
        "skipped": True,
        "reason": reason,
        "provider": provider_name,
        "model": getattr(provider, "read_model", None) or resolve_model_id(),
        "model_b": getattr(provider, "read_model_b", None),
        "orientation_model": getattr(provider, "orientation_model", None),
        "parser_version": vision_parser_version(provider_name),
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
        "no_transactions": False,
        "chunks": [],
        "effort": resolve_effort(),
        "orientation_mode": resolve_orientation_mode(),
        "pdf_sha256": None,
        "at": _now_iso(),
    }
    payload.update(extra)
    return payload


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# -- Page images + orientation ---------------------------------------------


def _image_part(png: bytes) -> dict[str, Any]:
    """One image in the neutral content form every provider serialises."""

    return {"kind": "image", "png": png}


def _text_part(text: str) -> dict[str, Any]:
    return {"kind": "text", "text": text}


def _page_zoom(
    rect: Any,
    max_long_edge: int = MAX_IMAGE_LONG_EDGE,
    dpi: int = RENDER_DPI,
) -> float:
    # pymupdf rounds pixel dimensions up, so aim one pixel under the cap.
    long_edge = float(max(rect.width, rect.height)) or 1.0
    return min(dpi / 72.0, (max_long_edge - 1) / long_edge)


def _render_page(
    page: Any,
    module: Any,
    base_rotation: int,
    rotation: int,
    max_long_edge: int = MAX_IMAGE_LONG_EDGE,
    dpi: int = RENDER_DPI,
) -> tuple[bytes, int, int]:
    """Render one page as PNG bytes with ``rotation`` degrees clockwise applied.

    ``page.set_rotation`` follows the PDF ``/Rotate`` convention (clockwise) and
    only changes the in-memory document. The zoom is chosen so the page renders
    at :data:`RENDER_DPI` unless that would push the long edge past
    ``max_long_edge``.
    """

    page.set_rotation((base_rotation + rotation) % 360)
    zoom = _page_zoom(page.rect, max_long_edge, dpi)
    pixmap = page.get_pixmap(matrix=module.Matrix(zoom, zoom), alpha=False)
    return pixmap.tobytes("png"), int(pixmap.width), int(pixmap.height)


def grid_strip_rects(
    width: float, height: float
) -> list[tuple[tuple[float, float, float, float], str]]:
    """Clip rectangles (x0, y0, x1, y1) and labels for the close-up strips."""

    strips = max(1, GRID_CROP_STRIPS)
    x0 = width * GRID_CROP_X_FRACTION
    step = height / strips
    overlap = height * GRID_CROP_OVERLAP
    rects: list[tuple[tuple[float, float, float, float], str]] = []
    for index in range(strips):
        y0 = max(0.0, index * step - (overlap if index else 0.0))
        y1 = min(height, (index + 1) * step + (overlap if index < strips - 1 else 0.0))
        if strips == 1:
            label = "full height"
        elif index == 0:
            label = "top part"
        elif index == strips - 1:
            label = "bottom part"
        else:
            label = f"part {index + 1}"
        rects.append(((x0, y0, width, y1), label))
    return rects


def _render_grid_strips(page: Any, module: Any, zoom_factor: float) -> list[dict[str, Any]]:
    """Close-up strips of the right-hand part of an already-rotated page.

    Each strip is rendered at ``zoom_factor`` times the full-page zoom, capped
    so its long edge stays within :data:`MAX_IMAGE_LONG_EDGE`.
    """

    rect = page.rect
    full_zoom = _page_zoom(rect)
    strips: list[dict[str, Any]] = []
    for (x0, y0, x1, y1), label in grid_strip_rects(float(rect.width), float(rect.height)):
        clip = module.Rect(rect.x0 + x0, rect.y0 + y0, rect.x0 + x1, rect.y0 + y1)
        long_edge = float(max(clip.width, clip.height)) or 1.0
        zoom = min(full_zoom * zoom_factor, (MAX_IMAGE_LONG_EDGE - 1) / long_edge)
        pixmap = page.get_pixmap(matrix=module.Matrix(zoom, zoom), clip=clip, alpha=False)
        strips.append(
            {
                "png": pixmap.tobytes("png"),
                "width": int(pixmap.width),
                "height": int(pixmap.height),
                "label": label,
                "zoom": round(zoom / full_zoom, 2),
            }
        )
    return strips


def orientation_heuristic(width: int, height: int) -> int:
    """Rotation to apply when the model cannot be asked.

    The House PTR paper form is landscape, so a portrait page is almost always a
    sideways scan of it. Which way it was turned is a coin toss; 90 is the
    convention here and the model confirms it when it can.
    """

    return 90 if height > width else 0


def _ask_orientation(provider: Any, parts: list[dict[str, Any]]) -> tuple[str, dict[str, int]]:
    response = provider.ask_short(
        parts,
        model=provider.orientation_model,
        system=ORIENTATION_SYSTEM_PROMPT,
        max_tokens=ORIENTATION_MAX_TOKENS,
    )
    return response["text"], response["usage"]


def _said_no(answer: str) -> bool:
    return bool(re.search(r"\bno\b", answer, re.IGNORECASE)) and not re.search(
        r"\byes\b", answer, re.IGNORECASE
    )


def detect_orientation(
    provider: Any,
    render: Any,
    *,
    width: int,
    height: int,
) -> tuple[int, str, dict[str, int]]:
    """Return ``(rotation, method, usage)`` for one page, asking a model.

    Only used when ``CAPITOL_PTR_VISION_ORIENTATION=model``; the default path
    is :func:`detect_orientation_from_grid`, which costs nothing.

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
            content.append(_text_part(f"Label {rotation}:"))
            content.append(_image_part(candidates[rotation]))
        content.append(_text_part(ORIENTATION_QUESTION))
        answer, call_usage = _ask_orientation(provider, content)
        usage = sum_usage(usage, call_usage)
        match = re.search(r"\b(0|90|180|270)\b", answer)
        if match is None:
            logger.warning("ptr_vision: orientation model answered %r; using heuristic", answer)
            return orientation_heuristic(width, height), "heuristic", usage
        rotation = int(match.group(1))

        confirmation, call_usage = _ask_orientation(
            provider,
            [_image_part(candidates[rotation]), _text_part(ORIENTATION_CONFIRM_QUESTION)],
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


#: A page whose own best rotation beats the document's consensus rotation by
#: less than this keeps the consensus. A filing goes through the scanner once,
#: so its pages share a rotation; on a ruled brokerage grid the upright and
#: upside-down scores can land within a few hundredths of each other, and on
#: those pages the other twenty-two pages are better evidence than this one.
ORIENTATION_CONSENSUS_MARGIN = 0.75


def score_page_orientations(analyze: Any) -> dict[int, float]:
    """Score one page at all four rotations. ``analyze`` is ``(rotation) -> grid``."""

    return {rotation: ptr_grid.orientation_score(analyze(rotation)) for rotation in ROTATIONS}


def choose_orientations(
    scores_per_page: list[dict[int, float]],
    fallbacks: list[int],
) -> list[tuple[int, str]]:
    """Pick a rotation per page from the scored rotations of the whole filing.

    Per page the highest-scoring rotation wins, except where the filing as a
    whole disagrees by less than :data:`ORIENTATION_CONSENSUS_MARGIN` -- then
    the filing wins, because one page's hundredths are not evidence against
    twenty-two pages. A page that scores zero everywhere (a cover sheet, a
    typed electronic form, a scan too poor to show rules) falls back to
    :func:`orientation_heuristic` via ``fallbacks``.
    """

    totals = {
        rotation: sum(scores.get(rotation, 0.0) for scores in scores_per_page)
        for rotation in ROTATIONS
    }
    consensus = max(ROTATIONS, key=lambda rotation: totals[rotation])
    if totals[consensus] <= 0:
        consensus = None  # type: ignore[assignment]

    chosen: list[tuple[int, str]] = []
    for scores, fallback in zip(scores_per_page, fallbacks):
        best = max(ROTATIONS, key=lambda rotation: scores.get(rotation, 0.0))
        if scores.get(best, 0.0) <= 0:
            # No ladder at any rotation: a cover sheet, a signature page, a
            # broker's letter. Follow the rest of the filing when it agrees
            # about the quarter turn -- the pages went through the scanner
            # together -- and otherwise fall back to the shape of the page.
            if (
                consensus is not None
                and consensus != fallback
                and consensus % 180 == fallback % 180
            ):
                chosen.append((consensus, "grid-consensus"))
                continue
            chosen.append((fallback, "heuristic"))
            continue
        if (
            consensus is not None
            and consensus != best
            and scores.get(consensus, 0.0) > 0
            and scores[best] - scores[consensus] <= ORIENTATION_CONSENSUS_MARGIN
        ):
            chosen.append((consensus, "grid-consensus"))
            continue
        chosen.append((best, "grid"))
    return chosen


def detect_orientation_from_grid(
    analyze: Any,
    *,
    width: int,
    height: int,
) -> tuple[int, str, dict[str, Any] | None]:
    """Return ``(rotation, method, grid)`` for a single page, with no model call.

    The page is analysed at all four rotations and
    :func:`capitol_pipeline.parsers.ptr_grid.orientation_score` says which one
    reads upright; the House form's own asymmetries (a complete A-K ladder,
    the header printed above the rows, the ladder at the right margin and the
    wide K column on the right) settle the half turn that defeats a bare "did I
    find a ladder" test.

    Use :func:`plan_orientations` for a whole filing: it adds the document-wide
    consensus, which is what settles the close calls.
    """

    scores = score_page_orientations(analyze)
    fallback = orientation_heuristic(width, height)
    (rotation, method), = choose_orientations([scores], [fallback])
    grid = analyze(rotation) if method != "heuristic" else None
    return rotation, method, grid


def plan_orientations(
    document: Any,
    module: Any,
    positions: Any,
) -> list[dict[str, Any]]:
    """Decide every page's upright rotation for one filing, with no model call.

    Returns one entry per position: ``{"position", "rotation", "method",
    "scores", "grid"}``, where ``grid`` is the detector's analysis at the
    chosen rotation (None when no ladder was found). The page objects are left
    rotated as chosen, ready to render.
    """

    entries: list[dict[str, Any]] = []
    scores_per_page: list[dict[int, float]] = []
    fallbacks: list[int] = []
    for position in positions:
        page = document[position]
        base_rotation = int(getattr(page, "rotation", 0) or 0)
        rect = page.rect

        def _analyze(
            rotation: int,
            _page: Any = page,
            _base: int = base_rotation,
        ) -> dict[str, Any] | None:
            _page.set_rotation((_base + rotation) % 360)
            return _analyze_page_grid(_page, module)

        scores_per_page.append(score_page_orientations(_analyze))
        fallbacks.append(orientation_heuristic(int(rect.width), int(rect.height)))
        entries.append({"position": position, "analyze": _analyze})

    chosen = choose_orientations(scores_per_page, fallbacks)
    for index, (entry, (rotation, method)) in enumerate(zip(entries, chosen)):
        analyze = entry.pop("analyze")
        entry["rotation"] = rotation
        entry["method"] = method
        entry["scores"] = scores_per_page[index]
        entry["grid"] = analyze(rotation) if method != "heuristic" else None
    return entries


def prepare_page_images(
    pdf_path: Path,
    provider: Any,
    *,
    orientation_mode: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int], int] | None:
    """Render every page upright.

    Returns ``(pages, orientation, usage, total_pages)`` or None when pymupdf
    is unavailable or cannot open the file. ``total_pages`` is the filing's
    page count even when a page range limited what was rendered. Each page is
    ``{"index", "png", "width", "height", "crops", "grid"}`` (``crops`` holds
    the close-up strips of a landscape page) and each orientation entry
    ``{"page", "rotation", "method", "width", "height", "strips",
    "gridColumns"}``. ``usage`` is the orientation model's summed token usage,
    which is empty unless ``CAPITOL_PTR_VISION_ORIENTATION=model``.
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
    grid_zoom = resolve_grid_zoom()
    page_range = resolve_page_range()
    # Pinned by a caller that must not reach a model even for orientation.
    mode = orientation_mode or resolve_orientation_mode()
    total_pages = 0
    try:
        with document:
            total_pages = int(document.page_count)
            positions = range(int(document.page_count))
            if page_range is not None:
                first, last = page_range
                positions = range(max(0, first - 1), min(int(document.page_count), last))
                logger.warning(
                    "ptr_vision: CAPITOL_PTR_VISION_PAGE_RANGE=%s-%s: reading %d of %d pages",
                    first,
                    last,
                    len(positions),
                    int(document.page_count),
                )
            planned: dict[int, dict[str, Any]] = {}
            if mode == "grid":
                planned = {
                    entry["position"]: entry
                    for entry in plan_orientations(document, module, positions)
                }
            for position in positions:
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

                def _analyze(
                    rotation: int,
                    _page: Any = page,
                    _base: int = base_rotation,
                ) -> dict[str, Any] | None:
                    _page.set_rotation((_base + rotation) % 360)
                    return _analyze_page_grid(_page, module)

                if mode == "model":
                    rotation, method, call_usage = detect_orientation(
                        provider, _render, width=int(natural_width), height=int(natural_height)
                    )
                    usage = sum_usage(usage, call_usage)
                    # The orientation model tells portrait from landscape
                    # reliably but confuses upright with upside down on typed
                    # tables. The amount ladder sits at the right edge only
                    # when the page is upright, so if the detector finds it
                    # only after a half turn, take the half turn.
                    grid = _analyze(rotation)
                    if grid is None:
                        flipped = (rotation + 180) % 360
                        flipped_grid = _analyze(flipped)
                        if flipped_grid is not None:
                            logger.info(
                                "ptr_vision: page %d: ladder found only after a half turn (%d -> %d)",
                                position + 1,
                                rotation,
                                flipped,
                            )
                            rotation, grid, method = flipped, flipped_grid, f"{method}+grid-flip"
                elif mode == "heuristic":
                    rotation = orientation_heuristic(int(natural_width), int(natural_height))
                    method = "heuristic"
                    grid = _analyze(rotation)
                else:
                    entry = planned[position]
                    rotation, method, grid = entry["rotation"], entry["method"], entry["grid"]

                png, width, height = _render_page(page, module, base_rotation, rotation)
                crops: list[dict[str, Any]] = []
                if width > height and grid_zoom > 0:
                    # Landscape after rotation: the paper checkbox form.
                    crops = _render_grid_strips(page, module, grid_zoom)
                pages.append(
                    {
                        "index": position + 1,
                        "png": png,
                        "width": width,
                        "height": height,
                        "crops": crops,
                        "grid": grid,
                    }
                )
                orientation.append(
                    {
                        "page": position + 1,
                        "rotation": rotation,
                        "method": method,
                        "width": width,
                        "height": height,
                        "strips": len(crops),
                        "gridColumns": len(grid["columns"]) if grid else 0,
                    }
                )
    except Exception as error:  # noqa: BLE001 - fall back to the document block
        logger.warning("ptr_vision: page rendering failed for %s: %s", pdf_path.name, error)
        return None
    if not pages:
        return None
    return pages, orientation, usage, total_pages


def _analyze_page_grid(page: Any, module: Any) -> dict[str, Any] | None:
    """Run the checkbox detector's grid analysis on an already-rotated page.

    Renders a gray pixmap at the page zoom (the same geometry the model sees)
    and hands it to :func:`capitol_pipeline.parsers.ptr_grid.analyze_amount_grid`.
    Never raises: the detector is an extra signal, not a dependency.
    """

    try:
        zoom = _page_zoom(page.rect)
        pixmap = page.get_pixmap(
            matrix=module.Matrix(zoom, zoom), alpha=False, colorspace=module.csGRAY
        )
        gray = ptr_grid.gray_from_pixmap(pixmap.samples, int(pixmap.width), int(pixmap.height), int(pixmap.n))
        return ptr_grid.analyze_amount_grid(gray)
    except Exception as error:  # noqa: BLE001 - detector failures must not fail the read
        logger.warning("ptr_vision: grid analysis failed (%s: %s)", type(error).__name__, error)
        return None


def _letter_from_band(row: dict[str, Any]) -> str | None:
    """The ladder letter implied by a row's amount band, when it is one."""

    band = _comparable(row, "amount")
    for letter, candidate in AMOUNT_LETTER_BANDS.items():
        if candidate == band:
            return letter
    return None


def _null_amount(row: dict[str, Any], note: str) -> None:
    row["amount_min"] = None
    row["amount_max"] = None
    row["amount_column_letter"] = None
    row["legibility"] = _downgrade(_worst_legibility(row.get("legibility")), "partial")
    existing = _merge_comment(row.get("comment"))
    row["comment"] = f"{existing}; {note}" if existing else note


def _has_amount(row: dict[str, Any]) -> bool:
    """Whether the row still carries a usable dollar band."""

    low, high = _comparable(row, "amount")
    return bool(low) and bool(high)


def _adopt_detector_amount(row: dict[str, Any], letter: str) -> None:
    """Take the amount from the box the detector says is ticked.

    Only ever called on a row that has no amount left: either the two reads
    reported different ladder columns, or one of them contradicted itself and
    :func:`apply_amount_letter_check` nulled it. The detector measures the ink
    on the page instead of counting boxes in a picture of it, so where it is
    confident it is the better witness -- but the row is still marked
    ``partial``, never ``clear``, and the rejected readings stay in the
    comment.
    """

    row["amount_min"], row["amount_max"] = AMOUNT_LETTER_BANDS[letter]
    row["amount_column_letter"] = letter
    row["legibility"] = _downgrade(_worst_legibility(row.get("legibility")), "partial")
    rejected = [candidate for candidate in (row.get("_amount_letters") or []) if candidate]
    note = f"amount read from the ticked column {letter} by the checkbox detector"
    if rejected:
        note = f"{note} after the two reads reported {' and '.join(sorted(set(rejected)))}"
    existing = _merge_comment(row.get("comment"))
    row["comment"] = f"{existing}; {note}" if existing else note
    row.pop("_amount_unresolved", None)


def apply_checkbox_detector(
    rows: list[dict[str, Any]],
    pages: list[dict[str, Any]] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Check each page's rows against the classical checkbox detector.

    Rows are grouped by ``page_number`` and, in merged order, aligned top to
    bottom with the ticks the detector found on that page (rows only one read
    saw are left out of the alignment: they are already illegible). Per row:
    the detector's letter agreeing with the model's confirms the band; a
    disagreement or an ambiguous cell nulls the amount and marks the row
    ``partial``; and a row that has no amount left at all -- because the two
    reads named different ladder columns, or one contradicted itself -- takes
    the detector's letter when the detector is confident, rather than being
    published with no amount. Where the detector is silent the row keeps the
    conservative outcome and stays without one. Returns
    ``(rows, page_summaries)``.
    """

    summaries: list[dict[str, Any]] = []
    if not pages:
        return rows, summaries
    by_page = {int(page["index"]): page for page in pages}
    for row in rows:
        row.setdefault("detectorLetter", None)
        row.setdefault("detectorStatus", "unchecked")
    for index, page in sorted(by_page.items()):
        page_rows = [row for row in rows if _page_of(row) == index]
        aligned_rows = [row for row in page_rows if not row.get("_unmatched")]
        result = ptr_grid.detect_page(page.get("grid"), len(aligned_rows))
        summary: dict[str, Any] = {
            "page": index,
            "status": result["status"],
            "columns": result["columns"],
            "bands": result["bands"],
            "candidates": result["candidates"],
            "rows": len(page_rows),
            "rowsAligned": 0,
            "agreed": 0,
            "disagreed": 0,
            "ambiguous": 0,
            "resolved": 0,
        }
        if result["status"] != "ok":
            for row in page_rows:
                row["detectorStatus"] = result["status"]
            summaries.append(summary)
            continue
        summary["rowsAligned"] = len(aligned_rows)
        for row, found in zip(aligned_rows, result["letters"]):
            row["detectorLetter"] = found["letter"]
            model_letter = _letter(row) or _letter_from_band(row)
            if found["kind"] == "ambiguous":
                row["detectorStatus"] = "ambiguous"
                summary["ambiguous"] += 1
                _null_amount(row, "checkbox detector could not tell which amount box is ticked")
            elif model_letter is None:
                # No model letter survived. When the row also has no band left
                # (the two reads named different columns, or one contradicted
                # itself) the detector is the only witness there is, and column
                # K is a flag rather than a band, so it cannot stand in for one.
                if found["letter"] in AMOUNT_LETTER_BANDS and not _has_amount(row):
                    _adopt_detector_amount(row, str(found["letter"]))
                    row["detectorStatus"] = "resolved"
                    summary["resolved"] += 1
                else:
                    row["detectorStatus"] = "unchecked"
            elif found["letter"] == model_letter:
                row["detectorStatus"] = "agree"
                summary["agreed"] += 1
            else:
                row["detectorStatus"] = "disagree"
                summary["disagreed"] += 1
                _null_amount(
                    row,
                    f"checkbox detector reads column {found['letter']} but the model reported {model_letter}",
                )
        for row in page_rows:
            if row.get("_unmatched"):
                row["detectorStatus"] = "unaligned"
        summaries.append(summary)
    return rows, summaries


def _page_of(row: dict[str, Any]) -> int | None:
    value = row.get("page_number")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


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


def chunk_context(pages: list[dict[str, Any]], total_pages: int) -> str:
    """Tell the model which pages of the filing it is looking at."""

    if not pages:
        return ""
    first, last = pages[0]["index"], pages[-1]["index"]
    if total_pages <= len(pages) and first == 1:
        return f"This filing has {total_pages} page(s); all of them are shown."
    shown = f"page {first}" if first == last else f"pages {first} to {last}"
    text = f"This filing has {total_pages} pages; you are shown {shown}."
    if first > 1:
        text += (
            " Earlier pages are not shown: transcribe only the rows on these pages, "
            "and report null for filer_name and filing_date unless they appear here."
        )
    return text


def build_image_content(
    pages: list[dict[str, Any]],
    filename: str,
    context: str = "",
    total_pages: int | None = None,
) -> list[dict[str, Any]]:
    """User content: per page a label, the image and any close-up strips, then the instruction.

    Returns the neutral part form (:mod:`ptr_vision_provider`), which each
    provider serialises into its own request shape.
    """

    total = total_pages or len(pages)
    blocks: list[dict[str, Any]] = []
    for page in pages:
        blocks.append(_text_part(f"Page {page['index']} of {total}:"))
        blocks.append(_image_part(page["png"]))
        strips = page.get("crops") or []
        for position, strip in enumerate(strips, start=1):
            blocks.append(
                _text_part(
                    f"Page {page['index']}, close-up strip {position} of {len(strips)} "
                    f"(right-hand part of the page, {strip['label']}, higher zoom):"
                )
            )
            blocks.append(_image_part(strip["png"]))
    text = "\n\n".join(
        part for part in (USER_INSTRUCTION, chunk_context(pages, total), context) if part
    )
    blocks.append(_text_part(f"{text}\n\nFile: {filename}"))
    return blocks


def chunk_page_list(pages: list[dict[str, Any]], chunk_pages: int) -> list[list[dict[str, Any]]]:
    """Split rendered pages into consecutive groups of at most ``chunk_pages``."""

    size = max(1, int(chunk_pages))
    return [pages[start : start + size] for start in range(0, len(pages), size)]


def build_document_content(
    pdf_bytes: bytes,
    filename: str,
    context: str = "",
) -> list[dict[str, Any]]:
    """User content when pymupdf is unavailable: the PDF itself.

    Only the Anthropic provider accepts this part. Gemini rasterises a PDF at
    its own resolution and then reads every two-digit year one low, so it
    refuses the part rather than returning a filing whose dates are a year out.
    """

    text = f"{USER_INSTRUCTION}\n\n{context}".rstrip() + f"\n\nFile: {filename}"
    return [{"kind": "document", "pdf": pdf_bytes}, _text_part(text)]


# -- Request ----------------------------------------------------------------


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
    provider: Any,
    parts: list[dict[str, Any]],
    state: _ReadState,
    *,
    label: str,
    filename: str,
    model: str,
) -> dict[str, Any]:
    """One transcription request with the retry / downgrade ladder.

    Returns ``{"ok", "reason", "payload", "usage", "cost_usd", "attempts",
    "stop_reason", "structured"}``. Never raises.
    """

    attempts = 0
    response: dict[str, Any] | None = None
    last_error: Exception | None = None

    while attempts < 2:
        attempts += 1
        kwargs: dict[str, Any] = {
            "model": model,
            "system": SYSTEM_PROMPT,
            "schema": PTR_VISION_SCHEMA,
            "effort": resolve_effort(),
            "max_tokens": MAX_OUTPUT_TOKENS,
            "structured": state.structured,
        }
        if not state.with_output_config:
            kwargs["with_output_config"] = False
        try:
            response = provider.read(parts, **kwargs)
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
        except Exception as error:  # noqa: BLE001 - classified by the provider
            last_error = error
            if not state.downgraded and provider.rejected_structured_output(error):
                # The API refused the schema-constrained format; fall back to
                # the looser form once before giving up. Not a retry.
                logger.warning(
                    "ptr_vision: %s rejected structured output (%s); retrying without the schema",
                    provider.name,
                    error,
                )
                state.structured = False
                state.downgraded = True
                attempts -= 1
                continue
            if attempts >= 2 or not provider.is_retryable(error):
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
    if response is None:
        logger.error("ptr_vision: %s failed for %s: %s", label, filename, last_error)
        outcome["reason"] = f"api error: {last_error}"
        return outcome

    usage = response["usage"]
    outcome["usage"] = usage
    outcome["cost_usd"] = estimate_cost_usd(usage, model)
    stop_reason = response["stop_reason"]
    outcome["stop_reason"] = stop_reason

    if stop_reason == "refusal":
        outcome["reason"] = f"model refused (category={response.get('detail')})"
        return outcome
    if stop_reason == "max_tokens":
        outcome["reason"] = "response truncated at max_tokens"
        return outcome

    payload = response["payload"]
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
    if field == "transaction_type":
        return _type_for_agreement(value)
    return str(value).strip().lower() or None


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


def _merged_transaction_type(
    row_a: dict[str, Any], row_b: dict[str, Any]
) -> tuple[str | None, str | None]:
    """The Type value two agreeing reads should carry, plus a note if asked twice.

    Returns ``(value, note)``. The value is canonical: two reads that spelled
    the same fact differently ("S" and "Sale") must not leave a raw "S" on the
    row, because ``house_ptr._VISION_TRANSACTION_TYPE_MAP`` maps anything it
    does not recognise to "purchase". ``note`` records the two raw spellings
    whenever they differed, so the reading is still auditable.
    """

    raw_a, raw_b = row_a.get("transaction_type"), row_b.get("transaction_type")
    canonical_a = canonical_transaction_type(raw_a)
    canonical_b = canonical_transaction_type(raw_b)
    if "sale_partial" in (canonical_a, canonical_b):
        value = "sale_partial"  # keep the more specific reading
    else:
        value = canonical_a if canonical_a is not None else canonical_b
    note = None
    text_a, text_b = str(raw_a or "").strip(), str(raw_b or "").strip()
    if text_a and text_b and text_a != text_b:
        note = f"transaction type read as {text_a!r} and {text_b!r}"
    return value, note


def _letter(row: dict[str, Any]) -> str | None:
    value = row.get("amount_column_letter")
    if value is None:
        return None
    text = str(value).strip().upper()
    return text if text in AMOUNT_LETTERS else None


#: Every band the form actually prints. A reported band that is not one of
#: these was mistyped, not read off a different column.
_LADDER_BANDS: frozenset[tuple[int, int]] = frozenset(AMOUNT_LETTER_BANDS.values())


def apply_amount_letter_check(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Reconcile each row's amount column letter with its dollar band.

    The prompt asks the model to count boxes from column A and to make the band
    agree with the letter. Two different things can go wrong when it does not,
    and they deserve different answers:

    * **The band is not a band the form prints.** ``$1,501-$50,000`` is not on
      the ladder; it is ``$15,001-$50,000`` with a digit dropped. (Measured:
      ``gemini-3.5-flash`` did exactly this on all six rows of Lamborn
      8220068.) The letter is the datum, the bounds beside it are printed on
      the form, and the checkbox detector confirms the letter independently, so
      the band is repaired from the letter and the row says so.
    * **The band is a real band, but a different one.** Then the two readings
      contradict each other, one of them is a miscount, and neither can be
      trusted: the amount is nulled, the row downgraded to ``partial``, and
      flagged so :func:`merge_matched_rows` treats its amount as unconfirmed.

    Returns ``(rows, conflicts)``, counting only the second kind.
    """

    conflicts = 0
    for row in rows:
        letter = _letter(row)
        band = AMOUNT_LETTER_BANDS.get(letter or "")
        if band is None:
            continue
        reported = _comparable(row, "amount")
        if reported == band:
            continue
        if reported not in _LADDER_BANDS:
            row["amount_min"], row["amount_max"] = band
            note = (
                f"amount ${reported[0]:,}-${reported[1]:,} is not a band on the form; "
                f"read from the ticked column {letter}"
            )
            existing = _merge_comment(row.get("comment"))
            row["comment"] = f"{existing}; {note}" if existing else note
            continue
        conflicts += 1
        row["amount_min"] = None
        row["amount_max"] = None
        row["_amount_letter_conflict"] = True
        row["legibility"] = _downgrade(
            _worst_legibility(row.get("legibility")), "partial"
        )
        note = f"amount column letter {letter} does not match the reported band"
        existing = _merge_comment(row.get("comment"))
        row["comment"] = f"{existing}; {note}" if existing else note
    return rows, conflicts


def merge_matched_rows(row_a: dict[str, Any], row_b: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """Merge one matched pair, keeping only fields both reads agree on.

    Returns ``(merged_row, disagreements)``. A disagreement on a
    :data:`CRITICAL_FIELDS` entry nulls the field and marks the row
    ``illegible``; on a :data:`SOFT_FIELDS` entry it nulls the field and marks
    the row at least ``partial``. A disagreement on the amount column letter,
    or a letter/band conflict inside either read, nulls the amount and marks
    the row ``partial`` -- and flags it ``_amount_unresolved`` so that
    :func:`apply_checkbox_detector`, which runs afterwards and reads the ticked
    box off the page itself, can settle it instead of the row losing its amount
    to two guesses. The asset description keeps the reading the model was more
    confident about (``clear`` over ``partial``), and the longer of the two
    when it rated both the same.
    """

    merged: dict[str, Any] = dict(row_a)
    merged.pop("_amount_letter_conflict", None)
    if merged.get("page_number") is None:
        merged["page_number"] = row_b.get("page_number")
    disagreements: list[str] = []
    notes: list[str] = []

    desc_a = str(row_a.get("asset_description") or "").strip()
    desc_b = str(row_b.get("asset_description") or "").strip()
    rank_a = _LEGIBILITY_RANK.get(str(row_a.get("legibility") or "partial").lower(), 1)
    rank_b = _LEGIBILITY_RANK.get(str(row_b.get("legibility") or "partial").lower(), 1)
    if rank_b < rank_a or (rank_b == rank_a and len(desc_b) > len(desc_a)):
        merged["asset_description"] = desc_b
    else:
        merged["asset_description"] = desc_a

    letter_a, letter_b = _letter(row_a), _letter(row_b)
    letter_conflict = bool(
        row_a.get("_amount_letter_conflict") or row_b.get("_amount_letter_conflict")
    )
    letters_disagree = bool(letter_a and letter_b and letter_a != letter_b)

    for field in CRITICAL_FIELDS + SOFT_FIELDS:
        value_a = _comparable(row_a, field)
        value_b = _comparable(row_b, field)
        if field == "amount":
            if letters_disagree or letter_conflict:
                merged["amount_min"] = None
                merged["amount_max"] = None
                merged["amount_column_letter"] = None
                merged["_amount_unresolved"] = "amount_column_letter"
                merged["_amount_letters"] = [letter_a, letter_b]
                disagreements.append("amount_column_letter")
            elif value_a != value_b:
                merged["amount_min"] = None
                merged["amount_max"] = None
                merged["amount_column_letter"] = None
                merged["_amount_unresolved"] = "amount"
                merged["_amount_letters"] = [letter_a, letter_b]
                disagreements.append("amount")
            else:
                merged["amount_column_letter"] = letter_a or letter_b
            continue
        if value_a == value_b:
            merged[field] = row_a.get(field) if row_a.get(field) is not None else row_b.get(field)
            if field == "transaction_type":
                merged[field], spelling_note = _merged_transaction_type(row_a, row_b)
                if spelling_note:
                    notes.append(spelling_note)
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
        notes.append("two reads disagreed on: " + ", ".join(disagreements))
    for note in notes:
        comment = f"{comment}; {note}" if comment else note
    merged["comment"] = comment
    return merged, disagreements


def _unmatched(row: dict[str, Any], label: str) -> dict[str, Any]:
    copy = dict(row)
    copy.pop("_amount_letter_conflict", None)
    copy["legibility"] = "illegible"
    copy["_unmatched"] = True
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


def _single_read_fallback(rows_a: list[dict[str, Any]], reason: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Only read A is usable: nothing can be confirmed, so every row goes to a human."""

    rows = [_unmatched(row, "read A") for row in rows_a]
    agreement = {
        "rowsA": len(rows_a),
        "rowsB": None,
        "matched": 0,
        "unmatchedA": len(rows_a),
        "unmatchedB": 0,
        "rowCountsAgree": False,
        "fieldDisagreements": {},
        "readBFailed": reason,
    }
    return rows, agreement


# -- Metadata ---------------------------------------------------------------


def build_vision_metadata(report: dict[str, Any]) -> dict[str, Any]:
    """Compact a vision result for ``house_filing_stubs.metadata.visionParse``.

    The raw transcription is deliberately dropped: the normalized rows already
    land in ``metadata.parsedTransactions``, and this record only has to explain
    what the model was asked, what it cost, and why a filing did or did not
    leave the review queue. ``pdfSha256`` and ``at`` let a later run reuse the
    result instead of paying for it again.
    """

    usage = report.get("usage") or {}
    model = report.get("model")
    price_in, price_out = pricing_for_model(model)
    # None whenever no model was asked about orientation, which is the
    # default: naming one there would put a model in the record that never
    # saw the page.
    orientation_model = report.get("orientation_model")
    orient_in, orient_out = pricing_for_model(orientation_model) if orientation_model else (0.0, 0.0)
    metadata: dict[str, Any] = {
        "provider": report.get("provider"),
        "model": model,
        "modelB": report.get("model_b"),
        "orientationModel": orientation_model,
        "orientationMode": report.get("orientation_mode"),
        "parserVersion": report.get("parser_version"),
        "effort": report.get("effort"),
        "at": report.get("at"),
        "pdfSha256": report.get("pdf_sha256"),
        "ok": bool(report.get("ok")),
        "skipped": bool(report.get("skipped")),
        "reason": report.get("reason"),
        "stopReason": report.get("stop_reason"),
        "attempts": report.get("attempts"),
        "structuredOutput": report.get("structuredOutput"),
        "rowCount": len(report.get("transactions") or []),
        "noTransactions": bool(report.get("no_transactions")),
        "legibility": report.get("legibility"),
        "confidence": report.get("confidence"),
        "needsReview": bool(report.get("needs_review", True)),
        "needsReviewReasons": report.get("needs_review_reasons") or [],
        "pageCount": report.get("page_count"),
        "chunkPages": report.get("chunk_pages"),
        "chunks": report.get("chunks") or [],
        "filerName": report.get("filer_name"),
        "filingDate": report.get("filing_date"),
        "notes": (report.get("notes") or None),
        "orientation": report.get("orientation"),
        "readAgreement": report.get("read_agreement"),
        "amountLetterConflicts": int(report.get("amount_letter_conflicts") or 0),
        "amountLetterIssues": int(report.get("amount_letter_issues") or 0),
        "amountsUnresolved": int(report.get("amounts_unresolved") or 0),
        "detector": report.get("detector"),
        "rows": _row_summaries(list(report.get("transactions") or []), limit=MAX_ROW_SUMMARIES_IN_METADATA),
        "calls": report.get("calls") or [],
        "usage": {
            "inputTokens": int(usage.get("input") or 0),
            "cacheReadTokens": int(usage.get("cache_read") or 0),
            "cacheWriteTokens": int(usage.get("cache_write") or 0),
            "outputTokens": int(usage.get("output") or 0),
        },
        "costUsd": report.get("cost_usd", 0.0),
        "costEstimateUsd": report.get("cost_estimate_usd"),
        "costCeilingUsd": report.get("cost_ceiling_usd"),
        "pricing": {
            "inputPerMTok": price_in,
            "outputPerMTok": price_out,
            "cacheReadMultiplier": CACHE_READ_MULTIPLIER,
            "cacheWriteMultiplier": CACHE_WRITE_MULTIPLIER,
            "orientation": {"inputPerMTok": orient_in, "outputPerMTok": orient_out},
        },
    }
    # Kept verbatim so the filing can be reconciled again later without paying
    # for a second read; the summaries above are for a human. Every merged row
    # is kept, including any the caller drops afterwards, because the checkbox
    # detector aligns its bands against the whole page, not against a subset.
    merged = report.get("merged_transactions")
    merged = merged if isinstance(merged, list) else list(report.get("transactions") or [])
    metadata["transcription"] = stored_transcription(merged)
    metadata["transcriptionComplete"] = len(merged) <= MAX_TRANSCRIPTION_ROWS
    scrubs = int(report.get("example_row_scrubs") or 0)
    if scrubs:
        metadata["exampleRowScrubs"] = scrubs
    return metadata


# -- Entry point ------------------------------------------------------------

#: Row summaries kept per call record; the rest is counted, not stored.
MAX_ROW_SUMMARIES_PER_CALL = 60
#: Merged-row summaries kept on ``visionParse.rows`` (the transcription a
#: reviewer sees while the filing is held back from ``trades``).
MAX_ROW_SUMMARIES_IN_METADATA = 500


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
    letter = _letter(row)
    if letter:
        amount = f"{amount} ({letter})"
    parts = [
        str(row.get("asset_description") or "").strip(),
        str(row.get("transaction_type") or "?"),
        str(row.get("transaction_date") or "?"),
        str(row.get("notification_date") or "-"),
        amount,
        str(row.get("owner") or "-"),
        str(row.get("legibility") or "?"),
    ]
    if row.get("page_number") is not None:
        parts.insert(0, f"p{row['page_number']}")
    status = row.get("detectorStatus")
    if status and status != "unchecked":
        parts.append(f"det:{row.get('detectorLetter') or '-'}/{status}")
    return " | ".join(parts)


def _row_summaries(rows: list[dict[str, Any]], limit: int = MAX_ROW_SUMMARIES_PER_CALL) -> list[str]:
    summaries = [row_summary(row) for row in rows[:limit]]
    if len(rows) > limit:
        summaries.append(f"... {len(rows) - limit} more row(s)")
    return summaries


def _page_range(pages: list[dict[str, Any]] | None) -> str:
    if not pages:
        return "all"
    first, last = pages[0]["index"], pages[-1]["index"]
    return str(first) if first == last else f"{first}-{last}"


def _read_pages(
    provider: Any,
    pages: list[dict[str, Any]] | None,
    state: _ReadState,
    *,
    label: str,
    filename: str,
    context: str,
    total_pages: int,
    model: str,
    content: list[dict[str, Any]] | None = None,
    depth: int = 0,
) -> dict[str, Any]:
    """One logical read of a page group.

    A read that stops at ``max_tokens`` is retried once as two half-size
    groups (``depth`` 1); a half that still truncates fails the read. Returns
    ``{"ok", "reason", "stop_reason", "payloads", "rows", "flags", "calls",
    "usage", "cost_usd", "attempts", "halved"}`` where ``flags`` collects
    ``no_transactions_stated`` from each payload.
    """

    if content is None:
        content = build_image_content(pages or [], filename, context, total_pages)
    page_range = _page_range(pages)
    outcome = _read_once(
        provider,
        content,
        state,
        label=f"{label} pages {page_range}",
        filename=filename,
        model=model,
    )
    rows = _coerce_transactions((outcome["payload"] or {}).get("transactions"))
    call = _call_record(
        label,
        model,
        outcome["usage"],
        outcome["cost_usd"],
        pages=page_range,
        ok=outcome["ok"],
        reason=outcome["reason"],
        attempts=outcome["attempts"],
        stopReason=outcome["stop_reason"],
        rows=_row_summaries(rows) if outcome["ok"] else None,
    )
    result: dict[str, Any] = {
        "ok": outcome["ok"],
        "reason": outcome["reason"],
        "stop_reason": outcome["stop_reason"],
        "payloads": [outcome["payload"]] if outcome["ok"] else [],
        "rows": rows if outcome["ok"] else [],
        "flags": [bool((outcome["payload"] or {}).get("no_transactions_stated"))] if outcome["ok"] else [],
        "calls": [call],
        "usage": dict(outcome["usage"]),
        "cost_usd": float(outcome["cost_usd"]),
        "attempts": int(outcome["attempts"]),
        "halved": False,
    }

    if outcome["stop_reason"] != "max_tokens" or not pages or len(pages) < 2 or depth > 0:
        return result

    # Truncated: retry once with the page group halved.
    middle = len(pages) // 2
    logger.warning(
        "ptr_vision: %s pages %s truncated at max_tokens; retrying as two halves",
        label,
        page_range,
    )
    combined: dict[str, Any] = {
        "ok": True,
        "reason": None,
        "stop_reason": None,
        "payloads": [],
        "rows": [],
        "flags": [],
        "calls": list(result["calls"]),
        "usage": dict(result["usage"]),
        "cost_usd": result["cost_usd"],
        "attempts": result["attempts"],
        "halved": True,
    }
    for half in (pages[:middle], pages[middle:]):
        sub = _read_pages(
            provider,
            half,
            state,
            label=label,
            filename=filename,
            context=context,
            total_pages=total_pages,
            model=model,
            depth=depth + 1,
        )
        combined["calls"].extend(sub["calls"])
        combined["usage"] = sum_usage(combined["usage"], sub["usage"])
        combined["cost_usd"] = round(combined["cost_usd"] + sub["cost_usd"], 6)
        combined["attempts"] += sub["attempts"]
        if not sub["ok"]:
            combined["ok"] = False
            combined["reason"] = (
                f"{sub['reason']} (pages {_page_range(half)}, after halving)"
                if sub["stop_reason"] == "max_tokens"
                else sub["reason"]
            )
            combined["stop_reason"] = sub["stop_reason"]
            break
        combined["payloads"].extend(sub["payloads"])
        combined["rows"].extend(sub["rows"])
        combined["flags"].extend(sub["flags"])
    return combined


def _first_header(payloads: list[dict[str, Any]], field: str) -> Any:
    for payload in payloads:
        value = payload.get(field)
        if value is not None:
            return value
    return None


def extract_via_vision(
    pdf_path: Path,
    *,
    filing_year: int | None = None,
    filing_date: str | None = None,
) -> dict[str, Any]:
    """Transcribe a House PTR PDF with a Claude vision model, twice, and agree.

    Always returns a dict. ``ok`` is False and ``reason`` is set whenever the
    filing was skipped or the call failed; the caller then leaves the stub in
    ``needs_review`` and records ``reason`` in its metadata. ``ok`` is True
    with an empty ``transactions`` list and ``no_transactions`` set when both
    reads agree the form states there is nothing to report. ``filing_year``
    drives the example-row scrub; ``filing_date`` (YYYY-MM-DD) is passed to
    the model as a hint for two-digit years.
    """

    if vision_disabled():
        logger.warning("ptr_vision: disabled by CAPITOL_PTR_VISION_DISABLED")
        return _skip("disabled by CAPITOL_PTR_VISION_DISABLED")

    provider = ptr_vision_provider.resolve_provider(anthropic_client_factory=_client_once)
    if not provider.has_credentials():
        logger.warning("ptr_vision: %s; skipping", provider.credentials_hint())
        return _skip(provider.credentials_hint(), provider=provider)

    if not pdf_path.exists():
        return _skip(f"pdf not found: {pdf_path}", provider=provider)

    pdf_bytes = pdf_path.read_bytes()
    pdf_sha256 = hashlib.sha256(pdf_bytes).hexdigest()
    size = len(pdf_bytes)
    if size > MAX_VISION_PDF_BYTES:
        logger.warning(
            "ptr_vision: %s is %d bytes (> %d)", pdf_path.name, size, MAX_VISION_PDF_BYTES
        )
        return _skip(
            f"pdf too large: {size} bytes (limit {MAX_VISION_PDF_BYTES})",
            provider=provider,
            pdf_sha256=pdf_sha256,
        )

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
            provider=provider,
            page_count=page_count,
            pdf_sha256=pdf_sha256,
        )
    if page_count is None:
        logger.debug("ptr_vision: no PDF reader available; skipping the page-count guardrail")

    model = provider.read_model
    model_b = provider.read_model_b
    orientation_mode = resolve_orientation_mode()
    orientation_model = provider.orientation_model if orientation_mode == "model" else None
    # The rotation is decided by the free detector unless a model was asked
    # for, so the orientation term of the estimate is usually a true zero.
    orientation_cost = (
        EST_ORIENTATION_COST_PER_PAGE_USD
        if orientation_mode == "model" and any(pricing_for_model(orientation_model))
        else 0.0
    )
    chunk_pages = resolve_chunk_pages()
    ceiling = resolve_max_filing_cost_usd()
    estimate = 0.0
    if page_count:
        # Pre-flight, before any API call: no close-up strips are assumed yet.
        estimate = estimate_filing_cost_usd(
            page_count,
            model=model,
            chunk_pages=chunk_pages,
            orientation_cost_per_page=orientation_cost,
        )
        if estimate > ceiling:
            logger.warning(
                "ptr_vision: %s refused: estimated $%.2f for %d pages > $%.2f ceiling",
                pdf_path.name,
                estimate,
                page_count,
                ceiling,
            )
            return _skip(
                f"estimated cost ${estimate:.2f} for {page_count} pages exceeds the "
                f"${ceiling:.2f} ceiling (CAPITOL_PTR_VISION_MAX_COST_USD)",
                provider=provider,
                page_count=page_count,
                pdf_sha256=pdf_sha256,
                cost_estimate_usd=estimate,
                cost_ceiling_usd=ceiling,
            )

    context = filing_context(filing_year, filing_date)
    calls: list[dict[str, Any]] = []

    def _cost_so_far() -> float:
        return round(sum(float(call["costUsd"]) for call in calls), 6)

    def _usage_so_far() -> dict[str, int]:
        return sum_usage(*(call["usage"] for call in calls))

    # Upright page images when pymupdf can render them, the raw PDF otherwise.
    orientation: list[dict[str, Any]] | None = None
    chunks: list[list[dict[str, Any]] | None]
    document_content: list[dict[str, Any]] | None = None
    prepared = prepare_page_images(pdf_path, provider)
    if prepared is not None:
        pages, orientation, orient_usage, document_pages = prepared
        if any(orient_usage.values()):
            orient_cost = estimate_cost_usd(orient_usage, orientation_model)
            calls.append(
                _call_record(
                    "orientation",
                    orientation_model,
                    orient_usage,
                    orient_cost,
                    pages=_page_range(pages),
                )
            )
        page_count = document_pages or len(pages)
        strips = sum(len(page.get("crops") or []) for page in pages)
        estimate = estimate_filing_cost_usd(
            len(pages),
            model=model,
            strips_per_page=strips / max(1, len(pages)),
            chunk_pages=chunk_pages,
            orientation_cost_per_page=orientation_cost,
        )
        if estimate > ceiling:
            logger.warning(
                "ptr_vision: %s refused after rendering: estimated $%.2f (%d pages, %d strips) > $%.2f",
                pdf_path.name,
                estimate,
                len(pages),
                strips,
                ceiling,
            )
            return _skip(
                f"estimated cost ${estimate:.2f} for {len(pages)} pages with {strips} close-up "
                f"strips exceeds the ${ceiling:.2f} ceiling (CAPITOL_PTR_VISION_MAX_COST_USD)",
                provider=provider,
                page_count=page_count,
                pdf_sha256=pdf_sha256,
                cost_estimate_usd=estimate,
                cost_ceiling_usd=ceiling,
                orientation=orientation,
                calls=calls,
                usage=_usage_so_far(),
                cost_usd=_cost_so_far(),
            )
        chunks = list(chunk_page_list(pages, chunk_pages))
        logger.info(
            "ptr_vision: %s -> %d upright page image(s), %d close-up strip(s), %d chunk(s) of <=%d; "
            "estimate $%.2f (ceiling $%.2f) %s",
            pdf_path.name,
            len(pages),
            strips,
            len(chunks),
            chunk_pages,
            estimate,
            ceiling,
            [(entry["rotation"], entry["method"]) for entry in orientation],
        )
    else:
        document_content = build_document_content(pdf_bytes, pdf_path.name, context)
        chunks = [None]
        logger.info("ptr_vision: %s -> sending the PDF as a document block", pdf_path.name)

    state = _ReadState()
    total_pages = int(page_count or 0)
    all_rows: list[dict[str, Any]] = []
    payloads: list[dict[str, Any]] = []
    chunk_records: list[dict[str, Any]] = []
    attempts = 0
    scrubs_total = 0
    letter_conflicts_total = 0
    disagreement_totals: dict[str, int] = {}
    rows_a_total = rows_b_total = matched_total = unmatched_a_total = unmatched_b_total = 0
    row_counts_agree = True
    read_b_failures: list[str] = []
    no_transactions_chunks = 0
    stop_reason: Any = None
    detector_pages: list[dict[str, Any]] = []

    for chunk_index, chunk in enumerate(chunks, start=1):
        page_range = _page_range(chunk)
        read_kwargs: dict[str, Any] = {
            "filename": pdf_path.name,
            "context": context,
            "total_pages": total_pages,
        }
        if chunk is None:
            read_kwargs["content"] = document_content

        read_a = _read_pages(provider, chunk, state, label="read A", model=model, **read_kwargs)
        calls.extend(read_a["calls"])
        attempts += read_a["attempts"]
        stop_reason = read_a["stop_reason"]
        if not read_a["ok"]:
            # A first read that refused, truncated twice, or errored would fail
            # the same way again; do not pay for the second.
            reason = str(read_a["reason"])
            if chunk is not None and len(chunks) > 1 and "pages" not in reason:
                reason = f"{reason} (pages {page_range})"
            return _skip(
                reason,
                provider=provider,
                usage=_usage_so_far(),
                cost_usd=_cost_so_far(),
                stop_reason=stop_reason,
                attempts=attempts,
                orientation=orientation,
                calls=calls,
                page_count=page_count,
                chunks=chunk_records,
                chunk_pages=chunk_pages,
                pdf_sha256=pdf_sha256,
                cost_estimate_usd=estimate,
                cost_ceiling_usd=ceiling,
            )

        read_b = _read_pages(provider, chunk, state, label="read B", model=model_b, **read_kwargs)
        calls.extend(read_b["calls"])
        attempts += read_b["attempts"]

        rows_a, scrubs_a = scrub_example_row_values(list(read_a["rows"]), filing_year)
        rows_a, conflicts_a = apply_amount_letter_check(rows_a)
        scrubs_total += scrubs_a
        letter_conflicts_total += conflicts_a
        payloads.extend(read_a["payloads"])
        if read_b["ok"]:
            rows_b, scrubs_b = scrub_example_row_values(list(read_b["rows"]), filing_year)
            rows_b, conflicts_b = apply_amount_letter_check(rows_b)
            scrubs_total += scrubs_b
            letter_conflicts_total += conflicts_b
            payloads.extend(read_b["payloads"])
            merged, agreement = reconcile_reads(rows_a, rows_b)
            merged, page_summaries = apply_checkbox_detector(merged, chunk)
            detector_pages.extend(page_summaries)
            if not rows_a and not rows_b and any(read_a["flags"]) and any(read_b["flags"]):
                no_transactions_chunks += 1
        else:
            merged, agreement = _single_read_fallback(rows_a, str(read_b["reason"]))
            read_b_failures.append(f"pages {page_range}: {read_b['reason']}")

        all_rows.extend(merged)
        rows_a_total += int(agreement["rowsA"] or 0)
        if agreement.get("rowsB") is not None:
            rows_b_total += int(agreement["rowsB"])
        matched_total += int(agreement["matched"])
        unmatched_a_total += int(agreement["unmatchedA"])
        unmatched_b_total += int(agreement["unmatchedB"])
        row_counts_agree = row_counts_agree and bool(agreement.get("rowCountsAgree"))
        for field, count in (agreement.get("fieldDisagreements") or {}).items():
            disagreement_totals[field] = disagreement_totals.get(field, 0) + int(count)
        chunk_usage = sum_usage(read_a["usage"], read_b["usage"])
        chunk_records.append(
            {
                "chunk": chunk_index,
                "pages": page_range,
                "rowsA": agreement["rowsA"],
                "rowsB": agreement.get("rowsB"),
                "matched": agreement["matched"],
                "fieldDisagreements": agreement.get("fieldDisagreements") or {},
                "halvedA": read_a["halved"],
                "halvedB": read_b["halved"],
                "readBFailed": None if read_b["ok"] else read_b["reason"],
                "letterConflicts": conflicts_a + (conflicts_b if read_b["ok"] else 0),
                "usage": chunk_usage,
                "costUsd": round(read_a["cost_usd"] + read_b["cost_usd"], 6),
            }
        )

        spent = _cost_so_far()
        if spent > ceiling * COST_OVERRUN_FACTOR and chunk_index < len(chunks):
            logger.error(
                "ptr_vision: %s abandoned after pages %s: $%.2f spent > %.1fx the $%.2f ceiling",
                pdf_path.name,
                page_range,
                spent,
                COST_OVERRUN_FACTOR,
                ceiling,
            )
            return _skip(
                f"cost ceiling exceeded mid-filing: ${spent:.2f} spent after pages {page_range} "
                f"(ceiling ${ceiling:.2f} x {COST_OVERRUN_FACTOR})",
                provider=provider,
                usage=_usage_so_far(),
                cost_usd=spent,
                stop_reason=stop_reason,
                attempts=attempts,
                orientation=orientation,
                calls=calls,
                page_count=page_count,
                chunks=chunk_records,
                chunk_pages=chunk_pages,
                pdf_sha256=pdf_sha256,
                cost_estimate_usd=estimate,
                cost_ceiling_usd=ceiling,
            )

    read_agreement: dict[str, Any] = {
        "rowsA": rows_a_total,
        "rowsB": None if read_b_failures and not rows_b_total else rows_b_total,
        "matched": matched_total,
        "unmatchedA": unmatched_a_total,
        "unmatchedB": unmatched_b_total,
        "rowCountsAgree": row_counts_agree,
        "fieldDisagreements": disagreement_totals,
        "chunks": len(chunk_records),
    }
    if read_b_failures:
        read_agreement["readBFailed"] = "; ".join(read_b_failures)

    transactions = all_rows
    no_transactions = not transactions and no_transactions_chunks > 0 and not read_b_failures
    counts = summarize_legibility(transactions)
    confidence = 0.9 if no_transactions else legibility_confidence(counts)

    review_reasons: list[str] = []
    if not no_transactions and majority_illegible(counts):
        review_reasons.append("majority illegible")
    if not read_agreement["rowCountsAgree"]:
        review_reasons.append("reads disagree on row count")
    critical = {
        field: count
        for field, count in disagreement_totals.items()
        if field in CRITICAL_FIELDS
    }
    if critical:
        review_reasons.append("reads disagree on " + ", ".join(sorted(critical)))
    # Only rows the detector could not settle still cost the filing its amount.
    unresolved_amounts = sum(1 for row in transactions if row.get("_amount_unresolved"))
    letter_issues = letter_conflicts_total + int(disagreement_totals.get("amount_column_letter") or 0)
    if unresolved_amounts:
        review_reasons.append("amount nulled after column-letter conflict")
    detector_summary = detector_totals(detector_pages)
    review_reasons.extend(detector_review_reasons(detector_summary))
    for row in transactions:
        row.pop("_unmatched", None)
        row.pop("_amount_letter_conflict", None)
        row.pop("_amount_unresolved", None)
        row.pop("_amount_letters", None)
    needs_review = bool(review_reasons)

    notes = [_clean_optional_text(payload.get("notes")) for payload in payloads]
    unique_notes = [note for index, note in enumerate(notes) if note and note not in notes[:index]]
    joined_notes = " | ".join(unique_notes)
    if len(joined_notes) > 2000:
        joined_notes = joined_notes[:1997] + "..."

    reported_pages = _first_header(payloads, "page_count") if len(chunks) == 1 else None
    if no_transactions:
        reason: str | None = "form states no transactions"
    elif transactions:
        reason = None
    else:
        reason = "model returned no transaction rows"

    result: dict[str, Any] = {
        "ok": bool(transactions) or no_transactions,
        "skipped": False,
        "reason": reason,
        "provider": provider.name,
        "model": model,
        "model_b": model_b,
        "orientation_model": orientation_model,
        "parser_version": vision_parser_version(provider.name),
        "effort": resolve_effort(),
        "orientation_mode": orientation_mode,
        "at": _now_iso(),
        "pdf_sha256": pdf_sha256,
        "filer_name": _clean_optional_text(_first_header(payloads, "filer_name")),
        "filing_date": _clean_optional_text(_first_header(payloads, "filing_date")),
        "page_count": reported_pages if reported_pages is not None else page_count,
        "notes": joined_notes or None,
        "no_transactions": no_transactions,
        "transactions": transactions,
        "legibility": counts,
        "confidence": confidence,
        "needs_review": needs_review,
        "needs_review_reasons": review_reasons,
        "usage": _usage_so_far(),
        "cost_usd": _cost_so_far(),
        "cost_estimate_usd": estimate,
        "cost_ceiling_usd": ceiling,
        "stop_reason": stop_reason,
        "attempts": attempts,
        "structuredOutput": state.structured,
        "orientation": orientation,
        "read_agreement": read_agreement,
        "chunks": chunk_records,
        "chunk_pages": chunk_pages,
        "example_row_scrubs": scrubs_total,
        "amount_letter_conflicts": letter_conflicts_total,
        "amount_letter_issues": letter_issues,
        "amounts_unresolved": unresolved_amounts,
        "detector": detector_summary,
        "calls": calls,
    }

    logger.info(
        "ptr_vision: %s -> %d rows (clear=%d partial=%d illegible=%d) noTransactions=%s "
        "agreement=%s chunks=%d confidence=%.2f needsReview=%s cost=$%.4f (estimate $%.2f) usage=%s",
        pdf_path.name,
        len(transactions),
        counts["clear"],
        counts["partial"],
        counts["illegible"],
        no_transactions,
        {k: read_agreement.get(k) for k in ("rowsA", "rowsB", "matched")},
        len(chunk_records),
        confidence,
        needs_review,
        result["cost_usd"],
        estimate,
        result["usage"],
    )
    return result


# -- Reconciling a stored transcription -------------------------------------
# Nothing below calls a model or spends anything. The two reads are gone by
# the time a filing is on the stub, but the checkbox detector is a local numpy
# pass over the rendered page, so the one thing the reads could not settle --
# which amount box is ticked -- can still be settled later for free.

#: Fields of a merged row that are worth keeping on the stub.
TRANSCRIPTION_FIELDS: tuple[str, ...] = tuple(TRANSACTION_ITEM_SCHEMA["required"]) + (
    "line_number",
    "detectorLetter",
    "detectorStatus",
)

#: Merged rows stored verbatim on ``visionParse.transcription``. Generous: the
#: longest filing measured (Khanna 8221322) carries 593 of them, and a
#: transcription that does not survive whole cannot be reconciled later.
MAX_TRANSCRIPTION_ROWS = 2000


def stored_transcription(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The merged rows as they are kept on the stub for a later reconcile."""

    return [
        {field: row.get(field) for field in TRANSCRIPTION_FIELDS if field in row}
        for row in rows[:MAX_TRANSCRIPTION_ROWS]
    ]


def parse_row_summary(line: str) -> dict[str, Any] | None:
    """Read one :func:`row_summary` line back into a row.

    A compatibility shim, and only that. Filings transcribed before
    ``visionParse.transcription`` existed kept nothing but these one-line
    summaries, so this is the only way to reconcile them without paying for a
    second read. Returns None for a line it cannot parse, including the
    ``"... N more row(s)"`` marker that says the list was truncated.
    """

    parts = [part.strip() for part in str(line or "").split(" | ")]
    if not parts or parts[0].startswith("..."):
        return None
    page: int | None = None
    if parts[0].startswith("p") and parts[0][1:].isdigit():
        page = int(parts[0][1:])
        parts = parts[1:]
    if len(parts) < 7:
        return None
    description, type_text, transaction, notification, amount, owner, legibility = parts[:7]
    letter: str | None = None
    if amount.endswith(")") and " (" in amount:
        amount, tail = amount.rsplit(" (", 1)
        letter = tail[:-1] or None
    low, _, high = amount.partition("-")

    def _number(text: str) -> int | None:
        text = text.strip()
        return int(text) if text.lstrip("-").isdigit() else None

    row: dict[str, Any] = {
        "page_number": page,
        "asset_description": description,
        "owner": None if owner == "-" else owner,
        "ticker": None,
        "asset_type_code": None,
        "transaction_type": None if type_text == "?" else type_text,
        "transaction_date": None if transaction == "?" else transaction,
        "notification_date": None if notification in {"-", "?"} else notification,
        "amount_min": _number(low),
        "amount_max": _number(high),
        "amount_column_letter": letter if letter in AMOUNT_LETTERS else None,
        "cap_gains_over_200": None,
        "comment": None,
        "legibility": legibility if legibility in _LEGIBILITY_RANK else "partial",
    }
    for part in parts[7:]:
        if part.startswith("det:"):
            found, _, status = part[4:].partition("/")
            row["detectorLetter"] = found if found in AMOUNT_LETTERS else None
            row["detectorStatus"] = status or "unchecked"
    return row


def transcription_from_metadata(vision: dict[str, Any]) -> tuple[list[dict[str, Any]], bool]:
    """The merged rows of a stored ``visionParse``, and whether they are all there.

    Prefers ``transcription`` (kept verbatim). Falls back to re-reading the
    ``rows`` summaries for filings read before that was stored, where the list
    is capped at :data:`MAX_ROW_SUMMARIES_IN_METADATA` -- a filing longer than
    that cannot be reconciled whole, and the caller is told so rather than
    being handed a silently short list.
    """

    rows = vision.get("transcription")
    if isinstance(rows, list) and rows:
        complete = bool(vision.get("transcriptionComplete", True))
        return [dict(row) for row in rows if isinstance(row, dict)], complete
    summaries = vision.get("rows")
    if not isinstance(summaries, list) or not summaries:
        return [], False
    complete = not any(str(line).startswith("...") for line in summaries)
    parsed = [parse_row_summary(str(line)) for line in summaries]
    return [row for row in parsed if row is not None], complete


def detector_totals(page_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    """The ``detector`` block of a vision report, from its per-page summaries."""

    return {
        "pages": page_summaries,
        "rowsAligned": sum(int(page["rowsAligned"]) for page in page_summaries),
        "agreed": sum(int(page["agreed"]) for page in page_summaries),
        "disagreed": sum(int(page["disagreed"]) for page in page_summaries),
        "ambiguous": sum(int(page["ambiguous"]) for page in page_summaries),
        "resolved": sum(int(page.get("resolved") or 0) for page in page_summaries),
        "unalignedPages": [
            int(page["page"])
            for page in page_summaries
            if page["status"] == "unaligned" and page["rows"]
        ],
    }


def detector_review_reasons(detector: dict[str, Any]) -> list[str]:
    """The review reasons the checkbox detector is responsible for."""

    reasons: list[str] = []
    if detector["disagreed"]:
        reasons.append(f"checkbox detector disagreed on {detector['disagreed']} row(s)")
    if detector["ambiguous"]:
        reasons.append(f"checkbox detector ambiguous on {detector['ambiguous']} row(s)")
    if detector["unalignedPages"]:
        reasons.append(
            "checkbox detector could not align rows on page(s) "
            + ", ".join(str(page) for page in detector["unalignedPages"])
        )
    return reasons


def reconcile_stored_transcription(
    pdf_path: Path,
    rows: list[dict[str, Any]],
    *,
    skip_pages: frozenset[int] = frozenset(),
) -> tuple[list[dict[str, Any]], dict[str, Any]] | None:
    """Re-run the checkbox detector over rows transcribed in an earlier run.

    Free and offline: the pages are rendered locally, the orientation is
    decided by the same numpy pass that decided it during the read, and
    :mod:`~capitol_pipeline.parsers.ptr_grid` reads the ticked amount boxes off
    the pixels. No model is called on any path, including orientation.

    A row that already carries an amount is left exactly as it is. A row that
    lost its amount to two disagreeing reads takes the detector's letter where
    the detector is confident, and keeps nothing where it is not. ``skip_pages``
    names pages whose row list is known to be incomplete, where a band could
    line up against the wrong row; they are left untouched. Returns
    ``(rows, detector)`` or None when the PDF cannot be rendered.
    """

    prepared = prepare_page_images(pdf_path, None, orientation_mode="grid")
    if prepared is None:
        return None
    pages = [page for page in prepared[0] if int(page["index"]) not in skip_pages]
    rows = [dict(row) for row in rows]
    for row in rows:
        # The detector's earlier verdict must not be mistaken for this one.
        row.pop("detectorLetter", None)
        row.pop("detectorStatus", None)
    rows, page_summaries = apply_checkbox_detector(rows, pages)
    return rows, detector_totals(page_summaries)
