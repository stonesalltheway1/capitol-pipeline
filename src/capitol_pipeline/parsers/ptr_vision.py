"""Claude vision extraction for scanned and handwritten House PTR PDFs.

Roughly 210 House periodic transaction reports sit in
``house_filing_stubs.status = 'needs_review'`` because they are photocopies or
handwritten forms. The OCR chain (``pymupdf`` -> ``surya`` -> ``docling``)
returns fragments like ``| 9 984 F 1 | Sale | 1 |``, the regex parser scores
0.0, and the filing never becomes trade rows. A vision-capable model reads
those pages directly.

This module sends the **PDF itself** to Claude as a ``document`` content block
and asks for the transaction grid back as schema-constrained JSON. It is a peer
of :mod:`capitol_pipeline.parsers.ptr_llm_fallback` (the Haiku text fallback,
still used when the OCR text layer is decent), not a replacement.

Guardrails
----------
* ``CAPITOL_PTR_VISION_DISABLED=1`` kills the path at runtime.
* PDFs over :data:`MAX_VISION_PDF_PAGES` pages or :data:`MAX_VISION_PDF_BYTES`
  bytes are skipped with a reason; the stub stays ``needs_review``.
* One filing per call, one retry on 429/5xx, and the caller caps filings per
  run with ``--limit``.

Nothing here touches the database. The caller records
``usage`` / ``cost_usd`` / ``reason`` into the stub's ``visionParse`` metadata.
"""

from __future__ import annotations

import re

import base64
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# -- Model + version --------------------------------------------------------

#: Default vision model. Override per-run with ``CAPITOL_PTR_VISION_MODEL``.
MODEL_ID = "claude-sonnet-5"

#: Recorded as ``parser_version`` on every row this path produces.
VISION_PARSER_VERSION = "claude-sonnet-5-vision-v1"

#: Used when the installed SDK rejects ``output_config`` and we fall back to
#: a single strict tool.
VISION_TOOL_NAME = "record_ptr_transactions"

# -- Guardrails -------------------------------------------------------------

MAX_VISION_PDF_PAGES = 25
MAX_VISION_PDF_BYTES = 20 * 1024 * 1024
MAX_OUTPUT_TOKENS = 16000
EFFORT = "medium"
RETRY_SLEEP_SECONDS = 5.0

# -- Pricing (Claude Sonnet 5: $2 / $10 per MTok) ---------------------------

PRICE_INPUT_PER_MTOK = 2.0
PRICE_OUTPUT_PER_MTOK = 10.0
CACHE_READ_MULTIPLIER = 0.1
CACHE_WRITE_MULTIPLIER = 1.25

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
# Kept stable and cached with ``cache_control: ephemeral``. Sonnet 5's minimum
# cacheable prefix is 1,024 tokens, so this is deliberately substantive: it has
# to clear that floor to cache at all, and every filing in the review queue
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


SYSTEM_PROMPT = """You are a careful transcriptionist for United States House of Representatives Periodic Transaction Reports (PTRs). You are given the pages of a single filing as a PDF. Most of the filings you will see are photocopies, faxes, phone photographs, or forms completed by hand, which is exactly why they reached you: automated text extraction already failed on them. Your job is to read the transaction grid off the page and return it as structured data, transcribing exactly what is written and never improving on it.

## The form

A House PTR is filed under the STOCK Act by a Member, officer, or employee of the House. It discloses purchases, sales, and exchanges of stocks, bonds, commodity futures, and other securities held by the filer, the filer's spouse, or the filer's dependent children, when the transaction exceeded $1,000. It is a transaction report, not a holdings report: each row is one event, not a position.

The header block, usually on page one, carries the filer's name (often prefixed "Hon."), their status (Member, Officer, Employee, Candidate), their state and district written together as a two letter state code plus a district number such as TX25 or MO02, the reporting period or calendar year, and a filing identification number typically written as "Filing ID #" followed by eight digits. There is frequently a signature and a date near the end of the document, and sometimes a separate "Date Received" stamp applied by the Legislative Resource Center. Handwritten filings often place the name on a ruled line rather than in a printed field.

Beneath the header is the transaction table. On the standard printed form the columns run left to right in this order:

1. **Owner** - who holds the asset.
2. **Asset** - the security description, sometimes with a ticker in parentheses and an asset-type code in square brackets.
3. **Transaction Type** - a single letter code.
4. **Date** - the date of the transaction itself.
5. **Notification Date** - the date the filer was notified of the transaction.
6. **Amount** - a checked or circled dollar range bucket.
7. **Cap. Gains > $200?** - a yes/no checkbox for capital gains over two hundred dollars.

Handwritten and older filings often collapse, reorder, or omit columns, and may run the amount bucket into the margin. Read the column headers on the page you are actually given rather than assuming this order, and map what you find onto the fields of the schema.

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
- **S (partial)** - a partial sale, sometimes written "S(partial)", "S - partial", or "SP" in the type column rather than the owner column. Report `sale_partial`.
- **E** - exchange. Report `exchange`.

Be careful with "SP": in the Owner column it means spouse, in the Type column it means a partial sale. Decide from which column the mark sits in, not from the letters alone.

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

Dates are printed on the form as MM/DD/YYYY. Convert every date to YYYY-MM-DD. Two-digit years belong to the reporting period; use the calendar year in the header to resolve them. The Transaction Date is when the trade happened; the Notification Date is when the filer learned of it, and for a self-directed account the two are often identical. The Notification Date is frequently blank on handwritten forms - report null, not a copy of the transaction date. If a date is smudged, cropped, or overwritten so that you cannot read it, report null and mark the row's legibility accordingly. A date you cannot read is never worth guessing: a wrong date silently corrupts the disclosure timeline that this data feeds.

## Cap. Gains > $200?

This is a single checkbox per row. A tick, an X, or a "Y" means true; an explicit "N" or "No" means false. A blank column, a column that does not exist on this form, or a mark you cannot resolve is null. Do not infer it from the amount bucket.

## Asset descriptions and subholdings

Transcribe the asset description as written. Keep issuer names, share-class wording such as "Common Stock" or "Class A", bond coupon rates and maturity dates, fund names, and municipal issuer names intact. Do not expand abbreviations, do not correct spelling, and do not normalize punctuation or capitalization.

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


def count_pdf_pages(pdf_path: Path) -> int | None:
    """Return the page count, or None when no PDF reader is importable."""

    for module_name in ("pymupdf", "fitz"):
        try:
            module = __import__(module_name)
            with module.open(str(pdf_path)) as document:
                return int(document.page_count)
        except Exception:  # pragma: no cover - depends on optional extras
            continue
    try:  # pragma: no cover - depends on optional extras
        from pypdf import PdfReader

        return len(PdfReader(str(pdf_path)).pages)
    except Exception:
        return None


def estimate_cost_usd(usage: dict[str, int]) -> float:
    """Estimate request cost in USD from a normalized usage dict."""

    dollars = (
        int(usage.get("input") or 0) * PRICE_INPUT_PER_MTOK
        + int(usage.get("cache_read") or 0) * PRICE_INPUT_PER_MTOK * CACHE_READ_MULTIPLIER
        + int(usage.get("cache_write") or 0) * PRICE_INPUT_PER_MTOK * CACHE_WRITE_MULTIPLIER
        + int(usage.get("output") or 0) * PRICE_OUTPUT_PER_MTOK
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


def _request_kwargs(
    pdf_b64: str,
    filename: str,
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
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "document",
                        "source": {
                            "type": "base64",
                            "media_type": "application/pdf",
                            "data": pdf_b64,
                        },
                    },
                    {"type": "text", "text": f"{USER_INSTRUCTION}\n\nFile: {filename}"},
                ],
            }
        ],
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
                "description": "Record every transaction row transcribed from the PTR PDF.",
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


def build_vision_metadata(report: dict[str, Any]) -> dict[str, Any]:
    """Compact a vision result for ``house_filing_stubs.metadata.visionParse``.

    The raw transcription is deliberately dropped: the normalized rows already
    land in ``metadata.parsedTransactions``, and this record only has to explain
    what the model was asked, what it cost, and why a filing did or did not
    leave the review queue.
    """

    usage = report.get("usage") or {}
    return {
        "model": report.get("model"),
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
        "usage": {
            "inputTokens": int(usage.get("input") or 0),
            "cacheReadTokens": int(usage.get("cache_read") or 0),
            "cacheWriteTokens": int(usage.get("cache_write") or 0),
            "outputTokens": int(usage.get("output") or 0),
        },
        "costUsd": report.get("cost_usd", 0.0),
        "pricing": {
            "inputPerMTok": PRICE_INPUT_PER_MTOK,
            "outputPerMTok": PRICE_OUTPUT_PER_MTOK,
            "cacheReadMultiplier": CACHE_READ_MULTIPLIER,
            "cacheWriteMultiplier": CACHE_WRITE_MULTIPLIER,
        },
    }


# -- Entry point ------------------------------------------------------------


def extract_via_vision(pdf_path: Path) -> dict[str, Any]:
    """Transcribe a House PTR PDF with a Claude vision model.

    Always returns a dict. ``ok`` is False and ``reason`` is set whenever the
    filing was skipped or the call failed; the caller then leaves the stub in
    ``needs_review`` and records ``reason`` in its metadata.
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

    # Base64 with no newlines, as the document content block requires.
    pdf_b64 = base64.standard_b64encode(pdf_bytes).decode("ascii")

    structured = True
    with_output_config = True
    downgraded = False
    attempts = 0
    message: Any = None
    last_error: Exception | None = None

    while attempts < 2:
        attempts += 1
        kwargs = _request_kwargs(
            pdf_b64,
            pdf_path.name,
            structured=structured,
            with_output_config=with_output_config,
        )
        try:
            message = _invoke(_client_once(), kwargs)
            break
        except TypeError as error:
            # The installed SDK does not accept output_config at all -> drop it
            # and use a single strict tool instead. Does not consume a retry.
            if not downgraded and "output_config" in str(error):
                logger.warning(
                    "ptr_vision: SDK rejected output_config; retrying with strict tool use"
                )
                structured = False
                with_output_config = False
                downgraded = True
                attempts -= 1
                continue
            last_error = error
            break
        except Exception as error:  # noqa: BLE001 - classified by _is_retryable
            last_error = error
            if not downgraded and _rejected_structured_output(error):
                # The API refused the json_schema format; fall back to the tool
                # form once before giving up. Does not consume a retry.
                logger.warning(
                    "ptr_vision: API rejected structured output (%s); retrying with strict tool use",
                    error,
                )
                structured = False
                downgraded = True
                attempts -= 1
                continue
            if attempts >= 2 or not _is_retryable(error):
                break
            logger.warning(
                "ptr_vision: retryable error on %s (%s); retrying once",
                pdf_path.name,
                type(error).__name__,
            )
            time.sleep(RETRY_SLEEP_SECONDS)

    if message is None:
        logger.error("ptr_vision: call failed for %s: %s", pdf_path.name, last_error)
        return _skip(f"api error: {last_error}", attempts=attempts)

    usage = _normalize_usage(getattr(message, "usage", None))
    cost = estimate_cost_usd(usage)
    stop_reason = getattr(message, "stop_reason", None)

    if stop_reason == "refusal":
        details = getattr(message, "stop_details", None)
        category = (
            details.get("category")
            if isinstance(details, dict)
            else getattr(details, "category", None)
        )
        return _skip(
            f"model refused (category={category})",
            usage=usage,
            cost_usd=cost,
            stop_reason=stop_reason,
            attempts=attempts,
        )
    if stop_reason == "max_tokens":
        return _skip(
            "response truncated at max_tokens",
            usage=usage,
            cost_usd=cost,
            stop_reason=stop_reason,
            attempts=attempts,
        )

    payload = _payload_from_message(message)
    if payload is None:
        return _skip(
            "no structured payload in response",
            usage=usage,
            cost_usd=cost,
            stop_reason=stop_reason,
            attempts=attempts,
        )

    transactions = _coerce_transactions(payload.get("transactions"))
    counts = summarize_legibility(transactions)
    confidence = legibility_confidence(counts)
    needs_review = majority_illegible(counts)

    reported_pages = payload.get("page_count")
    result: dict[str, Any] = {
        "ok": bool(transactions),
        "skipped": False,
        "reason": None if transactions else "model returned no transaction rows",
        "model": resolve_model_id(),
        "parser_version": VISION_PARSER_VERSION,
        "filer_name": _clean_optional_text(payload.get("filer_name")),
        "filing_date": _clean_optional_text(payload.get("filing_date")),
        "page_count": reported_pages if reported_pages is not None else page_count,
        "notes": _clean_optional_text(payload.get("notes")),
        "transactions": transactions,
        "legibility": counts,
        "confidence": confidence,
        "needs_review": needs_review,
        "usage": usage,
        "cost_usd": cost,
        "stop_reason": stop_reason,
        "attempts": attempts,
        "structuredOutput": structured,
    }

    logger.info(
        "ptr_vision: %s -> %d rows (clear=%d partial=%d illegible=%d) "
        "confidence=%.2f needsReview=%s cost=$%.4f usage=%s",
        pdf_path.name,
        len(transactions),
        counts["clear"],
        counts["partial"],
        counts["illegible"],
        confidence,
        needs_review,
        cost,
        usage,
    )
    return result
