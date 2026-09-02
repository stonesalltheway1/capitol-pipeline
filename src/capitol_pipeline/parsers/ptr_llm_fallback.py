"""Claude Haiku 4.5 structured-extraction fallback for House PTR PDFs.

Invoked by ``parse_house_ptr_pdf`` when the regex parser yields zero rows
despite the PDF containing text. Uses the Anthropic Messages API with tool-use
to force a strict JSON schema, PDF document blocks to pass the file directly,
and prompt caching on the system prompt.

Kill switch: set ``PTR_LLM_FALLBACK_ENABLED=false`` to disable at runtime.
"""

from __future__ import annotations

import base64
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

MODEL_ID = "claude-haiku-4-5"
MAX_PDF_BYTES = 25 * 1024 * 1024
MAX_OUTPUT_TOKENS = 8192

_EMPTY_USAGE: dict[str, int] = {"input": 0, "cached_input": 0, "output": 0}


TRANSACTIONS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "transactions": {
            "type": "array",
            "description": "Every transaction row visible on the PTR. Omit rows that are clearly summary lines or headers.",
            "items": {
                "type": "object",
                "properties": {
                    "ticker": {
                        "type": ["string", "null"],
                        "description": "Stock/fund ticker in parentheses, e.g. 'AAPL'. Null if not a publicly traded security.",
                    },
                    "asset_description": {
                        "type": "string",
                        "description": "Full description as printed: issuer name, asset type, maturity/coupon for bonds, etc.",
                    },
                    "transaction_type": {
                        "type": "string",
                        "enum": ["purchase", "sale", "exchange"],
                        "description": "P -> purchase, S -> sale (including partial), E -> exchange.",
                    },
                    "transaction_date": {
                        "type": "string",
                        "description": "ISO YYYY-MM-DD. If the form shows MM/DD/YYYY, convert.",
                    },
                    "notification_date": {
                        "type": ["string", "null"],
                        "description": "ISO YYYY-MM-DD or null.",
                    },
                    "amount_min": {
                        "type": "integer",
                        "description": "Lower bound of amount range in dollars. 0 only if truly unparseable.",
                    },
                    "amount_max": {
                        "type": "integer",
                        "description": "Upper bound of amount range in dollars. 0 only if truly unparseable.",
                    },
                    "owner": {
                        "type": "string",
                        "enum": ["self", "spouse", "joint", "dependent", "trust", "unknown"],
                    },
                    "asset_type": {
                        "type": "string",
                        "description": "Stock, Option, Mutual Fund, ETF, Government Security, Corporate Security, Cryptocurrency, Real Estate, Other.",
                    },
                },
                "required": [
                    "asset_description",
                    "transaction_type",
                    "transaction_date",
                    "amount_min",
                    "amount_max",
                    "owner",
                    "asset_type",
                ],
            },
        },
        "parser_notes": {
            "type": "string",
            "description": "Short commentary on any uncertainty. Empty string if extraction was straightforward.",
        },
    },
    "required": ["transactions"],
}


SYSTEM_PROMPT = """You extract transaction rows from U.S. House Periodic Transaction Reports (PTRs).

Output rules:
- Never invent transactions. If you cannot read a row, skip it and note it in parser_notes.
- Owner codes: JT/Joint -> "joint"; SP/Spouse/"Spouse/DC" -> "spouse"; DC/Dependent -> "dependent"; TR/Trust -> "trust"; blank/filer -> "self"; otherwise "unknown".
- Transaction codes: P -> "purchase"; S or S(partial) -> "sale"; E -> "exchange".
- Dates are ISO YYYY-MM-DD. Convert MM/DD/YYYY as printed.
- Ticker: only the bare symbol inside parentheses (e.g. "AAPL"). Null for muni bonds, private funds, real estate.
- asset_type: best guess from square-bracket code or context. [ST]=Stock, [OP]=Option, [MF]=Mutual Fund, [ETF]=ETF, [GS]=Government Security, [CS]=Corporate Security. Real estate -> "Real Estate". Crypto -> "Cryptocurrency". Otherwise "Other".

Amount range -> (amount_min, amount_max) in whole dollars:
  $1,001 - $15,000          -> (1001, 15000)
  $15,001 - $50,000         -> (15001, 50000)
  $50,001 - $100,000        -> (50001, 100000)
  $100,001 - $250,000       -> (100001, 250000)
  $250,001 - $500,000       -> (250001, 500000)
  $500,001 - $1,000,000     -> (500001, 1000000)
  $1,000,001 - $5,000,000   -> (1000001, 5000000)
  $5,000,001 - $25,000,000  -> (5000001, 25000000)
  $25,000,001 - $50,000,000 -> (25000001, 50000000)
  Over $50,000,000          -> (50000001, 100000000)
Only use (0, 0) if the amount is truly unreadable; record that in parser_notes.

Return every transaction via the record_transactions tool. Do not output prose."""


_client: Any = None


def _client_once() -> Any:
    global _client
    if _client is None:
        import anthropic

        _client = anthropic.Anthropic()
    return _client


def _disabled() -> bool:
    return os.environ.get("PTR_LLM_FALLBACK_ENABLED", "true").lower() == "false"


def _disabled_result(reason: str) -> dict[str, Any]:
    return {
        "transactions": [],
        "parser_notes": reason,
        "usage": dict(_EMPTY_USAGE),
        "confidence": 0.0,
    }


def _estimate_confidence(transactions: list[dict[str, Any]], parser_notes: str) -> float:
    if not transactions:
        return 0.0
    notes = (parser_notes or "").strip().lower()
    if not notes:
        return 1.0
    wholesale = any(
        phrase in notes
        for phrase in (
            "could not read",
            "illegible",
            "unable to extract",
            "too garbled",
            "handwriting unreadable",
            "mostly unreadable",
            "wholesale",
        )
    )
    if wholesale:
        return 0.4
    return 0.7


def extract_via_haiku(pdf_path: Path) -> dict[str, Any]:
    """Extract PTR transactions from a PDF via Claude Haiku 4.5 with tool-use."""

    if _disabled():
        logger.warning("ptr_llm_fallback: disabled by PTR_LLM_FALLBACK_ENABLED=false")
        return _disabled_result("disabled by env")

    if not os.environ.get("ANTHROPIC_API_KEY"):
        logger.warning("ptr_llm_fallback: ANTHROPIC_API_KEY not set; skipping")
        return _disabled_result("ANTHROPIC_API_KEY not set")

    if not pdf_path.exists():
        logger.warning("ptr_llm_fallback: pdf not found at %s", pdf_path)
        return _disabled_result(f"pdf not found: {pdf_path}")

    pdf_bytes = pdf_path.read_bytes()
    if len(pdf_bytes) > MAX_PDF_BYTES:
        logger.warning(
            "ptr_llm_fallback: pdf %s is %d bytes (> %d cap); refusing",
            pdf_path.name,
            len(pdf_bytes),
            MAX_PDF_BYTES,
        )
        return _disabled_result(f"pdf too large: {len(pdf_bytes)} bytes")

    pdf_b64 = base64.standard_b64encode(pdf_bytes).decode("ascii")

    try:
        import anthropic  # noqa: F401

        response = _client_once().messages.create(
            model=MODEL_ID,
            max_tokens=MAX_OUTPUT_TOKENS,
            system=[
                {
                    "type": "text",
                    "text": SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            tools=[
                {
                    "name": "record_transactions",
                    "description": "Record every transaction row extracted from the PTR PDF.",
                    "input_schema": TRANSACTIONS_SCHEMA,
                }
            ],
            tool_choice={"type": "tool", "name": "record_transactions"},
            messages=[
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
                        {
                            "type": "text",
                            "text": (
                                f"Extract every transaction row from {pdf_path.name}. "
                                "Call record_transactions exactly once with the full list."
                            ),
                        },
                    ],
                }
            ],
        )
    except Exception as exc:
        logger.exception("ptr_llm_fallback: API call failed for %s: %s", pdf_path.name, exc)
        return _disabled_result(f"api error: {exc}")

    tool_use = next((block for block in response.content if getattr(block, "type", None) == "tool_use"), None)
    if tool_use is None:
        logger.warning("ptr_llm_fallback: no tool_use block in response for %s", pdf_path.name)
        return _disabled_result("no tool_use block")

    payload = tool_use.input if isinstance(tool_use.input, dict) else {}
    transactions = payload.get("transactions") or []
    parser_notes = payload.get("parser_notes") or ""

    usage = getattr(response, "usage", None)
    usage_dict = {
        "input": int(getattr(usage, "input_tokens", 0) or 0),
        "cached_input": int(getattr(usage, "cache_read_input_tokens", 0) or 0),
        "output": int(getattr(usage, "output_tokens", 0) or 0),
    }

    return {
        "transactions": transactions,
        "parser_notes": parser_notes,
        "usage": usage_dict,
        "confidence": _estimate_confidence(transactions, parser_notes),
    }
