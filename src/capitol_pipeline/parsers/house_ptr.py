"""House PTR parsing for Capitol Pipeline."""

from __future__ import annotations

from datetime import date
import logging
import re
from pathlib import Path

from capitol_pipeline.config import OcrBackend, Settings
from capitol_pipeline.models.congress import (
    FilingStub,
    HousePtrParseResult,
    HousePtrTransaction,
    MemberMatch,
    NormalizedTradeRow,
)
from capitol_pipeline.normalizers.crypto_assets import classify_crypto_asset
from capitol_pipeline.parsers.ptr_llm_fallback import extract_via_haiku
from capitol_pipeline.processors.ocr import OcrProcessor

logger = logging.getLogger(__name__)

REGEX_PARSER_VERSION = "regex-v1"
LLM_PARSER_VERSION = "haiku-4.5-fallback-v1"

_LLM_OWNER_MAP: dict[str, str] = {
    "self": "self",
    "spouse": "spouse",
    "joint": "joint",
    "dependent": "child",
    "trust": "self",
    "unknown": "self",
}

def _range_pattern(low: str, high: str) -> re.Pattern[str]:
    return re.compile(
        rf"\$\s*{re.escape(low).replace(',', ',?')}\s*[-–—]\s*\$\s*{re.escape(high).replace(',', ',?')}",
        re.I,
    )


AMOUNT_RANGES: list[tuple[re.Pattern[str], int, int]] = [
    (_range_pattern("1,001", "15,000"), 1001, 15000),
    (_range_pattern("1,000", "15,000"), 1000, 15000),
    (_range_pattern("15,001", "50,000"), 15001, 50000),
    (_range_pattern("50,001", "100,000"), 50001, 100000),
    (_range_pattern("100,001", "250,000"), 100001, 250000),
    (_range_pattern("250,001", "500,000"), 250001, 500000),
    (_range_pattern("500,001", "1,000,000"), 500001, 1000000),
    (_range_pattern("1,000,001", "5,000,000"), 1000001, 5000000),
    (_range_pattern("5,000,001", "25,000,000"), 5000001, 25000000),
    (_range_pattern("25,000,001", "50,000,000"), 25000001, 50000000),
    (re.compile(r"(?:over|>)\s*\$\s*50,?000,?000", re.I), 50000001, 100000000),
]

TRANSACTION_PATTERN = re.compile(
    r"((?:[A-Z]{1,3}\s+)?[^[]{6,240}?)(?:\s*\(([A-Z.]{1,6})\))?"
    r"\s*(?:\[([A-Z]{2,4})\]\s*)?(P|S(?:\s*\(partial\))?|E)\s+"
    r"(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}/\d{1,2}/\d{4})\s+"
    r"(?:(Spouse/DC|JT|DC|SP|TR|XX)\s+)?"
    r"(\$[\d,]+\s*[-–—]\s*\$[\d,]+|(?:Over|>)\s*\$[\d,]+)(?:\s+[A-Z])?",
    re.I,
)


def normalize_text(raw: str) -> str:
    return (
        raw.replace("\x00", "")
        .replace("\u2019", "'")
        .replace("\u00a0", " ")
        .replace("\n", " ")
        .replace("\r", " ")
        .replace("\t", " ")
        .replace("•", " ")
    ).replace(" )", ")").replace("( ", "(").strip()


def squeeze_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def normalize_date(raw: str | None) -> str | None:
    if not raw or raw in {"--", "N/A"}:
        return None
    raw = raw.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        return raw
    match = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw)
    if not match:
        return None
    month, day, year = match.groups()
    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"


def parse_amount_range(raw: str) -> tuple[int, int]:
    for pattern, minimum, maximum in AMOUNT_RANGES:
        if pattern.search(raw):
            return minimum, maximum
    return 0, 0


def parse_transaction_type(raw: str) -> str:
    normalized = raw.strip().upper()
    if normalized.startswith("S"):
        return "sale"
    if normalized.startswith("E"):
        return "exchange"
    return "purchase"


def infer_asset_type(raw: str | None) -> str:
    if not raw:
        return "Asset"
    normalized = raw.strip().upper()
    if normalized == "ST":
        return "Stock"
    if normalized == "OP":
        return "Option"
    if normalized == "MF":
        return "Mutual Fund"
    if normalized == "ETF":
        return "ETF"
    if normalized == "GS":
        return "Government Security"
    if normalized == "CS":
        return "Corporate Security"
    return "Asset"


def clean_asset_description(raw: str, ticker: str | None) -> str:
    cleaned = squeeze_spaces(
        re.sub(
            r"\* For the complete list of asset type abbreviations.*$",
            "",
            raw,
            flags=re.I,
        )
    )
    cleaned = re.sub(r"^.*?\b[A-Z][a-z]+ Schwab \d+\s*", "", cleaned)
    cleaned = re.sub(r"^.*?\b\d{3,}\s+", "", cleaned)
    cleaned = re.sub(
        r"^.*?\bOwner Asset Transaction Type Date Notification Date Amount\s*",
        "",
        cleaned,
        flags=re.I,
    )
    cleaned = re.sub(
        r"^.*?\bName:\s+.*?\bState/District:\s+[A-Z]{2}\d*\s*",
        "",
        cleaned,
        flags=re.I,
    )
    cleaned = re.sub(r"^\s*(?:Spouse/DC|JT|DC|SP|TR|XX|[A-Z])\s+", "", cleaned, flags=re.I)

    structured_descriptor = re.match(r"^(?:[A-Z]\s+)?S:\s+New\s+S\s+O:\s+.+?\bD:\s+(.+)$", cleaned, flags=re.I)
    if structured_descriptor:
        cleaned = structured_descriptor.group(1).strip()
    else:
        descriptor_after_d = re.match(r"^.*?(?:Trust\s+)?D:\s+(.+)$", cleaned, flags=re.I)
        if descriptor_after_d and re.search(r"(?:Trust|S O:|Investment Fund|Capital call)", cleaned, flags=re.I):
            cleaned = descriptor_after_d.group(1).strip()

    cleaned = re.sub(r"^(?:F\s+)?S:\s+New\s+", "", cleaned, flags=re.I)
    cleaned = re.sub(r"^S\s+O:\s+", "", cleaned, flags=re.I)
    cleaned = re.sub(r"^Trust\s+", "", cleaned, flags=re.I)
    cleaned = re.sub(r"^Capital Call\b", "Capital call", cleaned, flags=re.I)
    cleaned = re.sub(r"^\s*(?:Spouse/DC|JT|DC|SP|TR|XX)\s+", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\s+\*\s*$", "", cleaned)
    cleaned = squeeze_spaces(cleaned)

    company_with_context = re.match(
        r"^(.+?,\s+[A-Za-z .'-]+,\s+[A-Z]{2})\s+([A-Z][A-Za-z0-9 .,&'/-]+)$",
        cleaned,
        flags=re.I,
    )
    if company_with_context:
        cleaned = (
            f"{company_with_context.group(2).strip()} "
            f"({company_with_context.group(1).strip()})"
        )

    cleaned = re.sub(r"^\s*(?:Spouse/DC|JT|DC|SP|TR|XX)\s+", "", cleaned, flags=re.I)
    owner_marker = re.search(r"\b(JT|DC|SP|TR|Spouse/DC)\s+", cleaned, flags=re.I)
    if owner_marker and owner_marker.start() > 0:
        cleaned = cleaned[owner_marker.end():].strip()

    if ticker and cleaned.endswith(f"({ticker})"):
        cleaned = cleaned[: -(len(ticker) + 2)].strip()

    return cleaned or ticker or "Pending House PTR extraction"


def parse_owner(asset_prefix: str, owner_hint: str | None = None) -> str:
    normalized = f"{asset_prefix} {owner_hint or ''}".upper()
    if re.search(r"\bSPOUSE/DC\b", normalized) or re.search(r"\bSP\b", normalized):
        return "spouse"
    if re.search(r"\bJT\b", normalized):
        return "joint"
    if re.search(r"\bDC\b", normalized):
        return "child"
    return "self"


def dedupe_transactions(
    stub: FilingStub,
    transactions: list[HousePtrTransaction],
) -> list[HousePtrTransaction]:
    seen: set[tuple[str, ...]] = set()
    unique: list[HousePtrTransaction] = []
    for transaction in transactions:
        canonical_asset_key = (
            (transaction.ticker or "").strip().upper()
            or squeeze_spaces(transaction.asset_description).lower()
        )
        key = (
            stub.doc_id,
            (stub.source_url or "").strip().lower(),
            canonical_asset_key,
            transaction.transaction_type,
            transaction.transaction_date or "",
            transaction.notification_date or "",
            str(transaction.amount_min),
            str(transaction.amount_max),
            transaction.owner,
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(transaction)
    return unique


def get_transaction_date_issue(
    transaction_date: str | None,
    filing_date: str | None,
) -> str | None:
    if not transaction_date:
        return "missing"
    try:
        parsed_transaction = date.fromisoformat(transaction_date)
    except ValueError:
        return "invalid"
    if filing_date:
        try:
            parsed_filing = date.fromisoformat(filing_date)
        except ValueError:
            parsed_filing = None
        if parsed_filing and parsed_transaction > parsed_filing:
            return "after_filing"
    if parsed_transaction > date.today():
        return "future"
    return None


def parse_header_name(text: str) -> str | None:
    match = re.search(r"Name:\s+([^:]+?)\s+Status:", text, flags=re.I)
    if not match:
        return None
    return squeeze_spaces(re.sub(r"^Hon\.\s*", "", match.group(1), flags=re.I))


def parse_header_state(text: str) -> str | None:
    match = re.search(r"State/District:\s+([A-Z]{2})\d*", text, flags=re.I)
    return match.group(1).upper() if match else None


def parse_transactions(text: str) -> list[HousePtrTransaction]:
    transactions: list[HousePtrTransaction] = []
    normalized = squeeze_spaces(normalize_text(text))
    normalized = re.sub(r"^.*?\bCap\.\s*Gains\s*>\s*\$200\?\s*", "", normalized, flags=re.I)
    for index, match in enumerate(TRANSACTION_PATTERN.finditer(normalized), start=1):
        asset_prefix = match.group(1).strip()
        ticker = match.group(2) or None
        asset_type = infer_asset_type(match.group(3))
        transaction_type = parse_transaction_type(match.group(4))
        transaction_date = normalize_date(match.group(5))
        notification_date = normalize_date(match.group(6))
        owner_hint = match.group(7) or None
        amount_min, amount_max = parse_amount_range(match.group(8))
        transactions.append(
            HousePtrTransaction(
                line_number=index,
                asset_description=clean_asset_description(asset_prefix, ticker),
                ticker=ticker,
                asset_type=asset_type,
                transaction_type=transaction_type,  # type: ignore[arg-type]
                transaction_date=transaction_date,
                notification_date=notification_date,
                amount_min=amount_min,
                amount_max=amount_max,
                owner=parse_owner(asset_prefix, owner_hint),  # type: ignore[arg-type]
            )
        )
    return transactions


def score_confidence(
    transactions: list[HousePtrTransaction],
    member: MemberMatch | None,
) -> float:
    if not transactions:
        return 0.0
    score = 0.35
    if member and member.name:
        score += 0.25
    dated = sum(1 for transaction in transactions if transaction.transaction_date)
    with_amount = sum(1 for transaction in transactions if transaction.amount_max > 0)
    with_ticker = sum(1 for transaction in transactions if transaction.ticker)
    score += min(0.2, dated * 0.05)
    score += min(0.15, with_amount * 0.04)
    score += min(0.1, with_ticker * 0.03)
    return max(0.0, min(0.95, round(score, 2)))


def build_trade_rows_from_house_ptr(
    parsed: HousePtrParseResult,
    stub: FilingStub,
) -> list[NormalizedTradeRow]:
    rows: list[NormalizedTradeRow] = []
    for transaction in parsed.transactions:
        normalized_asset = classify_crypto_asset(transaction.ticker, transaction.asset_description)
        asset_type = transaction.asset_type
        if normalized_asset.kind == "direct_crypto":
            asset_type = "Cryptocurrency"
        elif normalized_asset.kind == "crypto_etf":
            asset_type = "Crypto ETF"
        elif normalized_asset.kind == "crypto_equity":
            asset_type = "Crypto-Adjacent Equity"

        rows.append(
            NormalizedTradeRow(
                member=stub.member,
                source="house-clerk",
                disclosure_kind="house-ptr",
                source_id=f"{stub.doc_id}:{transaction.line_number}",
                source_url=stub.source_url,
                ticker=transaction.ticker,
                asset_description=transaction.asset_description,
                asset_type=asset_type,
                transaction_type=transaction.transaction_type,
                transaction_date=transaction.transaction_date,
                disclosure_date=stub.filing_date,
                amount_min=transaction.amount_min,
                amount_max=transaction.amount_max,
                owner=transaction.owner,
                comment=(
                    f"Parsed from House PTR {stub.doc_id} at "
                    f"{round(parsed.parser_confidence * 100)}% confidence "
                    f"[{parsed.parser_version}]"
                ),
                parser_confidence=parsed.parser_confidence,
                parser_version=parsed.parser_version,
                normalized_asset=normalized_asset,
            )
        )
    return rows


def parse_house_ptr_text(
    text: str,
    stub: FilingStub,
) -> tuple[HousePtrParseResult, list[NormalizedTradeRow]]:
    normalized = squeeze_spaces(normalize_text(text))
    transactions = dedupe_transactions(stub, parse_transactions(normalized))
    valid_transactions = [
        transaction
        for transaction in transactions
        if get_transaction_date_issue(transaction.transaction_date, stub.filing_date) is None
    ]
    member_name = parse_header_name(normalized) or stub.member.name
    state = parse_header_state(normalized) or stub.member.state
    parsed = HousePtrParseResult(
        doc_id=stub.doc_id,
        member_name=member_name,
        state=state,
        parser_confidence=score_confidence(valid_transactions, stub.member),
        parser_version=REGEX_PARSER_VERSION,
        raw_text_preview=normalized[:1200],
        transactions=valid_transactions,
    )
    return parsed, build_trade_rows_from_house_ptr(parsed, stub)


def _llm_amount_bounds(raw_min: object, raw_max: object) -> tuple[int, int]:
    try:
        amount_min = int(raw_min or 0)
    except (TypeError, ValueError):
        amount_min = 0
    try:
        amount_max = int(raw_max or 0)
    except (TypeError, ValueError):
        amount_max = 0
    if amount_min < 0:
        amount_min = 0
    if amount_max < 0:
        amount_max = 0
    if amount_min > amount_max and amount_max > 0:
        amount_min, amount_max = amount_max, amount_min
    return amount_min, amount_max


def _llm_transaction_type(raw: object) -> str:
    value = str(raw or "").strip().lower()
    if value in {"sale", "s"}:
        return "sale"
    if value in {"exchange", "e"}:
        return "exchange"
    return "purchase"


def _llm_owner(raw: object) -> str:
    value = str(raw or "").strip().lower()
    return _LLM_OWNER_MAP.get(value, "self")


def _llm_to_transactions(payload: list[dict]) -> list[HousePtrTransaction]:
    out: list[HousePtrTransaction] = []
    for index, raw_row in enumerate(payload, start=1):
        if not isinstance(raw_row, dict):
            continue
        description = (raw_row.get("asset_description") or "").strip()
        if not description:
            continue
        amount_min, amount_max = _llm_amount_bounds(raw_row.get("amount_min"), raw_row.get("amount_max"))
        ticker = raw_row.get("ticker")
        ticker = ticker.strip().upper() if isinstance(ticker, str) and ticker.strip() else None
        out.append(
            HousePtrTransaction(
                line_number=index,
                asset_description=description,
                ticker=ticker,
                asset_type=(raw_row.get("asset_type") or "Asset").strip() or "Asset",
                transaction_type=_llm_transaction_type(raw_row.get("transaction_type")),  # type: ignore[arg-type]
                transaction_date=normalize_date(raw_row.get("transaction_date")),
                notification_date=normalize_date(raw_row.get("notification_date")),
                amount_min=amount_min,
                amount_max=amount_max,
                owner=_llm_owner(raw_row.get("owner")),  # type: ignore[arg-type]
            )
        )
    return out


def _run_llm_fallback(
    pdf_path: Path,
    stub: FilingStub,
    text_preview: str,
) -> tuple[HousePtrParseResult, list[NormalizedTradeRow]] | None:
    """Invoke Haiku 4.5 fallback; return (parsed, rows) or None if nothing extracted."""

    try:
        llm_result = extract_via_haiku(pdf_path)
    except Exception as exc:
        logger.exception("house_ptr fallback: extract_via_haiku raised: %s", exc)
        return None

    raw_transactions = llm_result.get("transactions") or []
    if not raw_transactions:
        logger.info(
            "house_ptr fallback: no rows recovered for %s (notes=%r usage=%s)",
            pdf_path.name,
            (llm_result.get("parser_notes") or "")[:120],
            llm_result.get("usage"),
        )
        return None

    transactions = _llm_to_transactions(raw_transactions)
    transactions = dedupe_transactions(stub, transactions)
    valid_transactions = [
        transaction
        for transaction in transactions
        if get_transaction_date_issue(transaction.transaction_date, stub.filing_date) is None
    ]
    if not valid_transactions:
        logger.info("house_ptr fallback: all LLM rows filtered as invalid for %s", pdf_path.name)
        return None

    confidence = float(llm_result.get("confidence") or 0.0)
    usage = llm_result.get("usage") or {}
    logger.info(
        "house_ptr fallback: Haiku recovered %d rows for %s "
        "(parser_version=%s parser_confidence=%.2f usage=input=%s cached=%s output=%s notes=%r)",
        len(valid_transactions),
        pdf_path.name,
        LLM_PARSER_VERSION,
        confidence,
        usage.get("input"),
        usage.get("cached_input"),
        usage.get("output"),
        (llm_result.get("parser_notes") or "")[:120],
    )

    parsed = HousePtrParseResult(
        doc_id=stub.doc_id,
        member_name=parse_header_name(text_preview) or stub.member.name,
        state=parse_header_state(text_preview) or stub.member.state,
        parser_confidence=confidence,
        parser_version=LLM_PARSER_VERSION,
        raw_text_preview=text_preview[:1200],
        transactions=valid_transactions,
    )
    return parsed, build_trade_rows_from_house_ptr(parsed, stub)


def parse_house_ptr_pdf(
    pdf_path: Path,
    stub: FilingStub,
    settings: Settings | None = None,
    backend: str | OcrBackend = OcrBackend.AUTO,
) -> tuple[HousePtrParseResult, list[NormalizedTradeRow]]:
    settings = settings or Settings()
    processor = OcrProcessor(settings, backend=backend)
    result = processor.process_file(pdf_path)
    text = result.document.ocrText if result.document and result.document.ocrText else ""
    parsed, rows = parse_house_ptr_text(text, stub)

    if parsed.transactions:
        return parsed, rows

    text_has_content = bool((text or "").strip())
    if not text_has_content:
        logger.debug(
            "house_ptr: no OCR text for %s; invoking LLM fallback unconditionally",
            pdf_path.name,
        )
    else:
        logger.debug(
            "house_ptr: regex found 0 rows despite %d chars of text for %s; invoking LLM fallback",
            len(text),
            pdf_path.name,
        )

    fallback = _run_llm_fallback(pdf_path, stub, normalize_text(text))
    if fallback is None:
        return parsed, rows
    return fallback
