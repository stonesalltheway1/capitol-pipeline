"""House PTR parsing for Capitol Pipeline."""

from __future__ import annotations

from datetime import date
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
from capitol_pipeline.processors.ocr import OcrProcessor

AMOUNT_RANGES: list[tuple[re.Pattern[str], int, int]] = [
    (re.compile(r"\$1,001\s*-\s*\$15,000", re.I), 1001, 15000),
    (re.compile(r"\$15,001\s*-\s*\$50,000", re.I), 15001, 50000),
    (re.compile(r"\$50,001\s*-\s*\$100,000", re.I), 50001, 100000),
    (re.compile(r"\$100,001\s*-\s*\$250,000", re.I), 100001, 250000),
    (re.compile(r"\$250,001\s*-\s*\$500,000", re.I), 250001, 500000),
    (re.compile(r"\$500,001\s*-\s*\$1,000,000", re.I), 500001, 1000000),
    (re.compile(r"\$1,000,001\s*-\s*\$5,000,000", re.I), 1000001, 5000000),
    (re.compile(r"\$5,000,001\s*-\s*\$25,000,000", re.I), 5000001, 25000000),
    (re.compile(r"\$25,000,001\s*-\s*\$50,000,000", re.I), 25000001, 50000000),
    (re.compile(r"Over\s*\$50,000,000", re.I), 50000001, 100000000),
]

TRANSACTION_PATTERN = re.compile(
    r"((?:[A-Z]{1,3}\s+)?[^[]{6,240}?)(?:\s*\(([A-Z.]{1,6})\))?"
    r"\s*\[([A-Z]{2,4})\]\s*(P|S(?:\s*\(partial\))?|E)\s*"
    r"(\d{1,2}/\d{1,2}/\d{4})\s*(\d{1,2}/\d{1,2}/\d{4})\s*"
    r"(?:(Spouse/DC|JT|DC|SP|TR|XX)\s+)?"
    r"(\$[\d,]+\s*-\s*\$[\d,]+|Over\s*\$[\d,]+)(?:\s+[A-Z])?",
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


def infer_asset_type(raw: str) -> str:
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
                    f"{round(parsed.parser_confidence * 100)}% confidence"
                ),
                parser_confidence=parsed.parser_confidence,
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
        raw_text_preview=normalized[:1200],
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
    return parse_house_ptr_text(text, stub)
