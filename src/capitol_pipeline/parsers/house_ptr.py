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
from capitol_pipeline.parsers.ptr_vision import (
    VISION_PARSER_VERSION,
    build_vision_metadata,
    extract_via_vision,
    scrub_example_row_values,
)
from capitol_pipeline.processors.ocr import fix_font_mojibake, OcrProcessor

logger = logging.getLogger(__name__)

REGEX_PARSER_VERSION = "regex-v1"
LLM_PARSER_VERSION = "haiku-4.5-fallback-v1"

#: Vision parser selection. ``off`` never calls the model, ``auto`` calls it
#: only when the text path failed, ``claude`` always calls it for the filing.
VISION_BACKENDS: tuple[str, ...] = ("off", "auto", "claude")

#: Text-parser confidence below which ``auto`` hands the PDF to the vision model.
VISION_CONFIDENCE_FLOOR = 0.5

#: Below this much readable OCR text the text layer is treated as junk and the
#: Haiku text fallback is skipped in favour of reading the pages as images.
MIN_DECENT_OCR_CHARS = 400
MIN_DECENT_OCR_WORDS = 40
MIN_DECENT_OCR_ALPHA_RATIO = 0.55

_LLM_OWNER_MAP: dict[str, str] = {
    "self": "self",
    "spouse": "spouse",
    "joint": "joint",
    "dependent": "child",
    "trust": "self",
    "unknown": "self",
}

_VISION_OWNER_MAP: dict[str, str] = {
    "self": "self",
    "spouse": "spouse",
    "dependent": "child",
    "joint": "joint",
}

_VISION_TRANSACTION_TYPE_MAP: dict[str, str] = {
    "purchase": "purchase",
    "sale": "sale",
    "sale_partial": "sale",
    "exchange": "exchange",
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

#: The machine-readable tail of a House PTR row: optional ``(TICKER)``,
#: optional ``[ST]`` asset-type code, the P/S/E transaction code, the
#: transaction and notification dates, an optional owner code and the amount.
#: Everything before it on the row is the asset name; everything after it, up
#: to the next core, is that row's annotation block. Anchoring on the core
#: (instead of a bounded lazy prefix) is what keeps long per-row comments from
#: sliding into the next row's asset name.
_ROW_CORE_SOURCE = (
    r"(?:\(\s*(?P<ticker>[A-Z.]{1,6})\s*\)\s*)?"
    r"(?:\[\s*(?P<asset_type>[A-Z]{2,4})\s*\]\s*)?"
    r"(?<![A-Za-z0-9])(?P<tx_type>P|S(?:\s*\((?i:partial)\))?|E)\s+"
    r"(?P<date>\d{1,2}/\d{1,2}/\d{4})\s+(?P<notified>\d{1,2}/\d{1,2}/\d{4})\s+"
    r"(?:(?P<owner>Spouse/DC|JT|DC|SP|TR|XX)\s+)?"
    r"(?P<amount>\$\s*[\d,]+(?:\.\d+)?\s*[-–—]\s*\$\s*[\d,]+(?:\.\d+)?"
    r"|(?i:over|>)\s*\$\s*[\d,]+|\$\s*[\d,]+(?:\.\d+)?)"
)

ROW_CORE_PATTERN = re.compile(_ROW_CORE_SOURCE)

#: Legacy single-row pattern (asset prefix + core). ``parse_transactions`` no
#: longer uses it; it is kept for callers that match one already-isolated row.
TRANSACTION_PATTERN = re.compile(
    r"((?:[A-Z]{1,3}\s+)?[^[]{6,240}?)\s*" + _ROW_CORE_SOURCE,
    re.I,
)

#: Repeated per-page furniture on the House form. Stripped everywhere before
#: rows are segmented so a page break never glues a column heading, footer or
#: filer block onto an asset name.
_FIRST_COLUMN_HEADER_PATTERN = re.compile(
    r"^.*?\bCap\.\s*Gains\s*>\s*\$200\?[ \t]*", re.I | re.S
)
_COLUMN_HEADER_PATTERN = re.compile(
    r"(?:\bID\s+)?Owner\s+Asset\s+Transaction\s+Type\s+Date\s+Notification\s+Date\s+"
    r"Amount\s+Cap\.\s*Gains\s*>\s*\$200\?",
    re.I,
)
_CAP_GAINS_HEADER_PATTERN = re.compile(r"Cap\.\s*Gains\s*>\s*\$200\?", re.I)
_FILING_ID_FOOTER_PATTERN = re.compile(r"Filing ID\s*#\s*\d+", re.I)
_REPORT_TITLE_PATTERN = re.compile(
    r"(?:Periodic Transaction Report|\bP T R\b)"
    r"(?:\s*Clerk of the House of Representatives.*?Washington,\s*DC\s*20515)?",
    re.I | re.S,
)
_CLERK_LINE_PATTERN = re.compile(
    r"Clerk of the House of Representatives.*?Washington,\s*DC\s*20515", re.I | re.S
)
_FILER_BLOCK_PATTERN = re.compile(
    r"(?:Filer Information\s*)?(?:\bF\s+I\s+)?Name:\s.*?State/District:\s*[A-Z]{2}\s*\d*"
    r"(?:\s+Transactions\b)?",
    re.I | re.S,
)
_ASSET_TYPE_FOOTNOTE_PATTERN = re.compile(
    r"\*\s*For the complete list of asset type abbreviations[^\n]*", re.I
)
#: Sections that follow the transactions table. Nothing after the first of
#: them is a row, so the text is cut there.
_TRAILING_SECTIONS_PATTERN = re.compile(
    r"^[ \t]*(?:Investment Vehicle Details|Initial Public Offerings|Certification and Signature"
    r"|Asset class details)[ \t]*$.*\Z"
    r"|^[ \t]*(?:I CERTIFY that the statements|Digitally Signed:).*\Z",
    re.I | re.M | re.S,
)

#: Lines that open a per-row annotation on the House form (plus the legacy
#: text-layer spellings "F S:", "S O:", "D:" where the lowercase letters were
#: dropped).
_ANNOTATION_MARKER_PATTERN = re.compile(
    r"^(?:Filing\s+Status|Subholding\s+Of|Description|Comments?|Location|F\s+S|S\s+O|[DLCSO])\s*:",
    re.I,
)
#: A bare account number continuing a "Subholding Of:" line.
_ACCOUNT_NUMBER_LINE_PATTERN = re.compile(r"^[xX*#.-]*\d{3,}[xX*#.-]*$")
#: The tail of an asset name that pymupdf emitted after the row core (seen when
#: a row straddles a page break): ends with the ticker and/or type code.
_ASSET_TAIL_PATTERN = re.compile(
    r"^(?P<name>.*?)\s*(?:\(\s*(?P<ticker>[A-Z.]{1,6})\s*\))?\s*(?:\[\s*(?P<asset_type>[A-Z]{2,4})\s*\])?$"
)
_SENTENCE_END_PATTERN = re.compile(r"[.!?]['\")]?$")
_NAME_ABBREVIATIONS: frozenset[str] = frozenset(
    {
        "inc.", "corp.", "co.", "ltd.", "l.p.", "lp.", "llc.", "plc.", "n.v.", "s.a.", "a.g.",
        "jr.", "sr.", "st.", "mfg.", "intl.", "int'l.", "bros.", "hldgs.", "tr.", "sec.",
        "gen.", "oblig.", "ser.", "dtd.", "pct.", "cl.", "fd.", "adr.", "ag.", "sa.", "nv.",
    }
)

#: A wrapped body-text line on the House form runs well past this many
#: characters; asset-name lines wrap at roughly forty.
_LONG_LINE_CHARS = 80
#: A line this long that directly follows a long line is a wrapped
#: continuation, not an asset name.
_WIDE_CONTINUATION_CHARS = 50


def normalize_text(raw: str, *, keep_newlines: bool = False) -> str:
    """Normalise an extracted text layer.

    ``keep_newlines`` preserves line breaks (collapsing CR/LF variants) so the
    row segmenter can tell a wrapped comment line from an asset name; the
    default flattens everything to one line for header parsing and previews.
    """

    cleaned = (
        raw.replace("\x00", "")
        .replace("\u2019", "'")
        .replace("\u00a0", " ")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("\t", " ")
        .replace("•", " ")
    )
    if not keep_newlines:
        cleaned = cleaned.replace("\n", " ")
    return cleaned.replace(" )", ")").replace("( ", "(").strip()


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
    # Capital calls and similar rows carry an exact figure ("$647.63")
    # instead of a bracket; keep it as a degenerate range.
    exact = re.fullmatch(r"\s*\$\s*([\d,]+(?:\.\d+)?)\s*", raw)
    if exact:
        try:
            value = round(float(exact.group(1).replace(",", "")))
        except ValueError:
            return 0, 0
        return value, value
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
    raw = fix_font_mojibake(raw)
    cleaned = squeeze_spaces(
        re.sub(
            r"\* For the complete list of asset type abbreviations.*$",
            "",
            raw,
            flags=re.I,
        )
    )
    cleaned = re.sub(r"^.*?\b[A-Z][a-z]+ Schwab \d+\s*", "", cleaned)
    # A leading (possibly masked) account number left over from a
    # "Subholding Of:" line. Anchored so "iShares Russell 2000 ETF" survives.
    cleaned = re.sub(r"^[xX*#]*\d{3,}\s+", "", cleaned)
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
    cleaned = re.sub(r"^\s*(?:Spouse/DC|JT|DC|SP|TR|XX)\s+", "", cleaned, flags=re.I)
    # The legacy text layer prefixed rows with a lone "F" (from "Filing
    # Status"); a bare capital followed by a real word ("T MOBILE USA") is not that.
    cleaned = re.sub(r"^\s*F\s+(?=S:)", "", cleaned)

    structured_descriptor = re.match(r"^(?:[A-Z]\s+)?S:\s+New\s+S\s+O:\s+.+?\bD:\s+(.+)$", cleaned, flags=re.I)
    if structured_descriptor:
        cleaned = structured_descriptor.group(1).strip()
    else:
        descriptor_after_d = re.match(r"^.*?(?:Trust\s+)?D:\s+(.+)$", cleaned, flags=re.I)
        if descriptor_after_d and re.search(r"(?:Trust|S O:|Investment Fund|Capital call)", cleaned, flags=re.I):
            cleaned = descriptor_after_d.group(1).strip()

    full_word = re.match(r"^.*?(?:Description:)\s*(.+)$", cleaned, flags=re.I)
    if full_word and re.search(r"(?:Filing Status|Subholding Of|Location:)", cleaned, flags=re.I):
        cleaned = full_word.group(1).strip()
    else:
        subholding = re.search(r"Subholding Of:\s*(.+)$", cleaned, flags=re.I)
        if subholding:
            rest = subholding.group(1).strip()
            after_account = re.search(r"\(\d+\)\s*(\S.*)$", rest)
            cleaned = (after_account.group(1) if after_account else rest).strip()
    cleaned = re.sub(r"^\s*F?iling Status:\s*New\s*", "", cleaned, flags=re.I)
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
    # An owner code buried after annotation text ("S O: Fidelity Trust JT
    # Fremont IN ...") marks where the asset starts. Only when the text before
    # it is an annotation remnant: "Washington DC Water Bonds" keeps its DC.
    owner_marker = re.search(r"\b(JT|DC|SP|TR|Spouse/DC)\s+", cleaned, flags=re.I)
    if owner_marker and owner_marker.start() > 0 and ":" in cleaned[: owner_marker.start()]:
        cleaned = cleaned[owner_marker.end():].strip()

    if ticker and cleaned.endswith(f"({ticker})"):
        cleaned = cleaned[: -(len(ticker) + 2)].strip()

    return cleaned or ticker or "Pending House PTR extraction"


def parse_owner(asset_prefix: str, owner_hint: str | None = None) -> str:
    """Resolve the owner code printed in front of the asset or after the dates.

    Only a code at the very start of the asset text counts, so a name such as
    "Washington DC Water Bonds" is not read as a dependent-child holding.
    """

    leading = re.match(r"\s*(SPOUSE/DC|JT|DC|SP|TR|XX)(?=\s|$)", asset_prefix.upper())
    codes = [leading.group(1)] if leading else []
    if owner_hint:
        codes.append(owner_hint.strip().upper())
    for code in codes:
        if code in {"SPOUSE/DC", "SP"}:
            return "spouse"
        if code == "JT":
            return "joint"
        if code == "DC":
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
            # Two otherwise identical rows in different sub-accounts
            # ("Subholding Of: ... (5)" and "(6)") are two trades.
            squeeze_spaces(transaction.comment or "").lower(),
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


def strip_page_furniture(text: str) -> str:
    """Remove everything on a House PTR text layer that is not table content.

    Works on newline-preserving text. The first column heading takes the
    whole page-1 preamble with it; later page headings, "Filing ID" footers,
    the asset-type footnote and the sections after the table are removed
    wherever they occur so a page break never lands inside a row.
    """

    cleaned = fix_font_mojibake(text)
    cleaned = _TRAILING_SECTIONS_PATTERN.sub("", cleaned)
    # The asset-type footnote closes the table. Cut there when no row follows
    # it, so a bare "Comments" section heading cannot trail the last row.
    footnote = _ASSET_TYPE_FOOTNOTE_PATTERN.search(cleaned)
    if footnote and not ROW_CORE_PATTERN.search(cleaned, footnote.end()):
        cleaned = cleaned[: footnote.start()]
    if _CAP_GAINS_HEADER_PATTERN.search(cleaned):
        cleaned = _FIRST_COLUMN_HEADER_PATTERN.sub("", cleaned, count=1)
    else:
        cleaned = _REPORT_TITLE_PATTERN.sub("\n", cleaned)
        cleaned = _CLERK_LINE_PATTERN.sub("\n", cleaned)
        cleaned = _FILER_BLOCK_PATTERN.sub("\n", cleaned)
    cleaned = _COLUMN_HEADER_PATTERN.sub("\n", cleaned)
    cleaned = _CAP_GAINS_HEADER_PATTERN.sub("\n", cleaned)
    cleaned = _FILING_ID_FOOTER_PATTERN.sub("\n", cleaned)
    cleaned = _ASSET_TYPE_FOOTNOTE_PATTERN.sub("\n", cleaned)
    return cleaned


def _is_annotation_marker(line: str) -> bool:
    return bool(_ANNOTATION_MARKER_PATTERN.match(line))


def _ends_like_sentence(line: str) -> bool:
    """True for "direction." or "Morgan Stanley.", false for "Alexandria ..., Inc."."""

    if not _SENTENCE_END_PATTERN.search(line):
        return False
    last_word = line.split()[-1].lower().strip("'\")")
    return last_word not in _NAME_ABBREVIATIONS


def _is_annotation_line(line: str, previous: str | None) -> bool:
    """Classify one line of the text between two row cores.

    Annotation lines are the marker lines the House form prints under a row
    ("Filing Status:", "Subholding Of:", "Description:", "Comments:", ...),
    their wrapped continuations, and bare account numbers. Everything else is
    part of the next row's asset name.
    """

    if _is_annotation_marker(line):
        return True
    if _ACCOUNT_NUMBER_LINE_PATTERN.match(line):
        return True
    if len(line) >= _LONG_LINE_CHARS:
        return True
    if previous is not None and len(previous) >= _LONG_LINE_CHARS:
        # The line right after a full-width body line is its wrapped tail
        # unless it clearly starts something new. Lower-case starts
        # ("direction."), sentence endings and wide lines are tails.
        if line[0].islower() or len(line) >= _WIDE_CONTINUATION_CHARS:
            return True
        if _ends_like_sentence(line):
            return True
    return False


def split_row_segment(segment: str) -> tuple[list[str], list[str]]:
    """Split the text between two row cores into (annotation lines, asset lines).

    The asset lines are the trailing run of lines that do not read as
    annotation; the lines before them belong to the previous row. A segment
    without line breaks (flattened text) is treated as a single asset line and
    left to ``clean_asset_description`` to tidy, which is the old behaviour.
    """

    lines = [squeeze_spaces(line) for line in segment.split("\n")]
    lines = [line for line in lines if line]
    if not lines:
        return [], []
    split_at = len(lines)
    while split_at > 0:
        candidate = lines[split_at - 1]
        previous = lines[split_at - 2] if split_at >= 2 else None
        if _is_annotation_line(candidate, previous):
            break
        split_at -= 1
    if split_at == len(lines):
        # Every line looked like annotation; the last one is still the best
        # guess at the asset so the row is not lost.
        split_at = len(lines) - 1
    return lines[:split_at], lines[split_at:]


def _take_asset_tail(lines: list[str]) -> tuple[list[str], str, str | None, str | None]:
    """Pull a wrapped asset tail that pymupdf emitted after the row core.

    When a row straddles a page break the text layer can order it as
    ``Motorola Solutions, Inc. Common / P / dates / amount / Stock (MSI) [ST]``.
    A leading non-annotation line that ends in a ticker and/or type code is
    that tail. Returns (remaining lines, tail name, ticker, type code); the
    name is empty and both codes None when there is no tail.
    """

    if not lines or _is_annotation_line(lines[0], None):
        return lines, "", None, None
    match = _ASSET_TAIL_PATTERN.match(lines[0])
    if not match or not (match.group("ticker") or match.group("asset_type")):
        return lines, "", None, None
    return lines[1:], match.group("name").strip(), match.group("ticker"), match.group("asset_type")


def _expand_legacy_markers(line: str) -> str:
    """Spell out the "F S: New S O: Fidelity Trust" text-layer shorthand."""

    if not re.match(r"^(?:F\s+)?S:\s", line):
        return line
    expanded = re.sub(r"^(?:F\s+)?S:\s*", "Filing Status: ", line)
    expanded = re.sub(r"\s+S\s+O:\s*", " | Subholding Of: ", expanded)
    expanded = re.sub(r"\s+D:\s*", " | Description: ", expanded)
    expanded = re.sub(r"\s+L:\s*", " | Location: ", expanded)
    return expanded


def format_annotation(lines: list[str]) -> str | None:
    """Join a row's annotation lines into one comment string.

    Each marker line starts a chunk ("Filing Status: New", "Comments: ...");
    wrapped continuations are appended to the open chunk; chunks are joined
    with " | ", the same shape the Senate eFD comments use.
    """

    chunks: list[str] = []
    for raw_line in lines:
        line = _expand_legacy_markers(squeeze_spaces(raw_line))
        if not line:
            continue
        if _is_annotation_marker(line) or not chunks:
            chunks.append(line)
        else:
            chunks[-1] = f"{chunks[-1]} {line}"
    comment = " | ".join(squeeze_spaces(chunk) for chunk in chunks if chunk.strip())
    return comment or None


def _finish_row(transaction: HousePtrTransaction, lines: list[str]) -> HousePtrTransaction:
    """Attach the lines that follow a row core to that row.

    A leading asset tail (see ``_take_asset_tail``) completes the asset name,
    ticker and type code when the core did not carry them; the rest becomes
    the row's comment.
    """

    remaining, tail_name, tail_ticker, tail_code = _take_asset_tail(lines)
    update: dict[str, object] = {}
    if tail_ticker or tail_code:
        ticker = transaction.ticker or tail_ticker
        prefix = " ".join(part for part in (transaction.asset_description, tail_name) if part)
        update["ticker"] = ticker
        update["asset_description"] = clean_asset_description(prefix, ticker)
        if transaction.asset_type == "Asset" and tail_code:
            update["asset_type"] = infer_asset_type(tail_code)
    else:
        remaining = lines
    update["comment"] = format_annotation(remaining)
    return transaction.model_copy(update=update)


def parse_transactions(text: str) -> list[HousePtrTransaction]:
    """Segment a House PTR text layer into rows.

    Every row is anchored on its core (type code, dates, amount). The text
    between two cores is split into the previous row's annotation block and
    the next row's asset name by line shape, so a 300-character "Comments:"
    paragraph or a page heading can no longer leak into an asset description.
    Pass the text with its line breaks intact.
    """

    prepared = strip_page_furniture(normalize_text(text, keep_newlines=True))
    cores = list(ROW_CORE_PATTERN.finditer(prepared))
    transactions: list[HousePtrTransaction] = []
    previous_end = 0
    for index, match in enumerate(cores, start=1):
        annotation_lines, asset_lines = split_row_segment(prepared[previous_end:match.start()])
        if transactions:
            transactions[-1] = _finish_row(transactions[-1], annotation_lines)
        ticker = match.group("ticker") or None
        asset_prefix = " ".join(asset_lines)
        amount_min, amount_max = parse_amount_range(match.group("amount"))
        transactions.append(
            HousePtrTransaction(
                line_number=index,
                asset_description=clean_asset_description(asset_prefix, ticker),
                ticker=ticker,
                asset_type=infer_asset_type(match.group("asset_type")),
                transaction_type=parse_transaction_type(match.group("tx_type")),  # type: ignore[arg-type]
                transaction_date=normalize_date(match.group("date")),
                notification_date=normalize_date(match.group("notified")),
                amount_min=amount_min,
                amount_max=amount_max,
                owner=parse_owner(asset_prefix, match.group("owner") or None),  # type: ignore[arg-type]
            )
        )
        previous_end = match.end()

    if transactions:
        # Whatever follows the last core (the trailing sections are already
        # gone) is the last row's annotation block.
        trailing = [squeeze_spaces(line) for line in prepared[previous_end:].split("\n")]
        transactions[-1] = _finish_row(transactions[-1], [line for line in trailing if line])
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
                comment=" | ".join(
                    part
                    for part in (
                        (transaction.comment or "").strip(),
                        f"Parsed from House PTR {stub.doc_id} at "
                        f"{round(parsed.parser_confidence * 100)}% confidence "
                        f"[{parsed.parser_version}]",
                    )
                    if part
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
    # The segmenter needs the original line breaks; hand it the raw text.
    transactions = dedupe_transactions(stub, parse_transactions(text))
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


def ocr_text_is_decent(text: str | None) -> bool:
    """Return whether an OCR text layer is worth handing to the text fallback.

    Scanned PTRs come back as fragments like ``| 9 984 F 1 | Sale | 1 |``: long
    enough to look like output, useless to a text model. Those go straight to
    the vision path instead.
    """

    stripped = (text or "").strip()
    if len(stripped) < MIN_DECENT_OCR_CHARS:
        return False
    meaningful = [char for char in stripped if not char.isspace()]
    if not meaningful:
        return False
    alpha_ratio = sum(1 for char in meaningful if char.isalpha()) / len(meaningful)
    if alpha_ratio < MIN_DECENT_OCR_ALPHA_RATIO:
        return False
    return len(re.findall(r"[A-Za-z]{3,}", stripped)) >= MIN_DECENT_OCR_WORDS


def _vision_owner(raw: object) -> str:
    value = str(raw or "").strip().lower()
    return _VISION_OWNER_MAP.get(value, "self")


def _vision_transaction_type(raw: object) -> str:
    value = str(raw or "").strip().lower()
    return _VISION_TRANSACTION_TYPE_MAP.get(value, "purchase")


def _vision_to_transactions(payload: list[dict]) -> list[HousePtrTransaction]:
    """Normalize vision rows through the same helpers the text parser uses."""

    out: list[HousePtrTransaction] = []
    for index, raw_row in enumerate(payload, start=1):
        if not isinstance(raw_row, dict):
            continue
        ticker = raw_row.get("ticker")
        ticker = ticker.strip().upper() if isinstance(ticker, str) and ticker.strip() else None
        description = clean_asset_description(
            str(raw_row.get("asset_description") or "").strip(),
            ticker,
        )
        if not description:
            continue
        amount_min, amount_max = _llm_amount_bounds(
            raw_row.get("amount_min"), raw_row.get("amount_max")
        )
        comment = raw_row.get("comment")
        comment = squeeze_spaces(comment) if isinstance(comment, str) and comment.strip() else None
        out.append(
            HousePtrTransaction(
                line_number=index,
                asset_description=description,
                ticker=ticker,
                asset_type=infer_asset_type(raw_row.get("asset_type_code")),
                transaction_type=_vision_transaction_type(raw_row.get("transaction_type")),  # type: ignore[arg-type]
                transaction_date=normalize_date(raw_row.get("transaction_date")),
                notification_date=normalize_date(raw_row.get("notification_date")),
                amount_min=amount_min,
                amount_max=amount_max,
                owner=_vision_owner(raw_row.get("owner")),  # type: ignore[arg-type]
                comment=comment,
            )
        )
    return out


def _run_vision_parse(
    pdf_path: Path,
    stub: FilingStub,
    text_preview: str,
) -> tuple[tuple[HousePtrParseResult, list[NormalizedTradeRow]] | None, dict[str, object]]:
    """Transcribe a PDF with Claude vision.

    Returns ``(result_or_None, metadata)``. The metadata is always returned so
    the caller can record the attempt -- including a skip reason -- on the stub
    even when no usable rows came back.
    """

    try:
        report = extract_via_vision(
            pdf_path,
            filing_year=getattr(stub, "filing_year", None),
            filing_date=getattr(stub, "filing_date", None),
        )
    except Exception as exc:  # pragma: no cover - defensive
        logger.exception("house_ptr vision: extract_via_vision raised: %s", exc)
        return None, {"ok": False, "skipped": True, "reason": f"vision parser raised: {exc}"}

    # The vision module already scrubbed each read before reconciling them;
    # this pass is idempotent and only exists as a belt for the merged rows.
    raw_transactions, example_scrubs = scrub_example_row_values(
        list(report.get("transactions") or []),
        getattr(stub, "filing_year", None),
    )
    example_scrubs += int(report.get("example_row_scrubs") or 0)
    # A row whose transaction type the two reads could not agree on would
    # otherwise default to "purchase" in _vision_to_transactions. Dropping it is
    # the safe outcome; the row count mismatch already routes the stub to review.
    typed_transactions = [row for row in raw_transactions if row.get("transaction_type")]
    dropped_for_type = len(raw_transactions) - len(typed_transactions)
    raw_transactions = typed_transactions
    report["transactions"] = raw_transactions
    metadata = build_vision_metadata(report)
    if example_scrubs:
        metadata["exampleRowScrubs"] = example_scrubs
        logger.info(
            "house_ptr vision: scrubbed %d row(s) carrying the form's example values for %s",
            example_scrubs,
            pdf_path.name,
        )
    if dropped_for_type:
        metadata["rowsDroppedForType"] = dropped_for_type
        metadata["needsReview"] = True
        if not raw_transactions:
            metadata["reason"] = "every row dropped: the two reads disagreed on transaction type"
        logger.info(
            "house_ptr vision: dropped %d row(s) whose transaction type the reads disagreed on for %s",
            dropped_for_type,
            pdf_path.name,
        )

    if not raw_transactions:
        logger.info(
            "house_ptr vision: no rows recovered for %s (reason=%r cost=$%.4f)",
            pdf_path.name,
            report.get("reason"),
            float(report.get("cost_usd") or 0.0),
        )
        return None, metadata

    # clean_asset_description() was written for the text layer, where a run of
    # three or more digits is a brokerage account number to strip. Handwritten
    # municipal bonds carry series numbers and coupons in the name ("MINNESOTA
    # ST BD GRP 160 5%" came back as "5%"), so when cleaning ate most of the
    # model's transcription, keep the transcription. line_number is the 1-based
    # index into raw_transactions.
    def _gutted(cleaned: str, raw: str) -> bool:
        raw_alnum = sum(1 for char in raw if char.isalnum())
        cleaned_alnum = sum(1 for char in cleaned if char.isalnum())
        return raw_alnum >= 6 and cleaned_alnum * 2 < raw_alnum

    converted = _vision_to_transactions(raw_transactions)
    restored: list[HousePtrTransaction] = []
    restored_count = 0
    for transaction in converted:
        raw_row = raw_transactions[transaction.line_number - 1]
        raw_description = squeeze_spaces(str(raw_row.get("asset_description") or "").strip())
        if transaction.ticker:
            raw_description = re.sub(
                rf"\s*\(\s*{re.escape(transaction.ticker)}\s*\)\s*$", "", raw_description, flags=re.I
            ).strip()
        if raw_description and _gutted(transaction.asset_description, raw_description):
            transaction = transaction.model_copy(update={"asset_description": raw_description})
            restored_count += 1
        restored.append(transaction)
    if restored_count:
        metadata["descriptionsRestored"] = restored_count
        logger.info(
            "house_ptr vision: restored %d asset description(s) that cleaning gutted for %s",
            restored_count,
            pdf_path.name,
        )

    transactions = dedupe_transactions(stub, restored)
    valid_transactions = [
        transaction
        for transaction in transactions
        if get_transaction_date_issue(transaction.transaction_date, stub.filing_date) is None
    ]
    if not valid_transactions:
        logger.info("house_ptr vision: all rows filtered as invalid for %s", pdf_path.name)
        metadata["reason"] = "every transcribed row failed date validation"
        metadata["needsReview"] = True
        return None, metadata

    confidence = float(report.get("confidence") or 0.0)
    parsed = HousePtrParseResult(
        doc_id=stub.doc_id,
        member_name=parse_header_name(text_preview) or stub.member.name,
        state=parse_header_state(text_preview) or stub.member.state,
        parser_confidence=confidence,
        parser_version=VISION_PARSER_VERSION,
        raw_text_preview=text_preview[:1200],
        transactions=valid_transactions,
        vision_report=metadata,
    )
    logger.info(
        "house_ptr vision: recovered %d rows for %s "
        "(parser_version=%s parser_confidence=%.2f needsReview=%s cost=$%.4f)",
        len(valid_transactions),
        pdf_path.name,
        VISION_PARSER_VERSION,
        confidence,
        metadata.get("needsReview"),
        float(report.get("cost_usd") or 0.0),
    )
    return (parsed, build_trade_rows_from_house_ptr(parsed, stub)), metadata


def parse_house_ptr_pdf(
    pdf_path: Path,
    stub: FilingStub,
    settings: Settings | None = None,
    backend: str | OcrBackend = OcrBackend.AUTO,
    vision_backend: str = "off",
) -> tuple[HousePtrParseResult, list[NormalizedTradeRow]]:
    settings = settings or Settings()
    processor = OcrProcessor(settings, backend=backend)
    result = processor.process_file(pdf_path)
    text = result.document.ocrText if result.document and result.document.ocrText else ""
    parsed, rows = parse_house_ptr_text(text, stub)

    mode = str(vision_backend or "off").strip().lower()
    if mode not in VISION_BACKENDS:
        mode = "off"
    force_vision = mode == "claude"
    vision_enabled = mode in {"auto", "claude"}

    text_has_content = bool((text or "").strip())
    decent_text = ocr_text_is_decent(text)
    weak_text_parse = (
        not parsed.transactions
        or not text_has_content
        or parsed.parser_confidence < VISION_CONFIDENCE_FLOOR
    )

    if not weak_text_parse and not force_vision:
        return parsed, rows

    # The Haiku text fallback still owns the case the regex missed but the OCR
    # text layer is genuinely readable. Skip it when the caller forced vision.
    if not parsed.transactions and decent_text and not force_vision:
        logger.debug(
            "house_ptr: regex found 0 rows despite %d chars of usable text for %s; "
            "invoking the Haiku text fallback",
            len(text),
            pdf_path.name,
        )
        fallback = _run_llm_fallback(pdf_path, stub, normalize_text(text))
        if fallback is not None:
            return fallback

    if vision_enabled:
        logger.debug(
            "house_ptr: handing %s to the vision parser (mode=%s confidence=%.2f textChars=%d)",
            pdf_path.name,
            mode,
            parsed.parser_confidence,
            len(text or ""),
        )
        vision_result, vision_metadata = _run_vision_parse(pdf_path, stub, normalize_text(text))
        if vision_result is not None:
            return vision_result
        # Record the attempt even though it produced nothing usable.
        parsed = parsed.model_copy(update={"vision_report": vision_metadata})

    # Vision is off, unavailable, or empty-handed: fall back to Haiku on the
    # raw text exactly as this parser did before the vision path existed.
    if not parsed.transactions and not decent_text:
        logger.debug(
            "house_ptr: no usable OCR text for %s; invoking the Haiku text fallback",
            pdf_path.name,
        )
        fallback = _run_llm_fallback(pdf_path, stub, normalize_text(text))
        if fallback is not None:
            return fallback

    return parsed, rows
