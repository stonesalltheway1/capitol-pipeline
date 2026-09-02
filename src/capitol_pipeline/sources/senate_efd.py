"""Official Senate eFD (efdsearch.senate.gov) source adapter.

This is the free, terms-safe replacement for the lapsed Quiver subscription and
the senate-stock-watcher feed (frozen since 2020). It talks to the same public
search UI a human uses:

1. ``GET  /search/home/``          -- sets a ``csrftoken`` cookie and renders the
   prohibition agreement form containing a ``csrfmiddlewaretoken``.
2. ``POST /search/home/``          -- ``prohibition_agreement=1`` plus the CSRF
   token accepts the agreement and sets a ``sessionid`` cookie. Without it the
   report endpoints 403 or bounce back to the agreement page.
3. ``POST /search/report/data/``   -- a DataTables-style form that returns
   ``{"data": [[first_name, last_name, office, report_link_html, date], ...],
   "recordsTotal": N}``.

Report links point at either ``/search/view/ptr/<uuid>/`` (an electronic filing
rendered as an HTML table) or ``/search/view/paper/<uuid>/`` (a scanned filing
rendered as a carousel of GIF page images). The OCR chain in
``capitol_pipeline.processors.ocr`` only accepts PDFs, so paper filings are
deferred with their page-image URLs recorded rather than parsed here.

Endpoint shapes verified against the live site on 2026-09-02.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser
import json
import logging
import re
import time
from typing import Callable, Literal, Sequence
from urllib.parse import urljoin, urlparse

import httpx
from pydantic import BaseModel

from capitol_pipeline.bridges.capitol_exposed import build_canonical_senate_trade_id
from capitol_pipeline.config import Settings
from capitol_pipeline.models.congress import NormalizedTradeRow
from capitol_pipeline.normalizers.crypto_assets import classify_crypto_asset
from capitol_pipeline.registries.members import MemberRegistry
from capitol_pipeline.sources.senate_ethics import (
    _normalize_asset_type,
    normalize_senate_date,
    normalize_senate_transaction_type,
    parse_senate_amount_range,
)

logger = logging.getLogger(__name__)

EFD_SOURCE = "senate-efd"
"""Pipeline source id. ``normalize_trade_source_for_site`` maps it to ``senate_efd``."""

PTR_REPORT_TYPE = "11"
"""``report_types`` value for Periodic Transaction Reports."""

SENATOR_FILER_TYPE = "1"
"""``filer_types`` value for sitting senators."""

DEFAULT_MAX_REPORTS = 200
DEFAULT_PAGE_SIZE = 100
DEFAULT_MAX_RETRIES = 4
DEFAULT_TIMEOUT_SECONDS = 30.0

EMPTY_VALUES = {"", "--", "---", "n/a", "none"}

_REPORT_HREF_PATTERN = re.compile(r'href="([^"]+)"', re.I)
_REPORT_ID_PATTERN = re.compile(r"/search/view/(ptr|paper)/([0-9a-fA-F-]{16,})/?")
_IMG_SRC_PATTERN = re.compile(r'<img[^>]+class="[^"]*filingImage[^"]*"[^>]*>', re.I)
_SRC_ATTR_PATTERN = re.compile(r'src="([^"]+)"', re.I)
_CSRF_INPUT_PATTERN = re.compile(
    r'name="csrfmiddlewaretoken"[^>]*value="([^"]+)"', re.I
)
_CSRF_INPUT_REVERSED_PATTERN = re.compile(
    r'value="([^"]+)"[^>]*name="csrfmiddlewaretoken"', re.I
)

ReportKind = Literal["electronic", "paper"]


class SenateEfdError(RuntimeError):
    """Raised when the eFD site cannot be scraped."""


class EfdReport(BaseModel):
    """One Periodic Transaction Report row from the eFD search results."""

    report_id: str
    kind: ReportKind
    url: str
    first_name: str = ""
    last_name: str = ""
    office: str = ""
    title: str = ""
    submitted_date: str | None = None

    @property
    def senator_name(self) -> str:
        """Best-effort display name for the filer."""

        combined = f"{self.first_name.strip()} {self.last_name.strip()}".strip()
        return " ".join(combined.split())

    @property
    def is_amendment(self) -> bool:
        return "amendment" in (self.title or "").lower()


class EfdTransaction(BaseModel):
    """One parsed row of an electronic PTR transaction table."""

    line_number: int | None = None
    transaction_date: str | None = None
    owner_raw: str = ""
    ticker: str | None = None
    asset_description: str = ""
    asset_detail: str | None = None
    asset_type_raw: str = ""
    transaction_type_raw: str = ""
    amount_raw: str = ""
    comment: str | None = None


# ---------------------------------------------------------------------------
# Small value helpers
# ---------------------------------------------------------------------------


def _is_empty_value(value: str | None) -> bool:
    return (value or "").strip().lower() in EMPTY_VALUES


def clean_efd_ticker(raw: str | Sequence[str] | None) -> str | None:
    """Normalize an eFD ticker cell. ``--`` and blanks mean no ticker.

    Exchange rows render two lines in the ticker cell (the exchanged asset's
    ``--`` and the received asset's linked symbol), so a sequence of cell lines
    is accepted and the first real symbol wins.
    """

    if not raw:
        return None
    candidates = [raw] if isinstance(raw, str) else list(raw)
    for candidate in candidates:
        for token in re.split(r"[\s,;]+", str(candidate)):
            symbol = token.strip().upper()
            if not symbol or symbol.lower() in EMPTY_VALUES:
                continue
            if not re.fullmatch(r"[A-Z0-9.\-]{1,12}", symbol):
                continue
            return symbol
    return None


_EFD_ASSET_TYPES: dict[str, str] = {
    "stock": "Stock",
    "non-public stock": "Stock",
    "corporate bond": "Bond",
    "municipal security": "Bond",
    "other securities": "Other",
    "stock option": "Option",
    "cryptocurrency": "Cryptocurrency",
    "digital assets": "Cryptocurrency",
    "exchange traded fund": "ETF",
    "exchange traded fund (etf)": "ETF",
    "mutual fund": "Fund",
}


def normalize_efd_asset_type(raw: str | None, normalized_asset_kind: str = "unrelated") -> str:
    """Map the eFD asset-type vocabulary onto the site's asset types."""

    if normalized_asset_kind != "unrelated":
        return _normalize_asset_type(raw, normalized_asset_kind)
    mapped = _EFD_ASSET_TYPES.get(" ".join((raw or "").split()).lower())
    if mapped:
        return mapped
    return _normalize_asset_type(raw, normalized_asset_kind)


def normalize_efd_owner(raw: str | None) -> str:
    """Map the eFD owner column onto the site's owner taxonomy."""

    normalized = (raw or "").strip().lower()
    if "spouse" in normalized:
        return "spouse"
    if "child" in normalized or normalized in {"dc", "dependent"}:
        return "child"
    if "joint" in normalized or normalized == "jt":
        return "joint"
    return "self"


def parse_efd_amount_range(raw: str | None) -> tuple[int, int]:
    """Parse an eFD amount cell into ``(amount_min, amount_max)``.

    Delegates to the shared Senate parser so an eFD row and the same trade seen
    through Quiver produce identical bounds, which the canonical trade id hashes.
    """

    if _is_empty_value(raw):
        return 0, 0
    return parse_senate_amount_range(raw)


def parse_efd_date(raw: str | None) -> str | None:
    """Parse an eFD ``MM/DD/YYYY`` cell into ``YYYY-MM-DD``."""

    if _is_empty_value(raw):
        return None
    return normalize_senate_date(raw)


def format_efd_date(value: date) -> str:
    """Render a date the way the search form expects it."""

    return value.strftime("%m/%d/%Y")


def build_efd_submitted_since(
    latest_known_disclosure_date: str | None,
    *,
    lookback_days: int = 14,
    floor_days: int = 60,
    today: date | None = None,
) -> date:
    """Return the default ``submitted_since`` window start for a scheduled run.

    The window starts ``lookback_days`` before the newest disclosure date we
    already store so late amendments are replayed, but never earlier than
    ``floor_days`` ago so a 30-minute timer stays cheap.
    """

    current_day = today or datetime.now(timezone.utc).date()
    floor_start = current_day - timedelta(days=max(0, floor_days))

    normalized_latest = normalize_senate_date(latest_known_disclosure_date)
    if not normalized_latest:
        return floor_start

    window_start = date.fromisoformat(normalized_latest) - timedelta(days=max(0, lookback_days))
    if window_start < floor_start:
        return floor_start
    if window_start > current_day:
        return current_day
    return window_start


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _Cell:
    """One table cell, keeping line breaks and muted detail text apart."""

    lines: list[str] = field(default_factory=list)
    detail_lines: list[str] = field(default_factory=list)

    @property
    def text(self) -> str:
        return " / ".join(self.lines)

    @property
    def first_line(self) -> str:
        return self.lines[0] if self.lines else ""

    @property
    def detail(self) -> str | None:
        joined = " ".join(self.detail_lines).strip()
        return joined or None


class _TransactionTableParser(HTMLParser):
    """Collect the rows of the ``table table-striped`` transactions table.

    Uses the standard library parser so no extra dependency is pulled in.
    ``<br>`` splits a cell into lines, which matters for Exchange rows where the
    ticker and asset-name cells carry both the exchanged and received asset.
    """

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.headers: list[str] = []
        self.rows: list[list[_Cell]] = []
        self._table_depth = 0
        self._in_target_table = False
        self._in_head_row = False
        self._cell: _Cell | None = None
        self._buffer: list[str] = []
        self._row: list[_Cell] | None = None
        self._muted_depth: int | None = None
        self._div_depth = 0
        self._target_depth = 0

    # -- helpers ---------------------------------------------------------
    def _flush_line(self) -> None:
        if self._cell is None:
            return
        text = " ".join("".join(self._buffer).split()).strip()
        self._buffer = []
        if not text:
            return
        if self._muted_depth is not None:
            self._cell.detail_lines.append(text)
        else:
            self._cell.lines.append(text)

    # -- HTMLParser hooks ------------------------------------------------
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = {key.lower(): (value or "") for key, value in attrs}
        classes = attributes.get("class", "").split()

        if tag == "table":
            self._table_depth += 1
            if not self._in_target_table and "table" in classes and "table-striped" in classes:
                self._in_target_table = True
                self._target_depth = self._table_depth
            return

        if not self._in_target_table:
            return

        if tag == "tr":
            self._row = []
            self._in_head_row = False
            return
        if tag in {"td", "th"}:
            self._cell = _Cell()
            self._buffer = []
            self._muted_depth = None
            self._div_depth = 0
            self._in_head_row = self._in_head_row or tag == "th"
            return
        if tag == "br":
            self._flush_line()
            return
        if tag == "div":
            self._div_depth += 1
            if self._cell is not None and self._muted_depth is None and "text-muted" in classes:
                self._flush_line()
                self._muted_depth = self._div_depth
            return

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "br":
            self._flush_line()
            return
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def handle_endtag(self, tag: str) -> None:
        if tag == "table":
            if self._in_target_table and self._table_depth == self._target_depth:
                self._in_target_table = False
            self._table_depth = max(0, self._table_depth - 1)
            return

        if not self._in_target_table:
            return

        if tag == "div":
            if self._muted_depth is not None and self._div_depth == self._muted_depth:
                self._flush_line()
                self._muted_depth = None
            self._div_depth = max(0, self._div_depth - 1)
            return
        if tag in {"td", "th"}:
            self._flush_line()
            if self._row is not None and self._cell is not None:
                self._row.append(self._cell)
            self._cell = None
            return
        if tag == "tr":
            row = self._row or []
            self._row = None
            if not row:
                return
            if self._in_head_row:
                if not self.headers:
                    self.headers = [cell.text.strip().lower() for cell in row]
            else:
                self.rows.append(row)
            self._in_head_row = False
            return

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._buffer.append(data)


_COLUMN_ALIASES: dict[str, str] = {
    "#": "line_number",
    "transaction date": "transaction_date",
    "owner": "owner",
    "ticker": "ticker",
    "asset name": "asset_name",
    "asset type": "asset_type",
    "type": "transaction_type",
    "amount": "amount",
    "comment": "comment",
}

_DEFAULT_COLUMN_ORDER = [
    "line_number",
    "transaction_date",
    "owner",
    "ticker",
    "asset_name",
    "asset_type",
    "transaction_type",
    "amount",
    "comment",
]


def _build_column_index(headers: list[str]) -> dict[str, int]:
    index: dict[str, int] = {}
    for position, header in enumerate(headers):
        key = _COLUMN_ALIASES.get(" ".join(header.split()))
        if key and key not in index:
            index[key] = position
    if len(index) >= 6:
        return index
    return {name: position for position, name in enumerate(_DEFAULT_COLUMN_ORDER)}


def parse_electronic_ptr_html(html_text: str) -> list[EfdTransaction]:
    """Parse the transaction table of an electronic PTR view page."""

    parser = _TransactionTableParser()
    parser.feed(html_text)
    parser.close()

    if not parser.rows:
        return []

    columns = _build_column_index(parser.headers)
    transactions: list[EfdTransaction] = []

    for row in parser.rows:
        def cell(name: str) -> _Cell:
            position = columns.get(name)
            if position is None or position >= len(row):
                return _Cell()
            return row[position]

        if len(row) < 6:
            continue

        asset_cell = cell("asset_name")
        asset_description = asset_cell.text.strip()
        transaction_type_raw = cell("transaction_type").text.strip()
        amount_raw = cell("amount").text.strip()
        if not asset_description and not transaction_type_raw and not amount_raw:
            continue

        raw_line = cell("line_number").first_line.strip()
        comment_text = cell("comment").text.strip()

        transactions.append(
            EfdTransaction(
                line_number=int(raw_line) if raw_line.isdigit() else None,
                transaction_date=parse_efd_date(cell("transaction_date").text),
                owner_raw=cell("owner").text.strip(),
                ticker=clean_efd_ticker(cell("ticker").lines),
                asset_description=asset_description,
                asset_detail=asset_cell.detail,
                asset_type_raw=cell("asset_type").text.strip(),
                transaction_type_raw=transaction_type_raw,
                amount_raw=amount_raw,
                comment=None if _is_empty_value(comment_text) else comment_text,
            )
        )

    return transactions


def list_paper_page_images(html_text: str, *, base_url: str) -> list[str]:
    """Return the scanned page-image URLs on a paper filing view page."""

    urls: list[str] = []
    for tag in _IMG_SRC_PATTERN.findall(html_text):
        match = _SRC_ATTR_PATTERN.search(tag)
        if not match:
            continue
        url = urljoin(base_url, match.group(1).strip())
        if url not in urls:
            urls.append(url)
    return urls


def parse_efd_search_rows(payload: dict[str, object], *, base_url: str) -> list[EfdReport]:
    """Turn one ``/search/report/data/`` JSON payload into report records."""

    raw_rows = payload.get("data")
    if not isinstance(raw_rows, list):
        raise SenateEfdError(f"Unexpected eFD search payload: {sorted(payload)[:6]}")

    reports: list[EfdReport] = []
    for raw_row in raw_rows:
        if not isinstance(raw_row, (list, tuple)) or len(raw_row) < 5:
            continue
        first_name, last_name, office, link_html, submitted = (str(value) for value in raw_row[:5])

        href_match = _REPORT_HREF_PATTERN.search(link_html)
        if not href_match:
            continue
        href = href_match.group(1)
        id_match = _REPORT_ID_PATTERN.search(href)
        if not id_match:
            continue

        kind: ReportKind = "electronic" if id_match.group(1).lower() == "ptr" else "paper"
        title = " ".join(re.sub(r"<[^>]+>", " ", link_html).split())

        reports.append(
            EfdReport(
                report_id=id_match.group(2).lower(),
                kind=kind,
                url=urljoin(base_url, href),
                first_name=" ".join(first_name.split()),
                last_name=" ".join(last_name.split()),
                office=" ".join(office.split()),
                title=title,
                submitted_date=parse_efd_date(submitted),
            )
        )
    return reports


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------


class SenateEfdClient:
    """Polite scraping client for efdsearch.senate.gov.

    Holds the shared cookie jar (``csrftoken`` + ``sessionid``), accepts the
    prohibition agreement once, rate limits every request, and retries 5xx/429.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        client: httpx.Client | None = None,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        min_interval_seconds: float | None = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.settings = settings or Settings()
        self.base_url = self.settings.senate_efd_base_url.rstrip("/") + "/"
        self.home_url = urljoin(self.base_url, "search/home/")
        self.search_url = urljoin(self.base_url, "search/")
        self.data_url = urljoin(self.base_url, "search/report/data/")
        self.min_interval_seconds = (
            self.settings.senate_efd_request_interval_seconds
            if min_interval_seconds is None
            else min_interval_seconds
        )
        self.max_retries = max(1, max_retries)
        self._sleep = sleep  # injectable for tests
        self._last_request_at = 0.0
        self._agreed = False
        self._owns_client = client is None
        self._client = client or httpx.Client(
            headers={
                "User-Agent": self.settings.senate_efd_user_agent,
                "Accept-Language": "en-US,en;q=0.9",
            },
            follow_redirects=True,
            timeout=timeout_seconds,
        )

    # -- lifecycle -------------------------------------------------------
    def __enter__(self) -> "SenateEfdClient":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    # -- plumbing --------------------------------------------------------
    def _throttle(self) -> None:
        if self.min_interval_seconds <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        remaining = self.min_interval_seconds - elapsed
        if remaining > 0 and self._last_request_at:
            self._sleep(remaining)
        self._last_request_at = time.monotonic()

    def _request(self, method: str, url: str, **kwargs: object) -> httpx.Response:
        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            self._throttle()
            try:
                response = self._client.request(method, url, **kwargs)  # type: ignore[arg-type]
            except httpx.HTTPError as error:  # transport failures are retriable
                last_error = error
                self._sleep(min(30.0, max(1.0, self.min_interval_seconds) * (2**attempt)))
                continue

            if response.status_code < 500 and response.status_code != 429:
                return response

            last_error = SenateEfdError(f"{method} {url} returned {response.status_code}")
            logger.warning(
                "eFD %s %s returned %s, retrying (%s/%s)",
                method,
                url,
                response.status_code,
                attempt + 1,
                self.max_retries,
            )
            retry_after = response.headers.get("retry-after", "")
            delay = (
                float(retry_after)
                if retry_after.replace(".", "", 1).isdigit()
                else min(30.0, max(1.0, self.min_interval_seconds) * (2**attempt))
            )
            self._sleep(delay)

        raise SenateEfdError(f"{method} {url} failed after {self.max_retries} attempts") from last_error

    def _csrf_token(self, html_text: str = "") -> str:
        match = _CSRF_INPUT_PATTERN.search(html_text) or _CSRF_INPUT_REVERSED_PATTERN.search(html_text)
        if match:
            return match.group(1)
        cookie = self._client.cookies.get("csrftoken")
        if cookie:
            return cookie
        raise SenateEfdError("No CSRF token found on the eFD agreement page.")

    @staticmethod
    def _looks_like_agreement_page(response: httpx.Response) -> bool:
        """Detect the prohibition agreement form.

        The site bounces any report view back to ``/search/home/`` once the
        session cookie lapses. The accepted search page does not carry the
        ``prohibition_agreement`` input, so its presence is the reliable signal;
        the path is only a fallback for an empty body.
        """

        if "prohibition_agreement" in response.text:
            return True
        if response.text.strip():
            return False
        return urlparse(str(response.url)).path.rstrip("/").endswith("/search/home")

    # -- public API ------------------------------------------------------
    def accept_agreement(self, *, force: bool = False) -> None:
        """Accept the prohibition agreement so report endpoints stop 403-ing."""

        if self._agreed and not force:
            return

        home = self._request("GET", self.home_url)
        home.raise_for_status()
        token = self._csrf_token(home.text)

        accepted = self._request(
            "POST",
            self.home_url,
            data={"prohibition_agreement": "1", "csrfmiddlewaretoken": token},
            headers={"Referer": self.home_url, "X-CSRFToken": token},
        )
        accepted.raise_for_status()
        if self._looks_like_agreement_page(accepted):
            raise SenateEfdError("The eFD prohibition agreement was not accepted.")
        self._agreed = True

    def search_ptr_reports(
        self,
        *,
        submitted_start: date,
        submitted_end: date | None = None,
        start: int = 0,
        length: int = DEFAULT_PAGE_SIZE,
        senator_state: str | None = None,
        last_name: str | None = None,
        first_name: str | None = None,
        draw: int = 1,
    ) -> dict[str, object]:
        """POST one page of the PTR search and return the raw JSON payload."""

        self.accept_agreement()
        token = self._client.cookies.get("csrftoken") or ""
        form = {
            "start": str(max(0, start)),
            "length": str(max(1, length)),
            "report_types": f"[{PTR_REPORT_TYPE}]",
            "filer_types": f"[{SENATOR_FILER_TYPE}]",
            "submitted_start_date": f"{format_efd_date(submitted_start)} 00:00:00",
            "submitted_end_date": (
                f"{format_efd_date(submitted_end)} 23:59:59" if submitted_end else ""
            ),
            "candidate_state": "",
            "senator_state": (senator_state or "").strip().upper(),
            "office_id": "",
            "first_name": (first_name or "").strip(),
            "last_name": (last_name or "").strip(),
            "draw": str(draw),
            "order[0][column]": "4",
            "order[0][dir]": "desc",
        }
        response = self._request(
            "POST",
            self.data_url,
            data=form,
            headers={
                "Referer": self.search_url,
                "X-CSRFToken": token,
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        if response.status_code == 403:
            self.accept_agreement(force=True)
            token = self._client.cookies.get("csrftoken") or ""
            response = self._request(
                "POST",
                self.data_url,
                data=form,
                headers={
                    "Referer": self.search_url,
                    "X-CSRFToken": token,
                    "X-Requested-With": "XMLHttpRequest",
                },
            )
        response.raise_for_status()
        try:
            payload = response.json()
        except json.JSONDecodeError as error:
            raise SenateEfdError("eFD search returned a non-JSON payload.") from error
        if not isinstance(payload, dict):
            raise SenateEfdError(f"Unexpected eFD search payload type: {type(payload).__name__}")
        return payload

    def get_report_html(self, url: str) -> str:
        """Fetch a report view page, re-accepting the agreement if bounced."""

        self.accept_agreement()
        absolute = urljoin(self.base_url, url)
        response = self._request("GET", absolute, headers={"Referer": self.search_url})
        response.raise_for_status()
        if self._looks_like_agreement_page(response):
            self.accept_agreement(force=True)
            response = self._request("GET", absolute, headers={"Referer": self.search_url})
            response.raise_for_status()
            if self._looks_like_agreement_page(response):
                raise SenateEfdError(f"eFD kept redirecting {absolute} to the agreement page.")
        return response.text


# ---------------------------------------------------------------------------
# High level fetchers
# ---------------------------------------------------------------------------


def list_ptr_reports(
    settings: Settings,
    submitted_since: date,
    submitted_until: date | None = None,
    max_reports: int = DEFAULT_MAX_REPORTS,
    *,
    client: SenateEfdClient | None = None,
    senator_state: str | None = None,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> list[EfdReport]:
    """List senator Periodic Transaction Reports submitted in a date window.

    Newest first, hard-capped at ``max_reports`` so a 30-minute timer stays cheap.
    """

    owned_client = client is None
    active = client or SenateEfdClient(settings)
    cap = max(0, max_reports)
    if cap == 0:
        return []

    reports: list[EfdReport] = []
    seen: set[str] = set()
    try:
        start = 0
        draw = 1
        while len(reports) < cap:
            requested = max(1, min(page_size, cap - len(reports)))
            payload = active.search_ptr_reports(
                submitted_start=submitted_since,
                submitted_end=submitted_until,
                start=start,
                length=requested,
                senator_state=senator_state,
                draw=draw,
            )
            page = parse_efd_search_rows(payload, base_url=active.base_url)
            if not page:
                break
            for report in page:
                if report.report_id in seen:
                    continue
                seen.add(report.report_id)
                reports.append(report)
                if len(reports) >= cap:
                    break

            records_total = payload.get("recordsTotal")
            start += len(page)
            draw += 1
            if isinstance(records_total, int) and start >= records_total:
                break
            if len(page) < requested:
                break
    finally:
        if owned_client:
            active.close()

    return reports


def fetch_electronic_ptr(client: SenateEfdClient, report: EfdReport) -> list[EfdTransaction]:
    """Fetch and parse the transaction table of an electronic PTR."""

    if report.kind != "electronic":
        raise SenateEfdError(f"Report {report.report_id} is a {report.kind} filing, not electronic.")
    return parse_electronic_ptr_html(client.get_report_html(report.url))


def fetch_paper_ptr_pages(client: SenateEfdClient, report: EfdReport) -> list[str]:
    """Return the scanned page-image URLs for a paper PTR.

    The OCR chain in ``capitol_pipeline.processors.ocr`` only accepts PDFs, so
    these are recorded for a later needs-review pass rather than parsed here.
    """

    if report.kind != "paper":
        raise SenateEfdError(f"Report {report.report_id} is not a paper filing.")
    pages = list_paper_page_images(client.get_report_html(report.url), base_url=client.base_url)
    logger.info(
        "Deferring paper PTR %s (%s) with %d scanned page(s); OCR chain is PDF-only.",
        report.report_id,
        report.senator_name,
        len(pages),
    )
    return pages


def build_paper_report_stub(report: EfdReport, page_images: list[str] | None = None) -> dict[str, object]:
    """Describe a deferred paper filing for the run summary."""

    return {
        "reportId": report.report_id,
        "status": "needs_review",
        "reason": "paper_filing_requires_ocr",
        "member": report.senator_name,
        "office": report.office,
        "url": report.url,
        "submittedDate": report.submitted_date,
        "pageImages": page_images or [],
    }


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------


def normalize_efd_transaction(
    report: EfdReport,
    transaction: EfdTransaction,
    registry: MemberRegistry,
) -> NormalizedTradeRow | None:
    """Normalize one eFD transaction row into a site-ready trade row."""

    transaction_date = transaction.transaction_date
    if not transaction_date:
        return None

    member = registry.resolve(
        name=report.senator_name or None,
        first_name=report.first_name or None,
        last_name=report.last_name or None,
    )
    if not member or not member.id:
        return None

    ticker = transaction.ticker
    amount_min, amount_max = parse_efd_amount_range(transaction.amount_raw)
    transaction_type = normalize_senate_transaction_type(transaction.transaction_type_raw)
    asset_description = (transaction.asset_description or "").strip() or ticker or "Unknown asset"
    normalized_asset = classify_crypto_asset(ticker, asset_description)
    asset_type = normalize_efd_asset_type(transaction.asset_type_raw, normalized_asset.kind)

    comment_parts = [
        (transaction.comment or "").strip(),
        (transaction.asset_detail or "").strip(),
    ]
    comment = " | ".join(part for part in comment_parts if part) or None

    row = NormalizedTradeRow(
        member=member,
        source=EFD_SOURCE,
        disclosure_kind="senate-trade",
        source_id="",
        source_url=report.url,
        ticker=ticker,
        asset_description=asset_description,
        asset_type=asset_type,
        transaction_type=transaction_type,
        transaction_date=transaction_date,
        disclosure_date=report.submitted_date,
        amount_min=amount_min,
        amount_max=amount_max,
        owner=normalize_efd_owner(transaction.owner_raw),
        comment=comment,
        normalized_asset=None if normalized_asset.kind == "unrelated" else normalized_asset,
        parser_version="senate-efd-html-v1",
        # An electronic filing is structured data, not OCR: the table is exact.
        parser_confidence=1.0,
    )
    row.source_id = build_canonical_senate_trade_id(row)
    return row


def normalize_efd_report(
    report: EfdReport,
    transactions: list[EfdTransaction],
    registry: MemberRegistry,
) -> list[NormalizedTradeRow]:
    """Normalize every parsed transaction of one report, dropping unusable rows."""

    normalized: list[NormalizedTradeRow] = []
    for transaction in transactions:
        row = normalize_efd_transaction(report, transaction, registry)
        if row is not None:
            normalized.append(row)
    return normalized
