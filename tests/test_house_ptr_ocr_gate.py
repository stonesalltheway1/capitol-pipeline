"""The OCR gate in front of the House PTR text parser.

A typed PDF is parsed from its own text layer and never enters the OCR chain
(surya/docling + torch), whatever the segmenter makes of it. Only an
image-only scan is OCR'd, under a wall-clock cap, and a filing that yields no
rows carries a ``review_reason`` saying which of those it was.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import pytest

from capitol_pipeline.config import Settings
from capitol_pipeline.models.congress import FilingStub, HousePtrParseResult, MemberMatch
from capitol_pipeline.parsers import house_ptr


FIXTURES_DIR = Path(__file__).parent / "fixtures" / "house_ptr"
KHANNA_SCAN = FIXTURES_DIR / "9116206-pages-1-2.pdf"

TYPED_PTR_TEXT = "\n".join(
    [
        "Periodic Transaction Report",
        "Name: Hon. Roger Williams Status: Member State/District: TX25",
        "ID Owner Asset Transaction Type Date Notification Date Amount Cap. Gains > $200?",
        "Chevron Corporation Common Stock",
        "(CVX) [ST] S (partial) 12/22/2025 12/22/2025 $15,001 - $50,000",
        "Filing Status: New",
        "Subholding Of: Charles Schwab 4067",
        "JP Morgan Chase & Co. Common Stock",
        "(JPM) [ST] P 12/22/2025 12/22/2025 $1,001 - $15,000",
        "Filing Status: New",
        "Subholding Of: Charles Schwab 4067",
        "Filing ID #20033783",
    ]
)

TYPED_LETTER_TEXT = "\n".join(
    ["This is a typed cover letter from the member and it lists no transactions at all."] * 30
)


def _stub() -> FilingStub:
    return FilingStub(
        doc_id="20033783",
        filing_year=2026,
        filing_date="2026-01-05",
        member=MemberMatch(
            id="m-roger-williams",
            name="Roger Williams",
            slug="roger-williams",
            party="R",
            state="TX",
            district="25",
        ),
        source="house-clerk",
        source_url="https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20033783.pdf",
    )


def _typed_pdf(tmp_path: Path, text: str, *, pages: int = 1, name: str = "typed.pdf") -> Path:
    fitz = pytest.importorskip("fitz")
    document = fitz.open()
    for _ in range(pages):
        page = document.new_page(width=612, height=792)
        page.insert_text((36, 48), text, fontsize=7)
    pdf = tmp_path / name
    document.save(str(pdf))
    document.close()
    return pdf


def _image_only_pdf(tmp_path: Path, *, pages: int = 1, name: str = "scan.pdf") -> Path:
    fitz = pytest.importorskip("fitz")
    document = fitz.open()
    pixmap = fitz.Pixmap(fitz.csGRAY, fitz.IRect(0, 0, 64, 64), False)
    pixmap.clear_with(255)
    for _ in range(pages):
        page = document.new_page(width=612, height=792)
        page.insert_image(page.rect, pixmap=pixmap)
    pdf = tmp_path / name
    document.save(str(pdf))
    document.close()
    return pdf


def _forbid_ocr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        house_ptr,
        "_run_ocr_chain_capped",
        lambda *_a, **_k: pytest.fail("the OCR chain must not run on a typed PDF"),
    )

    class _NeverProcessor:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pytest.fail("OcrProcessor must not be built for a typed PDF in auto mode")

    monkeypatch.setattr(house_ptr, "OcrProcessor", _NeverProcessor)


# ---------------------------------------------------------------------------
# probe_text_layer
# ---------------------------------------------------------------------------


def test_probe_reports_a_typed_pdf(tmp_path: Path) -> None:
    probe = house_ptr.probe_text_layer(_typed_pdf(tmp_path, TYPED_PTR_TEXT, pages=3))
    assert probe is not None
    assert probe.page_count == 3
    assert probe.text_pages == 3
    assert probe.image_pages == 0
    assert probe.has_text_layer
    assert "Chevron Corporation Common Stock" in probe.text
    # Same assembly as PyMuPDFBackend: pages joined with a blank line.
    assert probe.text.count("Filing ID #20033783") == 3


def test_probe_reports_an_image_only_pdf(tmp_path: Path) -> None:
    probe = house_ptr.probe_text_layer(_image_only_pdf(tmp_path, pages=2))
    assert probe is not None
    assert probe.page_count == 2
    assert probe.text_pages == 0
    assert probe.image_pages == 2
    assert probe.chars == 0
    assert probe.text == ""
    assert not probe.has_text_layer
    assert probe.to_dict() == {
        "pageCount": 2,
        "textPages": 0,
        "imagePages": 2,
        "chars": 0,
        "hasTextLayer": False,
    }


def test_probe_needs_half_the_pages_to_carry_text(tmp_path: Path) -> None:
    fitz = pytest.importorskip("fitz")
    typed = fitz.open(str(_typed_pdf(tmp_path, TYPED_PTR_TEXT)))
    scan = fitz.open(str(_image_only_pdf(tmp_path, pages=3)))
    mixed = fitz.open()
    mixed.insert_pdf(typed)
    mixed.insert_pdf(scan)
    pdf = tmp_path / "mixed.pdf"
    mixed.save(str(pdf))
    probe = house_ptr.probe_text_layer(pdf)
    assert probe is not None
    assert (probe.page_count, probe.text_pages, probe.image_pages) == (4, 1, 3)
    assert not probe.has_text_layer


def test_probe_returns_none_for_a_file_pymupdf_cannot_open(tmp_path: Path) -> None:
    broken = tmp_path / "broken.pdf"
    broken.write_bytes(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\ntrailer\n%%EOF\n")
    assert house_ptr.probe_text_layer(broken) is None


def test_khanna_paper_filing_is_image_only() -> None:
    """Doc 9116206 (Ro Khanna, hand-delivered paper form): no text layer at all."""

    probe = house_ptr.probe_text_layer(KHANNA_SCAN)
    assert probe is not None
    assert probe.page_count == 2
    assert probe.text_pages == 0
    assert probe.image_pages == 2
    assert probe.chars == 0
    assert not probe.has_text_layer


# ---------------------------------------------------------------------------
# parse_house_ptr_pdf routing
# ---------------------------------------------------------------------------


def test_auto_parses_a_typed_pdf_from_its_text_layer_without_ocr(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _forbid_ocr(monkeypatch)
    parsed, rows = house_ptr.parse_house_ptr_pdf(
        _typed_pdf(tmp_path, TYPED_PTR_TEXT), stub=_stub(), backend="auto", vision_backend="off"
    )
    assert [row.ticker for row in rows] == ["CVX", "JPM"]
    assert rows[0].asset_description == "Chevron Corporation Common Stock"
    assert parsed.review_reason is None
    assert parsed.text_layer is not None
    assert parsed.text_layer["hasTextLayer"] is True
    assert parsed.text_layer["ocr"] == {"status": "skipped", "reason": "text layer present"}


def test_auto_sends_an_unsegmentable_typed_pdf_to_review_not_ocr(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _forbid_ocr(monkeypatch)
    parsed, rows = house_ptr.parse_house_ptr_pdf(
        _typed_pdf(tmp_path, TYPED_LETTER_TEXT), stub=_stub(), backend="auto", vision_backend="off"
    )
    assert rows == []
    assert parsed.review_reason is not None
    assert "has a text layer" in parsed.review_reason
    assert "OCR skipped" in parsed.review_reason
    assert parsed.text_layer is not None
    assert parsed.text_layer["ocr"]["status"] == "skipped"


def _capture_chain(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Path, float]]:
    """Replace the OCR chain with a recorder that returns a typed PTR text."""

    calls: list[tuple[Path, float]] = []

    def _fake_chain(
        pdf_path: Path, settings: Settings, backend: Any, cap: float
    ) -> tuple[str, dict]:
        calls.append((pdf_path, cap))
        return TYPED_PTR_TEXT, {
            "status": "finished",
            "capSeconds": cap,
            "chars": len(TYPED_PTR_TEXT),
        }

    monkeypatch.setattr(house_ptr, "_run_ocr_chain_capped", _fake_chain)
    return calls


def test_an_explicit_backend_ocrs_an_image_only_pdf_upright_under_the_cap(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    pytest.importorskip("fitz")
    calls = _capture_chain(monkeypatch)
    pdf = _image_only_pdf(tmp_path)
    settings = Settings(ptr_ocr_time_cap_seconds=123)
    parsed, rows = house_ptr.parse_house_ptr_pdf(
        pdf, stub=_stub(), settings=settings, backend="pymupdf", vision_backend="off"
    )
    # A scan still gets OCR'd and its text still parses, but what OCR is shown
    # is the upright render rather than the PDF as filed.
    assert len(calls) == 1
    ocr_path, cap = calls[0]
    assert cap == 123
    assert ocr_path != pdf
    assert ocr_path.name.endswith("-upright.pdf")
    assert [row.ticker for row in rows] == ["CVX", "JPM"]
    assert parsed.review_reason is None
    assert parsed.text_layer is not None
    assert parsed.text_layer["hasTextLayer"] is False
    assert parsed.text_layer["ocr"]["status"] == "finished"
    upright = parsed.text_layer["ocr"]["upright"]
    assert upright["applied"] is True
    assert upright["pages"] == 1
    assert upright["dpi"] == 200


def test_explicit_ocr_falls_back_to_the_filed_pdf_when_it_cannot_be_rendered_upright(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    calls = _capture_chain(monkeypatch)
    monkeypatch.setattr(house_ptr, "build_upright_pdf", lambda *_args, **_kwargs: None)
    pdf = _image_only_pdf(tmp_path)
    settings = Settings(ptr_ocr_time_cap_seconds=123)
    parsed, _rows = house_ptr.parse_house_ptr_pdf(
        pdf, stub=_stub(), settings=settings, backend="pymupdf", vision_backend="off"
    )
    assert calls == [(pdf, 123)]
    assert parsed.text_layer is not None
    assert parsed.text_layer["ocr"]["upright"] == {
        "applied": False,
        "reason": "could not render upright",
    }


def test_upright_render_can_be_switched_off(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("CAPITOL_PTR_UPRIGHT_OCR", "0")
    calls = _capture_chain(monkeypatch)
    pdf = _image_only_pdf(tmp_path)
    settings = Settings(ptr_ocr_time_cap_seconds=123)
    parsed, _rows = house_ptr.parse_house_ptr_pdf(
        pdf, stub=_stub(), settings=settings, backend="pymupdf", vision_backend="off"
    )
    assert calls == [(pdf, 123)]
    assert parsed.text_layer is not None
    assert "upright" not in parsed.text_layer["ocr"]


def test_explicit_ocr_reports_a_capped_run_as_the_review_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        house_ptr,
        "_run_ocr_chain_capped",
        lambda *_a, **_k: ("", {"status": "timeout", "capSeconds": 240}),
    )
    parsed, rows = house_ptr.parse_house_ptr_pdf(
        KHANNA_SCAN, stub=_stub(), backend="pymupdf", vision_backend="off"
    )
    assert rows == []
    assert parsed.review_reason == (
        "PDF is image-only (2 pages, 2 carrying a page image, no text layer); "
        "OCR exceeded the 240s cap"
    )
    assert parsed.text_layer is not None
    assert parsed.text_layer["ocr"]["status"] == "timeout"


def test_explicit_ocr_reports_text_that_yields_no_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    junk = "| 9 984 F 1 | Sale | 1 | " * 30
    monkeypatch.setattr(
        house_ptr,
        "_run_ocr_chain_capped",
        lambda *_a, **_k: (junk, {"status": "finished", "capSeconds": 240, "chars": len(junk)}),
    )
    parsed, rows = house_ptr.parse_house_ptr_pdf(
        KHANNA_SCAN, stub=_stub(), backend="pymupdf", vision_backend="off"
    )
    assert rows == []
    assert parsed.review_reason is not None
    assert parsed.review_reason.startswith("PDF is image-only")
    assert "yielded no transaction rows" in parsed.review_reason


def test_explicit_backend_on_a_typed_pdf_goes_straight_to_the_processor(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Naming a backend bypasses the gate: the caller asked for that backend.

    On a typed PDF that means OcrProcessor with no upright render and no cap.
    A scan takes the upright chain instead, because every backend reads a
    sideways page as noise and most House scans are stored rotated 270
    degrees -- an escape hatch that returns noise is not an escape hatch.
    """

    monkeypatch.setattr(
        house_ptr,
        "_run_ocr_chain_capped",
        lambda *_a, **_k: pytest.fail("a typed PDF runs through OcrProcessor"),
    )
    built: list[Any] = []

    class _Processor:
        def __init__(self, *_args: Any, **kwargs: Any) -> None:
            built.append(kwargs.get("backend"))

        def process_file(self, _path: Path) -> Any:
            class _Doc:
                ocrText = TYPED_PTR_TEXT

            class _Result:
                document = _Doc()

            return _Result()

    monkeypatch.setattr(house_ptr, "OcrProcessor", _Processor)
    parsed, rows = house_ptr.parse_house_ptr_pdf(
        _typed_pdf(tmp_path, TYPED_PTR_TEXT, name="typed-explicit.pdf"),
        stub=_stub(),
        backend="surya",
        vision_backend="off",
    )
    assert built == ["surya"]
    assert len(rows) == 2
    # The probe still ran, but no OCR decision was taken on the caller's behalf.
    assert parsed.text_layer is not None
    assert parsed.text_layer["hasTextLayer"] is True
    assert parsed.text_layer["ocr"]["status"] == "explicit"
    assert parsed.text_layer["ocr"]["backend"] == "surya"


# ---------------------------------------------------------------------------
# The time cap itself
# ---------------------------------------------------------------------------


def test_run_with_time_cap_returns_the_child_result() -> None:
    status, value = house_ptr.run_with_time_cap(os.getpid, (), 60)
    assert status == "finished"
    assert isinstance(value, int)
    assert value != os.getpid()


def test_run_with_time_cap_kills_a_runaway_worker() -> None:
    started = time.monotonic()
    status, value = house_ptr.run_with_time_cap(time.sleep, (60,), 0.5)
    assert status == "timeout"
    assert value is None
    assert time.monotonic() - started < 30


def test_run_with_time_cap_reports_a_crash() -> None:
    status, value = house_ptr.run_with_time_cap(int, ("not a number",), 60)
    assert status == "crashed"
    assert "ValueError" in str(value)


def test_ocr_chain_runs_inline_when_the_cap_is_disabled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class _Processor:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def process_file(self, _path: Path) -> Any:
            class _Doc:
                ocrText = "some ocr text"

            class _Result:
                document = _Doc()

            return _Result()

    monkeypatch.setattr(house_ptr, "OcrProcessor", _Processor)
    text, report = house_ptr._run_ocr_chain_capped(
        _image_only_pdf(tmp_path), Settings(), "auto", cap_seconds=0
    )
    assert text == "some ocr text"
    assert report["status"] == "finished"
    assert report["capSeconds"] == 0


# ---------------------------------------------------------------------------
# What the stub records
# ---------------------------------------------------------------------------


def test_describe_review_reason_covers_each_outcome() -> None:
    typed = house_ptr.TextLayerProbe(page_count=3, text_pages=3, image_pages=0, chars=900, text="x")
    scan = house_ptr.TextLayerProbe(page_count=32, text_pages=0, image_pages=32, chars=0, text="")
    assert house_ptr.describe_review_reason(None, None, "") == (
        "PDF could not be opened for a text-layer probe and no transaction rows were segmented"
    )
    assert house_ptr.describe_review_reason(typed, {"status": "skipped"}, "x") == (
        "PDF has a text layer (900 chars on 3/3 pages) but no transaction rows could be "
        "segmented; OCR skipped"
    )
    base = "PDF is image-only (32 pages, 32 carrying a page image, no text layer)"
    assert house_ptr.describe_review_reason(scan, None, "") == f"{base}; no OCR ran"
    assert house_ptr.describe_review_reason(scan, {"status": "timeout", "capSeconds": 240}, "") == (
        f"{base}; OCR exceeded the 240s cap"
    )
    assert house_ptr.describe_review_reason(scan, {"status": "crashed", "error": "boom"}, "") == (
        f"{base}; OCR failed: boom"
    )
    assert house_ptr.describe_review_reason(scan, {"status": "finished"}, "") == (
        f"{base}; OCR produced no text"
    )
    assert house_ptr.describe_review_reason(scan, {"status": "finished"}, "abc") == (
        f"{base}; OCR text (3 chars) yielded no transaction rows"
    )


def test_stub_last_error_keeps_the_queue_prefix_and_adds_the_reason() -> None:
    from capitol_pipeline.cli import (
        HOUSE_REVIEW_LAST_ERROR,
        build_house_stub_metadata_extra,
        house_stub_last_error,
    )

    empty = HousePtrParseResult(
        doc_id="9116206", review_reason="PDF is image-only; OCR exceeded the 240s cap"
    )
    assert house_stub_last_error(empty) == (
        f"{HOUSE_REVIEW_LAST_ERROR}: PDF is image-only; OCR exceeded the 240s cap"
    )
    assert house_stub_last_error(HousePtrParseResult(doc_id="1")) == HOUSE_REVIEW_LAST_ERROR
    parsed_with_rows = HousePtrParseResult(
        doc_id="1",
        transactions=[
            house_ptr.HousePtrTransaction(
                line_number=1,
                asset_description="Chevron Corporation Common Stock",
                ticker="CVX",
                asset_type="Stock",
                transaction_type="sale",
            )
        ],
    )
    assert house_stub_last_error(parsed_with_rows) is None
    assert build_house_stub_metadata_extra(HousePtrParseResult(doc_id="1")) is None
    assert build_house_stub_metadata_extra(
        HousePtrParseResult(doc_id="1", text_layer={"hasTextLayer": False})
    ) == {"textLayer": {"hasTextLayer": False}}


# ---------------------------------------------------------------------------
# There is no text-model fallback, and OCR is out of the scanned path
# ---------------------------------------------------------------------------


def test_house_ptr_carries_no_text_model_fallback() -> None:
    """The parser must not be able to hand a whole PDF to a text model.

    One did, and it invented filings: 1,876 rows across 112 documents, with
    plausible tickers for companies that do not exist, attributed to named
    members out of disclosures they had filed on time. It also stamped
    ``parser_confidence`` 1.000 on 710 of them, so the confidence gate never
    held them back. An env var is not a guardrail; the absence of the call
    site is.
    """

    source = Path(house_ptr.__file__).read_text(encoding="utf-8")
    assert "ptr_llm_fallback" not in source
    assert "extract_via_haiku" not in source
    assert not hasattr(house_ptr, "extract_via_haiku")
    assert not hasattr(house_ptr, "_run_llm_fallback")
    assert not hasattr(house_ptr, "LLM_PARSER_VERSION")


def test_typed_pdf_with_unsegmentable_text_goes_to_review_not_a_model(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _forbid_ocr(monkeypatch)
    # Readable text the segmenter cannot split. It used to go to a text model.
    # It goes to a person.
    letter = _typed_pdf(tmp_path, TYPED_LETTER_TEXT)
    parsed, rows = house_ptr.parse_house_ptr_pdf(
        letter, stub=_stub(), backend="auto", vision_backend="off"
    )
    assert rows == []
    assert parsed.transactions == []
    assert parsed.review_reason is not None
    assert "OCR skipped" in parsed.review_reason


def test_image_only_pdf_on_auto_does_not_run_ocr(monkeypatch: pytest.MonkeyPatch) -> None:
    """A scan skips the OCR chain entirely on ``auto``.

    Measured on the box: the OCR text is not an input to the vision prompt,
    not a cross-check on its result and not the source of the text-layer
    probe, and it has never produced a transaction row from an image-only
    filing. It did cost 3 h 16 min of CPU in 29 minutes of wall clock on one
    twelve-filing run, beside the Postgres three sites share.
    """

    monkeypatch.setattr(
        house_ptr,
        "_run_ocr_chain_capped",
        lambda *_a, **_k: pytest.fail("the OCR chain must not run on a scan in auto mode"),
    )
    monkeypatch.setattr(
        house_ptr,
        "_ocr_image_only_pdf",
        lambda *_a, **_k: pytest.fail("the OCR chain must not run on a scan in auto mode"),
    )

    class _NeverProcessor:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pytest.fail("OcrProcessor must not be built for a scan in auto mode")

    monkeypatch.setattr(house_ptr, "OcrProcessor", _NeverProcessor)

    parsed, rows = house_ptr.parse_house_ptr_pdf(
        KHANNA_SCAN, stub=_stub(), backend="auto", vision_backend="off"
    )
    assert rows == []
    assert parsed.vision_report is None
    assert parsed.text_layer is not None
    assert parsed.text_layer["ocr"]["status"] == "skipped"
    assert parsed.review_reason == (
        "PDF is image-only (2 pages, 2 carrying a page image, no text layer); "
        "OCR is not run on scans and the vision path produced no rows"
    )


def test_image_only_pdf_with_an_explicit_backend_still_ocrs_the_upright_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Naming a backend is the one flag that puts OCR back on a scan."""

    seen: list[Path] = []

    def _fake(pdf_path: Path, *_a: Any, **_k: Any) -> tuple[str, dict[str, Any]]:
        seen.append(Path(pdf_path))
        return "", {"status": "finished", "capSeconds": 240, "chars": 0}

    monkeypatch.setattr(house_ptr, "_ocr_image_only_pdf", _fake)
    parsed, rows = house_ptr.parse_house_ptr_pdf(
        KHANNA_SCAN, stub=_stub(), backend="pymupdf", vision_backend="off"
    )
    assert seen == [KHANNA_SCAN]
    assert rows == []
    assert parsed.review_reason is not None
    assert parsed.review_reason.startswith("PDF is image-only")


def test_image_only_pdf_with_vision_auto_goes_to_vision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from capitol_pipeline.parsers.ptr_vision import VISION_PARSER_VERSION
    from test_ptr_vision import CHEVRON_VISION_ROW, _enable, _install_fake_client, _payload

    _enable(monkeypatch)
    _, calls = _install_fake_client(monkeypatch, _payload(CHEVRON_VISION_ROW))
    parsed, rows = house_ptr.parse_house_ptr_pdf(
        KHANNA_SCAN, stub=_stub(), backend="auto", vision_backend="auto"
    )
    assert calls, "the vision model must be called for an image-only PDF"
    assert parsed.parser_version == VISION_PARSER_VERSION
    assert [row.ticker for row in rows] == ["CVX"]
    assert parsed.text_layer is not None
    assert parsed.text_layer["ocr"]["status"] == "skipped"


def test_image_only_pdf_with_vision_unavailable_lands_in_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from test_ptr_vision import CHEVRON_VISION_ROW, _enable, _install_fake_client, _payload

    _enable(monkeypatch)
    monkeypatch.setenv("CAPITOL_PTR_VISION_DISABLED", "1")
    _, calls = _install_fake_client(monkeypatch, _payload(CHEVRON_VISION_ROW))
    parsed, rows = house_ptr.parse_house_ptr_pdf(
        KHANNA_SCAN, stub=_stub(), backend="auto", vision_backend="auto"
    )
    assert calls == []
    assert rows == []
    assert isinstance(parsed.vision_report, dict)
    assert parsed.vision_report["ok"] is False
    assert parsed.review_reason == (
        "PDF is image-only (2 pages, 2 carrying a page image, no text layer); "
        "OCR is not run on scans and the vision path produced no rows"
    )
