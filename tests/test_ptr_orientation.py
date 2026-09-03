"""The free orientation pick: the checkbox detector instead of a model call.

Eighty-seven of the 108 House scans in the review queue are stored rotated 270
degrees, and every reader -- OCR or vision -- returns noise on a sideways page.
The rotation used to cost a pair of Haiku calls per page. These tests pin the
replacement: the detector's own ladder, scored at four rotations, plus the
filing-wide consensus that settles the half turn a single page cannot.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from capitol_pipeline.parsers import ptr_grid, ptr_vision
from capitol_pipeline.parsers.ptr_upright import build_upright_pdf, upright_ocr_enabled


def _analysis(
    *,
    columns: int = 11,
    width: int = 1500,
    right_edge: int = 1450,
    header: bool = True,
    wide_last: bool = True,
    bands: int = 10,
) -> dict[str, Any]:
    """A plausible ``analyze_amount_grid`` result, tuned per test."""

    pitch = 40
    last_width = pitch * 2 if wide_last else pitch
    xs: list[tuple[int, int]] = []
    x = right_edge - last_width - pitch * (columns - 1)
    for index in range(columns):
        span = last_width if index == columns - 1 else pitch
        xs.append((x, x + span))
        x += span
    return {
        "width": width,
        "height": 1100,
        "columns": xs,
        "y0": 100,
        "y1": 900,
        "hrules": [],
        "bands": [{"y0": 0, "y1": 1, "densities": []} for _ in range(bands)],
        "baseline": 0.0,
        "headerEnd": 180 if header else 100,
        "captionEnd": 200,
    }


# ---------------------------------------------------------------------------
# The score
# ---------------------------------------------------------------------------


def test_no_ladder_scores_zero() -> None:
    assert ptr_grid.orientation_score(None) == 0.0
    assert ptr_grid.orientation_score({}) == 0.0
    assert ptr_grid.orientation_score({"columns": [], "width": 100}) == 0.0


def test_an_upright_page_outscores_the_same_page_upside_down() -> None:
    upright = ptr_grid.orientation_score(_analysis())
    # Upside down: the header text lands below the rows, the ladder no longer
    # reaches the right margin, and the wide K column is at the wrong end.
    flipped = ptr_grid.orientation_score(
        _analysis(header=False, wide_last=False, right_edge=700)
    )
    assert upright > flipped
    assert upright - flipped > 2.0


def test_each_signal_moves_the_score_on_its_own() -> None:
    full = ptr_grid.orientation_score(_analysis())
    assert full > ptr_grid.orientation_score(_analysis(header=False))
    assert full > ptr_grid.orientation_score(_analysis(wide_last=False))
    assert full > ptr_grid.orientation_score(_analysis(right_edge=800))
    assert full > ptr_grid.orientation_score(_analysis(columns=9))
    assert full > ptr_grid.orientation_score(_analysis(bands=1))


# ---------------------------------------------------------------------------
# The choice
# ---------------------------------------------------------------------------


def test_a_page_with_no_ladder_falls_back_to_the_heuristic() -> None:
    chosen = ptr_vision.choose_orientations([{0: 0.0, 90: 0.0, 180: 0.0, 270: 0.0}], [90])
    assert chosen == [(90, "heuristic")]


def test_a_clear_winner_wins() -> None:
    chosen = ptr_vision.choose_orientations([{0: 0.5, 90: 0.0, 180: 0.0, 270: 4.9}], [90])
    assert chosen == [(270, "grid")]


def test_the_filing_settles_a_half_turn_that_is_nearly_a_tie() -> None:
    # Four pages read upright at 270; one page's own best is the half turn, by
    # three hundredths. On a ruled brokerage grid that is noise, not evidence.
    pages = [{0: 0.0, 90: 0.0, 180: 0.0, 270: 4.9} for _ in range(4)]
    pages.append({0: 0.0, 90: 4.93, 180: 0.0, 270: 4.90})
    chosen = ptr_vision.choose_orientations(pages, [90] * len(pages))
    assert chosen[-1] == (270, "grid-consensus")
    assert {entry[0] for entry in chosen} == {270}


def test_a_page_the_filing_only_just_disagrees_with_follows_the_filing() -> None:
    # Measured on the queue: a dense brokerage grid scores nearly the same at
    # 0 and at 270, because a ruled table is a ruled table either way round.
    # Eight hundredths is not evidence; sixteen other pages are.
    pages = [{0: 0.0, 90: 0.0, 180: 4.9, 270: 4.98} for _ in range(6)]
    pages.append({0: 5.06, 90: 0.0, 180: 4.9, 270: 4.98})
    chosen = ptr_vision.choose_orientations(pages, [90] * len(pages))
    assert chosen[-1] == (270, "grid-consensus")


def test_a_page_that_clearly_differs_is_left_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    # Khanna 8220534 page 10: its own best beats the filing's by 0.83, which
    # is past the margin, so the page keeps the rotation it argued for.
    pages = [{0: 0.0, 90: 0.0, 180: 0.0, 270: 4.9} for _ in range(9)]
    pages.append({0: 5.32, 90: 0.0, 180: 4.74, 270: 4.50})
    chosen = ptr_vision.choose_orientations(pages, [90] * len(pages))
    assert chosen[-1] == (0, "grid")


def test_a_page_that_really_does_differ_keeps_its_own_rotation() -> None:
    pages = [{0: 0.0, 90: 0.0, 180: 0.0, 270: 4.9} for _ in range(4)]
    pages.append({0: 4.9, 90: 0.0, 180: 0.0, 270: 0.0})
    chosen = ptr_vision.choose_orientations(pages, [90] * len(pages))
    assert chosen[-1] == (0, "grid")


def test_a_blank_page_follows_the_filing_when_the_quarter_turn_agrees() -> None:
    # A cover sheet or a broker's letter has no amount ladder. The pages of
    # one filing went through the scanner together, so the filing decides --
    # but only when it agrees with the page about which way is long.
    pages = [{0: 0.0, 90: 0.0, 180: 0.0, 270: 4.9} for _ in range(3)]
    pages.append({0: 0.0, 90: 0.0, 180: 0.0, 270: 0.0})
    chosen = ptr_vision.choose_orientations(pages, [90, 90, 90, 90])
    assert chosen[-1] == (270, "grid-consensus")

    # A landscape page (heuristic 0) does not take a quarter turn from the
    # portrait pages around it.
    chosen = ptr_vision.choose_orientations(pages, [90, 90, 90, 0])
    assert chosen[-1] == (0, "heuristic")


def test_the_consensus_never_overrides_a_rotation_with_no_ladder_at_all() -> None:
    pages = [{0: 0.0, 90: 0.0, 180: 0.0, 270: 4.9}, {0: 4.9, 90: 0.0, 180: 0.0, 270: 0.0}]
    chosen = ptr_vision.choose_orientations(pages, [90, 90])
    assert chosen == [(270, "grid"), (0, "grid")]


# ---------------------------------------------------------------------------
# End to end, on a real PDF
# ---------------------------------------------------------------------------


def _write_pdf(tmp_path: Path, *, portrait: bool = True, pages: int = 1) -> Path:
    fitz = pytest.importorskip("fitz")
    document = fitz.open()
    width, height = (612, 792) if portrait else (792, 612)
    for index in range(pages):
        page = document.new_page(width=width, height=height)
        page.insert_text((72, 100), f"PERIODIC TRANSACTION REPORT page {index + 1}", fontsize=18)
    pdf = tmp_path / "scan.pdf"
    document.save(str(pdf))
    document.close()
    return pdf


def test_preparing_pages_makes_no_model_call_by_default(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("CAPITOL_PTR_VISION_ORIENTATION", raising=False)

    class _Forbidden:
        name = "forbidden"
        read_model = orientation_model = read_model_b = "none"

        def ask_short(self, *_args: Any, **_kwargs: Any) -> Any:  # pragma: no cover
            raise AssertionError("the default orientation pick must not call a model")

    prepared = ptr_vision.prepare_page_images(_write_pdf(tmp_path, pages=2), _Forbidden())

    assert prepared is not None
    pages, orientation, usage, total = prepared
    assert total == 2 and len(pages) == 2
    assert usage == {"input": 0, "cache_read": 0, "cache_write": 0, "output": 0}
    # A blank test page carries no amount ladder, so the heuristic decides.
    assert [entry["method"] for entry in orientation] == ["heuristic", "heuristic"]
    assert [entry["rotation"] for entry in orientation] == [90, 90]


def test_the_upright_copy_is_a_real_pdf_of_the_same_length(tmp_path: Path) -> None:
    fitz = pytest.importorskip("fitz")
    source = _write_pdf(tmp_path, pages=3)
    destination = tmp_path / "upright.pdf"

    report = build_upright_pdf(source, destination)

    assert report is not None
    assert report["pages"] == 3
    assert report["dpi"] == 200
    assert report["rotations"] == [90, 90, 90]  # portrait scan of a landscape form
    assert destination.exists()
    with fitz.open(str(destination)) as document:
        assert document.page_count == 3
        page = document[0]
        # Rendered upright means landscape, and at 200 DPI rather than 150.
        assert page.rect.width > page.rect.height


def test_the_upright_render_can_be_switched_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CAPITOL_PTR_UPRIGHT_OCR", raising=False)
    assert upright_ocr_enabled() is True
    monkeypatch.setenv("CAPITOL_PTR_UPRIGHT_OCR", "0")
    assert upright_ocr_enabled() is False


def test_a_filing_longer_than_the_cap_is_not_rendered(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    pytest.importorskip("fitz")
    monkeypatch.setattr("capitol_pipeline.parsers.ptr_upright.UPRIGHT_MAX_PAGES", 2)
    source = _write_pdf(tmp_path, pages=3)
    assert build_upright_pdf(source, tmp_path / "upright.pdf") is None
