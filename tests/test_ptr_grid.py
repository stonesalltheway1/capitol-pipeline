"""The classical checkbox detector, on synthetic grids and on real pages.

The real pages are grayscale renders of upright House PTR pages at the
production zoom (long edge 1567 px), covering both form styles: the paper
form (Malliotakis 8219905, Burlison 8219414, Kelly 8219843, Lamborn 8220068,
Luetkemeyer 8220100, Rogers 8219362) and the brokerage-grid attachments
(Khanna 8219444 pages 2, 12 and 23). Expected letters are what a human reads
off the page.
"""

from __future__ import annotations

from pathlib import Path

import pytest

np = pytest.importorskip("numpy")
fitz = pytest.importorskip("fitz")

from capitol_pipeline.parsers import ptr_grid  # noqa: E402
from capitol_pipeline.parsers.ptr_grid import (  # noqa: E402
    align_rows,
    analyze_amount_grid,
    classify_bands,
    detect_page,
    draw_synthetic_grid,
    gray_from_pixmap,
)

FIXTURES = Path(__file__).parent / "fixtures" / "ptr_pages"


def _load(name: str):
    pixmap = fitz.Pixmap(str(FIXTURES / f"{name}.png"))
    return gray_from_pixmap(pixmap.samples, pixmap.width, pixmap.height, pixmap.n)


def _candidates(gray) -> list[str | None]:
    analysis = analyze_amount_grid(gray)
    assert analysis is not None
    return [
        band["letter"] if band["kind"] == "marked" else "?"
        for band in classify_bands(analysis)
        if band["kind"] in ("marked", "ambiguous")
    ]


# ---------------------------------------------------------------------------
# Synthetic grids
# ---------------------------------------------------------------------------


def test_synthetic_ruled_grid_reads_every_tick() -> None:
    page = draw_synthetic_grid(marks={0: 2, 1: 1, 2: 1, 3: 0, 5: 3}, ambiguous_rows=(4,))
    analysis = analyze_amount_grid(page)
    assert analysis is not None
    assert len(analysis["columns"]) == 11
    bands = classify_bands(analysis)
    assert [(band["kind"], band["letter"]) for band in bands if band["kind"] != "text"] == [
        ("marked", "C"),
        ("marked", "B"),
        ("marked", "B"),
        ("marked", "A"),
        ("ambiguous", None),
        ("marked", "D"),
    ]
    result = detect_page(analysis, expected_rows=6)
    assert result["status"] == "ok"
    assert [entry["letter"] for entry in result["letters"]] == ["C", "B", "B", "A", None, "D"]
    assert result["letters"][4]["kind"] == "ambiguous"


def test_synthetic_paper_form_drops_the_example_row() -> None:
    page = draw_synthetic_grid(marks={0: 0, 1: 0}, rows=4, example_row=True, box_style=True)
    analysis = analyze_amount_grid(page)
    assert analysis is not None
    assert len(analysis["columns"]) == 11
    bands = classify_bands(analysis)
    ticks = [band for band in bands if band["kind"] in ("marked", "ambiguous")]
    assert [band["letter"] for band in ticks] == ["B", "A", "A"]  # example row first
    aligned = align_rows(bands, expected_rows=2)
    assert aligned is not None and [band["letter"] for band in aligned] == ["A", "A"]
    # Two rows but three ticks whose first is not B: no alignment.
    page = draw_synthetic_grid(marks={0: 0, 1: 0, 2: 3}, rows=4, box_style=True)
    analysis = analyze_amount_grid(page)
    assert analysis is not None
    assert align_rows(classify_bands(analysis), expected_rows=2) is None
    assert detect_page(analysis, expected_rows=2)["status"] == "unaligned"


def test_synthetic_wide_k_column_is_kept() -> None:
    page = draw_synthetic_grid(marks={0: 10, 1: 5}, rows=3, wide_last_column=True)
    analysis = analyze_amount_grid(page)
    assert analysis is not None
    assert len(analysis["columns"]) == 11
    assert _candidates(page) == ["K", "F"]


def test_detect_page_statuses() -> None:
    assert detect_page(None, expected_rows=3)["status"] == "no-grid"
    page = draw_synthetic_grid(marks={0: 1}, rows=5)
    analysis = analyze_amount_grid(page)
    assert analysis is not None
    assert detect_page(analysis, expected_rows=0)["status"] == "no-rows"
    assert detect_page(analysis, expected_rows=1)["letters"] == [{"letter": "B", "kind": "marked"}]
    assert detect_page(analysis, expected_rows=4)["status"] == "unaligned"


def test_blank_page_has_no_grid() -> None:
    blank = np.full((1210, 1568), 255, dtype=np.uint8)
    assert analyze_amount_grid(blank) is None
    assert analyze_amount_grid(np.zeros((10, 10), dtype=np.uint8)) is None


# ---------------------------------------------------------------------------
# Real pages
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "columns", "ticks"),
    [
        # Khanna brokerage-grid attachments: the page-12 shift the model made twice.
        ("8219444_p12", 11, list("CCCCC") + list("B" * 17)),
        ("8219444_p23", 11, list("A" * 12)),
        ("8219444_p2", 11, list("AADAAAAA")),
        # Paper forms: the pre-printed example row (B) comes first.
        ("8219414_p1", 11, ["B", "B", "B", "A", "B"]),
        ("8219414_p2", 11, ["A"]),
        ("8219905_p1", 11, ["B", "A", "A"]),
        ("8220068_p1", 11, ["B", "B", "B", "B", "B"]),
        ("8220068_p2", 11, ["B", "B"]),
        ("8219843_p1", 11, ["B", "A"]),
    ],
)
def test_real_pages_read_the_ticked_columns(name: str, columns: int, ticks: list[str]) -> None:
    gray = _load(name)
    analysis = analyze_amount_grid(gray)
    assert analysis is not None, name
    assert len(analysis["columns"]) == columns
    assert _candidates(gray) == ticks


def test_real_paper_form_aligns_after_dropping_the_example_row() -> None:
    # Burlison page 1: four handwritten rows under the example row.
    result = detect_page(analyze_amount_grid(_load("8219414_p1")), expected_rows=4)
    assert result["status"] == "ok"
    assert [entry["letter"] for entry in result["letters"]] == ["B", "B", "A", "B"]
    # Khanna page 12: twenty-two typed rows, no example row.
    result = detect_page(analyze_amount_grid(_load("8219444_p12")), expected_rows=22)
    assert result["status"] == "ok"
    assert [entry["letter"] for entry in result["letters"]] == list("CCCCC") + list("B" * 17)


def test_older_form_with_ten_columns_and_no_rows() -> None:
    # Rogers: the A-J ladder, only the example row ticked.
    gray = _load("8219362_p1")
    analysis = analyze_amount_grid(gray)
    assert analysis is not None
    assert len(analysis["columns"]) == 10
    assert _candidates(gray) == ["B"]
    assert detect_page(analysis, expected_rows=0)["status"] == "no-rows"


def test_luetkemeyer_page_finds_the_ladder() -> None:
    analysis = analyze_amount_grid(_load("8220100_p1"))
    assert analysis is not None
    assert len(analysis["columns"]) in (10, 11)


def test_constants_are_sane() -> None:
    assert ptr_grid.AMOUNT_LETTERS == "ABCDEFGHIJK"
    assert 0 < ptr_grid.CELL_MARGIN < 0.5
    assert ptr_grid.MIN_LADDER_RULES <= ptr_grid.MAX_LADDER_RULES


# ---------------------------------------------------------------------------
# The Type block: Purchase against Sale
# ---------------------------------------------------------------------------
#
# Every expectation below is what two independent blind transcriptions of the
# page agreed on, and each was then confirmed by eye on the rendered page. The
# reads never saw the database, the pipeline's own result, or each other.


def _types(name: str, expected_rows: int) -> list[str]:
    result = detect_page(analyze_amount_grid(_load(name)), expected_rows=expected_rows)
    assert result["status"] == "ok", (name, result["status"])
    return [entry["kind"] for entry in result["types"]]


def test_type_columns_are_found_on_both_layouts() -> None:
    for name in ("8221322_p2", "8221322_p19", "8221358_p20", "8221360_p2", "9116141_p2"):
        analysis = analyze_amount_grid(_load(name))
        assert analysis is not None, name
        columns = analysis["typeColumns"]
        assert columns is not None, name
        assert len(columns) == 2, name
        # Purchase then Sale, adjacent, left of the ladder.
        assert columns[0][1] == columns[1][0], name
        assert columns[1][1] < analysis["columns"][0][0], name


def test_the_two_pages_both_model_reads_got_wrong() -> None:
    # 8221358 page 20 and 8221322 page 19 are the pages where gemini-3.8-flash
    # and gemini-3.5-flash BOTH took the Type column one column to the left and
    # returned purchase for rows the form ticks under Sale. Between them they
    # are most of the 623 wrong transaction types that were published.
    assert _types("8221322_p19", 19) == ["sale"] * 19
    assert set(_types("8221358_p20", 18)) <= {"sale", "none", "ambiguous"}
    assert "purchase" not in _types("8221358_p20", 18)


def test_mixed_page_reads_purchase_then_sale() -> None:
    # 8221322 page 2: six purchases, then thirteen sales. A page that is all
    # one answer cannot tell a working detector from one that is stuck.
    kinds = _types("8221322_p2", 19)
    assert kinds[:6] == ["purchase"] * 6
    assert set(kinds[6:]) <= {"sale", "none", "ambiguous"}
    assert "purchase" not in kinds[6:]


def test_house_form_reads_every_row_as_purchase() -> None:
    assert _types("9116141_p2", 26) == ["purchase"] * 26


def test_house_form_partial_sale_is_not_read_as_a_purchase() -> None:
    # 8221360 page 2 is the other House layout, which gives Partial Sale its
    # own column: Micron and the Treasury bill are Purchase, the other five are
    # Partial Sale. A partial sale leaves both columns this reads empty, so the
    # detector must be silent on them and must never call them purchases.
    kinds = _types("8221360_p2", 7)
    assert kinds[0] == "purchase"
    assert kinds[4] == "purchase"
    assert [k for i, k in enumerate(kinds) if i not in (0, 4)] == ["none"] * 5


def test_a_runt_band_does_not_shift_every_row_on_the_page() -> None:
    # 8221360 page 2 carries a five-pixel band among bands 25 to 31 pixels
    # tall, which made eight candidates for seven rows. The example-row rule
    # then dropped Micron Technology and every row after it took the next
    # row's amount: five of seven wrong. Runt bands go first now.
    result = detect_page(analyze_amount_grid(_load("8221360_p2")), expected_rows=7)
    assert result["status"] == "ok"
    assert [entry["letter"] for entry in result["letters"]] == ["B", "B", "C", "B", "F", "C", "D"]


def test_a_page_that_does_not_show_the_shape_says_nothing() -> None:
    # The detector must decline rather than guess. 8219362 is the older ten
    # column form; whatever it finds, it may not invent a Purchase/Sale pair
    # that is not two adjacent narrow columns left of the ladder.
    analysis = analyze_amount_grid(_load("8219362_p1"))
    assert analysis is not None
    columns = analysis["typeColumns"]
    assert columns is None or (len(columns) == 2 and columns[1][1] < analysis["columns"][0][0])


def test_a_header_block_does_not_stop_a_page_aligning() -> None:
    # 8221360 page 3 has six candidate bands for four rows: the amount block's
    # letter-and-range header (88 px), the pre-printed example row (10 px), and
    # the four real rows (25-32 px). The example-row rule only ever removed one
    # extra, so the page did not align at all -- and an unaligned page gets no
    # type check, which is how Paycom Software and Block Inc Class A came to be
    # published as purchases off a form that ticks Sale on both.
    result = detect_page(analyze_amount_grid(_load("8221360_p3")), expected_rows=4)
    assert result["status"] == "ok"
    assert [entry["letter"] for entry in result["letters"]] == ["B", "C", "C", "C"]
    # Paycom and Block Class A are Sales; Take-Two and Block A Class are ticked
    # under Partial Sale, which leaves both columns this reads empty.
    assert [entry["kind"] for entry in result["types"]] == ["sale", "sale", "none", "none"]
