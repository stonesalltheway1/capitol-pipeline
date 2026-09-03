"""Classical checkbox detector for the House PTR amount ladder.

Two reads of the same page by the same model share the same blind spots: on a
dense brokerage-grid attachment both reads put every tick one amount column
too far left, and the agreement check waved it through. This module is the
independent signal: plain image processing (numpy over the pymupdf pixmap, no
model) that finds the amount ladder's column rules, the ink runs between them
that are the ticked rows, and the ink in each cell, and names the ticked
column A-K.

It handles both layouts that reach the vision path:

* the House paper form (landscape once upright, a ladder of drawn boxes A-K at
  the right edge, a pre-printed example row ticked in column B), and
* the brokerage-grid attachments some filers staple behind it (a thin-ruled
  table, one row per trade, the same A-K ladder at the right).

How it reads a page
-------------------
1. Vertical rules: columns with a long enough dark run and enough ink
   overall, in the right-hand part of the page. Strong (long) rules keep
   their exact position and absorb the drawn box edges beside them; weak
   (short) runs far from any strong rule are kept for the attachments'
   broken rules.
2. The ladder: the longest run of rules whose pitch changes by at most 20%
   from one column to the next (the scans skew, so the pitch drifts). A run
   longer than twelve rules keeps its rightmost twelve; a wider final column
   (K) just past the run is appended.
3. Row bands: horizontal ink runs across the ladder interior (rules masked
   out). A tick is an ink run; the header text is an ink run too, but it inks
   every column and is discarded as text.
4. Cells: the central part of each column within a band; the ticked column is
   the one cell clearly above the empty-cell baseline, with no runner-up.

The detector never sets an amount. It confirms or contradicts the model's
``amount_column_letter``; :mod:`capitol_pipeline.parsers.ptr_vision` nulls the
amount on a contradiction or an ambiguous cell and routes the filing to review.

All coordinates are pixels in the rendered upright page.
"""

from __future__ import annotations

from typing import Any

AMOUNT_LETTERS = "ABCDEFGHIJK"

#: Gray level below which a pixel counts as ink (0 black .. 255 white).
DARK_THRESHOLD = 140
#: Only the right-hand part of the page is searched for the ladder.
SEARCH_X_FRACTION = 0.35
#: Rules come in two tiers. A strong rule has a continuous dark run at least
#: this fraction of the page height: the ladder's column rules run from the
#: header to the bottom row on both form styles (400+ px at this zoom). A weak
#: rule only needs the shorter run: the brokerage attachments' thin rules
#: break into pieces where the scan skews. Both need this much ink overall.
STRONG_RULE_RUN_FRACTION = 0.12
WEAK_RULE_RUN_FRACTION = 0.03
MIN_RULE_INK_FRACTION = 0.15
#: A dark run may skip this many rows (dashes, dropouts) and stay one run.
RULE_GAP_ROWS = 3
#: Dark columns closer than this are one rule (line thickness, anti-aliasing).
RULE_CLUSTER_PX = 6
#: Strong rules closer than this are one column boundary (a stack of box
#: edges bridged by the gap tolerance runs beside the real rule); the member
#: with the longest run is the rule. Real ladder pitches are 38 px and up.
STRONG_RULE_MERGE_PX = 16
#: A weak rule this close (fraction of the page width) to a strong one is the
#: drawn box edge beside that rule, or a handwritten stroke, and is absorbed:
#: the strong rule keeps its exact position so the pitch stays regular.
RULE_ABSORB_FRACTION = 0.022
#: Consecutive pitches may differ by this much (scan skew makes them drift).
PITCH_TOLERANCE = 0.25
#: The ladder is at least this many rules (A-J plus a border = 11).
MIN_LADDER_RULES = 9
#: At most this many rules are the ladder (A-K plus a border); a longer run
#: keeps its rightmost rules.
MAX_LADDER_RULES = 12
#: A rule just past the run within this many pitches is the wide K column.
TRAILING_COLUMN_MAX_PITCHES = 3.5
#: A horizontal rule (or a row of box edges) covers at least this much of the
#: central part of a cell, in at least this fraction of the ladder's columns.
MIN_HRULE_CELL_COVERAGE = 0.5
MIN_HRULE_COLUMNS_FRACTION = 0.6
HRULE_CELL_MARGIN = 0.15
HRULE_MERGE_PX = 6
#: Rows of a band sampled for cell density: the central part, clear of any
#: box edge the band touches.
BAND_ROW_MARGIN = 0.25
#: Cells at least this dark count as text for the header test regardless of
#: the empty-cell baseline.
TEXT_CELL_DENSITY = 0.05
#: The header zone ends at the bottom of the last header-text band found in
#: the top part of the ladder; candidate bands above it are header decoration.
HEADER_SEARCH_FRACTION = 0.5
#: A band this dark in every cell is a solid section bar, not header text.
SOLID_BAR_DENSITY = 0.95
#: A row band is a horizontal ink run across the ladder interior at least
#: this fraction of the interior width wide and this many pixels tall.
MIN_BAND_INK_FRACTION = 0.0025
MIN_BAND_HEIGHT_PX = 5
BAND_GAP_PX = 2
#: Fraction of the cell width kept clear on each side: the column rules and
#: the drawn box the paper form prints in every cell (its edges sit up to
#: ~28% in from the cell edge on some forms) stay out; a tick crosses the
#: centre and a typed "x" sits on it.
CELL_MARGIN = 0.30
#: Some forms draw the box off-centre, so an edge can still fall inside the
#: sampled window: a column of the window that is at least this dark along
#: a band tall enough to test is a box edge and is masked (with a pixel
#: either side). A tick is diagonal: even where its strokes cross, a column
#: is at most about half dark.
BOX_EDGE_FRACTION = 0.8
BOX_EDGE_MIN_ROWS = 12
BOX_EDGE_MASK_PX = 1
#: A cell is inked when its density clears both the floor and the baseline
#: (median cell density, i.e. an empty cell) times the factor.
MARK_MIN_DENSITY = 0.03
MARK_BASELINE_FACTOR = 3.0
MARK_BASELINE_OFFSET = 0.004
#: A tick is unambiguous when the runner-up cell is below this share of it.
MARK_DOMINANCE = 0.6
#: A band with this many inked cells is header text, not a row.
TEXT_BAND_CELLS = 5


def _np() -> Any:
    import numpy

    return numpy


def gray_from_pixmap(samples: bytes, width: int, height: int, channels: int) -> Any:
    """Build a 2-D uint8 gray array from pymupdf ``Pixmap.samples``."""

    np = _np()
    array = np.frombuffer(samples, dtype=np.uint8)
    if channels == 1:
        return array.reshape(height, width)
    array = array.reshape(height, width, channels)
    return array[:, :, :3].mean(axis=2).astype(np.uint8)


# -- Rules ----------------------------------------------------------------------


def _longest_vertical_runs(dark: Any, gap_rows: int = RULE_GAP_ROWS) -> tuple[Any, Any, Any]:
    """Per column: longest dark run (gaps of up to ``gap_rows`` bridged) and its rows."""

    np = _np()
    height, width = dark.shape
    run = np.zeros(width, dtype=np.int32)
    run_start = np.zeros(width, dtype=np.int32)
    gap = np.full(width, gap_rows + 1, dtype=np.int32)
    best = np.zeros(width, dtype=np.int32)
    best_start = np.zeros(width, dtype=np.int32)
    best_end = np.zeros(width, dtype=np.int32)
    for y in range(height):
        row = dark[y]
        gap = np.where(row, 0, gap + 1)
        active = row | (gap <= gap_rows)
        run = np.where(active, run + 1, 0)
        run_start = np.where(active & (run == 1), y, run_start)
        better = row & (run > best)
        best = np.where(better, run, best)
        best_start = np.where(better, run_start, best_start)
        best_end = np.where(better, y, best_end)
    return best, best_start, best_end


def _cluster(positions: list[int], gap: int) -> list[list[int]]:
    groups: list[list[int]] = []
    for position in sorted(positions):
        if groups and position - groups[-1][-1] <= gap:
            groups[-1].append(position)
        else:
            groups.append([position])
    return groups


def find_vertical_rules(dark: Any, x_start: int = 0) -> list[dict[str, Any]]:
    """Vertical rules right of ``x_start``: ``{"x", "y0", "y1", "tier"}`` each.

    Strong rules (long runs) keep their exact position and absorb weak
    candidates within :data:`RULE_ABSORB_FRACTION` of the page width (the
    drawn box edges either side of a column rule, handwriting nearby); weak
    candidates far from any strong rule are the attachments' broken rules and
    are kept. Each rule's extent is the union of its members' longest runs.
    """

    np = _np()
    height, width = dark.shape
    # Scans are slightly skewed, so a rule drifts across pixel columns along
    # its length; measure runs on a copy dilated sideways so it stays one run.
    dilated = dark.copy()
    for shift in (-2, -1, 1, 2):
        dilated |= np.roll(dark, shift, axis=1)
    best, best_start, best_end = _longest_vertical_runs(dilated)
    ink = dark.sum(axis=0) / float(height)
    inky = [x for x in range(max(0, x_start), width) if ink[x] >= MIN_RULE_INK_FRACTION]
    strong = [x for x in inky if best[x] >= STRONG_RULE_RUN_FRACTION * height]
    weak = [
        x
        for x in inky
        if WEAK_RULE_RUN_FRACTION * height <= best[x] < STRONG_RULE_RUN_FRACTION * height
    ]

    def _rules_from(columns: list[int], tier: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for group in _cluster(columns, RULE_CLUSTER_PX):
            out.append(
                {
                    "x": int(round(float(np.mean(group)))),
                    "y0": int(min(best_start[x] for x in group)),
                    "y1": int(max(best_end[x] for x in group)),
                    "tier": tier,
                }
            )
        return out

    rules: list[dict[str, Any]] = []
    for group in _cluster([rule["x"] for rule in _rules_from(strong, "strong")], STRONG_RULE_MERGE_PX):
        members = [rule for rule in _rules_from(strong, "strong") if rule["x"] in group]
        rules.append(max(members, key=lambda rule: rule["y1"] - rule["y0"]))
    absorb = max(RULE_CLUSTER_PX, int(round(width * RULE_ABSORB_FRACTION)))
    strong_xs = [rule["x"] for rule in rules]
    for rule in _rules_from(weak, "weak"):
        if any(abs(rule["x"] - x) <= absorb for x in strong_xs):
            continue
        rules.append(rule)
    rules.sort(key=lambda rule: rule["x"])
    return rules


def find_ladder(rule_xs: list[int]) -> list[int] | None:
    """The amount ladder: the longest evenly pitched run of rules.

    A pitch is accepted when it is within :data:`PITCH_TOLERANCE` of the
    previous pitch or of the run's median pitch. Among runs of at least
    :data:`MIN_LADDER_RULES` the one ending farthest right wins (the ladder
    sits at the page's right edge), then the longest; more than
    :data:`MAX_LADDER_RULES` keeps the rightmost; a wider column just past the
    run is appended as K.
    """

    np = _np()
    xs = sorted(set(int(x) for x in rule_xs))
    if len(xs) < MIN_LADDER_RULES:
        return None
    runs: list[list[int]] = []
    for start in range(len(xs) - 1):
        run = [xs[start], xs[start + 1]]
        pitches = [xs[start + 1] - xs[start]]
        for index in range(start + 2, len(xs)):
            pitch = xs[index] - run[-1]
            previous = pitches[-1]
            median = float(np.median(pitches))
            near_previous = abs(pitch - previous) <= PITCH_TOLERANCE * previous
            near_median = abs(pitch - median) <= PITCH_TOLERANCE * median
            if not (near_previous or near_median):
                break
            run.append(xs[index])
            pitches.append(pitch)
        if len(run) >= MIN_LADDER_RULES:
            runs.append(run)
    if not runs:
        return None
    # The ladder sits at the right edge of the page: prefer the run that ends
    # farthest right, then the longest.
    best = max(runs, key=lambda run: (run[-1], len(run)))
    if len(best) > MAX_LADDER_RULES:
        best = best[-MAX_LADDER_RULES:]
    if len(best) < MAX_LADDER_RULES:
        pitch = float(np.median([b - a for a, b in zip(best, best[1:])]))
        following = [x for x in xs if x > best[-1]]
        if following and following[0] - best[-1] <= TRAILING_COLUMN_MAX_PITCHES * pitch:
            best = best + [following[0]]
    return best


def find_horizontal_rules(dark: Any, ladder: list[int], y0: int, y1: int) -> list[int]:
    """Horizontal rules and box edges across the ladder between ``y0`` and ``y1``.

    A row of pixels is a rule when, in at least :data:`MIN_HRULE_COLUMNS_FRACTION`
    of the ladder's columns, the central part of the cell is at least
    :data:`MIN_HRULE_CELL_COVERAGE` dark. Counting per column rather than
    across the whole width finds the paper form's drawn box edges, which stop
    short of the column rules and leave gaps between boxes.
    """

    np = _np()
    y0 = max(0, y0)
    if y1 <= y0:
        return []
    columns = [(ladder[index], ladder[index + 1]) for index in range(len(ladder) - 1)]
    votes = np.zeros(y1 - y0 + 1, dtype=np.int32)
    for x0, x1 in columns:
        margin = int((x1 - x0) * HRULE_CELL_MARGIN)
        strip = dark[y0 : y1 + 1, x0 + margin : x1 - margin]
        if strip.shape[1] == 0:
            continue
        coverage = strip.sum(axis=1) / float(strip.shape[1])
        votes += (coverage >= MIN_HRULE_CELL_COVERAGE).astype(np.int32)
    needed = max(1, int(round(len(columns) * MIN_HRULE_COLUMNS_FRACTION)))
    candidates = [int(y) for y in np.nonzero(votes >= needed)[0]]
    return [int(round(float(np.mean(group)))) + y0 for group in _cluster(candidates, HRULE_MERGE_PX)]


# -- Grid analysis --------------------------------------------------------------


def analyze_amount_grid(gray: Any) -> dict[str, Any] | None:
    """Locate the amount ladder and measure the ink in every ticked band.

    Returns None when no ladder is found (typed electronic forms, a page with
    no table, a scan too poor to show rules). Otherwise::

        {"width", "height", "columns": [(x0, x1), ...], "y0", "y1",
         "hrules": [y, ...], "bands": [{"y0", "y1", "densities": [...]}, ...],
         "baseline": float}

    Bands are the horizontal ink runs across the ladder interior: every tick,
    plus header text and any rule the mask missed (those classify as text).
    """

    np = _np()
    if gray is None or getattr(gray, "ndim", 0) != 2:
        return None
    height, width = gray.shape
    if height < 50 or width < 50:
        return None
    dark = gray < DARK_THRESHOLD

    rules = find_vertical_rules(dark, int(width * SEARCH_X_FRACTION))
    ladder = find_ladder([rule["x"] for rule in rules])
    if ladder is None:
        return None
    by_x = {rule["x"]: rule for rule in rules}
    extents = [by_x[x] for x in ladder if x in by_x]
    if not extents:
        return None
    y0 = int(min(rule["y0"] for rule in extents))
    y1 = int(max(rule["y1"] for rule in extents))
    if y1 - y0 < 20:
        return None

    hrules = find_horizontal_rules(dark, ladder, y0 - 4, y1 + 4)
    columns = [(ladder[index], ladder[index + 1]) for index in range(len(ladder) - 1)]

    # Ladder interior with the rules masked: what is left is ticks and text.
    x_lo, x_hi = ladder[0], ladder[-1]
    interior = dark[y0 : y1 + 1, x_lo : x_hi + 1].copy()
    for x in ladder:
        lo, hi = max(0, x - x_lo - 4), min(interior.shape[1], x - x_lo + 5)
        interior[:, lo:hi] = False
    for y in hrules:
        lo, hi = max(0, y - y0 - 3), min(interior.shape[0], y - y0 + 4)
        interior[lo:hi, :] = False
    if interior.size == 0:
        return None
    profile = interior.sum(axis=1) / float(interior.shape[1])
    inked_rows = [int(y) for y in np.nonzero(profile >= MIN_BAND_INK_FRACTION)[0]]
    bands: list[dict[str, Any]] = []
    for group in _cluster(inked_rows, BAND_GAP_PX):
        if group[-1] - group[0] + 1 < MIN_BAND_HEIGHT_PX:
            continue
        a, b = y0 + group[0], y0 + group[-1] + 1
        trim = int((b - a) * BAND_ROW_MARGIN)
        ya, yb = a + trim, b - trim
        densities: list[float] = []
        for cx0, cx1 in columns:
            cell_w = cx1 - cx0
            xa, xb = cx0 + int(cell_w * CELL_MARGIN), cx1 - int(cell_w * CELL_MARGIN)
            densities.append(cell_ink_density(dark[ya:yb, xa:xb]))
        bands.append({"y0": int(a), "y1": int(b), "densities": densities})

    # The empty-cell baseline comes from the bands that are not header text.
    def _is_text(densities: list[float]) -> bool:
        return sum(1 for d in densities if d >= TEXT_CELL_DENSITY) >= TEXT_BAND_CELLS

    plain = [d for band in bands if not _is_text(band["densities"]) for d in band["densities"]]
    baseline = float(np.median(plain)) if plain else 0.0

    # Header zone: everything down to the last header-text band in the top
    # part of the ladder (the letters row, the dollar ranges). Brokerage
    # attachments print solid black section bars between groups of rows;
    # those are text to the classifier but not header, so they are skipped.
    # The K column's caption ("Transaction in a Spouse or Dependent Child
    # Asset over $1,000,000") runs below the other columns' text: a band
    # inked only in the last column, above the next horizontal rule, is that
    # caption rather than a tick (see classify_bands).
    header_limit = y0 + int((y1 - y0) * HEADER_SEARCH_FRACTION)
    header_end = y0
    for band in bands:
        if band["y0"] > header_limit or not _is_text(band["densities"]):
            continue
        if all(d >= SOLID_BAR_DENSITY for d in band["densities"]):
            continue  # a section bar, not header text
        header_end = max(header_end, band["y1"])
    below = [y for y in hrules if y >= header_end]
    caption_end = int(below[0]) if below else header_end
    return {
        "width": int(width),
        "height": int(height),
        "columns": columns,
        "y0": int(y0),
        "y1": int(y1),
        "hrules": hrules,
        "bands": bands,
        "baseline": baseline,
        "headerEnd": int(header_end),
        "captionEnd": int(caption_end),
    }


def cell_ink_density(cell: Any) -> float:
    """Ink fraction of the sampled (central) part of a cell.

    In bands at least :data:`BOX_EDGE_MIN_ROWS` tall, columns that are
    :data:`BOX_EDGE_FRACTION` dark are a drawn box edge that strayed into the
    window and are left out; a typed "x" in a six-row band is never tested.
    """

    np = _np()
    if cell.size == 0:
        return 0.0
    rows, cols = cell.shape
    if rows < BOX_EDGE_MIN_ROWS or cols < 3:
        return float(cell.mean())
    keep = cell.mean(axis=0) < BOX_EDGE_FRACTION
    for shift in range(1, BOX_EDGE_MASK_PX + 1):
        keep &= np.roll(keep, shift) & np.roll(keep, -shift)
    if keep.sum() < 0.3 * cols:
        return float(cell.mean())
    return float(cell[:, keep].mean())


def classify_band(densities: list[float], baseline: float) -> dict[str, Any]:
    """Name the ticked column of one band, or say why there is none."""

    threshold = max(MARK_MIN_DENSITY, baseline * MARK_BASELINE_FACTOR + MARK_BASELINE_OFFSET)
    inked = [index for index, density in enumerate(densities) if density >= threshold]
    texty = [index for index, density in enumerate(densities) if density >= TEXT_CELL_DENSITY]
    order = sorted(range(len(densities)), key=lambda index: densities[index], reverse=True)
    best = order[0] if order else None
    best_density = densities[best] if best is not None else 0.0
    second_density = densities[order[1]] if len(order) > 1 else 0.0
    record: dict[str, Any] = {
        "kind": "empty",
        "letter": None,
        "index": None,
        "best": round(best_density, 4),
        "second": round(second_density, 4),
        "threshold": round(threshold, 4),
    }
    if len(inked) >= TEXT_BAND_CELLS or len(texty) >= TEXT_BAND_CELLS:
        record["kind"] = "text"
    elif not inked:
        record["kind"] = "empty"
    elif len(inked) == 1 and best is not None and second_density < MARK_DOMINANCE * best_density:
        record["kind"] = "marked"
        record["index"] = int(best)
        record["letter"] = AMOUNT_LETTERS[best] if best < len(AMOUNT_LETTERS) else None
    else:
        record["kind"] = "ambiguous"
    return record


def classify_bands(analysis: dict[str, Any]) -> list[dict[str, Any]]:
    """Classify every band; bands inside the header zone are ``header``."""

    baseline = float(analysis.get("baseline") or 0.0)
    header_end = int(analysis.get("headerEnd") or 0)
    caption_end = int(analysis.get("captionEnd") or header_end)
    last = len(analysis["columns"]) - 1
    out = []
    for band in analysis["bands"]:
        record = classify_band(band["densities"], baseline)
        record["y0"], record["y1"] = band["y0"], band["y1"]
        if record["kind"] != "text":
            if band["y1"] <= header_end:
                record["kind"] = "header"
            elif (
                band["y1"] <= caption_end
                and record["kind"] in ("marked", "ambiguous")
                and all(d < record["threshold"] for d in band["densities"][:last])
            ):
                record["kind"] = "header"  # the K column's caption
        out.append(record)
    return out


def align_rows(bands: list[dict[str, Any]], expected_rows: int) -> list[dict[str, Any]] | None:
    """Pair the model's rows (top to bottom) with the ticked bands.

    Candidates are the marked and ambiguous bands in page order. An exact count
    match pairs them one to one. One extra candidate whose first band is a
    clean tick in column B is the paper form's pre-printed example row and is
    dropped. Anything else is a failed alignment.
    """

    candidates = [band for band in bands if band["kind"] in ("marked", "ambiguous")]
    if expected_rows <= 0:
        return None
    if len(candidates) == expected_rows:
        return candidates
    if (
        len(candidates) == expected_rows + 1
        and candidates[0]["kind"] == "marked"
        and candidates[0]["index"] == 1
    ):
        return candidates[1:]
    return None


def detect_page(analysis: dict[str, Any] | None, expected_rows: int) -> dict[str, Any]:
    """Run the detector for one page against ``expected_rows`` model rows.

    Returns ``{"status", "columns", "bands", "candidates", "letters"}`` where
    ``letters`` is one entry per expected row: ``{"letter", "kind"}`` (kind
    ``marked`` or ``ambiguous``). ``status`` is ``ok``, ``no-grid``,
    ``no-rows`` or ``unaligned``.
    """

    if analysis is None:
        return {"status": "no-grid", "columns": 0, "bands": 0, "candidates": 0, "letters": []}
    classified = classify_bands(analysis)
    candidates = [band for band in classified if band["kind"] in ("marked", "ambiguous")]
    base = {
        "columns": len(analysis["columns"]),
        "bands": len(classified),
        "candidates": len(candidates),
        "letters": [],
    }
    if expected_rows <= 0:
        return {"status": "no-rows", **base}
    aligned = align_rows(classified, expected_rows)
    if aligned is None:
        return {"status": "unaligned", **base}
    return {
        "status": "ok",
        **base,
        "letters": [{"letter": band["letter"], "kind": band["kind"]} for band in aligned],
    }


# -- Synthetic pages (tests) ------------------------------------------------------


def draw_synthetic_grid(
    *,
    width: int = 1568,
    height: int = 1210,
    columns: int = 11,
    rows: int = 6,
    marks: dict[int, int] | None = None,
    ambiguous_rows: tuple[int, ...] = (),
    example_row: bool = False,
    box_style: bool = False,
    wide_last_column: bool = False,
) -> Any:
    """Render a white page with a ruled amount ladder and X marks.

    ``marks`` maps row index (0-based, below the header) to the column index
    ticked; ``ambiguous_rows`` get ticks in two adjacent columns;
    ``example_row`` adds a small pre-printed tick in column B on the first
    row; ``box_style`` draws each cell as a separate box, like the paper form;
    ``wide_last_column`` makes the final column half as wide again (the K
    column). Returns a uint8 gray array.
    """

    np = _np()
    page = np.full((height, width), 255, dtype=np.uint8)
    marks = dict(marks or {})
    pitch = 62
    last = int(pitch * 1.5) if wide_last_column else pitch
    ladder_x0 = width - 60 - pitch * (columns - 1) - last
    row_h = 52
    header_h = 70
    grid_y0 = int(height * 0.35)
    total_rows = rows + (1 if example_row else 0)
    grid_y1 = grid_y0 + header_h + row_h * total_rows

    # Wider date columns and narrower type columns to the left, like the forms.
    lefts = [ladder_x0 - 90, ladder_x0 - 180, ladder_x0 - 225, ladder_x0 - 270, ladder_x0 - 315]
    for x in lefts:
        page[grid_y0 : grid_y1 + 1, x : x + 2] = 0

    xs = [ladder_x0 + pitch * index for index in range(columns)] + [ladder_x0 + pitch * (columns - 1) + last]
    ys = [grid_y0, grid_y0 + header_h] + [grid_y0 + header_h + row_h * r for r in range(1, total_rows + 1)]
    if box_style:
        # Thin column rules the whole height, like the real form, plus a drawn
        # box inside every cell.
        for x in xs:
            page[grid_y0 : grid_y1 + 1, x : x + 1] = 0
        for ya, yb in zip(ys[1:], ys[2:]):
            for xa, xb in zip(xs, xs[1:]):
                page[ya + 3 : ya + 5, xa + 3 : xb - 3] = 0
                page[yb - 5 : yb - 3, xa + 3 : xb - 3] = 0
                page[ya + 3 : yb - 3, xa + 3 : xa + 5] = 0
                page[ya + 3 : yb - 3, xb - 5 : xb - 3] = 0
        page[grid_y0 : grid_y0 + 2, xs[0] : xs[-1] + 2] = 0
        page[ys[1] : ys[1] + 2, xs[0] : xs[-1] + 2] = 0
    else:
        for x in xs:
            page[grid_y0 : grid_y1 + 1, x : x + 2] = 0
        for y in ys:
            page[y : y + 2, lefts[-1] : xs[-1] + 2] = 0
    # Header text: a dark blob in every column.
    for xa, xb in zip(xs, xs[1:]):
        page[grid_y0 + 15 : grid_y0 + 55, xa + 12 : xb - 12] = 90

    def _x_mark(row_index: int, column: int, size: int) -> None:
        ya = ys[1] + row_h * row_index
        cx = (xs[column] + xs[column + 1]) // 2
        cy = ya + row_h // 2
        for d in range(-size, size + 1):
            for t in range(-2, 3):
                page[cy + d, cx + d + t] = 0
                page[cy + d, cx - d + t] = 0

    offset = 0
    if example_row:
        _x_mark(0, 1, 5)
        offset = 1
    for row_index, column in marks.items():
        _x_mark(row_index + offset, column, 16)
    for row_index in ambiguous_rows:
        _x_mark(row_index + offset, 3, 14)
        _x_mark(row_index + offset, 4, 14)
    return page
