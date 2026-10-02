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

The paper form's pre-printed example row ("Example: Mega Corp. Common Stock",
an x under Sale and an x in column B) is part of the blank, not a filing, and
it inks the grid exactly where a first row would. :func:`find_example_row`
recognises it by what printing does that a hand or a filer's software does
not -- see the constants beside it -- and :func:`align_rows` never pairs a
model row with it.

All coordinates are pixels in the rendered upright page.
"""

from __future__ import annotations

from typing import Any

#: Bumped whenever a change here can move a letter or a type onto a different
#: row. A saved read checked by an older detector is not replayed as if this
#: one had checked it (``ptr_vision.VISION_READ_VERSION`` is the read's half).
#:   1  the amount ladder (cc2747d)
#:   2  the Purchase/Sale type block (17e12bc, abbbfd6)
#:   3  the pre-printed example row is recognised and never aligned; the
#:      header block runs to its last contiguous text band; box edges are
#:      masked in short bands; a page with no tick for a row says so
DETECTOR_VERSION = 3

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
#: The sampled window of a short band is too few rows to test for a box edge
#: (a 21-pixel band leaves 11), so in a band at least this tall the edge is
#: also looked for over the band's full height. Measured on 9116326 page 1 and
#: 9116257 page 1, computer-filled House forms whose drawn boxes are printed
#: twice, slightly offset: the doubled edges put 0.09-0.25 of ink in every
#: cell of a 21-pixel row, the row read as header text, and the page lost a
#: row it needed to align. A tick is diagonal and never fills a column of the
#: full band; the McCaul forms' rows are 10-14 pixels and never reach this.
BOX_EDGE_PROBE_MIN_ROWS = 16
#: A cell is inked when its density clears both the floor and the baseline
#: (median cell density, i.e. an empty cell) times the factor.
MARK_MIN_DENSITY = 0.03
MARK_BASELINE_FACTOR = 3.0
MARK_BASELINE_OFFSET = 0.004
#: A tick is unambiguous when the runner-up cell is below this share of it.
MARK_DOMINANCE = 0.6
#: A cell under this share of the band's darkest cell is not a second tick,
#: whatever the floor says: it is what a box edge leaves after masking. On
#: 9116257 page 1 a Honeywell row ticked in A at 0.364 read as ambiguous
#: because the H box's doubled edge left 0.0303 against a floor of 0.03.
MARK_NOISE_SHARE = 0.15
#: A band with this many inked cells is header text, not a row.
TEXT_BAND_CELLS = 5
#: A candidate band shorter than this fraction of the median candidate height
#: is stray ink -- a rule the mask missed, the tail of a neighbouring row --
#: and not a row. See :func:`align_rows` for what it costs to keep one.
RUNT_BAND_RATIO = 0.5
#: And a band this much taller than the median is not a row either: over the
#: ladder interior a row is one tick, so its height barely varies, while the
#: header's letter-and-range block runs several lines. On 8221360 page 3 that
#: block is 88 px against a median of 31 and it is the reason the page could
#: not be aligned at all -- which cost two rows their type check.
GIANT_BAND_RATIO = 2.0
#: Inside the header block a band shorter than this share of the ladder pitch
#: is a fleck of the header's own print -- the tail of a dollar range, a
#: speck -- and does not end the block. On 9116217 page 1 a 5-pixel fleck in
#: column I sat between two header text bands, outside the old top-half
#: search, and was taken for a row. Every tick measured is 0.18 of a pitch or
#: more (the example row's printed x on 9116257), every handwritten one 0.45.
HEADER_FLECK_MAX_PITCH_RATIO = 0.15

# -- The pre-printed example row -------------------------------------------
#
# The House paper form prints one example in its grid: "Example: Mega Corp.
# Common Stock", an x under Sale, 02/05/20 and 03/07/20 (other printings carry
# 02/05/24 or 8/14/12), and an x in column B. The detector sees that x in B as
# a tick on the first row. Until 2026-10-02 the only defence was a count rule
# -- one extra candidate whose first is B is the example -- and the count rule
# fails both ways. On 9116218 the one real row ticks no amount at all, so the
# example's x was the only candidate, the count matched, and the detector
# "resolved" an invented $15,001-$50,000 on a filing it then rated clean. On
# 9116217, 9116326 and 9116257 a fleck or a missed row made the count match
# with the example still in it, and every row took its upstairs neighbour's
# tick, contradicting reads that were right.
#
# So it is recognised by what printing does. Measured on every paper-form
# page available (the 44 staged 2026 PDFs and the ground-truth set):
#
#   the example's x is a typeset glyph: 8-12 px tall at the render zoom,
#   0.19-0.27 of the ladder pitch, on every printing seen; and
#   the rows filled in under it are 21-42 px (0.41-0.81 of a pitch), so the
#   example is at most 0.5 of the rows on its own page.
#
# The typed House form McCaul files (8221326, 8221359, 9116141) prints no
# example row and its own ticks are typeset glyphs too: 10-14 px against a
# 32-33 px pitch, 0.31-0.44. That is why the size test is relative to the
# page whenever the page has other ticks, and why the absolute test, used only
# when the example's x is the one tick on the page, stops below 0.31.

#: The example is printed in column B, under Sale.
EXAMPLE_ROW_LETTER_INDEX = 1
#: Its x is no taller than this share of the ladder pitch.
EXAMPLE_ROW_MAX_PITCH_RATIO = 0.29
#: And, when the page has other ticks, no taller than this share of theirs.
EXAMPLE_ROW_MAX_RELATIVE_HEIGHT = 0.6

# -- The Type block ---------------------------------------------------------
#
# The amount ladder has this independent check; the Type column had none, and
# that is where the damage was. On docs 8221322 and 8221358 both Gemini reads
# took the Type column one column to the left and agreed with each other, so
# ``merge_matched_rows`` waved it through: 623 of Ro Khanna's published
# transactions said purchase or exchange where the form ticks Sale, and not one
# went the other way. Two reads by two versions of one model family are not two
# independent reads. This is.
#
# It does not have to be a general Type detector, and deliberately is not. On
# BOTH layouts that reach this path the first tick column is Purchase and the
# second is Sale, and Purchase-against-Sale is the whole failure:
#
#   brokerage grid (Khanna)  Purchase | Sale | Exchange | Capital Gains
#                            Exceed $200 | Partial Transaction
#   House form (McCaul)      PURCHASE | SALE | EXCHANGE
#
# So no layout detection is needed. Note the House form has three type columns,
# not the four ("Purchase / Sale / Partial Sale / Exchange") the older notes
# claim -- read off 9116141 page 2 at the render zoom.
#
# Finding them takes no new geometry either. Left of the ladder, the asset-name
# column is far the widest thing on the page, and the tick block starts at its
# right edge. Measured on the fixtures at the zoom the detector actually gets:
#
#   page            asset col   next two columns   ladder pitch
#   8221322 p2        166           44, 26              52
#   8221322 p19       244           42, 26              50
#   8221358 p20       256           45, 27              52
#   8221358 p32       254           46, 27              52
#   8221360 p2        178           48, 44              55
#   8221359 p2        651           34, 33              32
#   8221326 p1        645           36, 32              32
#   9116141 p2        668           35, 34              33
#
#: The widest column left of the ladder must beat the runner-up by this much
#: before it is taken for the asset-name column. The tightest fixture is
#: 8221360 p2 at 178 against 114 (1.56); a brokerage grid runs to 7.2.
ASSET_COLUMN_DOMINANCE = 1.25
#: A tick column's width, as a fraction of the ladder's own pitch. The fixtures
#: span 0.50 to 1.13, so this is a guard band rather than a fit: it exists to
#: reject a page whose widest gap was not the asset column at all.
TYPE_COLUMN_MIN_PITCH_RATIO = 0.35
TYPE_COLUMN_MAX_PITCH_RATIO = 2.0
#: Names of the two columns read, in page order.
TYPE_COLUMN_NAMES = ("purchase", "sale")


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

    KNOWN LIMITATION, measured 2026-09-03 and not fixed here. The run's
    leftmost column is taken for A. When rules at the left of the block are too
    faint to find, the ladder is anchored on the wrong column and every letter
    is shifted, silently. On the 2023 handwritten paper forms, where the same
    blank produces an eleven-column ladder on four filings, it produces eight
    columns on three others -- 8219420, 8219455 and 8220037 -- anchored three
    columns right, so a tick in D reads as A. Checked against two independent
    blind transcriptions of 122 pages, this is the only remaining disagreement
    the detector has with the forms: 1,633 amount letters right out of 1,634,
    and the one wrong is 8219455 page 1.

    It fails safe as things stand. A shifted letter contradicts the model's,
    :func:`capitol_pipeline.parsers.ptr_vision.apply_checkbox_detector` nulls
    the amount, and a row with no amount band is withheld -- so the cost is a
    withheld row, not a wrong one. The residual risk is a shifted ladder that
    happens to agree with an already-wrong model letter.

    The fix needs the block's true left edge rather than the run's, and the
    fixtures for it are those three pages against 8219414, 8219415, 8219436 and
    8220431, which are the same blank read correctly.
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


def find_type_columns(
    rule_xs: list[int], ladder_x0: int, ladder_pitch: float
) -> list[tuple[int, int]] | None:
    """The Purchase and Sale tick columns, from the rules left of the ladder.

    The tick block begins at the right edge of the widest column left of the
    ladder, which on both layouts is the asset-name column, and its first two
    columns are Purchase and Sale. Returns ``[(x0, x1), (x0, x1)]``, or None
    when the page does not show that shape -- a missing rule, a page that is
    not a transaction grid, an asset column that did not stand out. None means
    "this page says nothing about the type", never "no tick".
    """

    left = sorted(x for x in rule_xs if x < ladder_x0 - 2)
    if len(left) < 4 or ladder_pitch <= 0:
        return None
    gaps = [(left[i + 1] - left[i], i) for i in range(len(left) - 1)]
    widest, at = max(gaps)
    runner_up = max((g for g, i in gaps if i != at), default=0)
    if runner_up and widest < runner_up * ASSET_COLUMN_DOMINANCE:
        return None
    # Two tick columns need three rules from the asset column's right edge.
    if at + 3 >= len(left):
        return None
    columns = [(left[at + 1 + k], left[at + 2 + k]) for k in range(2)]
    lo = TYPE_COLUMN_MIN_PITCH_RATIO * ladder_pitch
    hi = TYPE_COLUMN_MAX_PITCH_RATIO * ladder_pitch
    if not all(lo <= (x1 - x0) <= hi for x0, x1 in columns):
        return None
    return columns


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
    # The caller pads the ladder's extent by a few pixels to catch the rules
    # that bound it; on a page whose ladder runs to the bottom edge that pad
    # walks off the array, and the slice comes back short of ``votes``.
    y1 = min(int(y1), int(dark.shape[0]) - 1)
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
    # The Purchase and Sale columns, measured at the very same row bands. The
    # rows are already solved by the ladder: a row aligned there is aligned in
    # the tick block for free, which is why this costs one extra rule sweep and
    # nothing else. The first sweep starts at SEARCH_X_FRACTION, so the tick
    # block -- which is left of that on the House form -- needs a full-width one.
    pitches = [columns[i][1] - columns[i][0] for i in range(len(columns))]
    ladder_pitch = float(sorted(pitches)[len(pitches) // 2]) if pitches else 0.0
    type_columns = find_type_columns(
        [rule["x"] for rule in find_vertical_rules(dark, 0)], ladder[0], ladder_pitch
    )

    def _densities(
        ya: int, yb: int, cells: list[tuple[int, int]], a: int, b: int
    ) -> list[float]:
        out: list[float] = []
        for cx0, cx1 in cells:
            cell_w = cx1 - cx0
            xa, xb = cx0 + int(cell_w * CELL_MARGIN), cx1 - int(cell_w * CELL_MARGIN)
            probe = dark[a:b, xa:xb] if (b - a) >= BOX_EDGE_PROBE_MIN_ROWS else None
            out.append(cell_ink_density(dark[ya:yb, xa:xb], probe=probe))
        return out

    profile = interior.sum(axis=1) / float(interior.shape[1])
    inked_rows = [int(y) for y in np.nonzero(profile >= MIN_BAND_INK_FRACTION)[0]]
    bands: list[dict[str, Any]] = []
    for group in _cluster(inked_rows, BAND_GAP_PX):
        if group[-1] - group[0] + 1 < MIN_BAND_HEIGHT_PX:
            continue
        a, b = y0 + group[0], y0 + group[-1] + 1
        trim = int((b - a) * BAND_ROW_MARGIN)
        ya, yb = a + trim, b - trim
        band: dict[str, Any] = {
            "y0": int(a),
            "y1": int(b),
            "densities": _densities(ya, yb, columns, a, b),
        }
        if type_columns is not None:
            band["typeDensities"] = _densities(ya, yb, type_columns, a, b)
        bands.append(band)

    # The empty-cell baseline comes from the bands that are not header text.
    def _is_text(densities: list[float]) -> bool:
        return sum(1 for d in densities if d >= TEXT_CELL_DENSITY) >= TEXT_BAND_CELLS

    plain = [d for band in bands if not _is_text(band["densities"]) for d in band["densities"]]
    baseline = float(np.median(plain)) if plain else 0.0

    # The type baseline is the median of the *quieter* of the two cells in each
    # band, not the median of both pooled. On a page where every row is a Sale
    # -- which is exactly the page this exists for -- half the pooled values are
    # ticks, and their median lands between inked and empty and swallows the
    # signal. The quieter cell of a row that ticks one column is the empty one.
    quiet = [
        min(band["typeDensities"])
        for band in bands
        if "typeDensities" in band and not _is_text(band["densities"])
    ]
    type_baseline = float(np.median(quiet)) if quiet else 0.0

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
    # The header block is contiguous: on a page with few row slots its
    # vertical dollar-range labels run past the top half of the ladder
    # (9116217 page 1: to 71% of it). Text bands that follow the block with
    # nothing between them but empty bands and flecks of print are still the
    # block, and so is any fleck inside it. A band that could be a tick ends
    # it, as does a solid section bar (the brokerage grids), so no row is ever
    # taken into the header this way.
    if header_end > y0:
        threshold = max(MARK_MIN_DENSITY, baseline * MARK_BASELINE_FACTOR + MARK_BASELINE_OFFSET)
        fleck = HEADER_FLECK_MAX_PITCH_RATIO * ladder_pitch
        for band in sorted(bands, key=lambda entry: entry["y0"]):
            if band["y1"] <= header_end:
                continue
            densities = band["densities"]
            if _is_text(densities):
                if all(d >= SOLID_BAR_DENSITY for d in densities):
                    break
                header_end = band["y1"]
                continue
            if all(d < threshold for d in densities) or (band["y1"] - band["y0"]) < fleck:
                continue
            break
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
        "typeColumns": type_columns,
        "typeBaseline": type_baseline,
        "headerEnd": int(header_end),
        "captionEnd": int(caption_end),
        "pitch": ladder_pitch,
    }


#: Weights for :func:`orientation_score`. The four terms are independent
#: pieces of evidence that a rendered page is the right way up, and they are
#: summed rather than multiplied so one missing signal cannot veto the rest.
ORIENTATION_WEIGHT_LADDER = 2.0
ORIENTATION_WEIGHT_HEADER = 1.5
ORIENTATION_WEIGHT_MARGIN = 1.0
ORIENTATION_WEIGHT_WIDE_K = 0.5
ORIENTATION_WEIGHT_ROWS = 0.5
#: The K column has to be this much wider than the ladder's other end before
#: its width counts as evidence either way.
ORIENTATION_WIDE_K_RATIO = 1.15
#: Row bands at which the "this page carries rows" term saturates.
ORIENTATION_ROWS_FULL = 8


def orientation_score(analysis: dict[str, Any] | None) -> float:
    """How strongly a page analysis says the page is upright. 0 when it does not.

    :func:`analyze_amount_grid` finds a ladder on a page that is upside down
    almost as readily as on one that is upright -- a ruled brokerage grid is
    a ruled brokerage grid either way up -- so the half turn has to be settled
    by asymmetries in the form itself:

    * **the ladder is complete.** Eleven columns is A-K; a partial run is
      more often a lucky match on some other ruling.
    * **the header prints above the rows.** ``headerEnd`` only moves off
      ``y0`` when the letters row and the dollar ranges were found in the top
      half of the ladder. Upside down they are in the bottom half and this
      term is zero. This is the single strongest signal.
    * **the ladder runs to the right margin.** The amount columns are the
      last thing on an upright House PTR line.
    * **K is the wide column, and it is on the right.** The spouse/dependent
      flag column is wider than the bands beside it; upside down the wide
      column lands at the ladder's left end.
    * **the page carries row bands at all.**

    Returns a number in ``0..5.5``; callers compare it across the four
    rotations rather than against a threshold.
    """

    if not isinstance(analysis, dict):
        return 0.0
    columns = analysis.get("columns") or []
    width = int(analysis.get("width") or 0)
    if not columns or width <= 0:
        return 0.0

    score = ORIENTATION_WEIGHT_LADDER * min(1.0, len(columns) / float(MAX_LADDER_RULES - 1))

    if int(analysis.get("headerEnd") or 0) > int(analysis.get("y0") or 0):
        score += ORIENTATION_WEIGHT_HEADER

    right_edge = float(columns[-1][1]) / float(width)
    score += ORIENTATION_WEIGHT_MARGIN * max(0.0, min(1.0, right_edge))

    first_width = float(columns[0][1] - columns[0][0])
    last_width = float(columns[-1][1] - columns[-1][0])
    if first_width > 0 and last_width > first_width * ORIENTATION_WIDE_K_RATIO:
        score += ORIENTATION_WEIGHT_WIDE_K
    elif last_width > 0 and first_width > last_width * ORIENTATION_WIDE_K_RATIO:
        score -= ORIENTATION_WEIGHT_WIDE_K

    bands = analysis.get("bands") or []
    score += ORIENTATION_WEIGHT_ROWS * min(1.0, len(bands) / float(ORIENTATION_ROWS_FULL))
    return round(score, 4)


def cell_ink_density(cell: Any, probe: Any = None) -> float:
    """Ink fraction of the sampled (central) part of a cell.

    In bands at least :data:`BOX_EDGE_MIN_ROWS` tall, columns that are
    :data:`BOX_EDGE_FRACTION` dark are a drawn box edge that strayed into the
    window and are left out; a typed "x" in a six-row band is never tested.
    ``probe`` is the same columns over the band's full height: a column dark
    through all of it is a box edge too, which is how a short band's edges
    are found (:data:`BOX_EDGE_PROBE_MIN_ROWS`).
    """

    np = _np()
    if cell.size == 0:
        return 0.0
    rows, cols = cell.shape
    tested = rows >= BOX_EDGE_MIN_ROWS
    probed = probe is not None and getattr(probe, "shape", (0, 0))[1] == cols
    if cols < 3 or not (tested or probed):
        return float(cell.mean())
    edge = np.zeros(cols, dtype=bool)
    if tested:
        edge |= cell.mean(axis=0) >= BOX_EDGE_FRACTION
    if probed:
        edge |= probe.mean(axis=0) >= BOX_EDGE_FRACTION
    keep = ~edge
    for shift in range(1, BOX_EDGE_MASK_PX + 1):
        keep &= np.roll(keep, shift) & np.roll(keep, -shift)
    if keep.sum() < 0.3 * cols:
        return float(cell.mean())
    return float(cell[:, keep].mean())


def classify_band(densities: list[float], baseline: float) -> dict[str, Any]:
    """Name the ticked column of one band, or say why there is none."""

    threshold = max(MARK_MIN_DENSITY, baseline * MARK_BASELINE_FACTOR + MARK_BASELINE_OFFSET)
    peak = max(densities) if densities else 0.0
    inked = [
        index
        for index, density in enumerate(densities)
        if density >= threshold and density >= MARK_NOISE_SHARE * peak
    ]
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


def classify_type(densities: list[float] | None, baseline: float) -> dict[str, Any]:
    """Say whether one band ticks Purchase, ticks Sale, or neither.

    ``kind`` is ``purchase``, ``sale``, ``none`` (neither column inked) or
    ``ambiguous`` (both, or one not clearly dominant). ``none`` is a normal,
    frequent answer -- an Exchange row ticks neither of the two columns read --
    so this can confirm or contradict a claim of purchase or sale and says
    nothing at all about any other type. That is the whole failure it exists
    for: a Sale published as a purchase (435 rows) or as an exchange (188).
    """

    record: dict[str, Any] = {"kind": "unknown", "name": None, "best": None, "second": None}
    if not densities or len(densities) < 2:
        return record
    threshold = max(MARK_MIN_DENSITY, baseline * MARK_BASELINE_FACTOR + MARK_BASELINE_OFFSET)
    order = sorted(range(len(densities)), key=lambda i: densities[i], reverse=True)
    best, second = order[0], order[1]
    best_d, second_d = densities[best], densities[second]
    record.update(
        best=round(best_d, 4), second=round(second_d, 4), threshold=round(threshold, 4)
    )
    if best_d < threshold:
        record["kind"] = "none"
    elif second_d >= threshold or second_d >= MARK_DOMINANCE * best_d:
        record["kind"] = "ambiguous"
    else:
        record["kind"] = TYPE_COLUMN_NAMES[best]
        record["name"] = TYPE_COLUMN_NAMES[best]
    return record


def _band_height(band: dict[str, Any]) -> int:
    return int(band["y1"]) - int(band["y0"])


def _could_be_example(band: dict[str, Any], pitch: float) -> bool:
    """A clean tick in column B, typeset-small, not under Purchase."""

    return (
        band.get("kind") == "marked"
        and band.get("index") == EXAMPLE_ROW_LETTER_INDEX
        and (band.get("type") or {}).get("kind") != "purchase"
        and pitch > 0
        and _band_height(band) <= EXAMPLE_ROW_MAX_PITCH_RATIO * pitch
    )


def find_example_row(bands: list[dict[str, Any]], pitch: float) -> int | None:
    """Index into ``bands`` of the form's pre-printed example row, or None.

    The example is the first row of the grid, so only the first tick below
    the header can be it. It must be :func:`_could_be_example` -- a clean tick
    in B, no taller than :data:`EXAMPLE_ROW_MAX_PITCH_RATIO` of the ladder
    pitch, and not read under Purchase -- and, when the page has other ticks,
    no taller than :data:`EXAMPLE_ROW_MAX_RELATIVE_HEIGHT` of theirs. A
    continuation page has no example row and its first tick is a real one; a
    form whose own ticks are typeset glyphs (the McCaul filings) fails the
    relative test because every tick on it is the same size.
    """

    candidates = [i for i, band in enumerate(bands) if band.get("kind") in ("marked", "ambiguous")]
    if not candidates:
        return None
    first = candidates[0]
    band = bands[first]
    if not _could_be_example(band, pitch):
        return None
    others = sorted(_band_height(bands[i]) for i in candidates[1:])
    if others:
        median = others[len(others) // 2]
        if _band_height(band) > EXAMPLE_ROW_MAX_RELATIVE_HEIGHT * median:
            return None
    return first


def classify_bands(analysis: dict[str, Any]) -> list[dict[str, Any]]:
    """Classify every band; bands inside the header zone are ``header``.

    The form's pre-printed example row keeps its kind (it is a tick, and the
    ladder reads it like one) and is flagged ``example``; :func:`align_rows`
    never pairs a model row with it.
    """

    baseline = float(analysis.get("baseline") or 0.0)
    type_baseline = float(analysis.get("typeBaseline") or 0.0)
    header_end = int(analysis.get("headerEnd") or 0)
    caption_end = int(analysis.get("captionEnd") or header_end)
    last = len(analysis["columns"]) - 1
    out = []
    for band in analysis["bands"]:
        record = classify_band(band["densities"], baseline)
        record["y0"], record["y1"] = band["y0"], band["y1"]
        record["type"] = classify_type(band.get("typeDensities"), type_baseline)
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
    pitch = float(analysis.get("pitch") or 0.0)
    if pitch <= 0 and analysis.get("columns"):
        widths = sorted(x1 - x0 for x0, x1 in analysis["columns"])
        pitch = float(widths[len(widths) // 2])
    example = find_example_row(out, pitch)
    if example is not None:
        out[example]["example"] = True
    return out


def align_rows(
    bands: list[dict[str, Any]], expected_rows: int, pitch: float = 0.0
) -> list[dict[str, Any]] | None:
    """Pair the model's rows (top to bottom) with the ticked bands.

    Candidates are the marked and ambiguous bands in page order, less the
    form's pre-printed example row (flagged by :func:`classify_bands`). An
    exact count match pairs them one to one. Otherwise runt bands are dropped,
    and anything else is a failed alignment. Fewer ticks than rows is a failed
    alignment too, never a reason to borrow a band: a row whose amount box is
    empty has no band, and pairing it with the example's x is how 9116218 got
    an amount nobody ticked.

    The order matters, and it is measured. A count rule for the example row
    (one extra candidate whose first band is a clean tick in column B) cannot
    tell a real first row from a stray band, and column B is also the
    commonest real amount, so on any page with one spurious band it silently
    drops the first row and shifts every row after it. On 8221360 page 2 that
    is exactly what happened: a five-pixel band among bands 25 to 31 pixels
    tall made nine candidates for eight rows, the rule dropped Micron
    Technology, and five of the seven rows then carried another row's amount
    -- checked against two independent blind transcriptions of the page.
    Dropping the runt first makes the count match on its own. The count rule
    survives only as a last resort, and only for a first band that is small
    enough to be the printed x (:func:`_could_be_example`).
    """

    if expected_rows <= 0:
        return None
    candidates = [
        band
        for band in bands
        if band["kind"] in ("marked", "ambiguous") and not band.get("example")
    ]
    if len(candidates) == expected_rows:
        return candidates
    if len(candidates) > expected_rows:
        heights = sorted(_band_height(band) for band in candidates)
        median = heights[len(heights) // 2]
        kept = [
            band
            for band in candidates
            if median * RUNT_BAND_RATIO <= _band_height(band) <= median * GIANT_BAND_RATIO
        ]
        if expected_rows <= len(kept) < len(candidates):
            candidates = kept
    if len(candidates) == expected_rows:
        return candidates
    if (
        len(candidates) == expected_rows + 1
        and not any(band.get("example") for band in bands)
        and _could_be_example(candidates[0], pitch)
    ):
        return candidates[1:]
    return None


def detect_page(analysis: dict[str, Any] | None, expected_rows: int) -> dict[str, Any]:
    """Run the detector for one page against ``expected_rows`` model rows.

    Returns ``{"status", "columns", "bands", "candidates", "letters"}`` where
    ``letters`` is one entry per expected row: ``{"letter", "kind"}`` (kind
    ``marked`` or ``ambiguous``). ``status`` is ``ok``, ``no-grid``,
    ``no-rows``, ``no-ticks`` (a ladder with nothing ticked on it but the
    example row) or ``unaligned``. ``exampleRow`` is where the pre-printed
    example row was found, or None.
    """

    if analysis is None:
        return {
            "status": "no-grid",
            "columns": 0,
            "bands": 0,
            "candidates": 0,
            "letters": [],
            "types": [],
            "typeColumns": None,
            "exampleRow": None,
        }
    classified = classify_bands(analysis)
    example = next((band for band in classified if band.get("example")), None)
    candidates = [
        band
        for band in classified
        if band["kind"] in ("marked", "ambiguous") and not band.get("example")
    ]
    base = {
        "columns": len(analysis["columns"]),
        "bands": len(classified),
        "candidates": len(candidates),
        "letters": [],
        "types": [],
        "typeColumns": analysis.get("typeColumns"),
        # Where the pre-printed example row was found, so a reviewer can see
        # what was set aside: [y0, y1] in page pixels, or None.
        "exampleRow": [int(example["y0"]), int(example["y1"])] if example else None,
    }
    if expected_rows <= 0:
        return {"status": "no-rows", **base}
    if not candidates:
        # The ladder is there and nothing on it is ticked (the example's x
        # aside). That is a finding, not a failure to align: no row on this
        # page has an amount box the detector can see, and it will not lend
        # one any row.
        return {"status": "no-ticks", **base}
    aligned = align_rows(classified, expected_rows, float(analysis.get("pitch") or 0.0))
    if aligned is None:
        return {"status": "unaligned", **base}
    return {
        "status": "ok",
        **base,
        "letters": [{"letter": band["letter"], "kind": band["kind"]} for band in aligned],
        # One entry per expected row, in the same order as `letters`, because
        # both come off the same aligned bands. `kind` is purchase / sale /
        # none / ambiguous / unknown; only the first two are an assertion.
        "types": [
            {"kind": band["type"]["kind"], "name": band["type"].get("name")}
            for band in aligned
        ],
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
        # box inside every cell -- except the example row's, which the real
        # form prints bare (9116217, 9116257, 9116326 page 1).
        for x in xs:
            page[grid_y0 : grid_y1 + 1, x : x + 1] = 0
        for slot, (ya, yb) in enumerate(zip(ys[1:], ys[2:])):
            if example_row and slot == 0:
                continue
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
