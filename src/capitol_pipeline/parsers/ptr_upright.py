"""Render a scanned House PTR upright before anything reads it.

Eighty-seven of the 108 House scans in the review queue are stored rotated 270
degrees. Until now the OCR chain was handed the PDF exactly as filed, so
docling read those pages sideways: page 1 of Khanna 8220534 came back as
``| 984 5 | Purchase Sale Exchange | ... | 5 | 1 | H | 8 |``. The same page
rendered upright first came back as ``UNITED STATES HOUSE OF REPRESENTATIVES
... AMOUNT OF TRANSACTION | G | H | K``, and a typed brokerage grid on page 7
came back as a readable table with the dates and issuers intact.

The rotation is decided by the checkbox detector
(:mod:`capitol_pipeline.parsers.ptr_grid`) scored at all four rotations, which
is free, deterministic and needs no model. The vision path uses the same
signal; this module is what makes it available to OCR.

Nothing here is required for a typed PDF: those are parsed from their own text
layer and never enter OCR.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from capitol_pipeline.parsers import ptr_vision

logger = logging.getLogger(__name__)

#: OCR reads small print better than a vision model needs to, so the upright
#: copy is rendered larger than the images sent to a model.
UPRIGHT_OCR_DPI = 200
UPRIGHT_OCR_MAX_LONG_EDGE = 2400

#: Above this many pages the render is skipped: the OCR chain has its own
#: wall-clock cap and a 51-page scan would spend it on rasterising.
UPRIGHT_MAX_PAGES = 60


def upright_ocr_enabled() -> bool:
    """Whether OCR gets the upright copy (``CAPITOL_PTR_UPRIGHT_OCR=0`` turns it off)."""

    raw = (os.environ.get("CAPITOL_PTR_UPRIGHT_OCR") or "").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def build_upright_pdf(pdf_path: Path, destination: Path) -> dict[str, Any] | None:
    """Write an upright copy of ``pdf_path`` to ``destination``.

    Returns a report -- ``{"pages", "rotations", "methods", "dpi",
    "ladderPages"}`` -- or None when pymupdf is unavailable, the file cannot be
    opened, it is longer than :data:`UPRIGHT_MAX_PAGES`, or every page was
    already upright (in which case there is nothing to gain and the caller
    should keep using the original).
    """

    module = ptr_vision._pdf_module()
    if module is None:
        return None
    try:
        document = module.open(str(pdf_path))
    except Exception as error:  # noqa: BLE001 - the caller falls back to the original
        logger.warning("ptr_upright: could not open %s: %s", pdf_path.name, error)
        return None

    rotations: list[int] = []
    methods: list[str] = []
    ladder_pages = 0
    images: list[bytes] = []
    try:
        with document:
            page_count = int(document.page_count)
            if page_count == 0 or page_count > UPRIGHT_MAX_PAGES:
                return None
            for position in range(page_count):
                page = document[position]
                base_rotation = int(getattr(page, "rotation", 0) or 0)
                rect = page.rect

                def _analyze(
                    rotation: int,
                    _page: Any = page,
                    _base: int = base_rotation,
                ) -> dict[str, Any] | None:
                    _page.set_rotation((_base + rotation) % 360)
                    return ptr_vision._analyze_page_grid(_page, module)

                rotation, method, grid = ptr_vision.detect_orientation_from_grid(
                    _analyze, width=int(rect.width), height=int(rect.height)
                )
                if grid is not None:
                    ladder_pages += 1
                rotations.append(rotation)
                methods.append(method)
                png, _width, _height = ptr_vision._render_page(
                    page,
                    module,
                    base_rotation,
                    rotation,
                    UPRIGHT_OCR_MAX_LONG_EDGE,
                    UPRIGHT_OCR_DPI,
                )
                images.append(png)
    except Exception as error:  # noqa: BLE001 - the caller falls back to the original
        logger.warning("ptr_upright: rendering failed for %s: %s", pdf_path.name, error)
        return None

    if not images:
        return None

    try:
        out = module.open()
        try:
            for png in images:
                with module.open(stream=png, filetype="png") as image:
                    page_pdf = image.convert_to_pdf()
                with module.open("pdf", page_pdf) as one_page:
                    out.insert_pdf(one_page)
            destination.parent.mkdir(parents=True, exist_ok=True)
            out.save(str(destination))
        finally:
            out.close()
    except Exception as error:  # noqa: BLE001 - the caller falls back to the original
        logger.warning("ptr_upright: could not assemble %s: %s", destination.name, error)
        return None

    report = {
        "pages": len(images),
        "rotations": rotations,
        "methods": methods,
        "dpi": UPRIGHT_OCR_DPI,
        "ladderPages": ladder_pages,
        "rotated": sum(1 for rotation in rotations if rotation),
    }
    logger.info(
        "ptr_upright: %s -> %d upright page(s) at %d DPI (%d rotated, ladder found on %d)",
        pdf_path.name,
        len(images),
        UPRIGHT_OCR_DPI,
        report["rotated"],
        ladder_pages,
    )
    return report
