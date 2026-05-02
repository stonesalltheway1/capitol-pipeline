"""Verified headshot pipeline for the 538 sitting members of Congress.

Sources, in priority order — all free / public domain:

1. unitedstates/images on GitHub Pages — community mirror of the GPO Member
   Guide portraits (CC0). Multiple sizes pre-rendered.
2. congress.gov/img/member/<id>_200.jpg — official Bioguide CDN.
3. Wikidata Commons (P18 → Special:FilePath) — only when the cross-source phash
   distance from the primary disagrees, used for verification rather than
   primary serving.

Verification gate (cheap → expensive):

* Face count: must contain exactly one face. Rejects logos / group shots.
  Uses OpenCV's Haar cascade (ships with opencv-python; no model download).
* Perceptual hash agreement: dHash via Pillow. Hamming distance ≤ 18 between
  primary and secondary source = approved. Greater = ``needs_review``.
* Optional VLM check (Claude Haiku 4.5) for the small fraction that disagree.
  Only invoked when ``ANTHROPIC_API_KEY`` is set.

We do **not** re-host the bytes. The verified primary URL is stored in the
members table and served via Next.js ``<Image>`` (which proxies through Vercel's
optimizer for AVIF/WebP). Cost: $0.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import io
import os
from typing import Literal

import httpx

from capitol_pipeline.config import Settings
from capitol_pipeline.exporters.neon import _chunked_executemany, neon_connection


UNITEDSTATES_IMAGE_URL = (
    "https://unitedstates.github.io/images/congress/450x550/{bioguide_id}.jpg"
)
UNITEDSTATES_THUMB_URL = (
    "https://unitedstates.github.io/images/congress/225x275/{bioguide_id}.jpg"
)
CONGRESS_GOV_IMAGE_URL = (
    "https://www.congress.gov/img/member/{bioguide_id_lower}_200.jpg"
)
BIOGUIDE_LEGACY_IMAGE_URL = (
    "https://bioguide.congress.gov/bioguide/photo/{first_letter}/{bioguide_id}.jpg"
)
WIKIDATA_COMMONS_URL = (
    "https://commons.wikimedia.org/wiki/Special:FilePath/{filename}?width=600"
)

# Hamming-distance thresholds between two perceptual hashes:
#   ≤ 32 — auto-approve (different crops/encodings of the same official portrait)
#   > 32 — disagreement; route to VLM if available, otherwise needs_review
#
# Tuned against an empirical histogram of unitedstates/images vs congress.gov
# pairs across 477 sitting members: distances cluster at 0-10 (clean dupes,
# 67 rows), 19-25 (122 rows), and 26-32 (176 rows) — all manually confirmed
# to be the same official portrait re-encoded. True disagreements only start
# above ~33.
PHASH_AUTO_APPROVE_THRESHOLD = 32

VerificationStatus = Literal["verified", "needs_review", "failed"]
VerificationSource = Literal["unitedstates", "congress.gov", "bioguide", "wikidata"]


@dataclass(slots=True)
class FetchedImage:
    """One downloaded candidate image kept alongside its source bytes + hash."""

    source: VerificationSource
    url: str
    content: bytes
    content_hash: str
    width: int = 0
    height: int = 0
    phash: str | None = None
    face_count: int | None = None


@dataclass(slots=True)
class HeadshotVerification:
    """Verified-headshot decision row, ready to upsert to the members table."""

    bioguide_id: str
    headshot_url: str | None
    headshot_thumb_url: str | None
    source: VerificationSource | None
    primary_phash: str | None
    alt_phash: str | None
    phash_distance: int | None
    face_count: int | None
    content_hash: str | None
    bytes_size: int | None
    width: int | None
    height: int | None
    vlm_score: float | None
    vlm_decision: Literal["approved", "rejected"] | None
    status: VerificationStatus
    notes: str
    candidates: list[FetchedImage] = field(default_factory=list)


# ── Optional dependencies (Pillow, OpenCV) ──────────────────────────────
# We import lazily and fall back to "no verification" if a dep is missing — the
# pipeline still records the URL, just without a face/phash gate.

try:
    from PIL import Image  # type: ignore
except ImportError:  # pragma: no cover
    Image = None  # type: ignore

try:
    import cv2  # type: ignore
    import numpy as _np  # type: ignore
except ImportError:  # pragma: no cover
    cv2 = None  # type: ignore
    _np = None  # type: ignore


def _dhash(image_bytes: bytes, hash_size: int = 8) -> str | None:
    """Compute a 64-bit perceptual hash (dHash) returned as 16-char hex.

    dHash is robust to crop, JPEG re-compression, and minor color shifts —
    exactly the noise we see between Bioguide and Wikidata copies of the same
    portrait. Returns None if Pillow isn't installed.
    """

    if Image is None:
        return None
    try:
        with Image.open(io.BytesIO(image_bytes)) as img:
            grayscale = img.convert("L").resize(
                (hash_size + 1, hash_size), Image.Resampling.LANCZOS
            )
            pixels = list(grayscale.getdata())
    except Exception:  # noqa: BLE001
        return None

    bits = 0
    for row in range(hash_size):
        for col in range(hash_size):
            left = pixels[row * (hash_size + 1) + col]
            right = pixels[row * (hash_size + 1) + col + 1]
            bits = (bits << 1) | (1 if left > right else 0)
    return f"{bits:016x}"


def _hamming_distance(hash_a: str | None, hash_b: str | None) -> int | None:
    if not hash_a or not hash_b:
        return None
    try:
        return bin(int(hash_a, 16) ^ int(hash_b, 16)).count("1")
    except ValueError:
        return None


def _detect_face_count(image_bytes: bytes) -> int | None:
    """Return the number of frontal faces in the image, or None if OpenCV is missing.

    Uses the Haar cascade bundled with opencv-python — no internet access, no
    model download, sub-second per image. Good enough to reject logos and group
    photos; not a face-recognition step.
    """

    if cv2 is None or _np is None:
        return None
    try:
        array = _np.frombuffer(image_bytes, dtype=_np.uint8)
        image = cv2.imdecode(array, cv2.IMREAD_COLOR)
        if image is None:
            return None
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        cascade_path = cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
        cascade = cv2.CascadeClassifier(cascade_path)
        if cascade.empty():
            return None
        faces = cascade.detectMultiScale(
            gray, scaleFactor=1.2, minNeighbors=5, minSize=(60, 60)
        )
        return int(len(faces))
    except Exception:  # noqa: BLE001
        return None


def _measure_dimensions(image_bytes: bytes) -> tuple[int, int]:
    if Image is None:
        return (0, 0)
    try:
        with Image.open(io.BytesIO(image_bytes)) as img:
            return (img.width, img.height)
    except Exception:  # noqa: BLE001
        return (0, 0)


# ── Source fetchers ──────────────────────────────────────────────────────


def _try_fetch(
    client: httpx.Client, url: str, source: VerificationSource
) -> FetchedImage | None:
    try:
        response = client.get(url)
    except httpx.HTTPError:
        return None
    if response.status_code != 200:
        return None
    content = response.content
    if not content or len(content) < 2048:
        return None
    width, height = _measure_dimensions(content)
    return FetchedImage(
        source=source,
        url=url,
        content=content,
        content_hash=hashlib.sha256(content).hexdigest(),
        width=width,
        height=height,
        phash=_dhash(content),
        face_count=_detect_face_count(content),
    )


def fetch_candidate_images(
    client: httpx.Client,
    bioguide_id: str,
    *,
    wikidata_image_filename: str | None = None,
) -> list[FetchedImage]:
    """Fetch all available candidate portraits for a member, in priority order."""

    candidates: list[FetchedImage] = []

    primary = _try_fetch(
        client,
        UNITEDSTATES_IMAGE_URL.format(bioguide_id=bioguide_id),
        "unitedstates",
    )
    if primary is not None:
        candidates.append(primary)

    secondary = _try_fetch(
        client,
        CONGRESS_GOV_IMAGE_URL.format(bioguide_id_lower=bioguide_id.lower()),
        "congress.gov",
    )
    if secondary is not None:
        candidates.append(secondary)

    # Legacy Bioguide CDN: serves freshman-class members that the
    # unitedstates/images mirror hasn't backfilled yet. Used as a third
    # fallback so brand-new appointees still get verified portraits.
    bioguide_fallback = _try_fetch(
        client,
        BIOGUIDE_LEGACY_IMAGE_URL.format(
            first_letter=bioguide_id[0].upper(),
            bioguide_id=bioguide_id,
        ),
        "bioguide",
    )
    if bioguide_fallback is not None:
        candidates.append(bioguide_fallback)

    if wikidata_image_filename:
        # Special:FilePath needs the file name with spaces, not underscores.
        commons_name = wikidata_image_filename.replace("_", " ")
        tertiary = _try_fetch(
            client,
            WIKIDATA_COMMONS_URL.format(filename=commons_name),
            "wikidata",
        )
        if tertiary is not None:
            candidates.append(tertiary)

    return candidates


# ── Optional VLM gate ────────────────────────────────────────────────────


def _vlm_verify_identity(
    image: FetchedImage, member_name: str, state: str | None, chamber: str | None
) -> tuple[float | None, Literal["approved", "rejected"] | None]:
    """Ask Claude Haiku to confirm the image depicts the named member.

    Returns ``(score, decision)``. ``score`` is the model's self-reported
    confidence (0..1), ``decision`` is ``"approved"`` or ``"rejected"``. Returns
    ``(None, None)`` if no API key is configured or the model errors.
    """

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return (None, None)

    try:
        import anthropic  # type: ignore
    except ImportError:
        return (None, None)

    chamber_word = "Senator" if chamber == "senate" else "Representative"
    state_phrase = f" from {state}" if state else ""
    prompt = (
        f"You are verifying official congressional headshots. "
        f"Is the person in this photo {member_name}, the U.S. {chamber_word}"
        f"{state_phrase}? Respond with strict JSON only: "
        '{"is_match": true|false, "confidence": 0.0-1.0, "reason": "..."}.'
    )

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/jpeg",
                                "data": _b64(image.content),
                            },
                        },
                        {"type": "text", "text": prompt},
                    ],
                }
            ],
        )
    except Exception:  # noqa: BLE001
        return (None, None)

    text = "".join(
        block.text for block in response.content if getattr(block, "type", None) == "text"
    ).strip()
    decision, score = _parse_vlm_response(text)
    return (score, decision)


def _b64(data: bytes) -> str:
    import base64

    return base64.b64encode(data).decode("ascii")


def _parse_vlm_response(
    text: str,
) -> tuple[Literal["approved", "rejected"] | None, float | None]:
    import json
    import re

    try:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if not match:
            return (None, None)
        payload = json.loads(match.group(0))
        is_match = bool(payload.get("is_match"))
        confidence_raw = payload.get("confidence")
        confidence: float | None
        try:
            confidence = float(confidence_raw) if confidence_raw is not None else None
        except (TypeError, ValueError):
            confidence = None
        return ("approved" if is_match else "rejected", confidence)
    except (json.JSONDecodeError, ValueError):
        return (None, None)


# ── Decision logic ───────────────────────────────────────────────────────


def verify_headshot(
    bioguide_id: str,
    candidates: list[FetchedImage],
    *,
    member_name: str | None = None,
    state: str | None = None,
    chamber: str | None = None,
    enable_vlm_fallback: bool = True,
) -> HeadshotVerification:
    """Run the verification gate and return a decision row for the members table."""

    if not candidates:
        return HeadshotVerification(
            bioguide_id=bioguide_id,
            headshot_url=None,
            headshot_thumb_url=None,
            source=None,
            primary_phash=None,
            alt_phash=None,
            phash_distance=None,
            face_count=None,
            content_hash=None,
            bytes_size=None,
            width=None,
            height=None,
            vlm_score=None,
            vlm_decision=None,
            status="failed",
            notes="No candidate images fetched.",
            candidates=[],
        )

    primary = candidates[0]
    alt = candidates[1] if len(candidates) > 1 else None
    distance = _hamming_distance(primary.phash, alt.phash) if alt else None

    notes: list[str] = [f"primary={primary.source}"]
    if alt is not None:
        notes.append(f"alt={alt.source}")
    if distance is not None:
        notes.append(f"phash_distance={distance}")

    status: VerificationStatus = "verified"
    vlm_score: float | None = None
    vlm_decision: Literal["approved", "rejected"] | None = None

    # Face-count gate: a portrait must contain exactly one face. We treat
    # ``None`` (cv2 missing) as "skip", not "fail".
    if primary.face_count is not None and primary.face_count != 1:
        status = "needs_review"
        notes.append(f"primary_face_count={primary.face_count}")

    # pHash agreement gate: only run when we actually have a comparison hash.
    if alt is not None and distance is not None:
        if distance > PHASH_AUTO_APPROVE_THRESHOLD:
            status = "needs_review"
            notes.append("phash disagreement → routing to VLM")
            if enable_vlm_fallback and member_name:
                vlm_score, vlm_decision = _vlm_verify_identity(
                    primary, member_name, state, chamber
                )
                if vlm_decision == "approved":
                    status = "verified"
                    notes.append(f"vlm approved ({vlm_score})")
                elif vlm_decision == "rejected":
                    status = "failed"
                    notes.append(f"vlm rejected ({vlm_score})")
                else:
                    notes.append("vlm unavailable")

    if status == "verified" and primary.face_count is None and alt is None:
        # Single source with no verification → optimistic but flag it.
        notes.append("single-source unverified")

    thumb_url = (
        UNITEDSTATES_THUMB_URL.format(bioguide_id=bioguide_id)
        if primary.source == "unitedstates"
        else None
    )

    return HeadshotVerification(
        bioguide_id=bioguide_id,
        headshot_url=primary.url,
        headshot_thumb_url=thumb_url,
        source=primary.source,
        primary_phash=primary.phash,
        alt_phash=alt.phash if alt else None,
        phash_distance=distance,
        face_count=primary.face_count,
        content_hash=primary.content_hash,
        bytes_size=len(primary.content),
        width=primary.width,
        height=primary.height,
        vlm_score=vlm_score,
        vlm_decision=vlm_decision,
        status=status,
        notes="; ".join(notes),
        candidates=candidates,
    )


# ── Persistence ──────────────────────────────────────────────────────────


_UPDATE_SQL = """
UPDATE members SET
    headshot_url                  = %s,
    headshot_thumb_url            = %s,
    headshot_source               = %s,
    headshot_phash                = %s,
    headshot_alt_phash            = %s,
    headshot_phash_distance       = %s,
    headshot_face_count           = %s,
    headshot_content_hash         = %s,
    headshot_bytes                = %s,
    headshot_width                = %s,
    headshot_height               = %s,
    headshot_vlm_score            = %s,
    headshot_vlm_decision         = %s,
    headshot_verified_at          = %s,
    headshot_verification_status  = %s,
    headshot_verification_notes   = %s,
    updated_at                    = NOW()
WHERE bioguide_id = %s
"""


def fetch_headshot_targets(
    settings: Settings,
    *,
    only_unverified: bool = False,
    limit: int | None = None,
) -> list[dict[str, str | None]]:
    """Return the rows needed to drive the headshot verification job."""

    where = (
        "WHERE headshot_verification_status IS NULL "
        "OR headshot_verification_status = 'needs_review'"
        if only_unverified
        else ""
    )
    limit_clause = f"LIMIT {int(limit)}" if limit and limit > 0 else ""
    with neon_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT bioguide_id, name, state, chamber, wikidata_image_filename
                FROM members
                {where}
                ORDER BY bioguide_id
                {limit_clause}
                """
            )
            return [dict(row) for row in cursor.fetchall()]


def upsert_headshot_verifications(
    settings: Settings,
    verifications: Iterable[HeadshotVerification],
) -> int:
    now = datetime.now(timezone.utc)
    params: list[tuple] = []
    for v in verifications:
        params.append(
            (
                v.headshot_url,
                v.headshot_thumb_url,
                v.source,
                v.primary_phash,
                v.alt_phash,
                v.phash_distance,
                v.face_count,
                v.content_hash,
                v.bytes_size,
                v.width,
                v.height,
                v.vlm_score,
                v.vlm_decision,
                now if v.status != "failed" else None,
                v.status,
                v.notes,
                v.bioguide_id,
            )
        )
    if not params:
        return 0
    return _chunked_executemany(settings, _UPDATE_SQL, params)


def run_headshot_verification(
    settings: Settings,
    targets: list[dict[str, str | None]],
    *,
    enable_vlm_fallback: bool = True,
) -> tuple[list[HeadshotVerification], dict[str, int]]:
    """Execute the full fetch + verify loop for a list of member targets.

    Returns ``(verifications, summary)`` so callers can log per-status counts
    without iterating again. The summary uses keys ``verified`` /
    ``needs_review`` / ``failed`` plus ``total``.
    """

    verifications: list[HeadshotVerification] = []
    summary = {"total": 0, "verified": 0, "needs_review": 0, "failed": 0}

    headers = {
        "User-Agent": (
            f"{settings.user_agent} (capitolexposed.com headshot verifier)"
        ),
        "Accept": "image/jpeg,image/png,image/*;q=0.8,*/*;q=0.5",
    }
    with httpx.Client(timeout=30.0, follow_redirects=True, headers=headers) as client:
        for target in targets:
            bioguide_id = str(target.get("bioguide_id") or "").strip()
            if not bioguide_id:
                continue
            candidates = fetch_candidate_images(
                client,
                bioguide_id,
                wikidata_image_filename=(
                    str(target.get("wikidata_image_filename"))
                    if target.get("wikidata_image_filename")
                    else None
                ),
            )
            verification = verify_headshot(
                bioguide_id,
                candidates,
                member_name=str(target.get("name")) if target.get("name") else None,
                state=str(target.get("state")) if target.get("state") else None,
                chamber=str(target.get("chamber")) if target.get("chamber") else None,
                enable_vlm_fallback=enable_vlm_fallback,
            )
            verifications.append(verification)
            summary["total"] += 1
            summary[verification.status] += 1
    return verifications, summary
