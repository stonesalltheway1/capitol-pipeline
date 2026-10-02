"""Member registry and lookup logic for Capitol Pipeline."""

from __future__ import annotations

import json
import unicodedata
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Mapping

from capitol_pipeline.models.congress import MemberMatch
from capitol_pipeline.registries.legislator_service import (
    LegislatorService,
    ServiceTerm,
    parse_iso_date,
)


#: Generational suffixes that never help identify a member.
NAME_SUFFIX_TOKENS = frozenset({"jr", "sr", "ii", "iii", "iv"})

#: Courtesy titles and post-nominal credentials the House Clerk feed sometimes
#: folds into the first or last name ("Marjorie Taylor Mrs Greene",
#: "Neal Patrick Dunn, MD, FACS"). None of these is a real name token for any
#: member since 2012, so dropping them is safe for every lookup key.
HONORIFIC_TOKENS = frozenset({"mr", "mrs", "ms", "miss", "mx", "dr", "hon"})
CREDENTIAL_TOKENS = frozenset(
    {
        "md",
        "facs",
        "facp",
        "faap",
        "phd",
        "dds",
        "dmd",
        "dvm",
        "jd",
        "esq",
        "cpa",
        "rn",
        "mba",
        "mph",
        "ret",
    }
)

_DROPPED_NAME_TOKENS = NAME_SUFFIX_TOKENS | HONORIFIC_TOKENS | CREDENTIAL_TOKENS


def strip_diacritics(value: str) -> str:
    """Collapse accented characters into their ASCII base form."""

    return "".join(
        char for char in unicodedata.normalize("NFD", value) if unicodedata.category(char) != "Mn"
    )


def normalize_member_lookup_value(raw: str | None) -> str:
    """Normalize a member name for fuzzy-but-safe matching."""

    if not raw:
        return ""

    normalized = strip_diacritics(raw)
    normalized = normalized.strip()

    for prefix in ("hon", "rep", "representative", "sen", "senator"):
        if normalized.lower().startswith(f"{prefix}. "):
            normalized = normalized[len(prefix) + 2 :]
            break
        if normalized.lower().startswith(f"{prefix} "):
            normalized = normalized[len(prefix) + 1 :]
            break

    # Typographic apostrophes are split like a plain one: congress-legislators
    # and the members table spell "Beto O’Rourke", the House feed "O'Rourke".
    normalized = (
        normalized.replace(".", " ")
        .replace(",", " ")
        .replace("'", " ")
        .replace("’", " ")
        .replace("‘", " ")
        .replace("-", " ")
    )

    tokens: list[str] = []
    all_tokens: list[str] = []
    for token in normalized.split():
        cleaned = "".join(char for char in token if char.isalnum() or char.isspace())
        if not cleaned:
            continue
        lowered = cleaned.lower()
        all_tokens.append(lowered)
        if lowered in _DROPPED_NAME_TOKENS:
            continue
        tokens.append(lowered)

    # Never normalize a name down to nothing: if every token was a title or
    # credential, keep the original tokens rather than returning "".
    if not tokens:
        tokens = all_tokens

    return " ".join(tokens).strip()


def build_member_lookup_keys(
    *,
    name: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
    state: str | None = None,
) -> list[str]:
    """Build lookup keys compatible with the CapitolExposed site resolver."""

    keys: list[str] = []
    seen: set[str] = set()
    state_code = (state or "").strip().upper()

    def add_key(value: str | None) -> None:
        normalized = normalize_member_lookup_value(value)
        if not normalized:
            return
        if normalized not in seen:
            seen.add(normalized)
            keys.append(normalized)
        if state_code:
            state_key = f"{normalized}|{state_code}"
            if state_key not in seen:
                seen.add(state_key)
                keys.append(state_key)

        parts = [part for part in normalized.split(" ") if part]
        if len(parts) >= 2:
            first_last = f"{parts[0]} {parts[-1]}"
            if first_last not in seen:
                seen.add(first_last)
                keys.append(first_last)
            if state_code:
                state_first_last = f"{first_last}|{state_code}"
                if state_first_last not in seen:
                    seen.add(state_first_last)
                    keys.append(state_first_last)

            last_only = parts[-1]
            if last_only not in seen:
                seen.add(last_only)
                keys.append(last_only)
            if state_code:
                state_last_only = f"{last_only}|{state_code}"
                if state_last_only not in seen:
                    seen.add(state_last_only)
                    keys.append(state_last_only)

    add_key(name)

    normalized_first = normalize_member_lookup_value(first_name)
    normalized_last = normalize_member_lookup_value(last_name)
    if normalized_first and normalized_last:
        add_key(f"{normalized_first} {normalized_last}")
    elif normalized_last:
        add_key(normalized_last)

    return keys


#: How far outside a term a filing can still belong to that term's holder. A
#: final PTR, a termination report or a late one arrives after the member has
#: left: the House record has them up to four months out. Before the start, a
#: little slack absorbs swearing-in dates that differ by a few days between
#: sources.
SERVICE_GRACE_BEFORE = timedelta(days=30)
SERVICE_GRACE_AFTER = timedelta(days=365)

#: An end date for a sitting member whose row carries none.
_OPEN_END = date(9000, 1, 1)


def _stable_key(match: MemberMatch) -> str:
    if match.id:
        return match.id
    if match.bioguide_id:
        return f"bioguide:{match.bioguide_id}"
    return f"{match.name}|{match.state}|{match.slug}"


def _dedupe_matches(matches: Iterable[MemberMatch]) -> list[MemberMatch]:
    seen: set[str] = set()
    deduped: list[MemberMatch] = []
    for match in matches:
        stable_key = _stable_key(match)
        if stable_key in seen:
            continue
        seen.add(stable_key)
        deduped.append(match)
    return deduped


def normalize_chamber(value: str | None) -> str | None:
    """Map the spellings in use ("Senate", "sen", "rep", "House") to house/senate."""

    text = (value or "").strip().lower()
    if not text:
        return None
    if text in {"senate", "sen", "senator"}:
        return "senate"
    if text in {"house", "rep", "representative"}:
        return "house"
    raise ValueError(f"Unknown chamber: {value!r}")


def _row_value(row: Mapping[str, object], key: str) -> object:
    getter = getattr(row, "get", None)
    return getter(key) if getter else None


def fallback_terms_from_row(row: Mapping[str, object]) -> tuple[ServiceTerm, ...]:
    """The one term a ``members`` row can vouch for: its chamber, latest term.

    Used only when congress-legislators has no record of the person. It is
    narrow on purpose (a member's earlier terms, in either chamber, are not in
    the row), so it can refuse a correct match but cannot make a cross-chamber
    one.
    """

    try:
        chamber = normalize_chamber(str(_row_value(row, "chamber") or ""))
    except ValueError:
        return ()
    if chamber is None:
        return ()
    start = parse_iso_date(_row_value(row, "term_start"))
    end = parse_iso_date(_row_value(row, "term_end"))
    if start is None:
        return ()
    if end is None:
        if not bool(_row_value(row, "in_office")):
            return ()
        end = _OPEN_END
    state = str(_row_value(row, "state") or "").strip().upper() or None
    return (ServiceTerm(chamber=chamber, start=start, end=end, state=state),)


@dataclass(slots=True)
class MemberRegistry:
    """In-memory lookup registry for CapitolExposed members.

    Two lookups live here. ``resolve()`` without ``chamber`` is the original
    name matcher and is what FARA, offshore and search-document linking use.
    ``resolve(chamber=..., as_of=...)`` is the one a trade disclosure must use:
    it only considers people who sat in that chamber around that date, and it
    considers *everyone* congress-legislators knows, not just the people in the
    ``members`` table, so a filer missing from ``members`` resolves to nothing
    instead of to the one member who happens to share a surname.
    """

    records: list[MemberMatch]
    key_index: dict[str, list[MemberMatch]] = field(default_factory=dict)
    bioguide_index: dict[str, MemberMatch] = field(default_factory=dict)
    #: congress-legislators roster keyed by bioguide; empty when unavailable.
    service: dict[str, LegislatorService] = field(default_factory=dict)
    #: Latest-term fallback per member, from the ``members`` row.
    fallback_terms: dict[str, tuple[ServiceTerm, ...]] = field(default_factory=dict)
    #: Name index for chamber-constrained lookups: members under every name
    #: they are known by, plus roster people who have no ``members`` row.
    service_key_index: dict[str, list[MemberMatch]] = field(default_factory=dict)
    terms_by_key: dict[str, tuple[ServiceTerm, ...]] = field(default_factory=dict)
    names_by_key: dict[str, tuple[str, ...]] = field(default_factory=dict)
    service_records: list[MemberMatch] = field(default_factory=list)

    @classmethod
    def from_records(
        cls,
        records: Iterable[MemberMatch],
        *,
        service: Mapping[str, LegislatorService] | None = None,
        fallback_terms: Mapping[str, tuple[ServiceTerm, ...]] | None = None,
    ) -> "MemberRegistry":
        registry = cls(
            records=list(records),
            service=dict(service or {}),
            fallback_terms=dict(fallback_terms or {}),
        )
        registry.rebuild_index()
        return registry

    @classmethod
    def from_rows(
        cls,
        rows: Iterable[Mapping[str, object]],
        *,
        service: Mapping[str, LegislatorService] | None = None,
    ) -> "MemberRegistry":
        records: list[MemberMatch] = []
        fallback: dict[str, tuple[ServiceTerm, ...]] = {}
        for row in rows:
            record = MemberMatch(
                id=str(row.get("id") or "") or None,
                bioguide_id=str(row.get("bioguide_id") or "").strip().upper() or None,
                name=str(row.get("name") or "").strip(),
                slug=str(row.get("slug") or "") or None,
                party=str(row.get("party") or "") or None,
                state=str(row.get("state") or "") or None,
                district=str(row.get("district") or "") or None,
            )
            records.append(record)
            terms = fallback_terms_from_row(row)
            if terms:
                fallback[_stable_key(record)] = terms
        return cls.from_records(records, service=service, fallback_terms=fallback)

    def rebuild_index(self) -> None:
        index: dict[str, list[MemberMatch]] = {}
        bioguide_index: dict[str, MemberMatch] = {}
        for record in self.records:
            if record.bioguide_id:
                bioguide_index[record.bioguide_id.strip().upper()] = record
            keys = build_member_lookup_keys(
                name=record.name,
                state=record.state,
            )
            for key in keys:
                index.setdefault(key, []).append(record)
        self.key_index = index
        self.bioguide_index = bioguide_index
        self._rebuild_service_index()

    def _rebuild_service_index(self) -> None:
        service_index: dict[str, list[MemberMatch]] = {}
        terms_by_key: dict[str, tuple[ServiceTerm, ...]] = {}
        names_by_key: dict[str, tuple[str, ...]] = {}
        service_records: list[MemberMatch] = []

        def add(record: MemberMatch, names: Iterable[str], state: str | None) -> None:
            seen: set[str] = set()
            for known_name in names:
                for key in build_member_lookup_keys(name=known_name, state=state):
                    if key in seen:
                        continue
                    seen.add(key)
                    service_index.setdefault(key, []).append(record)

        for record in self.records:
            stable = _stable_key(record)
            known = self.service.get((record.bioguide_id or "").upper())
            names = tuple(
                dict.fromkeys(
                    known_name
                    for known_name in (record.name, *(known.names if known else ()))
                    if known_name
                )
            )
            terms_by_key[stable] = known.terms if known else self.fallback_terms.get(stable, ())
            names_by_key[stable] = names
            service_records.append(record)
            add(record, names, record.state)

        for bioguide, known in self.service.items():
            if bioguide in self.bioguide_index or not known.names:
                continue
            # Someone who served but has no members row. Never returned as a
            # match: finding them means the filer is known and is not ours.
            placeholder = MemberMatch(
                id=None,
                bioguide_id=bioguide,
                name=known.names[0],
                state=known.state,
            )
            stable = _stable_key(placeholder)
            terms_by_key[stable] = known.terms
            names_by_key[stable] = known.names
            service_records.append(placeholder)
            add(placeholder, known.names, known.state)

        self.service_key_index = service_index
        self.terms_by_key = terms_by_key
        self.names_by_key = names_by_key
        self.service_records = service_records

    def terms_for(self, record: MemberMatch) -> tuple[ServiceTerm, ...]:
        return self.terms_by_key.get(_stable_key(record), ())

    def served_in(self, record: MemberMatch, chamber: str, when: date | None = None) -> bool:
        """Did ``record`` sit in ``chamber`` (within the grace window of ``when``, if given)?"""

        for term in self.terms_for(record):
            if term.chamber != chamber:
                continue
            if when is None or term.covers(
                when, before=SERVICE_GRACE_BEFORE, after=SERVICE_GRACE_AFTER
            ):
                return True
        return False

    def save_json(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        members: list[dict[str, object]] = []
        for record in self.records:
            row: dict[str, object] = record.model_dump()
            fallback = self.fallback_terms.get(_stable_key(record))
            if fallback:
                row["chamber"] = fallback[0].chamber
                row["term_start"] = fallback[0].start.isoformat()
                row["term_end"] = (
                    None if fallback[0].end == _OPEN_END else fallback[0].end.isoformat()
                )
                row["in_office"] = fallback[0].end == _OPEN_END
            members.append(row)
        payload = {
            "members": members,
            "legislators": [known.to_json() for known in self.service.values()],
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return path

    def resolve(
        self,
        *,
        bioguide_id: str | None = None,
        name: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        state: str | None = None,
        chamber: str | None = None,
        as_of: date | str | None = None,
    ) -> MemberMatch | None:
        """Resolve a filer to a member.

        With ``chamber`` ("house"/"senate") the match is restricted to people
        who sat in that chamber within the grace window around ``as_of`` (the
        filing date; when it is ``None``, anyone who ever sat there). A stated
        ``state`` is then a requirement rather than a preference. Anything
        ambiguous, or a filer known to congress-legislators who has no
        ``members`` row, resolves to ``None`` for review instead of to a guess.
        """

        constrained = normalize_chamber(chamber)
        if constrained is not None:
            return self._resolve_in_chamber(
                bioguide_id=bioguide_id,
                name=name,
                first_name=first_name,
                last_name=last_name,
                state=state,
                chamber=constrained,
                when=parse_iso_date(as_of),
            )

        normalized_bioguide = (bioguide_id or "").strip().upper()
        if normalized_bioguide:
            return self.bioguide_index.get(normalized_bioguide)

        state_code = (state or "").strip().upper()
        keys = build_member_lookup_keys(
            name=name,
            first_name=first_name,
            last_name=last_name,
            state=state_code or None,
        )

        for key in keys:
            matches = self.key_index.get(key, [])
            resolved = self._pick_unique(matches, state_code)
            if resolved:
                return resolved

        normalized_name = normalize_member_lookup_value(name)
        if normalized_name:
            fuzzy_matches: list[MemberMatch] = []
            normalized_compact = normalized_name.replace(" ", "")
            for record in self.records:
                record_name = normalize_member_lookup_value(record.name)
                record_compact = record_name.replace(" ", "")
                if state_code and (record.state or "").upper() != state_code:
                    continue
                if (
                    record_name == normalized_name
                    or record_compact == normalized_compact
                    or record_name.startswith(normalized_name)
                    or normalized_name.startswith(record_name)
                ):
                    fuzzy_matches.append(record)
            resolved = self._pick_unique(fuzzy_matches, state_code)
            if resolved:
                return resolved

        return None

    def _resolve_in_chamber(
        self,
        *,
        bioguide_id: str | None,
        name: str | None,
        first_name: str | None,
        last_name: str | None,
        state: str | None,
        chamber: str,
        when: date | None,
    ) -> MemberMatch | None:
        def eligible(record: MemberMatch) -> bool:
            return self.served_in(record, chamber, when)

        normalized_bioguide = (bioguide_id or "").strip().upper()
        if normalized_bioguide:
            record = self.bioguide_index.get(normalized_bioguide)
            return record if record is not None and eligible(record) else None

        state_code = (state or "").strip().upper()

        def in_state(records: Iterable[MemberMatch]) -> list[MemberMatch]:
            deduped = _dedupe_matches(records)
            if not state_code:
                return deduped
            return [record for record in deduped if (record.state or "").strip().upper() == state_code]

        def settle(named: list[MemberMatch]) -> tuple[bool, MemberMatch | None]:
            """Decide on the people a name points at: ``(stop, match)``.

            The roster holds everyone who served, so when a name points at
            people and none of them sat in this chamber at this date, the
            filer has been identified and is not eligible. Stop there rather
            than fall through to a looser key: "Scott H Peters" on a Senate
            filing must not become the one senator named Peters.
            """

            if not named:
                return False, None
            eligible_named = [record for record in named if eligible(record)]
            if not eligible_named:
                return True, None
            if len(eligible_named) == 1:
                picked = eligible_named[0]
                # A roster person with no members row is a known filer we do
                # not carry: unresolved, never a stand-in.
                return True, picked if picked.id else None
            return False, None  # ambiguous; a state-qualified key may still settle it

        keys = build_member_lookup_keys(
            name=name,
            first_name=first_name,
            last_name=last_name,
            state=state_code or None,
        )
        for key in keys:
            stop, picked = settle(in_state(self.service_key_index.get(key, [])))
            if stop:
                return picked

        normalized_name = normalize_member_lookup_value(name)
        # Prefix matching on a single token ("James") would pick whoever's name
        # starts with it; only a full name is allowed to match loosely.
        if normalized_name and len(normalized_name.split()) >= 2:
            normalized_compact = normalized_name.replace(" ", "")
            fuzzy_matches: list[MemberMatch] = []
            for record in self.service_records:
                for known_name in self.names_by_key.get(_stable_key(record), (record.name,)):
                    record_name = normalize_member_lookup_value(known_name)
                    if not record_name:
                        continue
                    if (
                        record_name == normalized_name
                        or record_name.replace(" ", "") == normalized_compact
                        or record_name.startswith(normalized_name)
                        or normalized_name.startswith(record_name)
                    ):
                        fuzzy_matches.append(record)
                        break
            _stop, picked = settle(in_state(fuzzy_matches))
            return picked

        return None

    def resolve_feed_member(
        self,
        first_name: str | None,
        last_name: str | None,
        state: str | None,
        filing_date: date | str | None = None,
    ) -> MemberMatch | None:
        """Resolve a House Clerk filer: someone in the House around the filing date."""

        return self.resolve(
            first_name=first_name,
            last_name=last_name,
            state=state,
            chamber="house",
            as_of=filing_date,
        )

    def _pick_unique(self, matches: Iterable[MemberMatch], state_code: str) -> MemberMatch | None:
        deduped = _dedupe_matches(matches)
        if state_code:
            state_filtered = [
                match for match in deduped if (match.state or "").strip().upper() == state_code
            ]
            if len(state_filtered) == 1:
                return state_filtered[0]
            if len(state_filtered) > 1:
                deduped = state_filtered

        if len(deduped) == 1:
            return deduped[0]
        return None


def load_member_registry_from_json(path: Path) -> MemberRegistry:
    """Load a member registry from a cached JSON export.

    Accepts the current ``{"members": [...], "legislators": [...]}`` shape and
    the older bare list of member rows.
    """

    payload = json.loads(path.read_text(encoding="utf-8"))
    service: dict[str, LegislatorService] = {}
    if isinstance(payload, dict):
        rows = payload.get("members")
        for raw in payload.get("legislators") or []:
            if isinstance(raw, Mapping):
                known = LegislatorService.from_json(raw)
                if known is not None:
                    service[known.bioguide_id] = known
    else:
        rows = payload
    if not isinstance(rows, list):
        raise ValueError(f"Expected a list of member rows in {path}")
    return MemberRegistry.from_rows(rows, service=service)
