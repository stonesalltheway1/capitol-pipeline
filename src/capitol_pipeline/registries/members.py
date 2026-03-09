"""Member registry and lookup logic for Capitol Pipeline."""

from __future__ import annotations

import json
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping

from capitol_pipeline.models.congress import MemberMatch


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

    normalized = (
        normalized.replace(".", " ")
        .replace(",", " ")
        .replace("'", " ")
        .replace("-", " ")
    )

    tokens: list[str] = []
    for token in normalized.split():
        lowered = token.lower()
        if lowered in {"jr", "sr", "ii", "iii", "iv"}:
            continue
        cleaned = "".join(char for char in token if char.isalnum() or char.isspace())
        if cleaned:
            tokens.append(cleaned.lower())

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


def _dedupe_matches(matches: Iterable[MemberMatch]) -> list[MemberMatch]:
    seen: set[str] = set()
    deduped: list[MemberMatch] = []
    for match in matches:
        stable_key = match.id or f"{match.name}|{match.state}|{match.slug}"
        if stable_key in seen:
            continue
        seen.add(stable_key)
        deduped.append(match)
    return deduped


@dataclass(slots=True)
class MemberRegistry:
    """In-memory lookup registry for CapitolExposed members."""

    records: list[MemberMatch]
    key_index: dict[str, list[MemberMatch]] = field(default_factory=dict)

    @classmethod
    def from_records(cls, records: Iterable[MemberMatch]) -> "MemberRegistry":
        registry = cls(records=list(records))
        registry.rebuild_index()
        return registry

    @classmethod
    def from_rows(cls, rows: Iterable[Mapping[str, object]]) -> "MemberRegistry":
        records: list[MemberMatch] = []
        for row in rows:
            records.append(
                MemberMatch(
                    id=str(row.get("id") or "") or None,
                    name=str(row.get("name") or "").strip(),
                    slug=str(row.get("slug") or "") or None,
                    party=str(row.get("party") or "") or None,
                    state=str(row.get("state") or "") or None,
                    district=str(row.get("district") or "") or None,
                )
            )
        return cls.from_records(records)

    def rebuild_index(self) -> None:
        index: dict[str, list[MemberMatch]] = {}
        for record in self.records:
            keys = build_member_lookup_keys(
                name=record.name,
                state=record.state,
            )
            for key in keys:
                index.setdefault(key, []).append(record)
        self.key_index = index

    def save_json(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = [record.model_dump() for record in self.records]
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return path

    def resolve(
        self,
        *,
        name: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        state: str | None = None,
    ) -> MemberMatch | None:
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

    def resolve_feed_member(
        self,
        first_name: str | None,
        last_name: str | None,
        state: str | None,
    ) -> MemberMatch | None:
        return self.resolve(first_name=first_name, last_name=last_name, state=state)

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
    """Load a member registry from a cached JSON export."""

    rows = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        raise ValueError(f"Expected a list of member rows in {path}")
    return MemberRegistry.from_rows(rows)
