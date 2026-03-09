from capitol_pipeline.models.congress import MemberMatch
from capitol_pipeline.registries.members import (
    MemberRegistry,
    build_member_lookup_keys,
    normalize_member_lookup_value,
)


def test_normalize_member_lookup_value_strips_titles_suffixes_and_diacritics() -> None:
    assert normalize_member_lookup_value("Rep. Linda T. Sánchez, Jr.") == "linda t sanchez"
    assert normalize_member_lookup_value("Senator Thomas H. Kean Jr.") == "thomas h kean"


def test_build_member_lookup_keys_includes_state_variants() -> None:
    keys = build_member_lookup_keys(name="Thomas H. Kean Jr.", state="NJ")
    assert "thomas h kean" in keys
    assert "thomas kean|NJ" in keys
    assert "kean|NJ" in keys


def test_member_registry_resolves_state_aware_ambiguities() -> None:
    registry = MemberRegistry.from_records(
        [
            MemberMatch(id="m-1", name="Linda T. Sanchez", slug="linda-t-sanchez", party="D", state="CA"),
            MemberMatch(id="m-2", name="Linda Sanchez", slug="linda-sanchez-tx", party="D", state="TX"),
            MemberMatch(id="m-3", name="Thomas H. Kean Jr.", slug="thomas-h-kean-jr", party="R", state="NJ"),
        ]
    )

    ca_match = registry.resolve(first_name="Linda", last_name="Sanchez", state="CA")
    assert ca_match is not None
    assert ca_match.id == "m-1"

    tx_match = registry.resolve(name="Linda Sanchez", state="TX")
    assert tx_match is not None
    assert tx_match.id == "m-2"

    kean_match = registry.resolve(name="Thomas H. Kean Jr.", state="NJ")
    assert kean_match is not None
    assert kean_match.id == "m-3"
