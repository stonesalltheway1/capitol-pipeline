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


def test_normalize_member_lookup_value_drops_honorifics_and_credentials() -> None:
    # Forms the House Clerk feed actually emits for the same two people.
    assert normalize_member_lookup_value("Marjorie Taylor Mrs Greene") == "marjorie taylor greene"
    assert normalize_member_lookup_value("Neal Patrick Dunn, MD, FACS") == "neal patrick dunn"
    assert normalize_member_lookup_value("Gerald E. Connolly") == "gerald e connolly"
    # A name made only of dropped tokens is kept rather than collapsed to "".
    assert normalize_member_lookup_value("Mrs") == "mrs"


def test_member_registry_resolves_feed_names_with_credentials() -> None:
    registry = MemberRegistry.from_records(
        [
            MemberMatch(id="m-D000628", name="Neal P. Dunn", slug="neal-p-dunn", party="R", state="FL"),
            MemberMatch(id="m-G000596", name="Marjorie Taylor Greene", slug="marjorie-taylor-greene", party="R", state="GA"),
        ]
    )

    dunn = registry.resolve(first_name="Neal Patrick", last_name="Dunn, MD, FACS", state="FL")
    assert dunn is not None and dunn.id == "m-D000628"

    greene = registry.resolve(first_name="Marjorie Taylor Mrs", last_name="Greene", state="GA")
    assert greene is not None and greene.id == "m-G000596"

    by_name = registry.resolve(name="Neal Patrick Dunn, MD, FACS", state="FL")
    assert by_name is not None and by_name.id == "m-D000628"
