from capitol_pipeline.bridges.search_documents import build_fara_registrant_search_document
from capitol_pipeline.models.fara import FaraRegistrantBundle
from capitol_pipeline.sources.fara import (
    build_fara_document_record,
    build_fara_foreign_principal_record,
    build_fara_registrant_record,
    build_fara_short_form_record,
    normalize_fara_date,
)


def test_normalize_fara_date_handles_multiple_formats() -> None:
    assert normalize_fara_date("02/26/2026") == "2026-02-26"
    assert normalize_fara_date("2026-02-26T00:00:00") == "2026-02-26"


def test_build_fara_registrant_record_normalizes_core_fields() -> None:
    record = build_fara_registrant_record(
        {
            "Registration_Number": 7701,
            "Name": "James P. Fabiani LLC",
            "Registration_Date": "02/26/2026",
            "Address_1": "26 Willow Lane",
            "City": "Lenox",
            "State": "MA",
            "Zip": "01240",
        }
    )
    assert record.registration_number == 7701
    assert record.normalized_name == "james p fabiani llc"
    assert record.registration_date == "2026-02-26"


def test_build_fara_related_rows_generate_stable_keys() -> None:
    principal = build_fara_foreign_principal_record(
        {
            "REG_NUMBER": 579,
            "FP_NAME": "Visit Wales",
            "REGISTRANT_NAME": "VisitBritain",
            "COUNTRY_NAME": "GREAT BRITAIN",
            "FP_REG_DATE": "2018-05-03T00:00:00",
        }
    )
    short_form = build_fara_short_form_record(
        {
            "REG_NUMBER": 579,
            "REGISTRANT_NAME": "VisitBritain",
            "SF_FIRST_NAME": "Taryn",
            "SF_LAST_NAME": "McCarthy",
            "SHORTFORM_DATE": "2025-06-04T00:00:00",
        }
    )
    document = build_fara_document_record(
        {
            "REGISTRATION_NUMBER": 579,
            "REGISTRANT_NAME": "VisitBritain",
            "DOCUMENT_TYPE": "Supplemental Statement",
            "DATE_STAMPED": "2025-12-22T00:00:00",
            "URL": "https://efile.fara.gov/docs/579-Supplemental-Statement-20251222-46.pdf",
        }
    )
    assert principal.principal_key
    assert short_form.short_form_key
    assert document.document_key


def test_build_fara_registrant_search_document_collects_related_entities() -> None:
    registrant = build_fara_registrant_record(
        {
            "Registration_Number": 579,
            "Name": "VisitBritain",
            "Registration_Date": "02/26/2026",
        }
    )
    bundle = FaraRegistrantBundle(
        registrant=registrant,
        foreign_principals=[
            build_fara_foreign_principal_record(
                {
                    "REG_NUMBER": 579,
                    "FP_NAME": "Visit Wales",
                    "REGISTRANT_NAME": "VisitBritain",
                    "COUNTRY_NAME": "GREAT BRITAIN",
                    "FP_REG_DATE": "2018-05-03T00:00:00",
                }
            )
        ],
        short_forms=[
            build_fara_short_form_record(
                {
                    "REG_NUMBER": 579,
                    "REGISTRANT_NAME": "VisitBritain",
                    "SF_FIRST_NAME": "Taryn",
                    "SF_LAST_NAME": "McCarthy",
                    "SHORTFORM_DATE": "2025-06-04T00:00:00",
                }
            )
        ],
        documents=[
            build_fara_document_record(
                {
                    "REGISTRATION_NUMBER": 579,
                    "REGISTRANT_NAME": "VisitBritain",
                    "DOCUMENT_TYPE": "Supplemental Statement",
                    "DATE_STAMPED": "2025-12-22T00:00:00",
                    "URL": "https://efile.fara.gov/docs/579-Supplemental-Statement-20251222-46.pdf",
                }
            )
        ],
    )

    search_document = build_fara_registrant_search_document(bundle)
    assert search_document.source == "fara"
    assert search_document.category == "lobbying"
    assert search_document.metadata["registrationNumber"] == 579
    assert "Visit Wales" in search_document.content
