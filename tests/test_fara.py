from capitol_pipeline.bridges.search_documents import build_fara_registrant_search_document
from capitol_pipeline.models.fara import FaraRegistrantBundle
from capitol_pipeline.sources.fara import (
    build_fara_document_record,
    build_fara_foreign_principal_record,
    build_fara_registrant_record,
    build_fara_short_form_record,
    normalize_fara_date,
    repair_bulk_foreign_principal_row,
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


def test_build_fara_registrant_record_supports_bulk_headers() -> None:
    record = build_fara_registrant_record(
        {
            "Registration Number": "7701",
            "Name": "James P. Fabiani LLC",
            "Registration Date": "02/26/2026",
            "Address 1": "26 Willow Lane",
            "City": "Lenox",
            "State": "MA",
            "Zip": "01240",
        }
    )
    assert record.registration_number == 7701
    assert record.address_1 == "26 Willow Lane"
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


def test_build_fara_related_rows_support_bulk_headers() -> None:
    principal = build_fara_foreign_principal_record(
        {
            "Registration Number": "6790",
            "Foreign Principal": "Embassy of Sudan",
            "Foreign Principal Registration Date": "03/06/2026",
            "Country/Location Represented": "SUDAN",
            "Registrant Name": "Williams Group",
            "Address 1": "2210 Massachusetts Ave NW",
            "City": "Washington",
            "State": "DC",
            "Zip": "20008",
        }
    )
    short_form = build_fara_short_form_record(
        {
            "Registration Number": "7701",
            "Registrant Name": "James P. Fabiani LLC",
            "Short Form First Name": "James Parnham",
            "Short Form Last Name": "Fabiani",
            "Short Form Date": "02/26/2026",
            "Address 1": "26 Willow Lane",
            "City": "Lenox",
            "State": "MA",
            "Zip": "01240",
        }
    )
    document = build_fara_document_record(
        {
            "Registration Number": "6170",
            "Registrant Name": "Mercury Public Affairs, LLC",
            "Document Type": "Amendment",
            "Date Stamped": "03/06/2026",
            "URL": "https://efile.fara.gov/docs/6170-Amendment-20260306-3.pdf",
        }
    )
    assert principal.foreign_principal_name == "Embassy of Sudan"
    assert short_form.full_name == "James Parnham Fabiani"
    assert document.document_type == "Amendment"


def test_repair_bulk_foreign_principal_row_handles_shifted_registration_number() -> None:
    repaired = repair_bulk_foreign_principal_row(
        {
            "Foreign Principal Termination Date": "09/01/2024",
            "Foreign Principal": 'NGO "Free and Faithful",03/25/2024"',
            "Foreign Principal Registration Date": "UKRAINE",
            "Country/Location Represented": "7383",
            "Registration Number": "03/20/2024",
            "Registrant Date": "SIC Group USA LLC",
            "Registrant Name": "Kruglouniversytetska street 7, 27 suite",
            "Address 1": "",
            "Address 2": "Kyiv",
            "City": "",
            "State": "",
            "Zip": "",
        }
    )
    assert repaired["Registration Number"] == "7383"
    assert repaired["Country/Location Represented"] == "UKRAINE"
    assert repaired["Foreign Principal"] == "NGO \"Free and Faithful"


def test_repair_bulk_foreign_principal_row_handles_shifted_country_and_registrant() -> None:
    repaired = repair_bulk_foreign_principal_row(
        {
            "Foreign Principal Termination Date": "12/01/2022",
            "Foreign Principal": 'Saudi Arabia Railways (SAR") ',
            "Foreign Principal Registration Date": ' through HIll +Knowlton Strategies GMBH"',
            "Country/Location Represented": "10/12/2022",
            "Registration Number": "SAUDI ARABIA",
            "Registrant Date": "3301",
            "Registrant Name": "11/10/1981",
            "Address 1": "Hill and Knowlton Strategies, LLC",
            "Address 2": "",
            "City": "",
            "State": "",
            "Zip": "",
        }
    )
    assert repaired["Registration Number"] == "3301"
    assert repaired["Country/Location Represented"] == "SAUDI ARABIA"
    assert repaired["Registrant Name"] == "Hill and Knowlton Strategies, LLC"


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
