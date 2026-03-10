from capitol_pipeline.sources.congress_gov import (
    build_congress_bill_action_records,
    build_congress_bill_record,
    build_congress_bill_summary_records,
    strip_congress_html,
)


def test_strip_congress_html_preserves_readable_summary_text() -> None:
    html_text = (
        "<p><strong>Advanced Capabilities for Emergency Response Operations Act</strong></p>"
        "<p>This bill provides statutory authority for the ACERO project.&nbsp;</p>"
    )

    cleaned = strip_congress_html(html_text)
    assert "Advanced Capabilities for Emergency Response Operations Act" in cleaned
    assert "This bill provides statutory authority for the ACERO project." in cleaned


def test_build_congress_bill_record_collects_summary_and_action_context() -> None:
    site_row = {
        "id": "hr-119-390",
        "congress": 119,
        "bill_type": "hr",
        "number": "390",
        "title": "ACERO Act",
        "short_title": "",
        "subjects": ["Science, Technology, Communications"],
    }
    bill_payload = {
        "bill": {
            "congress": 119,
            "type": "HR",
            "number": "390",
            "title": "ACERO Act",
            "originChamber": "House",
            "introducedDate": "2025-01-14",
            "latestAction": {
                "actionDate": "2026-02-24",
                "text": "Received in the Senate and Read twice and referred to the Committee on Commerce, Science, and Transportation.",
            },
            "policyArea": {"name": "Science, Technology, Communications"},
            "legislationUrl": "https://www.congress.gov/bill/119th-congress/house-bill/390",
            "sponsors": [
                {
                    "bioguideId": "F000480",
                    "fullName": "Rep. Fong, Vince [R-CA-20]",
                    "party": "R",
                    "state": "CA",
                    "district": 20,
                }
            ],
            "summaries": {"count": 1},
            "actions": {"count": 2},
            "committees": {"count": 2},
            "cosponsors": {"count": 7},
            "subjects": {"count": 9},
            "textVersions": {"count": 4},
            "updateDate": "2026-02-26T07:38:15Z",
            "updateDateIncludingText": "2026-02-26T07:48:42Z",
        }
    }
    summaries = [
        {
            "actionDate": "2025-01-14",
            "actionDesc": "Introduced in House",
            "text": "<p>This bill provides statutory authority for the ACERO project.</p>",
            "updateDate": "2025-03-13T19:21:12Z",
            "versionCode": "00",
        }
    ]
    actions = [
        {
            "actionDate": "2026-02-24",
            "text": "Received in the Senate and Read twice and referred to the Committee on Commerce, Science, and Transportation.",
            "type": "IntroReferral",
            "committees": [
                {
                    "name": "Commerce, Science, and Transportation Committee",
                    "systemCode": "sscm00",
                }
            ],
        }
    ]

    record = build_congress_bill_record(
        site_bill_row=site_row,
        bill_payload=bill_payload,
        summaries=summaries,
        actions=actions,
    )
    summary_records = build_congress_bill_summary_records(
        site_bill_id="hr-119-390",
        congress=119,
        bill_type="hr",
        bill_number="390",
        bill_key=record.bill_key,
        source_url=record.legislation_url,
        items=summaries,
    )
    action_records = build_congress_bill_action_records(
        site_bill_id="hr-119-390",
        congress=119,
        bill_type="hr",
        bill_number="390",
        bill_key=record.bill_key,
        source_url=record.legislation_url,
        items=actions,
    )

    assert record.bill_key == "119-hr-390"
    assert record.summary == "This bill provides statutory authority for the ACERO project."
    assert record.committee_codes == ["sscm00"]
    assert summary_records[0].version_code == "00"
    assert action_records[0].committee_names == ["Commerce, Science, and Transportation Committee"]
