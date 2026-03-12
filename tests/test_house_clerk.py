from capitol_pipeline.sources.house_clerk import parse_house_feed


SAMPLE_XML = """
<FinancialDisclosure>
  <Member>
    <DocID>20034034</DocID>
    <First>Thomas</First>
    <Last>Kean</Last>
    <StateDst>NJ07</StateDst>
    <FilingDate>02/18/2026</FilingDate>
    <FilingType>P</FilingType>
  </Member>
  <Member>
    <DocID>10073174</DocID>
    <First>Example</First>
    <Last>Annual</Last>
    <StateDst>TX01</StateDst>
    <FilingDate>02/18/2026</FilingDate>
    <FilingType>Y</FilingType>
  </Member>
</FinancialDisclosure>
""".strip()


def test_parse_house_feed_basic_row() -> None:
    rows = parse_house_feed(SAMPLE_XML, year=2026)
    assert len(rows) == 1
    assert rows[0].doc_id == "20034034"
    assert rows[0].member.name == "Thomas Kean"
    assert rows[0].filing_date == "2026-02-18"
    assert rows[0].filing_type == "P"
