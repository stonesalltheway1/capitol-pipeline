from capitol_pipeline.models.congress import FilingStub, MemberMatch
from capitol_pipeline.parsers.house_ptr import parse_house_ptr_text


ANN_WAGNER_PREVIEW = """
P T R Clerk of the House of Representatives • Legislative Resource Center • B81 Cannon Building • Washington, DC 20515
F I Name: Hon. Ann Wagner Status: Member State/District: MO02 T ID Owner Asset Transaction Type Date Notification Date Amount Cap. Gains > $200?
JT Floyd County IN 4.00% 12/30/2031 [GS] P 12/05/2025 12/08/2025 $100,001 - $250,000
F S: New S O: Fidelity Trust
JT Fremont IN Cmnty Schs Gen Oblig Bds 4.00% 1/15/2032 [GS] P 12/05/2025 12/08/2025 $100,001 - $250,000
F S: New S O: Fidelity Trust
JT Logan Rogersville MO Reorg No R 08 Sch 4.00% 3/1/2033 [GS] P 12/04/2025 12/05/2025 $100,001 - $250,000
F S: New S O: Fidelity Trust
JT Teays Valley OH 5.00% 12/1/2033 [GS] P 12/18/2025 12/19/2025 $100,001 - $250,000
F S: New S O: Fidelity Trust
Filing ID #20033718
""".strip()


ROGER_WILLIAMS_PREVIEW = """
P T R Clerk of the House of Representatives • Legislative Resource Center • B81 Cannon Building • Washington, DC 20515
F I Name: Hon. Roger Williams Status: Member State/District: TX25 T ID Owner Asset Transaction Type Date Notification Date Amount Cap. Gains > $200?
Chevron Corporation Common Stock (CVX) [ST] S (partial) 12/22/2025 12/22/2025 $15,001 - $50,000
F S: New S O: Charles Schwab 4067
Diamondback Energy, Inc. - Common Stock (FANG) [ST] S (partial) 12/22/2025 12/22/2025 $1,001 - $15,000
F S: New S O: Charles Schwab 4067
JP Morgan Chase & Co. Common Stock (JPM) [ST] P 12/22/2025 12/22/2025 $1,001 - $15,000
F S: New S O: Charles Schwab 4067
RTX Corporation Common Stock (RTX) [ST] P 12/22/2025 12/22/2025 $1,001 - $15,000
F S: New S O: Charles Schwab 4067
Filing ID #20033783
""".strip()


BITCOIN_PREVIEW = """
P T R Clerk of the House of Representatives • Legislative Resource Center • B81 Cannon Building • Washington, DC 20515
F I Name: Hon. Example Member Status: Member State/District: TX01 T ID Owner Asset Transaction Type Date Notification Date Amount Cap. Gains > $200?
Bitcoin (BTC) [ST] P 01/02/2026 01/05/2026 $1,001 - $15,000
Filing ID #20039999
""".strip()


def build_stub(doc_id: str, name: str, state: str, filing_date: str = "2026-01-10") -> FilingStub:
    return FilingStub(
        doc_id=doc_id,
        filing_year=2026,
        filing_date=filing_date,
        member=MemberMatch(
            id=f"m-{doc_id}",
            name=name,
            slug=name.lower().replace(" ", "-"),
            party="R",
            state=state,
        ),
        source="house-clerk",
        source_url=f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/{doc_id}.pdf",
    )


def test_parse_house_ptr_bond_rows() -> None:
    parsed, trades = parse_house_ptr_text(
        ANN_WAGNER_PREVIEW,
        build_stub("20033718", "Ann Wagner", "MO"),
    )
    assert parsed.member_name == "Ann Wagner"
    assert parsed.state == "MO"
    assert len(parsed.transactions) == 4
    assert parsed.parser_confidence >= 0.8
    assert parsed.transactions[0].asset_type == "Government Security"
    assert parsed.transactions[0].owner == "joint"
    assert parsed.transactions[0].amount_min == 100001
    assert parsed.transactions[0].amount_max == 250000
    assert trades[0].asset_type == "Government Security"
    assert trades[0].disclosure_date == "2026-01-10"


def test_parse_house_ptr_stock_rows() -> None:
    parsed, trades = parse_house_ptr_text(
        ROGER_WILLIAMS_PREVIEW,
        build_stub("20033783", "Roger Williams", "TX"),
    )
    assert len(parsed.transactions) == 4
    assert parsed.transactions[0].ticker == "CVX"
    assert parsed.transactions[0].asset_description == "Chevron Corporation Common Stock"
    assert parsed.transactions[0].transaction_type == "sale"
    assert parsed.transactions[0].owner == "self"
    assert parsed.transactions[1].ticker == "FANG"
    assert parsed.transactions[2].transaction_type == "purchase"
    assert trades[0].member.slug == "roger-williams"
    assert trades[0].source_id == "20033783:1"


def test_parse_house_ptr_crypto_row() -> None:
    parsed, trades = parse_house_ptr_text(
        BITCOIN_PREVIEW,
        build_stub("20039999", "Example Member", "TX"),
    )
    assert len(parsed.transactions) == 1
    assert parsed.transactions[0].ticker == "BTC"
    assert trades[0].normalized_asset is not None
    assert trades[0].normalized_asset.kind == "direct_crypto"
    assert trades[0].asset_type == "Cryptocurrency"
