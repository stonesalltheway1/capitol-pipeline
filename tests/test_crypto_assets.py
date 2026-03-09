from capitol_pipeline.normalizers.crypto_assets import classify_crypto_asset


def test_classifies_direct_crypto_from_description() -> None:
    match = classify_crypto_asset(None, "Bitcoin (CRYPTO:BTC)")
    assert match.kind == "direct_crypto"
    assert match.canonical_symbol == "BTC"


def test_classifies_bitcoin_etf_from_ticker() -> None:
    match = classify_crypto_asset("IBIT", "iShares Bitcoin Trust ETF")
    assert match.kind == "crypto_etf"
    assert match.canonical_symbol == "IBIT"


def test_classifies_crypto_equity_from_ticker() -> None:
    match = classify_crypto_asset("COIN", "Coinbase Global Class A Common Stock")
    assert match.kind == "crypto_equity"
