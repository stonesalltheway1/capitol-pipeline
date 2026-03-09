"""Crypto asset normalization and classification for congressional trades."""

from __future__ import annotations

import re

from capitol_pipeline.models.congress import NormalizedAsset

DIRECT_CRYPTO_SYMBOLS: dict[str, tuple[str, ...]] = {
    "BTC": ("BITCOIN", "CRYPTO:BTC", "WBTC"),
    "ETH": ("ETHEREUM", "ETHER", "CRYPTO:ETH", "WETH"),
    "SOL": ("SOLANA", "CRYPTO:SOL"),
    "XRP": ("RIPPLE", "CRYPTO:XRP"),
    "ADA": ("CARDANO", "CRYPTO:ADA"),
    "DOGE": ("DOGECOIN", "CRYPTO:DOGE"),
    "LTC": ("LITECOIN", "CRYPTO:LTC"),
    "DOT": ("POLKADOT", "CRYPTO:DOT"),
    "AVAX": ("AVALANCHE", "CRYPTO:AVAX"),
    "LINK": ("CHAINLINK", "CRYPTO:LINK"),
}

CRYPTO_ETF_TICKERS: dict[str, str] = {
    "IBIT": "iShares Bitcoin Trust ETF",
    "FBTC": "Fidelity Wise Origin Bitcoin Fund",
    "GBTC": "Grayscale Bitcoin Trust",
    "BITB": "Bitwise Bitcoin ETF",
    "ARKB": "ARK 21Shares Bitcoin ETF",
    "HODL": "VanEck Bitcoin Trust",
    "EZBC": "Franklin Bitcoin ETF",
    "BRRR": "Valkyrie Bitcoin Fund",
    "BTCW": "WisdomTree Bitcoin Fund",
    "BTCO": "Invesco Galaxy Bitcoin ETF",
    "ETHA": "iShares Ethereum Trust ETF",
    "FETH": "Fidelity Ethereum Fund",
    "ETHW": "Bitwise Ethereum ETF",
    "ETHE": "Grayscale Ethereum Trust",
}

CRYPTO_EQUITY_TICKERS: dict[str, str] = {
    "COIN": "Coinbase Global",
    "MSTR": "MicroStrategy",
    "MARA": "MARA Holdings",
    "RIOT": "Riot Platforms",
    "HUT": "Hut 8",
    "CLSK": "CleanSpark",
    "BITF": "Bitfarms",
    "CIFR": "Cipher Mining",
    "BTBT": "Bit Digital",
    "CORZ": "Core Scientific",
    "IREN": "IREN",
    "HIVE": "Hive Digital Technologies",
    "WULF": "TeraWulf",
    "HOOD": "Robinhood Markets",
}


def _clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "")).strip()


def classify_crypto_asset(
    ticker: str | None,
    description: str | None,
) -> NormalizedAsset:
    """Classify a raw security into direct crypto, ETF/trust, adjacent equity, or unrelated."""

    raw_ticker = (ticker or "").strip().upper() or None
    raw_description = _clean_text(description)
    haystack = f"{raw_ticker or ''} {raw_description}".upper().strip()

    if raw_ticker and raw_ticker in CRYPTO_ETF_TICKERS:
        return NormalizedAsset(
            raw_ticker=raw_ticker,
            raw_description=raw_description,
            canonical_symbol=raw_ticker,
            canonical_name=CRYPTO_ETF_TICKERS[raw_ticker],
            kind="crypto_etf",
            matched_by="ticker",
            confidence=0.98,
            aliases=[CRYPTO_ETF_TICKERS[raw_ticker]],
        )

    if raw_ticker and raw_ticker in CRYPTO_EQUITY_TICKERS:
        return NormalizedAsset(
            raw_ticker=raw_ticker,
            raw_description=raw_description,
            canonical_symbol=raw_ticker,
            canonical_name=CRYPTO_EQUITY_TICKERS[raw_ticker],
            kind="crypto_equity",
            matched_by="ticker",
            confidence=0.97,
            aliases=[CRYPTO_EQUITY_TICKERS[raw_ticker]],
        )

    for symbol, aliases in DIRECT_CRYPTO_SYMBOLS.items():
        if raw_ticker == symbol or any(alias in haystack for alias in aliases):
            return NormalizedAsset(
                raw_ticker=raw_ticker,
                raw_description=raw_description,
                canonical_symbol=symbol,
                canonical_name=aliases[0].title(),
                kind="direct_crypto",
                matched_by="ticker" if raw_ticker == symbol else "description",
                confidence=0.99 if raw_ticker == symbol else 0.94,
                aliases=list(aliases),
            )

    for ticker_symbol, label in CRYPTO_ETF_TICKERS.items():
        if label.upper() in haystack:
            return NormalizedAsset(
                raw_ticker=raw_ticker,
                raw_description=raw_description,
                canonical_symbol=ticker_symbol,
                canonical_name=label,
                kind="crypto_etf",
                matched_by="description",
                confidence=0.92,
                aliases=[label],
            )

    for ticker_symbol, label in CRYPTO_EQUITY_TICKERS.items():
        if label.upper() in haystack:
            return NormalizedAsset(
                raw_ticker=raw_ticker,
                raw_description=raw_description,
                canonical_symbol=ticker_symbol,
                canonical_name=label,
                kind="crypto_equity",
                matched_by="description",
                confidence=0.9,
                aliases=[label],
            )

    return NormalizedAsset(
        raw_ticker=raw_ticker,
        raw_description=raw_description,
        canonical_symbol=raw_ticker,
        canonical_name=raw_description or raw_ticker,
        kind="unrelated",
        matched_by=None,
        confidence=0.0,
    )


def is_crypto_related(ticker: str | None, description: str | None) -> bool:
    """Return whether the raw asset is crypto-linked."""

    return classify_crypto_asset(ticker, description).kind != "unrelated"
