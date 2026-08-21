"""Deterministic yfinance stand-in when YF_FIXTURE_MODE=1."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
FIXTURE_PATH = BASE_DIR / "tests" / "fixtures" / "market_ohlc.json"

_CACHE: dict[str, Any] | None = None


def _load() -> dict[str, Any]:
    global _CACHE
    if _CACHE is None:
        with open(FIXTURE_PATH, "r", encoding="utf-8") as f:
            _CACHE = json.load(f)
    return _CACHE


def _frame_from_rows(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close"])
    df = pd.DataFrame(rows)
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.set_index("Date").sort_index()
    return df[["Open", "High", "Low", "Close"]].astype(float)


def fixture_download(
    tickers=None,
    start=None,
    end=None,
    interval="1d",
    progress=False,
    auto_adjust=False,
    group_by="ticker",
    timeout=20,
    **kwargs,
):
    """Drop-in replacement for yf.download used in fixture mode."""
    data = _load()
    ticker = tickers if isinstance(tickers, str) else (tickers[0] if tickers else None)
    rows = data.get("daily", {}).get(ticker, [])
    df = _frame_from_rows(rows)
    if start:
        df = df[df.index >= pd.Timestamp(start)]
    if end:
        df = df[df.index < pd.Timestamp(end)]
    return df


class FixtureTicker:
    def __init__(self, symbol: str):
        self.symbol = symbol

    def history(self, period: str = "5d", **kwargs):
        data = _load()
        rows = data.get("history_5d", {}).get(self.symbol, [])
        return _frame_from_rows(rows)


def fixture_ticker(symbol: str) -> FixtureTicker:
    return FixtureTicker(symbol)
