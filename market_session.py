"""Yahoo Finance session that bypasses TLS fingerprint / 429 blocks.

Plain `requests` (used by older yfinance defaults) gets rate-limited on
Vercel and many cloud IPs. curl_cffi Chrome impersonation works.
"""
from __future__ import annotations

import os
import time
from typing import Any, Sequence

import pandas as pd
import yfinance as yf

YF_FIXTURE_MODE = os.environ.get("YF_FIXTURE_MODE", "").strip() in (
    "1",
    "true",
    "True",
    "yes",
)

_SESSION = None


def get_yf_session():
    """Shared curl_cffi Chrome session (or None in fixture mode)."""
    global _SESSION
    if YF_FIXTURE_MODE:
        return None
    if _SESSION is None:
        from curl_cffi import requests as curl_requests

        _SESSION = curl_requests.Session(impersonate="chrome")
    return _SESSION


def yf_download(*args: Any, **kwargs: Any) -> pd.DataFrame:
    session = get_yf_session()
    if session is not None:
        kwargs.setdefault("session", session)
    return yf.download(*args, **kwargs)


def yf_ticker(symbol: str):
    session = get_yf_session()
    if session is not None:
        return yf.Ticker(symbol, session=session)
    return yf.Ticker(symbol)


def normalize_ohlc_df(df: pd.DataFrame, ticker: str | None = None) -> pd.DataFrame:
    """Flatten MultiIndex OHLC frames from yfinance into Open/High/Low/Close."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close"])

    work = df.copy()
    if isinstance(work.columns, pd.MultiIndex):
        picked = None
        if ticker:
            for level in (1, 0):
                try:
                    picked = work.xs(ticker, level=level, axis=1)
                    break
                except (KeyError, IndexError, ValueError):
                    continue
        if picked is None:
            # Single-ticker download: OHLC on level 0, symbol on level 1 (yf 1.x)
            try:
                if work.columns.nlevels >= 2 and len(work.columns.get_level_values(-1).unique()) == 1:
                    picked = work.copy()
                    picked.columns = picked.columns.get_level_values(0)
                else:
                    picked = work.copy()
                    picked.columns = picked.columns.get_level_values(-1)
            except Exception:
                picked = work.copy()
                picked.columns = ["_".join(map(str, c)) if isinstance(c, tuple) else str(c) for c in picked.columns]
        work = picked

    cols = {str(c).strip(): c for c in work.columns}
    rename = {}
    for want in ("Open", "High", "Low", "Close"):
        if want in work.columns:
            continue
        for key, original in cols.items():
            if key.lower() == want.lower():
                rename[original] = want
                break
    if rename:
        work = work.rename(columns=rename)

    keep = [c for c in ["Open", "High", "Low", "Close"] if c in work.columns]
    if len(keep) < 4:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close"])
    out = work[keep].apply(pd.to_numeric, errors="coerce").dropna(how="any")
    return out


def download_ohlc(
    ticker: str,
    *,
    start: str | None = None,
    end: str | None = None,
    period: str | None = None,
    interval: str = "1d",
    retries: int = 3,
    pause_sec: float = 0.75,
) -> pd.DataFrame:
    """Download OHLC with retries; returns normalized empty frame on failure."""
    attempts = 1 if YF_FIXTURE_MODE else max(1, retries)
    last: pd.DataFrame = pd.DataFrame(columns=["Open", "High", "Low", "Close"])
    for i in range(attempts):
        try:
            kwargs: dict[str, Any] = {
                "tickers": ticker,
                "interval": interval,
                "progress": False,
                "auto_adjust": False,
                "timeout": 20,
            }
            if period:
                kwargs["period"] = period
            else:
                kwargs["start"] = start
                kwargs["end"] = end
            raw = yf_download(**kwargs)
            last = normalize_ohlc_df(raw, ticker)
            if not last.empty:
                return last
        except Exception:
            last = pd.DataFrame(columns=["Open", "High", "Low", "Close"])
        if i + 1 < attempts:
            time.sleep(pause_sec * (i + 1))
    return last


def history_ohlc(ticker: str, period: str = "5d", retries: int = 3) -> pd.DataFrame:
    attempts = 1 if YF_FIXTURE_MODE else max(1, retries)
    last = pd.DataFrame(columns=["Open", "High", "Low", "Close"])
    for i in range(attempts):
        try:
            hist = yf_ticker(ticker).history(period=period)
            last = normalize_ohlc_df(hist, ticker)
            if not last.empty:
                return last
        except Exception:
            last = pd.DataFrame(columns=["Open", "High", "Low", "Close"])
        if i + 1 < attempts:
            time.sleep(0.5 * (i + 1))
    return last


def first_working_ticker(candidates: Sequence[str], *, period: str = "1mo") -> str | None:
    """Return first Yahoo symbol that yields OHLC rows."""
    for symbol in candidates:
        df = history_ohlc(symbol, period=period, retries=2)
        if len(df) >= 2:
            return symbol
    # Accept sparse but non-empty for fixtures / thin history
    for symbol in candidates:
        df = history_ohlc(symbol, period=period, retries=1)
        if not df.empty:
            return symbol
    return candidates[0] if candidates else None
