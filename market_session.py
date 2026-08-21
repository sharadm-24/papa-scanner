"""Yahoo Finance session that bypasses TLS fingerprint / 429 blocks.

Plain `requests` (used by older yfinance defaults) gets rate-limited on
Vercel and many cloud IPs. curl_cffi Chrome impersonation works.
"""
from __future__ import annotations

import os
from typing import Any

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
