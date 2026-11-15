"""3-day breakout scanner: candle logic, RSI, and /three_day_data contract (YF_FIXTURE_MODE)."""
from __future__ import annotations

import json
import os

import pandas as pd
import pytest
from httpx import ASGITransport, AsyncClient

os.environ["YF_FIXTURE_MODE"] = "1"

import three_day  # noqa: E402
from app import app  # noqa: E402


def daily(closes, start="2026-01-01"):
    idx = pd.bdate_range(start, periods=len(closes))
    s = pd.Series(closes, index=idx, dtype=float)
    return pd.DataFrame({"Open": s, "High": s + 1, "Low": s - 1, "Close": s})


@pytest.mark.parametrize("n", [30, 31, 32])
def test_candles_anchor_on_latest_day(n):
    df = daily(range(100, 100 + n))
    c = three_day.three_day_candles(df)
    # Newest candle always ends on the latest day, whatever n % 3 is.
    assert c["end"].iloc[-1] == df.index[-1]
    assert c["start"].iloc[-1] == df.index[-3]
    assert len(c) == n // 3
    assert (c["days"] == 3).all()
    last3 = df.iloc[-3:]
    assert c["Open"].iloc[-1] == last3["Open"].iloc[0]
    assert c["High"].iloc[-1] == last3["High"].max()
    assert c["Low"].iloc[-1] == last3["Low"].min()
    assert c["Close"].iloc[-1] == last3["Close"].iloc[-1]


def test_candles_need_min_history():
    assert three_day.three_day_candles(daily(range(10))) is None


def test_find_signals_needs_close_above_high_and_close():
    c = pd.DataFrame({
        "High":  [10, 12, 11, 13, 14, 15],
        "Close": [9,  11, 10, 12.5, 13.5, 12],
    })
    sig = three_day.find_signals(c)
    # i=3: 12.5 > max High(10,12,11)=12 and > max Close 11 -> breakout
    # i=4: 13.5 > max High(12,11,13)=13 -> breakout
    # i=5: 12 < 14 -> no
    assert sig.tolist() == [False, False, False, True, True, False]


def test_find_signals_is_strict():
    c = pd.DataFrame({"High": [10, 10, 10, 10.5], "Close": [9, 9, 9, 10.0]})
    assert not three_day.find_signals(c).iloc[-1]  # closing at the prior high is not a breakout
    c.loc[3, "Close"] = 10.01
    assert three_day.find_signals(c).iloc[-1]


def test_wilder_rsi_range_and_extremes():
    up = three_day.wilder_rsi(pd.Series(range(1, 40), dtype=float))
    assert up.iloc[:14].isna().all()
    assert up.iloc[-1] == pytest.approx(100)
    mixed = three_day.wilder_rsi(pd.Series([10, 11, 10.5, 11.5, 11, 12] * 6, dtype=float))
    valid = mixed.dropna()
    assert ((valid > 0) & (valid < 100)).all()


def test_scan_fixture_rows_are_json_clean():
    result = three_day.scan("nifty50")
    json.dumps(result, allow_nan=False)  # no NaN for non-F&O lots / missing RSI
    assert result["asof"]
    assert result["with_data"] == len(three_day.FIXTURE_UNIVERSE)
    assert result["rows"], "fixture data should contain at least one breakout"
    for r in result["rows"]:
        assert r["bars_ago"] in (0, 1)
        assert r["sl"] < r["close"] < r["target"]
        assert r["target"] == pytest.approx(r["close"] + 2 * (r["close"] - r["sl"]), abs=0.02)
        assert r["fno"] == (r["lot"] is not None)
        if r["rsi_cross"]:
            assert r["rsi_above"] and r["rsi_prev"] <= result["rsi_level"]
        assert r["symbol"] in result["charts"]
    latest = [r for r in result["rows"] if r["bars_ago"] == 0]
    assert all(r["candle_end"] == result["asof"] for r in latest)
    # One case per fixture symbol (see three_day.FIXTURE_PATH comment); INFY has none.
    by_key = {(r["symbol"], r["bars_ago"]): r for r in result["rows"]}
    assert set(by_key) == {("TCS", 0), ("RELIANCE", 0), ("HDFCBANK", 1), ("TCS", 1)}
    assert by_key[("TCS", 0)]["rsi_cross"]
    assert by_key[("RELIANCE", 0)]["rsi_above"] and not by_key[("RELIANCE", 0)]["rsi_cross"]
    assert not by_key[("HDFCBANK", 1)]["fno"] and by_key[("HDFCBANK", 1)]["lot"] is None
    assert not by_key[("TCS", 1)]["rsi_above"]


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_three_day_page(client):
    r = await client.get("/three_day")
    assert r.status_code == 200
    assert "3-Day Breakout" in r.text


@pytest.mark.asyncio
async def test_three_day_data(client):
    r = await client.get("/three_day_data", params={"universe": "nifty50", "refresh": "true"})
    assert r.status_code == 200
    body = r.json()
    assert body["cached"] is False
    assert body["rows"] and body["charts"]
    assert body["rsi_level"] == 58
    r2 = await client.get("/three_day_data", params={"universe": "nifty50"})
    assert r2.json()["cached"] is True


@pytest.mark.asyncio
async def test_three_day_data_bad_universe(client):
    r = await client.get("/three_day_data", params={"universe": "sensex"})
    assert r.status_code == 400
    assert "universe" in r.json()["error"]


def test_fetch_daily_retry_stops_when_nothing_recovered(monkeypatch):
    sleeps, calls = [], []
    good = daily(range(100, 130))

    def fake_download(tickers, **kwargs):
        calls.append(list(tickers))
        have = [t for t in tickers if t != "NEWCO.NS"]
        if not have:
            return pd.DataFrame()
        return pd.concat({t: good for t in have}, axis=1)

    monkeypatch.setattr(three_day, "YF_FIXTURE_MODE", False)
    monkeypatch.setattr(three_day, "yf_download", fake_download)
    monkeypatch.setattr(three_day.time, "sleep", sleeps.append)
    frames = three_day.fetch_daily(["A.NS", "B.NS", "NEWCO.NS"], 3)
    assert set(frames) == {"A.NS", "B.NS"}
    assert len(calls) == 2 and sum(sleeps) <= 2  # one retry, then give up


@pytest.mark.asyncio
async def test_three_day_data_skips_caching_partial_scan(client, monkeypatch):
    import app as app_module

    partial = {"universe": "nifty500", "asof": "2026-11-13", "symbols": 500, "with_data": 40,
               "fno_loaded": True, "rsi_level": 58, "rows": [], "charts": {}}
    monkeypatch.setattr(three_day, "scan", lambda universe: dict(partial))
    app_module._three_day_cache.pop("nifty500", None)
    r = await client.get("/three_day_data", params={"universe": "nifty500"})
    assert r.json()["with_data"] == 40
    assert "nifty500" not in app_module._three_day_cache
