"""3-day candle breakout scanner (NSE Nifty 50 / Nifty 500).

Daily bars are grouped into non-overlapping 3-trading-day candles, counted back so
the newest candle ends on the latest trading day. A candle is a breakout when it
closes above the highest Close of the previous 3 candles AND its High is above their
highest High.
Only the FIRST breakout of an up-move is a signal: after a signal, later breakouts are
repeats until a breakdown (a candle closing below the lowest Close of the previous 3
candles with its Low below their lowest Low) shows the trend turned down; the next breakout after that is new.

Each breakout also carries RSI(14, Wilder, on 3-day closes) and the NSE F&O lot
size, so the page can filter on "RSI crosses above 58", "RSI is above 58" and
"F&O only". Reference plan: entry = signal close, SL = signal candle low, 1:2 target.
"""
from __future__ import annotations

import io
import json
import logging
import sys
import time
import urllib.request
from pathlib import Path

import pandas as pd

from market_session import YF_FIXTURE_MODE, normalize_ohlc_df, yf_download

log = logging.getLogger(__name__)

RSI_LEVEL = 58
MIN_DAYS = 15
CHART_CANDLES = 40
LOOKBACK_MAX = 5  # rows cover first signals on the last N candles; the page filters down
UNIVERSE_URLS = {
    "nifty50": "https://archives.nseindia.com/content/indices/ind_nifty50list.csv",
    "nifty500": "https://archives.nseindia.com/content/indices/ind_nifty500list.csv",
    # Every equity in the NSE cash market (about 2,600 symbols).
    "nse_all": "https://archives.nseindia.com/content/equities/EQUITY_L.csv",
}
CASH_SERIES = {"EQ", "BE"}  # rolling settlement and trade-to-trade; skips SME/other series
CHUNK_SIZE = 250  # Yahoo rate-limits very large single downloads
CHUNK_PAUSE_SEC = 2
RATE_LIMIT_BACKOFF_SEC = (15, 30, 60)
RATE_LIMIT_MIN_MISSING = 10
IST = "Asia/Kolkata"
MARKET_CLOSE = (15, 30)
FNO_URLS = [
    "https://nsearchives.nseindia.com/content/fo/fo_mktlots.csv",
    "https://archives.nseindia.com/content/fo/fo_mktlots.csv",
]
# Symbols in the NSE list that Yahoo has no data for; skipped in retries.
DELISTED = {"DUMMYHEG.NS", "BAGMANE.NS"}

# Fixture mode (tests / CI): no NSE or Yahoo calls. The fixture holds one case each:
# RELIANCE latest first signal (after a breakdown) with RSI above 58, TCS latest first signal
# with RSI crossing 58, HDFCBANK previous-candle first signal with RSI below 58 (not F&O),
# INFY breakouts on the latest candles that are repeats of an old up-move (hidden),
# ITC first signal 3 candles ago (only shown with a lookback of 4+ candles).
FIXTURE_PATH = Path(__file__).resolve().parent / "tests" / "fixtures" / "three_day_ohlc.json"
FIXTURE_UNIVERSE = {
    "RELIANCE.NS": "Reliance Industries Ltd.",
    "TCS.NS": "Tata Consultancy Services Ltd.",
    "INFY.NS": "Infosys Ltd.",
    "HDFCBANK.NS": "HDFC Bank Ltd.",
    "ITC.NS": "ITC Ltd.",
}
FIXTURE_FNO = {"RELIANCE": 500, "TCS": 175, "INFY": 400}

_LIST_TTL_SEC = 6 * 3600
_list_cache: dict[str, tuple[float, dict]] = {}


def _fetch_csv(url: str) -> pd.DataFrame:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    raw = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", errors="replace")
    df = pd.read_csv(io.StringIO(raw), skipinitialspace=True)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _cached(key: str, loader):
    hit = _list_cache.get(key)
    if hit and time.time() - hit[0] < _LIST_TTL_SEC:
        return hit[1]
    value = loader()
    if value:
        _list_cache[key] = (time.time(), value)
    return value


def load_universe(name: str) -> dict[str, str]:
    """{'RELIANCE.NS': 'Reliance Industries Ltd.', ...} from the NSE index or equity list."""
    if name not in UNIVERSE_URLS:
        raise ValueError(f"universe must be one of: {', '.join(UNIVERSE_URLS)}")
    if YF_FIXTURE_MODE:
        return dict(FIXTURE_UNIVERSE)

    def loader():
        df = _fetch_csv(UNIVERSE_URLS[name])
        if name == "nse_all":
            df = df[df["SERIES"].astype(str).str.strip().isin(CASH_SERIES)]
            sym_col, name_col = "SYMBOL", "NAME OF COMPANY"
        else:
            sym_col, name_col = "Symbol", "Company Name"
        return {f"{str(r[sym_col]).strip()}.NS": str(r[name_col]).strip() for _, r in df.iterrows()}

    return _cached(f"universe:{name}", loader)


def load_fno() -> dict[str, int]:
    """NSE F&O underlyings -> lot size (nearest expiry). Empty dict if NSE is unreachable."""
    if YF_FIXTURE_MODE:
        return dict(FIXTURE_FNO)

    def loader():
        for url in FNO_URLS:
            try:
                df = _fetch_csv(url)
                syms = df["SYMBOL"].astype(str).str.strip()
                lots = pd.to_numeric(df[df.columns[2]].astype(str).str.strip(), errors="coerce")
                out = {s: int(l) for s, l in zip(syms, lots) if pd.notna(l)}
                if len(out) > 50:
                    return out
            except Exception as e:  # try the next URL
                log.warning("F&O list fetch failed (%s): %s", url, e)
        return {}

    return _cached("fno", loader)


def _split_download(data: pd.DataFrame, tickers: list[str]) -> dict[str, pd.DataFrame]:
    out = {}
    if data is None or data.empty:
        return out
    level0 = set(data.columns.get_level_values(0)) if isinstance(data.columns, pd.MultiIndex) else set()
    for t in tickers:
        if t not in level0:
            continue
        df = normalize_ohlc_df(data[t])
        if not df.empty:
            out[t] = df
    return out


def _fixture_frames(tickers: list[str]) -> dict[str, pd.DataFrame]:
    with open(FIXTURE_PATH, "r", encoding="utf-8") as f:
        daily = json.load(f)["daily"]
    out = {}
    for t in tickers:
        if daily.get(t):
            df = pd.DataFrame(daily[t])
            df["Date"] = pd.to_datetime(df["Date"])
            out[t] = df.set_index("Date")[["Open", "High", "Low", "Close"]].astype(float)
    return out


def fetch_daily(tickers: list[str], years: int) -> tuple[dict[str, pd.DataFrame], bool]:
    """Daily split-adjusted OHLC per ticker, and whether Yahoo blocked us (rate limit).

    Yahoo rate-limits bursts, so empties are retried. If a chunk is still mostly empty after
    the full backoff, Yahoo is blocking this machine: stop and return what we have.
    """
    if YF_FIXTURE_MODE:
        return _fixture_frames(tickers), False

    kwargs = dict(period=f"{years}y", interval="1d", auto_adjust=True,
                  group_by="ticker", progress=False, timeout=30)
    frames = {}
    for k in range(0, len(tickers), CHUNK_SIZE):
        chunk = tickers[k:k + CHUNK_SIZE]
        if k:
            time.sleep(CHUNK_PAUSE_SEC)
        # A chunk that comes back mostly empty means Yahoo is rate-limiting us. A few symbols
        # Yahoo doesn't carry (or a tiny last chunk) must not trigger this, hence the floor.
        for wait in (0, *RATE_LIMIT_BACKOFF_SEC):
            todo = [t for t in chunk if t not in frames]
            if wait:
                log.warning("Rate-limited: %d of %d missing, waiting %ds", len(todo), len(chunk), wait)
                time.sleep(wait)
            try:
                frames.update(_split_download(yf_download(todo, threads=True, **kwargs), todo))
            except Exception as e:  # the retry loop below picks these up
                log.warning("Chunk download failed (%d symbols): %s", len(todo), e)
            missing = sum(t not in frames and t not in DELISTED for t in chunk)
            if missing <= max(RATE_LIMIT_MIN_MISSING, len(chunk) / 2):
                break
        else:
            log.warning("Yahoo is still blocking after backoff; stopping with %d of %d symbols",
                        len(frames), len(tickers))
            return frames, True
    # Short pauses (12 s worst case) so a symbol Yahoo never serves can't stall the request;
    # stop as soon as a retry recovers nothing.
    for attempt in range(3):
        missing = [t for t in tickers if t not in frames and t not in DELISTED]
        if not missing:
            break
        log.warning("Retrying %d symbols (attempt %d)", len(missing), attempt + 1)
        time.sleep(2 * (attempt + 1))
        try:
            more = _split_download(yf_download(missing, threads=False, **kwargs), missing)
        except Exception as e:
            log.warning("Retry download failed: %s", e)
            continue
        if not more:
            break
        frames.update(more)
    return frames, False


def three_day_candles(df: pd.DataFrame) -> pd.DataFrame | None:
    """Group daily bars into 3-day candles, counting back from the latest day."""
    df = df.dropna(subset=["Open", "High", "Low", "Close"]).copy()
    n = len(df)
    if n < MIN_DAYS:
        return None
    # block 0 = oldest; a leading partial block is dropped below
    df["blk"] = (n - 1 - pd.Series(range(n), index=df.index)) // 3
    df["blk"] = df["blk"].max() - df["blk"]
    df["ts"] = df.index
    g = df.groupby("blk", sort=True)
    c = pd.DataFrame({
        "start": g["ts"].first(),
        "end": g["ts"].last(),
        "Open": g["Open"].first(),
        "High": g["High"].max(),
        "Low": g["Low"].min(),
        "Close": g["Close"].last(),
        "days": g["Close"].count(),
    })
    return c[c["days"] == 3].reset_index(drop=True)


def find_signals(c: pd.DataFrame) -> pd.Series:
    """Boolean Series: candle i's Close > max Close and High > max High of candles i-3..i-1."""
    prev_high = c["High"].shift(1).rolling(3).max()
    prev_close = c["Close"].shift(1).rolling(3).max()
    return (c["Close"] > prev_close) & (c["High"] > prev_high)


def find_breakdowns(c: pd.DataFrame) -> pd.Series:
    """Boolean Series: candle i's Close < min Close and Low < min Low of candles i-3..i-1."""
    prev_low = c["Low"].shift(1).rolling(3).min()
    prev_close = c["Close"].shift(1).rolling(3).min()
    return (c["Close"] < prev_close) & (c["Low"] < prev_low)


def first_signals(brk: pd.Series, bdn: pd.Series) -> pd.Series:
    """Keep only the first breakout after each breakdown (and the first in history)."""
    armed, out = True, []
    for b, d in zip(brk.tolist(), bdn.tolist()):
        out.append(bool(b and armed))
        if b:
            armed = False
        if d:
            armed = True
    return pd.Series(out, index=brk.index)


def wilder_rsi(close: pd.Series, n: int = 14) -> pd.Series:
    d = close.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    return 100 - 100 / (1 + up / dn)


def _num(v, nd=2):
    return None if v is None or pd.isna(v) else round(float(v), nd)


def is_live(asof: str | None, now: pd.Timestamp | None = None) -> bool:
    """True when the latest bar is today's and NSE hasn't closed yet (intraday prices)."""
    now = now if now is not None else pd.Timestamp.now(tz=IST)
    if not asof or f"{now:%Y-%m-%d}" != asof:
        return False
    return (now.hour, now.minute) < MARKET_CLOSE


def scan(universe: str = "nifty50", years: int = 3) -> dict:
    """Run the scan. Returns JSON-ready rows (first signals on the last LOOKBACK_MAX candles) and charts."""
    names = load_universe(universe)
    tickers = list(names)
    fno = load_fno()
    frames, rate_limited = fetch_daily(tickers, years)

    candles = {}
    for t, df in frames.items():
        if getattr(df.index, "tz", None) is not None:
            df = df.tz_localize(None)
        c = three_day_candles(df)
        if c is not None and len(c) >= 6:
            candles[t] = c
    if not candles:
        return {"universe": universe, "asof": None, "symbols": len(tickers), "with_data": len(frames),
                "fno_loaded": bool(fno), "rsi_level": RSI_LEVEL, "latest_candle": None, "live": False,
                "skipped_stale": 0, "rate_limited": rate_limited, "rows": [], "charts": {}}

    latest = max(c["end"].iloc[-1] for c in candles.values())
    # Most stocks share the market calendar; one with a missing day would shift its own start.
    latest_start = pd.Series([c["start"].iloc[-1] for c in candles.values()
                              if c["end"].iloc[-1] == latest]).mode().iloc[0]
    rows, charts, stale = [], {}, 0
    for t, c in candles.items():
        if c["end"].iloc[-1] != latest:  # no bar for the latest day (suspended / not traded)
            stale += 1
            continue
        c["rsi"] = wilder_rsi(c["Close"])
        bdn = find_breakdowns(c)
        sig = first_signals(find_signals(c), bdn)
        sym = t.removesuffix(".NS")
        last = len(c) - 1
        found = False
        for bars_ago in range(LOOKBACK_MAX):
            i = last - bars_ago
            if i < 3 or not sig.iloc[i]:
                continue
            found = True
            entry, sl = float(c.loc[i, "Close"]), float(c.loc[i, "Low"])
            risk = entry - sl
            level = c["Close"].iloc[i - 3:i].max()  # breakout % = close vs highest prior close
            rp, rn = c.loc[i - 1, "rsi"], c.loc[i, "rsi"]
            has_rsi = pd.notna(rp) and pd.notna(rn)
            rows.append({
                "symbol": sym,
                "name": names.get(t, sym),
                "bars_ago": bars_ago,
                "candle_start": f"{c.loc[i, 'start']:%Y-%m-%d}",
                "candle_end": f"{c.loc[i, 'end']:%Y-%m-%d}",
                "close": round(entry, 2),
                "sl": round(sl, 2),
                "target": round(entry + 2 * risk, 2),
                "risk_pct": round(risk / entry * 100, 2),
                "breakout_pct": round((entry / level - 1) * 100, 2),
                "rsi_prev": _num(rp, 1),
                "rsi": _num(rn, 1),
                "rsi_above": bool(has_rsi and rn > RSI_LEVEL),
                "rsi_cross": bool(has_rsi and rp <= RSI_LEVEL < rn),
                "fno": sym in fno,
                "lot": fno.get(sym),
            })
        if found:
            tail = c.tail(CHART_CANDLES)
            charts[sym] = {
                "start": [f"{s:%Y-%m-%d}" for s in tail["start"]],
                "end": [f"{e:%Y-%m-%d}" for e in tail["end"]],
                "open": tail["Open"].round(2).tolist(),
                "high": tail["High"].round(2).tolist(),
                "low": tail["Low"].round(2).tolist(),
                "close": tail["Close"].round(2).tolist(),
                "signals": [f"{e:%Y-%m-%d}" for e, s in zip(tail["end"], sig.loc[tail.index]) if s],
                "breakdowns": [f"{e:%Y-%m-%d}" for e, s in zip(tail["end"], bdn.loc[tail.index]) if s],
            }

    rows.sort(key=lambda r: (r["bars_ago"], -r["breakout_pct"]))
    asof = f"{pd.Timestamp(latest):%Y-%m-%d}"
    return {
        "universe": universe,
        "asof": asof,
        "latest_candle": {"start": f"{pd.Timestamp(latest_start):%Y-%m-%d}", "end": asof},
        "live": is_live(asof),
        "skipped_stale": stale,
        "rate_limited": rate_limited,
        "symbols": len(tickers),
        "with_data": len(frames),
        "fno_loaded": bool(fno),
        "rsi_level": RSI_LEVEL,
        "rows": rows,
        "charts": charts,
    }


if __name__ == "__main__":
    # Local run without the web app: python three_day.py [nifty50|nifty500] > signals.csv
    result = scan(sys.argv[1] if len(sys.argv) > 1 else "nifty500")
    print(f"{len(result['rows'])} breakouts, data as of {result['asof']}", file=sys.stderr)
    pd.DataFrame(result["rows"]).to_csv(sys.stdout, index=False)
