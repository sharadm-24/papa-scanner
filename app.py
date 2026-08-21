import os
import io
import asyncio
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf
from dateutil.relativedelta import relativedelta
from fastapi import FastAPI, UploadFile, File, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from market_session import download_ohlc, history_ohlc, yf_download, yf_ticker
from yf_fixture import fixture_download, fixture_ticker

logging.getLogger("yfinance").setLevel(logging.CRITICAL)

BASE_DIR = Path(__file__).resolve().parent
YF_FIXTURE_MODE = os.environ.get("YF_FIXTURE_MODE", "").strip() in ("1", "true", "True", "yes")
MAX_UPLOAD_ROWS = int(os.environ.get("MAX_UPLOAD_ROWS", "500"))
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", str(2 * 1024 * 1024)))

if YF_FIXTURE_MODE:
    yf.download = fixture_download
    yf.Ticker = fixture_ticker

app = FastAPI(title="Papa Scanner")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

static_dir = BASE_DIR / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


def read_html(name: str) -> HTMLResponse:
    path = BASE_DIR / name
    with open(path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())


def get_nse_ticker(symbol: str) -> str:
    s = str(symbol).upper().strip()
    if s == "NIFTY":
        return "^NSEI"
    ticker_map = {
        "LTM": "LTIM",
        "ADANITRANS": "ADANIENSOL",
        "MOTHERSUMI": "MOTHERSON",
    }
    s = ticker_map.get(s, s)
    return s + ".NS"


def normalize_csv_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def validate_signal_csv(df: pd.DataFrame) -> None:
    required = {"symbol", "date"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"CSV must include columns: symbol, date. Missing: {', '.join(sorted(missing))}"
        )
    if len(df) == 0:
        raise ValueError("CSV has no data rows.")
    if len(df) > MAX_UPLOAD_ROWS:
        raise ValueError(f"CSV exceeds max rows ({MAX_UPLOAD_ROWS}).")


def skip_entry(symbol: str, reason: str) -> dict:
    return {"symbol": symbol, "reason": reason}


def pct_vs_base(value: float, base: float) -> float:
    """Percent change vs prior close (not vs period open)."""
    return round(float((value - base) / base * 100), 2)


def _ensure_naive_index(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    idx = df.index
    if getattr(idx, "tz", None) is not None:
        out = df.copy()
        out.index = idx.tz_localize(None)
        return out
    return df


async def process_row(row, current_time, cache, skipped):
    symbol = str(row["symbol"]).strip()
    try:
        entry_date = pd.to_datetime(row["date"], dayfirst=True, errors="coerce")
        if pd.isna(entry_date):
            skipped.append(skip_entry(symbol, "invalid_date"))
            return None

        target_month_start = (entry_date + relativedelta(months=1)).replace(day=1)
        target_month_end = target_month_start + relativedelta(months=1)
        if target_month_start > current_time:
            skipped.append(skip_entry(symbol, "future_target_month"))
            return None

        # Include prior month so % can use last month's close as baseline.
        fetch_start = (target_month_start - relativedelta(months=1)).strftime("%Y-%m-%d")
        fetch_end = target_month_end.strftime("%Y-%m-%d")
        ticker = get_nse_ticker(symbol)
        cache_key = (ticker, fetch_start, fetch_end, "monthly_prev_close")

        if cache_key not in cache:
            df_clean = await asyncio.to_thread(
                download_ohlc,
                ticker,
                start=fetch_start,
                end=fetch_end,
                interval="1d",
                retries=2 if not YF_FIXTURE_MODE else 1,
            )
            cache[cache_key] = None if df_clean.empty else df_clean

        data = cache[cache_key]
        if data is None or data.empty:
            skipped.append(skip_entry(symbol, "no_market_data"))
            return None

        data = _ensure_naive_index(data)
        start_ts = pd.Timestamp(target_month_start)
        end_ts = pd.Timestamp(target_month_end)
        prior = data[data.index < start_ts]
        month = data[(data.index >= start_ts) & (data.index < end_ts)]
        if prior.empty:
            skipped.append(skip_entry(symbol, "no_prior_close"))
            return None
        if month.empty:
            skipped.append(skip_entry(symbol, "no_market_data"))
            return None

        base = float(prior["Close"].iloc[-1])
        if base == 0:
            skipped.append(skip_entry(symbol, "zero_prior_close"))
            return None

        m_open = float(month["Open"].iloc[0])
        m_high = float(month["High"].max())
        m_low = float(month["Low"].min())
        m_close = float(month["Close"].iloc[-1])
        return {
            "entry_date": str(row["date"]),
            "symbol": symbol,
            "target_month": target_month_start.strftime("%b-%Y"),
            "prev_close": round(base, 2),
            "open": round(m_open, 2),
            "high": round(m_high, 2),
            "low": round(m_low, 2),
            "close": round(m_close, 2),
            "p_high": pct_vs_base(m_high, base),
            "p_low": pct_vs_base(m_low, base),
            "p_close": pct_vs_base(m_close, base),
        }
    except Exception as e:
        skipped.append(skip_entry(symbol, f"error:{type(e).__name__}"))
        return None


async def process_row_days(row, current_time, cache, days_count, skipped):
    symbol = str(row["symbol"]).strip()
    try:
        entry_date = pd.to_datetime(row["date"], dayfirst=True, errors="coerce")
        if pd.isna(entry_date):
            skipped.append(skip_entry(symbol, "invalid_date"))
            return None

        start_date = entry_date + timedelta(days=1)
        end_date = start_date + timedelta(days=days_count * 3)
        if start_date > current_time:
            skipped.append(skip_entry(symbol, "future_window"))
            return None

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        ticker = get_nse_ticker(symbol)
        cache_key = (ticker, start_str, end_str, "days", days_count)

        if cache_key not in cache:
            df_raw = await asyncio.to_thread(
                yf_download,
                ticker,
                start=start_str,
                end=end_str,
                progress=False,
                auto_adjust=False,
                group_by="ticker",
                timeout=20,
            )
            if df_raw.empty and not YF_FIXTURE_MODE:
                await asyncio.sleep(0.5)
                df_raw = await asyncio.to_thread(
                    yf_download,
                    ticker,
                    start=start_str,
                    end=end_str,
                    progress=False,
                    auto_adjust=False,
                    group_by="ticker",
                    timeout=20,
                )
            if df_raw.empty:
                cache[cache_key] = None
            else:
                if isinstance(df_raw.columns, pd.MultiIndex):
                    if ticker in df_raw.columns.levels[0]:
                        df_clean = df_raw[ticker].copy()
                    else:
                        df_clean = df_raw.copy()
                        df_clean.columns = df_clean.columns.get_level_values(1)
                else:
                    df_clean = df_raw.copy()
                df_clean = df_clean.head(days_count)
                cache[cache_key] = df_clean

        data = cache[cache_key]
        if data is None or data.empty:
            skipped.append(skip_entry(symbol, "no_market_data"))
            return None

        cols = data.columns.tolist()
        if not all(c in cols for c in ["Open", "High", "Low", "Close"]):
            skipped.append(skip_entry(symbol, "incomplete_ohlc"))
            return None

        m_open = float(data["Open"].iloc[0])
        if m_open == 0:
            skipped.append(skip_entry(symbol, "zero_open"))
            return None

        m_high = float(data["High"].max())
        m_low = float(data["Low"].min())
        m_close = float(data["Close"].iloc[-1])
        return {
            "entry_date": str(row["date"]),
            "symbol": symbol,
            "target_period": f"Next {len(data)} Days",
            "open": round(m_open, 2),
            "high": round(m_high, 2),
            "low": round(m_low, 2),
            "close": round(m_close, 2),
            "p_high": pct_vs_base(m_high, m_open),
            "p_low": pct_vs_base(m_low, m_open),
            "p_close": pct_vs_base(m_close, m_open),
        }
    except Exception as e:
        skipped.append(skip_entry(symbol, f"error:{type(e).__name__}"))
        return None


@app.get("/")
async def get_home():
    return read_html("home.html")


@app.get("/backtest")
async def get_backtest():
    return read_html("index.html")


@app.get("/days")
async def get_days_page():
    return read_html("days.html")


@app.get("/index_scanner")
async def get_index_scanner_page():
    return read_html("index_scanner.html")


@app.get("/sample.csv")
async def get_sample_csv():
    path = BASE_DIR / "tests" / "fixtures" / "sample_signals.csv"
    return FileResponse(
        path,
        media_type="text/csv",
        filename="sample_signals.csv",
    )


@app.get("/health")
async def health():
    return {"ok": True, "fixture_mode": YF_FIXTURE_MODE}


@app.post("/scan")
async def scan(file: UploadFile = File(...)):
    cache = {}
    current_time = datetime.now()
    try:
        content = await file.read()
        if len(content) > MAX_UPLOAD_BYTES:
            raise ValueError(f"Upload exceeds max size ({MAX_UPLOAD_BYTES} bytes).")
        df = normalize_csv_columns(pd.read_csv(io.BytesIO(content)))
        validate_signal_csv(df)
    except Exception as e:
        message = str(e)

        async def error_gen():
            yield "data: " + json.dumps({"type": "error", "message": message}) + "\n\n"

        return StreamingResponse(error_gen(), media_type="text/event-stream")

    async def event_generator():
        try:
            total_rows = len(df)
            results = []
            skipped = []
            for i, (_, row) in enumerate(df.iterrows()):
                res = await process_row(row, current_time, cache, skipped)
                if res:
                    results.append(res)
                yield (
                    "data: "
                    + json.dumps(
                        {
                            "type": "progress",
                            "current": i + 1,
                            "total": total_rows,
                            "symbol": str(row["symbol"]),
                        }
                    )
                    + "\n\n"
                )
            yield (
                "data: "
                + json.dumps(
                    {
                        "type": "complete",
                        "results": results,
                        "skipped": skipped,
                        "skipped_count": len(skipped),
                    }
                )
                + "\n\n"
            )
        except Exception as ex:
            yield "data: " + json.dumps({"type": "error", "message": str(ex)}) + "\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.post("/scan_days")
async def scan_days(request: Request, file: UploadFile = File(...)):
    raw_days = request.query_params.get("days", "5")
    try:
        days_count = int(raw_days)
    except ValueError:
        return JSONResponse(
            {"type": "error", "message": "days must be an integer between 1 and 60"},
            status_code=400,
        )
    if days_count < 1 or days_count > 60:
        return JSONResponse(
            {"type": "error", "message": "days must be between 1 and 60"},
            status_code=400,
        )

    cache = {}
    current_time = datetime.now()
    try:
        content = await file.read()
        if len(content) > MAX_UPLOAD_BYTES:
            raise ValueError(f"Upload exceeds max size ({MAX_UPLOAD_BYTES} bytes).")
        df = normalize_csv_columns(pd.read_csv(io.BytesIO(content)))
        validate_signal_csv(df)
    except Exception as e:
        message = str(e)

        async def error_gen():
            yield "data: " + json.dumps({"type": "error", "message": message}) + "\n\n"

        return StreamingResponse(error_gen(), media_type="text/event-stream")

    async def event_generator():
        try:
            total_rows = len(df)
            results = []
            skipped = []
            for i, (_, row) in enumerate(df.iterrows()):
                res = await process_row_days(
                    row, current_time, cache, days_count, skipped
                )
                if res:
                    results.append(res)
                yield (
                    "data: "
                    + json.dumps(
                        {
                            "type": "progress",
                            "current": i + 1,
                            "total": total_rows,
                            "symbol": str(row["symbol"]),
                        }
                    )
                    + "\n\n"
                )
            yield (
                "data: "
                + json.dumps(
                    {
                        "type": "complete",
                        "results": results,
                        "skipped": skipped,
                        "skipped_count": len(skipped),
                    }
                )
                + "\n\n"
            )
        except Exception as e:
            yield "data: " + json.dumps({"type": "error", "message": str(e)}) + "\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/api/ticker_data")
async def get_ticker_data():
    # (primary symbol, display name, optional fallbacks)
    tickers = [
        ("^NSEI", "NIFTY 50", ()),
        ("^NSEBANK", "NIFTY BANK", ()),
        ("^CNXPHARMA", "NIFTY PHARMA", ()),
        ("^CNXREALTY", "NIFTY REALTY", ()),
        ("^CNXCONSUM", "NIFTY CONSUM", ()),
        ("^CNXMETAL", "NIFTY METAL", ()),
        ("^CNXAUTO", "NIFTY AUTO", ()),
        ("^CNXIT", "NIFTY IT", ()),
        ("^CNXINFRA", "NIFTY INFRA", ()),
        ("^CNXFMCG", "NIFTY FMCG", ()),
        ("NIFTY_MID_SELECT.NS", "NIFTY MID SEL", ("^NSEMDCP50",)),
        ("NIFTY_PVT_BANK.NS", "NIFTY PVT BANK", ()),
        ("^VIX", "INDIA VIX", ()),
    ]

    async def fetch_ticker(primary, name, fallbacks):
        try:
            for symbol in (primary, *fallbacks):
                hist = await asyncio.to_thread(history_ohlc, symbol, "5d", 3)
                if hist.empty or len(hist) < 2:
                    continue
                prev_close = float(hist["Close"].iloc[-2])
                current_close = float(hist["Close"].iloc[-1])
                if prev_close == 0:
                    continue
                pct_change = ((current_close - prev_close) / prev_close) * 100
                return {"name": name, "change": round(pct_change, 2)}
            return None
        except Exception:
            return None

    # Serial on live hosts — concurrent Yahoo hits from Vercel drop sector indices.
    results = []
    for primary, name, fallbacks in tickers:
        item = await fetch_ticker(primary, name, fallbacks)
        if item:
            results.append(item)

    payload = {"data": results}
    if not results:
        payload["error"] = (
            "Yahoo Finance returned no symbols. "
            "The provider may be rate-limiting this host; retry shortly."
        )
    return payload


@app.get("/index_data")
async def get_index_data(ticker: str, months: int = 3, interval: str = "1mo"):
    try:
        end_date = datetime.now()
        start_date = end_date - relativedelta(months=months)
        if interval == "1mo":
            start_date = datetime(start_date.year, start_date.month, 1) - relativedelta(
                months=1
            )
        else:
            start_date -= timedelta(weeks=1)

        df = await asyncio.to_thread(
            download_ohlc,
            ticker,
            start=start_date.strftime("%Y-%m-%d"),
            end=(end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
            interval="1d",
            retries=3,
        )
        if df.empty:
            return {
                "error": (
                    "No data found for the selected index and period. "
                    "Yahoo may be rate-limiting this host; retry shortly."
                )
            }

        freq = "ME" if interval == "1mo" else "W-MON"
        try:
            df = (
                df.resample(freq)
                .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last"})
                .dropna()
            )
        except ValueError:
            fallback_freq = "M" if interval == "1mo" else "W-MON"
            df = (
                df.resample(fallback_freq)
                .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last"})
                .dropna()
            )

        # Monthly/weekly % vs prior period close (e.g. Jan vs Dec close), not vs period open.
        df["Prev_Close"] = df["Close"].shift(1)

        results = []
        for timestamp, row in df.iterrows():
            if pd.isna(row.get("Open")) or pd.isna(row.get("Close")):
                continue
            if pd.isna(row.get("Prev_Close")):
                continue
            base = float(row["Prev_Close"])
            if base == 0:
                continue
            m_open = float(row["Open"])
            m_high = float(row["High"])
            m_low = float(row["Low"])
            m_close = float(row["Close"])
            period_label = (
                timestamp.strftime("%b %Y")
                if interval == "1mo"
                else f"Week of {timestamp.strftime('%d %b')}"
            )
            results.append(
                {
                    "period": period_label,
                    "prev_close": round(base, 2),
                    "open": round(m_open, 2),
                    "high": round(m_high, 2),
                    "low": round(m_low, 2),
                    "close": round(m_close, 2),
                    "p_high": pct_vs_base(m_high, base),
                    "p_low": pct_vs_base(m_low, base),
                    "p_close": pct_vs_base(m_close, base),
                }
            )
        return {"ticker": ticker, "results": results}
    except Exception as e:
        return {"error": str(e)}


@app.get("/all_indices_data")
async def get_all_indices_data(
    months: int = Query(3, ge=1, le=24),
    interval: str = Query("1mo"),
):
    if interval not in ("1mo", "1wk"):
        return {"error": "interval must be 1mo or 1wk"}

    # primary + optional Yahoo fallbacks (sector indices flake on Vercel)
    tickers = [
        (("^NSEI",), "Nifty 50"),
        (("^NSEBANK",), "Nifty Bank"),
        (("NIFTY_PVT_BANK.NS",), "Nifty Pvt Bank"),
        (("^CNXPHARMA",), "Nifty Pharma"),
        (("^CNXREALTY",), "Nifty Realty"),
        (("^CNXCONSUM",), "Nifty Consumption"),
        (("^CNXMETAL",), "Nifty Metal"),
        (("^CNXAUTO",), "Nifty Auto"),
        (("^CNXIT",), "Nifty IT"),
        (("^CNXINFRA",), "Nifty Infra"),
        (("^CNXFMCG",), "Nifty FMCG"),
        # Mid Select has almost no Yahoo history; Midcap 50 is the usable proxy.
        (("NIFTY_MID_SELECT.NS", "^NSEMDCP50"), "Nifty Mid Select"),
        (("^VIX",), "India VIX"),
    ]

    try:
        end_date = datetime.now()
        start_date = end_date - relativedelta(months=months + 1)
        if interval == "1mo":
            start_date = datetime(start_date.year, start_date.month, 1)

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")
        freq = "ME" if interval == "1mo" else "W-MON"
        # Serial on live hosts; fixture mode can fan out.
        sem = asyncio.Semaphore(8 if YF_FIXTURE_MODE else 1)

        async def fetch_one(symbols, name):
            async with sem:
                try:
                    last_status = {
                        "name": name,
                        "results": [],
                        "status": "no_yahoo_data",
                        "message": "Yahoo returned no OHLC for this index",
                    }
                    for ticker in symbols:
                        df = await asyncio.to_thread(
                            download_ohlc,
                            ticker,
                            start=start_str,
                            end=end_str,
                            interval="1d",
                            retries=3 if not YF_FIXTURE_MODE else 1,
                            pause_sec=0.9,
                        )
                        if df.empty:
                            continue

                        try:
                            df_r = (
                                df.resample(freq)
                                .agg(
                                    {
                                        "Open": "first",
                                        "High": "max",
                                        "Low": "min",
                                        "Close": "last",
                                    }
                                )
                                .dropna()
                            )
                        except ValueError:
                            fallback = "M" if interval == "1mo" else "W-MON"
                            df_r = (
                                df.resample(fallback)
                                .agg(
                                    {
                                        "Open": "first",
                                        "High": "max",
                                        "Low": "min",
                                        "Close": "last",
                                    }
                                )
                                .dropna()
                            )
                        df_r["Prev_Close"] = df_r["Close"].shift(1)

                        results = []
                        for ts, row in df_r.iterrows():
                            if pd.isna(row["Prev_Close"]):
                                continue
                            m_open = float(row["Open"])
                            m_close = float(row["Close"])
                            base = float(row["Prev_Close"])
                            if base == 0:
                                continue
                            label = (
                                ts.strftime("%b %Y")
                                if interval == "1mo"
                                else f"W{ts.isocalendar()[1]}"
                            )
                            results.append(
                                {
                                    "period": label,
                                    "prev_close": round(base, 2),
                                    "open": round(m_open, 2),
                                    "close": round(m_close, 2),
                                    # % vs prior period close (Dec close → Jan), not vs period open.
                                    "p_close": pct_vs_base(m_close, base),
                                }
                            )
                        if results:
                            return {
                                "name": name,
                                "results": results,
                                "status": "ok",
                                "ticker": ticker,
                            }
                        last_status = {
                            "name": name,
                            "results": [],
                            "status": "insufficient_history",
                            "ticker": ticker,
                            "message": "Not enough history to build period returns",
                        }
                    return last_status
                except Exception as ex:
                    return {
                        "name": name,
                        "results": [],
                        "status": "error",
                        "message": f"{type(ex).__name__}: {ex}",
                    }

        results_list = list(
            await asyncio.gather(*[fetch_one(syms, n) for syms, n in tickers])
        )
        all_periods = []
        for res in results_list:
            for r in res["results"]:
                if r["period"] not in all_periods:
                    all_periods.append(r["period"])
        empty = [r["name"] for r in results_list if not r["results"]]
        payload = {"periods": all_periods, "indices": results_list}
        if empty:
            payload["partial"] = True
            payload["empty_indices"] = empty
        return payload
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
