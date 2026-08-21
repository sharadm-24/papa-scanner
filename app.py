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

from market_session import yf_download, yf_ticker
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


async def process_row(row, current_time, cache, skipped):
    symbol = str(row["symbol"]).strip()
    try:
        entry_date = pd.to_datetime(row["date"], dayfirst=True, errors="coerce")
        if pd.isna(entry_date):
            skipped.append(skip_entry(symbol, "invalid_date"))
            return None

        target_month_start = (entry_date + relativedelta(months=1)).replace(day=1)
        if target_month_start > current_time:
            skipped.append(skip_entry(symbol, "future_target_month"))
            return None

        start_str = target_month_start.strftime("%Y-%m-%d")
        end_str = (target_month_start + relativedelta(months=1)).strftime("%Y-%m-%d")
        ticker = get_nse_ticker(symbol)
        cache_key = (ticker, start_str, end_str)

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
            "target_month": target_month_start.strftime("%b-%Y"),
            "open": round(m_open, 2),
            "high": round(m_high, 2),
            "low": round(m_low, 2),
            "close": round(m_close, 2),
            "p_high": round(float((m_high - m_open) / m_open * 100), 2),
            "p_low": round(float((m_low - m_open) / m_open * 100), 2),
            "p_close": round(float((m_close - m_open) / m_open * 100), 2),
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
            "p_high": round(float((m_high - m_open) / m_open * 100), 2),
            "p_low": round(float((m_low - m_open) / m_open * 100), 2),
            "p_close": round(float((m_close - m_open) / m_open * 100), 2),
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
    tickers = {
        "^NSEI": "NIFTY 50",
        "^NSEBANK": "NIFTY BANK",
        "^CNXPHARMA": "NIFTY PHARMA",
        "^CNXREALTY": "NIFTY REALTY",
        "^CNXCONSUM": "NIFTY CONSUM",
        "^CNXMETAL": "NIFTY METAL",
        "^CNXAUTO": "NIFTY AUTO",
        "^CNXIT": "NIFTY IT",
        "^CNXINFRA": "NIFTY INFRA",
        "^CNXFMCG": "NIFTY FMCG",
        "NIFTY_MID_SELECT.NS": "NIFTY MID SEL",
        "NIFTY_PVT_BANK.NS": "NIFTY PVT BANK",
        "^VIX": "INDIA VIX",
    }

    async def fetch_ticker(symbol, name):
        try:
            tkr = yf_ticker(symbol)
            hist = await asyncio.to_thread(tkr.history, period="5d")
            if hist.empty or len(hist) < 2:
                return None
            prev_close = hist["Close"].iloc[-2]
            current_close = hist["Close"].iloc[-1]
            if prev_close == 0:
                return None
            pct_change = ((current_close - prev_close) / prev_close) * 100
            return {"name": name, "change": round(pct_change, 2)}
        except Exception:
            return None

    fetched_data = await asyncio.gather(
        *[fetch_ticker(sym, name) for sym, name in tickers.items()]
    )
    results = [item for item in fetched_data if item is not None]
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
            yf_download,
            ticker,
            start=start_date.strftime("%Y-%m-%d"),
            end=(end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
            interval="1d",
            progress=False,
            auto_adjust=False,
            group_by="ticker",
        )
        if df.empty:
            return {
                "error": (
                    "No data found for the selected index and period. "
                    "Yahoo may be rate-limiting this host; retry shortly."
                )
            }

        if isinstance(df.columns, pd.MultiIndex):
            if ticker in df.columns.levels[0]:
                df = df[ticker].copy()
            else:
                df.columns = df.columns.get_level_values(1)

        df = df.dropna()
        if not df.empty:
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

        results = []
        for timestamp, row in df.iterrows():
            if pd.isna(row.get("Open")) or pd.isna(row.get("Close")):
                continue
            m_open = float(row["Open"])
            if m_open == 0:
                continue
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
                    "open": round(m_open, 2),
                    "high": round(m_high, 2),
                    "low": round(m_low, 2),
                    "close": round(m_close, 2),
                    "p_high": round(float((m_high - m_open) / m_open * 100), 2),
                    "p_low": round(float((m_low - m_open) / m_open * 100), 2),
                    "p_close": round(float((m_close - m_open) / m_open * 100), 2),
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

    tickers = {
        "^NSEI": "Nifty 50",
        "^NSEBANK": "Nifty Bank",
        "NIFTY_PVT_BANK.NS": "Nifty Pvt Bank",
        "^CNXPHARMA": "Nifty Pharma",
        "^CNXREALTY": "Nifty Realty",
        "^CNXCONSUM": "Nifty Consumption",
        "^CNXMETAL": "Nifty Metal",
        "^CNXAUTO": "Nifty Auto",
        "^CNXIT": "Nifty IT",
        "^CNXINFRA": "Nifty Infra",
        "^CNXFMCG": "Nifty FMCG",
        "NIFTY_MID_SELECT.NS": "Nifty Mid Select",
        "^VIX": "India VIX",
    }

    try:
        end_date = datetime.now()
        start_date = end_date - relativedelta(months=months + 1)
        if interval == "1mo":
            start_date = datetime(start_date.year, start_date.month, 1)

        start_str = start_date.strftime("%Y-%m-%d")
        end_str = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")
        freq = "ME" if interval == "1mo" else "W-MON"
        # curl_cffi session is safer than plain requests; allow modest concurrency.
        sem = asyncio.Semaphore(3 if not YF_FIXTURE_MODE else 8)

        async def fetch_one(ticker, name):
            async with sem:
                try:
                    df = await asyncio.to_thread(
                        yf_download,
                        tickers=ticker,
                        start=start_str,
                        end=end_str,
                        interval="1d",
                        progress=False,
                        auto_adjust=False,
                        timeout=20,
                    )
                    if df.empty:
                        return {"name": name, "results": []}

                    if isinstance(df.columns, pd.MultiIndex):
                        try:
                            df = df.xs(ticker, level=1, axis=1)
                        except KeyError:
                            try:
                                df = df.xs(ticker, level=0, axis=1)
                            except KeyError:
                                return {"name": name, "results": []}

                    missing = [c for c in ["Open", "High", "Low", "Close"] if c not in df.columns]
                    if missing:
                        return {"name": name, "results": []}

                    df = df[["Open", "High", "Low", "Close"]].dropna()
                    if df.empty:
                        return {"name": name, "results": []}

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
                        p_close = float(row["Prev_Close"])
                        if p_close == 0:
                            continue
                        label = (
                            ts.strftime("%b %Y")
                            if interval == "1mo"
                            else f"W{ts.isocalendar()[1]}"
                        )
                        results.append(
                            {
                                "period": label,
                                "open": round(m_open, 2),
                                "close": round(m_close, 2),
                                "p_close": round(
                                    float((m_close - p_close) / p_close * 100), 2
                                ),
                            }
                        )
                    return {"name": name, "results": results}
                except Exception:
                    return {"name": name, "results": []}

        results_list = list(
            await asyncio.gather(*[fetch_one(t, n) for t, n in tickers.items()])
        )
        all_periods = []
        for res in results_list:
            for r in res["results"]:
                if r["period"] not in all_periods:
                    all_periods.append(r["period"])
        return {"periods": all_periods, "indices": results_list}
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
