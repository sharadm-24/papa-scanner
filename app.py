import os
import io
import asyncio
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, UploadFile, File, Request
import logging

# Suppress yfinance logs
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_nse_ticker(symbol):
    s = str(symbol).upper().strip()
    if s == "NIFTY":
        return "^NSEI"
    ticker_map = {
        "LTM": "LTIM",
        "ADANITRANS": "ADANIENSOL",
        "MOTHERSUMI": "MOTHERSON"
    }
    s = ticker_map.get(s, s)
    return s + ".NS"

async def process_row(row, current_time, cache, i, total_rows):
    symbol = str(row['symbol'])
    try:
        # Parse DD-MM-YYYY
        entry_date = pd.to_datetime(row['date'], dayfirst=True)
        target_month_start = (entry_date + relativedelta(months=1)).replace(day=1)
        
        if target_month_start > current_time:
            return None

        start_str = target_month_start.strftime('%Y-%m-%d')
        end_str = (target_month_start + relativedelta(months=1)).strftime('%Y-%m-%d')
        
        ticker = get_nse_ticker(symbol)
        cache_key = (ticker, start_str, end_str)

        if cache_key not in cache:
            # Sequential download
            df_raw = await asyncio.to_thread(
                yf.download, 
                ticker, 
                start=start_str, 
                end=end_str, 
                progress=False, 
                auto_adjust=False,
                group_by='ticker', 
                timeout=20
            )

            if df_raw.empty:
                # Simple retry
                await asyncio.sleep(0.5)
                df_raw = await asyncio.to_thread(
                    yf.download, ticker, start=start_str, end=end_str, 
                    progress=False, auto_adjust=False, group_by='ticker', timeout=20
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
        
        if data is not None and not data.empty:
            cols = data.columns.tolist()
            if all(c in cols for c in ['Open', 'High', 'Low', 'Close']):
                m_open = float(data['Open'].iloc[0])
                m_high = float(data['High'].max())
                m_low = float(data['Low'].min())
                m_close = float(data['Close'].iloc[-1])
                
                return {
                    'entry_date': row['date'],
                    'symbol': symbol,
                    'target_month': target_month_start.strftime('%b-%Y'),
                    'open': round(m_open, 2),
                    'high': round(m_high, 2),
                    'low': round(m_low, 2),
                    'close': round(m_close, 2),
                    'p_high': round(float((m_high - m_open) / m_open * 100), 2),
                    'p_low': round(float((m_low - m_open) / m_open * 100), 2),
                    'p_close': round(float((m_close - m_open) / m_open * 100), 2)
                }
    except Exception:
        pass
    return None

@app.post("/scan")
async def scan(file: UploadFile = File(...)):
    cache = {}
    current_time = datetime.now()
    
    async def event_generator():
        try:
            content = await file.read()
            df = pd.read_csv(io.BytesIO(content))
            total_rows = len(df)
            
            results = []
            for i, (idx, row) in enumerate(df.iterrows()):
                res = await process_row(row, current_time, cache, i, total_rows)
                if res:
                    results.append(res)
                
                # Send progress update
                yield f"data: {json.dumps({'type': 'progress', 'current': i + 1, 'total': total_rows, 'symbol': row['symbol']})}\n\n"
            
            # Send completion
            yield f"data: {json.dumps({'type': 'complete', 'results': results})}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.get("/")
async def get_home():
    with open("home.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/api/ticker_data")
async def get_ticker_data():
    tickers = {
        "^NSEI": "NIFTY 50",
        "^NSEBANK": "NIFTY BANK",
        "^CNXPSUBANK": "NIFTY PSU",
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
        "^VIX": "INDIA VIX"
    }

    results = []
    
    # Run requests concurrently for speed in background threads
    async def fetch_ticker(symbol, name):
        try:
            # period=2d gives us max yesterday and today to calculate recent change
            tkr = yf.Ticker(symbol)
            hist = await asyncio.to_thread(tkr.history, period="5d")
            if hist.empty or len(hist) < 2:
                return None
            
            # Using closing price vs previous closing price
            prev_close = hist['Close'].iloc[-2]
            current_close = hist['Close'].iloc[-1]
            if prev_close == 0:
                return None
            
            pct_change = ((current_close - prev_close) / prev_close) * 100
            
            return {
                "name": name,
                "change": round(pct_change, 2)
            }
        except Exception:
            return None

    tasks = [fetch_ticker(sym, name) for sym, name in tickers.items()]
    fetched_data = await asyncio.gather(*tasks)
    
    for item in fetched_data:
        if item is not None:
            results.append(item)
            
    return {"data": results}

@app.get("/backtest")
async def get_backtest():
    with open("index.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/days")
async def get_days_page():
    with open("days.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/index_scanner")
async def get_index_scanner_page():
    with open("index_scanner.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/index_data")
async def get_index_data(ticker: str, months: int = 3, interval: str = "1mo"):
    try:
        end_date = datetime.now()
        start_date = end_date - relativedelta(months=months)
        if interval == "1mo":
            start_date = datetime(start_date.year, start_date.month, 1)

        df = await asyncio.to_thread(
            yf.download,
            ticker,
            start=start_date.strftime('%Y-%m-%d'),
            end=(end_date + timedelta(days=1)).strftime('%Y-%m-%d'),
            interval="1d",
            progress=False,
            auto_adjust=False,
            group_by='ticker'
        )

        if df.empty:
            return {"error": "No data found for the selected index and period."}

        if isinstance(df.columns, pd.MultiIndex):
            if ticker in df.columns.levels[0]:
                df = df[ticker].copy()
            else:
                df.columns = df.columns.get_level_values(1)

        df = df.dropna()
        if not df.empty:
            freq = 'ME' if interval == '1mo' else 'W-MON'
            try:
                df = df.resample(freq).agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}).dropna()
            except ValueError:
                fallback_freq = 'M' if interval == '1mo' else 'W-MON'
                df = df.resample(fallback_freq).agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}).dropna()

        results = []
        for timestamp, row in df.iterrows():
            if pd.isna(row.get('Open')) or pd.isna(row.get('Close')):
                continue
                
            m_open = float(row['Open'])
            m_high = float(row['High'])
            m_low = float(row['Low'])
            m_close = float(row['Close'])
            
            period_label = ""
            if interval == "1mo":
                period_label = timestamp.strftime('%b %Y')
            else:
                period_label = f"Week of {timestamp.strftime('%d %b')}"

            results.append({
                'period': period_label,
                'open': round(m_open, 2),
                'high': round(m_high, 2),
                'low': round(m_low, 2),
                'close': round(m_close, 2),
                'p_high': round(float((m_high - m_open) / m_open * 100), 2),
                'p_low': round(float((m_low - m_open) / m_open * 100), 2),
                'p_close': round(float((m_close - m_open) / m_open * 100), 2)
            })

        return {"ticker": ticker, "results": results}
    except Exception as e:
        return {"error": str(e)}

@app.get("/all_indices_data")
async def get_all_indices_data(months: int = 3, interval: str = "1mo"):
    tickers = {
        "^NSEI":        "Nifty 50",
        "^NSEBANK":     "Nifty Bank",
        "NIFTY_PVT_BANK.NS": "Nifty Pvt Bank",
        "^CNXPSUBANK":  "Nifty PSU Bank",
        "^CNXPHARMA":   "Nifty Pharma",
        "^CNXREALTY":   "Nifty Realty",
        "^CNXCONSUM":   "Nifty Consumption",
        "^CNXMETAL":    "Nifty Metal",
        "^CNXAUTO":     "Nifty Auto",
        "^CNXIT":       "Nifty IT",
        "^CNXINFRA":    "Nifty Infra",
        "^CNXFMCG":     "Nifty FMCG",
        "NIFTY_MID_SELECT.NS": "Nifty Mid Select",
        "^VIX":         "India VIX",
    }
    
    try:
        end_date = datetime.now()
        # Fetch one extra month/week of data to provide a previous close to compare against
        start_date = end_date - relativedelta(months=months + 1)
        if interval == "1mo":
            start_date = datetime(start_date.year, start_date.month, 1)

        start_str = start_date.strftime('%Y-%m-%d')
        end_str = (end_date + timedelta(days=1)).strftime('%Y-%m-%d')
        freq = 'ME' if interval == '1mo' else 'W-MON'

        # yfinance uses a shared HTTP session; limit concurrency to avoid data corruption
        sem = asyncio.Semaphore(1)

        async def fetch_one(ticker, name):
            """Fetch a single ticker sequentially under semaphore to avoid session collisions."""
            async with sem:
                try:
                    df = await asyncio.to_thread(
                        yf.download,
                        tickers=ticker,
                        start=start_str,
                        end=end_str,
                        interval="1d",
                        progress=False,
                        auto_adjust=False,
                        timeout=20
                    )
                    if df.empty:
                        return {"name": name, "results": []}

                    # New yfinance returns (Price, Ticker) MultiIndex
                    if isinstance(df.columns, pd.MultiIndex):
                        try:
                            df = df.xs(ticker, level=1, axis=1)
                        except KeyError:
                            try:
                                df = df.xs(ticker, level=0, axis=1)
                            except KeyError:
                                return {"name": name, "results": []}

                    # Keep only OHLC columns
                    missing = [c for c in ['Open', 'High', 'Low', 'Close'] if c not in df.columns]
                    if missing:
                        return {"name": name, "results": []}

                    df = df[['Open', 'High', 'Low', 'Close']].dropna()
                    if df.empty:
                        return {"name": name, "results": []}

                    # Resample to monthly or weekly
                    try:
                        df_r = df.resample(freq).agg(
                            {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}
                        ).dropna()
                    except ValueError:
                        fallback = 'M' if interval == '1mo' else 'W-MON'
                        df_r = df.resample(fallback).agg(
                            {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}
                        ).dropna()
                    df_r['Prev_Close'] = df_r['Close'].shift(1)

                    results = []
                    for ts, row in df_r.iterrows():
                        if pd.isna(row['Prev_Close']):
                            continue
                        m_open = float(row['Open'])
                        m_close = float(row['Close'])
                        p_close = float(row['Prev_Close'])
                        if p_close == 0:
                            continue
                        label = ts.strftime('%b %Y') if interval == "1mo" else f"W{ts.isocalendar()[1]}"
                        results.append({
                            'period': label,
                            'open': round(m_open, 2),
                            'close': round(m_close, 2),
                            'p_close': round(float((m_close - p_close) / p_close * 100), 2)
                        })
                    return {"name": name, "results": results}
                except Exception:
                    return {"name": name, "results": []}

        # Run with limited concurrency to prevent yfinance session corruption
        tasks = [fetch_one(t, n) for t, n in tickers.items()]
        results_list = list(await asyncio.gather(*tasks))
        
        # Determine unique periods from all results to create columns
        all_periods = []
        for res in results_list:
            for r in res['results']:
                if r['period'] not in all_periods:
                    all_periods.append(r['period'])
        
        return {
            "periods": all_periods,
            "indices": results_list
        }
    except Exception as e:
        return {"error": str(e)}

async def process_row_days(row, current_time, cache, i, total_rows, days_count):
    symbol = str(row['symbol'])
    try:
        # Parse DD-MM-YYYY
        entry_date = pd.to_datetime(row['date'], dayfirst=True)
        
        # We start looking from the day after entry_date
        start_date = entry_date + timedelta(days=1)
        
        # To get N trading days, we might need to fetch more calendar days (weekends/holidays)
        # Fetching 3 times the count to be safe
        end_date = start_date + timedelta(days=days_count * 3)
        
        if start_date > current_time:
            return None

        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        ticker = get_nse_ticker(symbol)
        # Add days_count to cache_key to avoid collisions between different backtest periods
        cache_key = (ticker, start_str, end_str, "days", days_count)

        if cache_key not in cache:
            # Sequential download
            df_raw = await asyncio.to_thread(
                yf.download, 
                ticker, 
                start=start_str, 
                end=end_str, 
                progress=False, 
                auto_adjust=False,
                group_by='ticker', 
                timeout=20
            )

            if df_raw.empty:
                # Simple retry
                await asyncio.sleep(0.5)
                df_raw = await asyncio.to_thread(
                    yf.download, ticker, start=start_str, end=end_str, 
                    progress=False, auto_adjust=False, group_by='ticker', timeout=20
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
                
                # Take only the first N trading days
                df_clean = df_clean.head(days_count)
                cache[cache_key] = df_clean

        data = cache[cache_key]
        
        if data is not None and not data.empty:
            cols = data.columns.tolist()
            if all(c in cols for c in ['Open', 'High', 'Low', 'Close']):
                m_open = float(data['Open'].iloc[0])
                m_high = float(data['High'].max())
                m_low = float(data['Low'].min())
                m_close = float(data['Close'].iloc[-1])
                
                return {
                    'entry_date': row['date'],
                    'symbol': symbol,
                    'target_period': f"Next {len(data)} Days",
                    'open': round(m_open, 2),
                    'high': round(m_high, 2),
                    'low': round(m_low, 2),
                    'close': round(m_close, 2),
                    'p_high': round(float((m_high - m_open) / m_open * 100), 2),
                    'p_low': round(float((m_low - m_open) / m_open * 100), 2),
                    'p_close': round(float((m_close - m_open) / m_open * 100), 2)
                }
    except Exception:
        pass
    return None

@app.post("/scan_days")
async def scan_days(request: Request, file: UploadFile = File(...)):
    # Get days from query params
    days_count = int(request.query_params.get("days", 5))
    
    cache = {}
    current_time = datetime.now()
    
    async def event_generator():
        try:
            content = await file.read()
            df = pd.read_csv(io.BytesIO(content))
            total_rows = len(df)
            
            # Use a limited concurrency to avoid IP blocking
            results = []
            for i, (idx, row) in enumerate(df.iterrows()):
                res = await process_row_days(row, current_time, cache, i, total_rows, days_count)
                if res:
                    results.append(res)
                
                # Send progress update
                yield f"data: {json.dumps({'type': 'progress', 'current': i + 1, 'total': total_rows, 'symbol': row['symbol']})}\n\n"
            
            # Send completion
            yield f"data: {json.dumps({'type': 'complete', 'results': results})}\n\n"
            
        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
