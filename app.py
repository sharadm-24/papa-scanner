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
async def get_index():
    with open("index.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/days")
async def get_days_page():
    with open("days.html", "r") as f:
        return HTMLResponse(content=f.read())

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
