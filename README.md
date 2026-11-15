# Papa Scanner

NSE stock backtest, index heatmap and 3-day breakout scanner (FastAPI).

## Public URL

https://papa-scanner.vercel.app

## Local run

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Open http://localhost:8000

Deterministic market data for tests:

```bash
export YF_FIXTURE_MODE=1
uvicorn app:app --host 127.0.0.1 --port 8000
```

## 3-Day Breakout scanner

Page: `/three_day` (data API: `/three_day_data?universe=nifty50|nifty500&refresh=false`).

Builds 3-trading-day candles counted back from the latest trading day and flags a candle whose
Close is above the highest Close of the previous 3 candles AND whose High is above their highest
High. Only the first breakout of an up-move counts: later
breakouts are repeats until a breakdown (Close below the previous 3 candles' lowest Close and
Low below their lowest Low) shows the trend turned down. Each row carries RSI(14, Wilder, on 3-day closes) and the NSE F&O lot size; the page
filters on "RSI crosses above 58" / "RSI is above 58", signal within the last 1–5 candles
(default 2) and F&O only, and shows
SL (signal candle low) and a 1:2 target. Results are cached in memory for 15 minutes
(`THREE_DAY_TTL_SEC`); tick "Force fresh data" to rescan.

A Nifty 500 scan pulls ~500 symbols from Yahoo (about 20–60 s). Run it locally with
`python app.py` and open http://localhost:8000/three_day, or without the web app:

```bash
python three_day.py nifty500 > signals.csv
```

## Tests

```bash
export YF_FIXTURE_MODE=1
pytest tests/test_api.py tests/test_three_day.py -q

# E2E (server must already be running with YF_FIXTURE_MODE=1)
playwright install chromium
pytest tests/test_e2e.py -q
```

## CI / Deploy

GitHub Actions (`.github/workflows/ci-deploy.yml`):

1. API + Playwright E2E on PR and `main`
2. Vercel production deploy on `main` after green tests

Required secrets: `VERCEL_TOKEN`, `VERCEL_ORG_ID`, `VERCEL_PROJECT_ID`

## CSV format

```csv
symbol,date
RELIANCE,15-01-2025
TCS,10-01-2025
```

Dates: `DD-MM-YYYY`. Sample: `/sample.csv`
