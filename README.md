# Papa Scanner

NSE stock backtest + index heatmap app (FastAPI).

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

## Tests

```bash
export YF_FIXTURE_MODE=1
pytest tests/test_api.py -q

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
