"""
NSE Monthly OHLC Percentage Calculator
Fetches data from Yahoo Finance (.NS suffix for NSE stocks)
Computes High, Low, Close as % from the monthly Open price.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
#  CONFIGURATION  – edit here
# ─────────────────────────────────────────────
STOCKS = [
    "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK",
    "WIPRO", "BAJFINANCE", "SBIN", "BHARTIARTL", "ADANIENT",
]

OUTPUT_CSV = "nse_monthly_ohlc_percent.csv"
# ─────────────────────────────────────────────


def fetch_daily_data(symbol: str, start: str, end: str) -> pd.DataFrame:
    ticker = symbol.upper().strip() + ".NS"
    # Set timeout to avoid hanging
    df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)

    if df.empty:
        print(f"  ⚠️  No data for {ticker}")
        return pd.DataFrame()

    # Flatten MultiIndex columns (yfinance v0.2+)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df.index = pd.to_datetime(df.index)
    needed = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns]
    return df[needed]


def compute_monthly_ohlc(daily_df: pd.DataFrame) -> pd.DataFrame:
    # Monthly resampling: start-of-month
    monthly = daily_df.resample("MS").agg(
        Open=("Open",    "first"),
        High=("High",    "max"),
        Low=("Low",      "min"),
        Close=("Close",  "last"),
        Volume=("Volume","sum"),
    )
    monthly.dropna(subset=["Open", "Close"], inplace=True)
    return monthly


def compute_pct_from_open(monthly_df: pd.DataFrame) -> pd.DataFrame:
    df = monthly_df.copy()
    # Handle division by zero just in case
    df["%High"]  = ((df["High"]  - df["Open"]) / df["Open"] * 100).round(2)
    df["%Low"]   = ((df["Low"]   - df["Open"]) / df["Open"] * 100).round(2)
    df["%Close"] = ((df["Close"] - df["Open"]) / df["Open"] * 100).round(2)
    return df


def process_stock(symbol: str, start: str, end: str) -> pd.DataFrame:
    print(f"  📥 Fetching {symbol}.NS   ({start} → {end})")
    daily = fetch_daily_data(symbol, start, end)
    if daily.empty:
        return pd.DataFrame()

    monthly = compute_monthly_ohlc(daily)
    if monthly.empty:
        print(f"  ⚠️  Not enough data for monthly bar: {symbol}")
        return pd.DataFrame()

    result = compute_pct_from_open(monthly)
    result.index.name = "Month"
    result.reset_index(inplace=True)
    result.insert(0, "Symbol", symbol.upper())
    return result


def main():
    today      = datetime.today()
    start_date = today.replace(day=1)               # 1st of current month
    end_date   = today + timedelta(days=1)          # tomorrow (to include today)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")

    print(f"\n{'='*65}")
    print(f"  NSE Monthly OHLC % Calculator  |  {today.strftime('%d-%b-%Y')}")
    print(f"  Range  : {start_str}  →  {end_str}  (current month)")
    print(f"  Stocks : {len(STOCKS)}")
    print(f"{'='*65}\n")

    frames = []
    for symbol in STOCKS:
        result = process_stock(symbol, start_str, end_str)
        if not result.empty:
            frames.append(result)

    if not frames:
        print("\n❌ No data fetched. Check network / ticker symbols.")
        return

    combined = pd.concat(frames, ignore_index=True)
    combined["Month"] = pd.to_datetime(combined["Month"]).dt.strftime("%b-%Y")

    cols = ["Symbol", "Month", "Open", "High", "Low", "Close", "%High", "%Low", "%Close"]
    combined = combined[[c for c in cols if c in combined.columns]]
    for col in ["Open", "High", "Low", "Close"]:
        if col in combined.columns:
            combined[col] = combined[col].round(2)

    pd.set_option("display.width", 130)
    pd.set_option("display.max_rows", 200)

    print("\n📊  MONTHLY OHLC — % values relative to monthly Open\n")
    print(combined.to_string(index=False))

    print(f"\n{'='*65}")
    pct_cols = ["%High", "%Low", "%Close"]
    summary = combined.groupby("Symbol")[pct_cols].mean().round(2)
    summary.columns = ["Avg %High", "Avg %Low", "Avg %Close"]
    print("  SUMMARY — average % move from open\n")
    print(summary.to_string())

    combined.to_csv(OUTPUT_CSV, index=False)
    print(f"\n✅ Saved → {OUTPUT_CSV}\n")


if __name__ == "__main__":
    main()