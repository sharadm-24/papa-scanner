import yfinance as yf
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os

# ─────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────
INPUT_CSV = "/Users/sharadmaheshwari/Downloads/Backtest MONTHLY + 10 % - 10 %, Technical Analysis Scanner.csv"
OUTPUT_CSV = "/Users/sharadmaheshwari/papa scanner /backtest_results.csv"
# ─────────────────────────────────────────────

def get_nse_ticker(symbol):
    # Some symbols in technical analysis might differ from Yahoo Finance tickers
    # Common ones: M&M -> M&M.NS, BAJAJ-AUTO -> BAJAJ-AUTO.NS
    # Indices like NIFTY need special handling, but usually we focus on stocks
    if symbol.upper() == "NIFTY":
        return "^NSEI" 
    return symbol.strip().upper() + ".NS"

def main():
    if not os.path.exists(INPUT_CSV):
        print(f"File not found: {INPUT_CSV}")
        return

    # Load scanner file
    df = pd.read_csv(INPUT_CSV)
    
    # Ensure dates are parsed correctly (format is DD-MM-YYYY)
    df['date_dt'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
    
    results = []
    
    # Cache for downloaded data to avoid redundant API calls
    # Key: (ticker, start_of_next_month, end_of_next_month)
    data_cache = {}

    print(f"Processing {len(df)} entries...\n")

    current_time = datetime.now()

    for idx, row in df.iterrows():
        symbol = row['symbol']
        entry_date = row['date_dt']
        
        # Target month is entry month + 1
        target_month_date = entry_date + relativedelta(months=1)
        target_month_start = target_month_date.replace(day=1)
        
        # If the target month is in the future (or current month if it's not finished), skip or handle
        # User asked to check for certain months, including recent ones.
        if target_month_start > current_time:
            # print(f"Skipping {symbol} for {target_month_start.strftime('%b %Y')} (future date)")
            continue

        # Range for the target month
        start_str = target_month_start.strftime('%Y-%m-%d')
        end_date = target_month_start + relativedelta(months=1)
        end_str = end_date.strftime('%Y-%m-%d')
        
        ticker = get_nse_ticker(symbol)
        cache_key = (ticker, start_str, end_str)

        if cache_key not in data_cache:
            try:
                # Fetch a bit extra around the range to ensure we capture the full month
                data = yf.download(ticker, start=start_str, end=end_str, progress=False, auto_adjust=True)
                if isinstance(data.columns, pd.MultiIndex):
                    data.columns = data.columns.get_level_values(0)
                data_cache[cache_key] = data
            except Exception as e:
                print(f"Error fetching {ticker}: {e}")
                data_cache[cache_key] = pd.DataFrame()

        data = data_cache[cache_key]

        if not data.empty:
            m_open = data['Open'].iloc[0]
            m_high = data['High'].max()
            m_low = data['Low'].min()
            m_close = data['Close'].iloc[-1]
            
            p_high = ((m_high - m_open) / m_open * 100)
            p_low = ((m_low - m_open) / m_open * 100)
            p_close = ((m_close - m_open) / m_open * 100)
            
            results.append({
                'Entry Date': row['date'],
                'Symbol': symbol,
                'MarketCap': row['marketcapname'],
                'Sector': row['sector'],
                'Target Month': target_month_start.strftime('%b-%Y'),
                'Month Open': round(m_open, 2),
                'Month High': round(m_high, 2),
                'Month Low': round(m_low, 2),
                'Month Close': round(m_close, 2),
                '%High': round(p_high, 2),
                '%Low': round(p_low, 2),
                '%Close': round(p_close, 2)
            })
        else:
            # Entry with no data
            pass
        
        if (len(results) % 20 == 0) and (len(results) > 0):
             print(f"Processed {len(results)} results...")

    # Create final dataframe
    result_df = pd.DataFrame(results)
    
    if not result_df.empty:
        print("\n📊 BACKTEST COMPLETED\n")
        print(result_df.tail(20).to_string(index=False))
        
        result_df.to_csv(OUTPUT_CSV, index=False)
        print(f"\n✅ Results saved to {OUTPUT_CSV}")
    else:
        print("\n❌ No data could be fetched for the specified entries.")

if __name__ == "__main__":
    main()
