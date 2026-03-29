import yfinance as yf
import pandas as pd

tickers = [
    "MIDCPSE.NS", "^MIDCPSE", "NIFTY_MID_SELECT.NS", 
    "NIFPVTBNK.NS", "^NIFTYPVT", "^CNXPVT",
    "^CNXPSU", "NIFTY_PSU_BANK.NS", "^NIFTYPSU",
    "BSE-MIDCAP.BO"
]

for t in tickers:
    data = yf.download(t, period="5d", progress=False)
    if not data.empty:
        print(f"Success: {t}")
    else:
        print(f"Failed: {t}")
