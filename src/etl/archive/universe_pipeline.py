
import os
import requests
import pandas as pd
import json
from dotenv import load_dotenv
load_dotenv()

# Constants
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
EODHD_EXCHANGE_URL = "https://eodhd.com/api/exchanges/US"
EODHD_API_KEY = os.getenv("EODHD_API_KEY")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "bronze", "universe")
HEADERS = {
    "User-Agent": "MyResearchProject/1.0 (savusilviu@hotmail.com)"
}

def setup_directory():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        print(f"Created directory: {DATA_DIR}")

def fetch_universe():
    print(f"Fetching official SEC tickers from {SEC_TICKERS_URL}...")
    try:
        r = requests.get(SEC_TICKERS_URL, headers=HEADERS)
        r.raise_for_status()
        data = r.json()
        
        tickers_list = []
        for key, val in data.items():
            tickers_list.append(val)
            
        df = pd.DataFrame(tickers_list)
        df = df.rename(columns={"cik_str": "cik", "ticker": "ticker", "title": "name"})
        df['cik'] = df['cik'].astype(str).str.zfill(10)
        
        filepath = os.path.join(DATA_DIR, "tickers.csv")
        df.to_csv(filepath, index=False)
        print(f"Successfully saved {len(df)} active SEC tickers to {filepath}")
        return df
    except Exception as e:
        print(f"Error fetching SEC universe: {e}")
        return pd.DataFrame()

def fetch_eodhd_universe():
    """
    Fetches the full US Exchange Symbol List from EODHD.
    Includes Active + Delisted.
    Enriches with CIK from SEC data (Active + Historical/Delisted Map).
    """
    if not EODHD_API_KEY:
        print("Skipping EODHD Universe: No API Key found.")
        return

    print(f"Fetching Master Universe (Active + Delisted) from EODHD...")
    url = f"{EODHD_EXCHANGE_URL}"
    params = {
        "api_token": EODHD_API_KEY,
        "fmt": "json"
    }
    
    try:
        r = requests.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        
        df_eod = pd.DataFrame(data)
        
        # 1. Load Active SEC Tickers
        sec_map = {}
        tickers_file = os.path.join(DATA_DIR, "tickers.csv")
        if os.path.exists(tickers_file):
            print("Loading Active SEC Tickers...")
            df_sec = pd.read_csv(tickers_file)
            df_sec['ticker'] = df_sec['ticker'].astype(str).str.upper()
            df_sec['cik'] = df_sec['cik'].astype(str).str.zfill(10)
            sec_map.update(pd.Series(df_sec.cik.values, index=df_sec.ticker).to_dict())
            
        # 2. Load Historical SEC Tickers (Delisted & Historical)
        # This acts as both a source of CIKs AND a source of missing tickers (e.g. TWTR)
        historical_file = os.path.join(DATA_DIR, "historical_ciks.csv")
        new_rows = []
        
        if os.path.exists(historical_file):
            print("Loading Historical SEC Tickers (Delisted)...")
            df_hist = pd.read_csv(historical_file)
            df_hist['ticker'] = df_hist['ticker'].astype(str).str.upper()
            df_hist['cik'] = df_hist['cik'].astype(str).str.zfill(10)
            
            # Map for enrichment
            hist_map = pd.Series(df_hist.cik.values, index=df_hist.ticker).to_dict()
            for t, c in hist_map.items():
                if t not in sec_map:
                    sec_map[t] = c
            
            # MERGE LOGIC: Add missing tickers to universe
            # Get set of existing tickers in EODHD
            existing_codes = set(df_eod['Code'].astype(str).str.upper())
            
            # Identify tickers in Historical that are NOT in EODHD
            for _, row in df_hist.iterrows():
                t = row['ticker']
                c = row['cik']
                n = row['name']
                if t not in existing_codes:
                    # Add new row
                    new_rows.append({
                        "Code": t,
                        "Name": n,
                        "Country": "USA",
                        "Exchange": "Unknown", # Likely Delisted/OTC
                        "Currency": "USD",
                        "Type": "Common Stock",
                        "Isin": None,
                        "cik": c
                    })
        
        if new_rows:
            print(f"Adding {len(new_rows)} historical/delisted tickers to Master Universe...")
            df_new = pd.DataFrame(new_rows)
            df_eod = pd.concat([df_eod, df_new], ignore_index=True)
            
        print(f"Total Ticker->CIK Mappings available: {len(sec_map)}")
            
        # Map CIK to EODHD dataframe (Refreshed map)
        # We re-map to ensure existing rows get CIKs if we found them in history
        # (Though we appended rows already have CIKs, we want to fill EODHD rows too)
        df_eod['cik'] = df_eod['Code'].astype(str).str.upper().map(sec_map).fillna(df_eod['cik'])
        
        # Count matches
        matches = df_eod['cik'].notna().sum()
        print(f"Mapped {matches} CIKs to EODHD universe (coverage: {matches/len(df_eod):.1%}).")
        
        filepath = os.path.join(DATA_DIR, "eodhd_master_universe.csv")
        df_eod.to_csv(filepath, index=False)
        print(f"Successfully saved {len(df_eod)} tickers (Active + Delisted) to {filepath}")
        return df_eod
        
    except Exception as e:
        print(f"Error fetching EODHD universe: {e}")

if __name__ == "__main__":
    setup_directory()
    fetch_universe()
    fetch_eodhd_universe()
