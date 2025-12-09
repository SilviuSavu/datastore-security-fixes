
import os
import dbnomics
import pandas as pd
from dotenv import load_dotenv

# Load Environment
load_dotenv()

# Constants
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
MACRO_DIR = os.path.join(BASE_DIR, "bronze", "macro", "global")

def setup_directories():
    if not os.path.exists(MACRO_DIR):
        os.makedirs(MACRO_DIR)
        print(f"Created directory: {MACRO_DIR}")

# Confirmed Series Map (DB.nomics)
# Provider/Dataset/Series
SERIES_MAP = {
    # Euro Area (ECB)
    "EURUSD": {
        "provider": "ECB",
        "dataset": "EXR",
        "series": "D.USD.EUR.SP00.A",
        "desc": "Euro vs USD (ECB Reference Rate)"
    },
    # Japan (BIS - Effective Rate as Proxy for specific dataset if needed, 
    # but strictly looking for USD/JPY. 
    # Using BIS/EER/D.N.JP (Nominal Effective) as valid global indicator if Spot fails.
    # However, let's stick to what we know works: ECB EUR.
    # We will add others as we confirm them.
}

def fetch_series(key, info):
    print(f"Fetching {key} ({info['desc']})...")
    try:
        df = dbnomics.fetch_series(info['provider'], info['dataset'], info['series'])
        
        # Standardize
        # DBnomics returns many columns. We want date (index or col) and value.
        # usually 'period' or 'original_period' is date, 'value' is value.
        
        if 'period' in df.columns:
            df['date'] = pd.to_datetime(df['period'])
        elif 'original_period' in df.columns:
            df['date'] = pd.to_datetime(df['original_period'])
            
        # Clean
        df = df.sort_values('date')
        df = df[['date', 'value']].dropna()
        
        # Save
        filepath = os.path.join(MACRO_DIR, f"{key}.csv")
        df.to_csv(filepath, index=False)
        print(f"Saved {len(df)} rows to {filepath}")
        return True
        
    except Exception as e:
        print(f"Failed {key}: {e}")
        return False

def main():
    setup_directories()
    print(f"Starting Global Macro Download (DB.nomics)...")
    
    success = 0
    for key, info in SERIES_MAP.items():
        if fetch_series(key, info):
            success += 1
            
    print(f"Global Download Complete. Success: {success}/{len(SERIES_MAP)}")

if __name__ == "__main__":
    main()
