
import polars as pl
import pandas as pd
import os

BASE_DIR = "/Users/savusilviu/Desktop/SilviuCorneliuSavu/DataStore"
SILVER_MARKET = os.path.join(BASE_DIR, "silver", "market_data", "eodhd")
MASTER_LIST = os.path.join(BASE_DIR, "references", "master_cik_list.parquet")

def check_coverage():
    print(f"Checking Price Coverage in {SILVER_MARKET}...")
    
    if not os.path.exists(SILVER_MARKET):
        print("Silver Market path does not exist.")
        return

    try:
        # Scan Delta Table
        df = pl.scan_delta(SILVER_MARKET)
        
        # Get basic stats
        stats = df.select([
            pl.col("ticker").n_unique().alias("unique_tickers"),
            pl.col("date").min().alias("min_date"),
            pl.col("date").max().alias("max_date"),
            pl.col("cik").n_unique().alias("unique_ciks")
        ]).collect()
        
        print("--- Silver Market Data Stats ---")
        print(stats)
        
        # Load Master Universe Count
        if os.path.exists(MASTER_LIST):
            master_df = pd.read_parquet(MASTER_LIST)
            print(f"\nMaster Universe Count: {len(master_df)} tickers/CIKs")
            
            # Intersection
            market_ciks = set(df.select("cik").unique().collect()["cik"].to_list())
            master_ciks = set(master_df["cik"].astype(str).str.zfill(10))
            
            missing = len(master_ciks - market_ciks)
            print(f"Missing Price Data for {missing} CIKs from Master List")
        else:
            print("\nMaster Universe file not found.")

    except Exception as e:
        print(f"Error reading Delta Table: {e}")

if __name__ == "__main__":
    check_coverage()
