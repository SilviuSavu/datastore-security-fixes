"""Quick script to add target_20d_fwd to existing Gold layer without full rebuild."""
import os
import polars as pl
from deltalake import write_deltalake, DeltaTable
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GOLD_PATH = os.path.join(BASE_DIR, "gold", "features")
GOLD_PATH_NEW = os.path.join(BASE_DIR, "gold", "features_v2")

def main():
    print("Adding target_20d_fwd to Gold layer...")
    print(f"Reading from: {GOLD_PATH}")
    print(f"Writing to: {GOLD_PATH_NEW}")
    start = time.time()
    
    # Get years
    years = pl.scan_delta(GOLD_PATH).select("year").unique().collect()["year"].to_list()
    years = sorted(years)
    
    print(f"Processing {len(years)} years: {min(years)} - {max(years)}")
    
    for i, year in enumerate(years):
        t0 = time.time()
        print(f"  Year {year} ({i+1}/{len(years)})...", end="", flush=True)
        
        df = pl.scan_delta(GOLD_PATH).filter(pl.col("year") == year).collect()
        
        if df.height == 0:
            print(" empty")
            continue
        
        # Sort and compute target
        df = df.sort(["ticker", "date"])
        df = df.with_columns(
            pl.col("log_return_1d").rolling_sum(window_size=20, center=False).shift(-20).over("ticker").alias("target_20d_fwd")
        )
        
        # Write to new table
        write_deltalake(
            GOLD_PATH_NEW,
            df.to_arrow(),
            partition_by=["year"],
            mode="append",
            schema_mode="merge"
        )
        
        print(f" {df.height} rows ({time.time()-t0:.1f}s)")
    
    print(f"\nDone. Total time: {time.time()-start:.1f}s")
    print(f"New Gold table at: {GOLD_PATH_NEW}")
    print("To use: rename 'features' to 'features_old' and 'features_v2' to 'features'")

if __name__ == "__main__":
    main()
