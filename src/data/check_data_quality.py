import os
import sys
import numpy as np
import polars as pl
from deltalake import DeltaTable

# Setup Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GOLD_PATH = os.path.join(BASE_DIR, "gold", "features")

def main():
    print(f"Scanning Gold Layer: {GOLD_PATH}")
    
    # 1. Get Schema
    lf_schema = pl.scan_delta(GOLD_PATH).collect_schema()
    exclude = ["date", "ticker", "target_1d", "cik", "year", "adsh", "cik_right", "name", 
               "period", "filed", "form", "fy", "fp", "currency_symbol", "filing_date", "date_right", "year_right",
               "filed_date", "id"]
    feats = [c for c in lf_schema.names() if c not in exclude and lf_schema[c].is_numeric()]
    print(f"Features: {len(feats)}")

    # 2. Iterate Batches and Count NaNs/Infs
    dt = DeltaTable(GOLD_PATH)
    dataset = dt.to_pyarrow_dataset()
    scanner = dataset.scanner(columns=feats, batch_size=1_000_000)
    
    nan_counts = np.zeros(len(feats), dtype=np.int64)
    inf_counts = np.zeros(len(feats), dtype=np.int64)
    total_rows = 0
    
    print("Streaming Data...")
    for i, batch in enumerate(scanner.to_batches()):
        # No sampling, we want exact counts
        if i % 10 == 0:
             print(f"  > Batch {i}...", end="\r", flush=True)
             
        df = pl.from_arrow(batch)
        X = df.to_numpy().astype(np.float64) # Force float64 for safety
        
        # Check NaNs
        nan_counts += np.sum(np.isnan(X), axis=0).astype(np.int64)
        
        # Check Infs
        inf_counts += np.sum(np.isinf(X), axis=0).astype(np.int64)
        
        total_rows += len(X)
        
    print(f"\nScanned {total_rows} rows.")
    
    # 3. Report
    print("\n=== Data Quality Report ===")
    print(f"{'Feature':<40} | {'NaNs':<10} | {'Infs':<10} | {'% Bad':<6}")
    print("-" * 75)
    
    bad_features = []
    
    for i, f in enumerate(feats):
        n_nan = nan_counts[i]
        n_inf = inf_counts[i]
        
        if n_nan > 0 or n_inf > 0:
            pct_bad = (n_nan + n_inf) / total_rows * 100
            print(f"{f:<40} | {n_nan:<10} | {n_inf:<10} | {pct_bad:>5.1f}%")
            bad_features.append(f)
            
    if not bad_features:
        print("No NaNs or Infs found! Data is clean.")
    else:
        print(f"\nFound {len(bad_features)} problematic features.")

if __name__ == "__main__":
    main()
