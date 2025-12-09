import polars as pl
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GOLD_PATH = os.path.join(BASE_DIR, "gold", "features")

def check_leak():
    print("Checking for Data Leakage...")
    
    # Load a sample year (e.g. 2024 - Test Set)
    lf = pl.scan_delta(GOLD_PATH).filter(pl.col("year") == 2024)
    
    # Target
    lf = lf.sort(["ticker", "date"]).with_columns([
        pl.col("log_return_1d").shift(-1).over("ticker").alias("target_1d")
    ]).filter(pl.col("target_1d").is_not_null())
    
    # Exclude non-features
    exclude = ["date", "ticker", "target_1d", "cik", "year", "adsh", "cik_right", "name"]
    cols = [c for c in lf.collect_schema().names() if c not in exclude]
    
    print(f"Scanning {len(cols)} features for high correlation with target...")
    
    df = lf.select(cols + ["target_1d"]).collect()
    
    high_corr = []
    
    for c in cols:
        if df[c].dtype.is_numeric():
            corr = df.select(pl.corr(c, "target_1d")).item()
            if abs(corr) > 0.15:
                high_corr.append((c, corr))
                print(f"⚠️ High Correlation: {c} = {corr:.4f}")
            
    if not high_corr:
        print("No obvious single-feature leaks found (Max Corr < 0.15).")
    else:
        print(f"Found {len(high_corr)} features with high correlation.")

if __name__ == "__main__":
    check_leak()
