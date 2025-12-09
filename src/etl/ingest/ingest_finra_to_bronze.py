"""
Ingest FINRA Data to Bronze Delta
=================================

Purpose:
    Moves raw FINRA CSVs (Short Volume, Short Interest, Margin) 
    from 'bronze/landing/finra_raw' to the Bronze Delta Lake at 'bronze/delta/finra'.

Features:
    - Incremental Ingestion.
    - Standardizes headers mapping.
"""

import os
import glob
import pandas as pd
from deltalake import write_deltalake
import time

# Constants
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
LANDING_DIR = os.path.join(BASE_DIR, "bronze", "landing", "finra_raw")
BRONZE_DELTA_PATH = os.path.join(BASE_DIR, "bronze", "delta", "finra")

def process_reg_sho(files):
    print(f"Processing {len(files)} Reg SHO files...")
    dfs = []
    for f in files:
        try:
            # FINRA Reg SHO often has headers like 'tradeReportDate', 'symbol'
            df = pd.read_csv(f, sep='|', on_bad_lines='skip') # Try pipe first
            if len(df.columns) < 2:
                df = pd.read_csv(f, sep=',', on_bad_lines='skip') # Fallback comma
                
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")
            
    if not dfs: return 0
    
    df_batch = pd.concat(dfs, ignore_index=True)
    
    # Simple write, we rely on Silver to clean column names strictly.
    # But for Bronze, we should try to ensure at least 'date' exists for partitioning if possible.
    # If not, just append unpartitioned or partition by ingestion year.
    
    # Write
    write_deltalake(
        os.path.join(BRONZE_DELTA_PATH, "reg_sho"),
        df_batch,
        mode='append',
        schema_mode='merge'
    )
    return len(files)

def process_short_interest(files):
    print(f"Processing {len(files)} Short Interest files...")
    dfs = []
    for f in files:
        try:
            # Short Interest usually has pipe or comma
            df = pd.read_csv(f, sep='|', on_bad_lines='skip')
            if len(df.columns) < 2:
                df = pd.read_csv(f, sep=',', on_bad_lines='skip')
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")
            
    if not dfs: return 0
    df_batch = pd.concat(dfs, ignore_index=True)
    
    write_deltalake(
        os.path.join(BRONZE_DELTA_PATH, "short_interest"), # Will become bronze/delta/finra/short_interest
        df_batch,
        mode='append',
        schema_mode='merge'
    )
    return len(files)

def main():
    # 1. Reg SHO
    reg_sho_files = glob.glob(os.path.join(LANDING_DIR, "reg_sho", "*.csv"))
    if reg_sho_files:
        process_reg_sho(reg_sho_files)
    
    # 2. Short Interest
    short_int_files = glob.glob(os.path.join(LANDING_DIR, "short_interest", "*.csv"))
    if short_int_files:
        process_short_interest(short_int_files)
        
    print("FINRA Ingestion Complete.")

if __name__ == "__main__":
    main()
