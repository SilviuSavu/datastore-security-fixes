"""
Ingest Global Macro Data to Bronze Delta
=======================================

Purpose:
    Moves raw Global Macro CSV data from 'bronze/landing/global_raw'
    to the Bronze Delta Lake at 'bronze/delta/global'.

Features:
    - Incremental Ingestion.
    - Standardizes global economic indicators.
"""
import os
import glob
import pandas as pd
from deltalake import write_deltalake
import time

# Constants
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
LANDING_DIR = os.path.join(BASE_DIR, "bronze", "landing", "global_raw")
BRONZE_DELTA_PATH = os.path.join(BASE_DIR, "bronze", "delta", "global")

def process_global_series(files):
    """Process Global Macro series files and write to Delta Lake"""
    print(f"Processing {len(files)} Global Macro series files...")

    dfs = []
    for f in files:
        try:
            # Read Global Macro CSV files
            df = pd.read_csv(f)

            # Add source metadata
            basename = os.path.basename(f)
            series_id = basename.replace(".csv", "")

            df['series_id'] = series_id
            df['source_file'] = basename
            df['ingestion_timestamp'] = pd.Timestamp.now()

            # Standardize column names
            if 'date' in df.columns and 'value' in df.columns:
                df = df[['date', 'value', 'series_id', 'source_file', 'ingestion_timestamp']]

            dfs.append(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")

    if not dfs:
        return 0

    df_batch = pd.concat(dfs, ignore_index=True)

    # Write to Bronze Delta
    write_deltalake(
        BRONZE_DELTA_PATH,
        df_batch,
        mode='append',
        partition_by=['series_id'],
        schema_mode='merge'
    )

    return len(files)

def main():
    print(f"Scanning {LANDING_DIR} for Global Macro data...")

    # Create landing directory if it doesn't exist
    os.makedirs(LANDING_DIR, exist_ok=True)

    files = glob.glob(os.path.join(LANDING_DIR, "*.csv"))

    if not files:
        print("No Global Macro files found in landing zone.")
        return

    print(f"Found {len(files)} Global Macro files. Starting ingestion...")

    start_time = time.time()
    count = process_global_series(files)
    elapsed = time.time() - start_time

    print(f"Global Macro Ingestion Complete. Processed {count} files in {elapsed:.2f}s.")

if __name__ == "__main__":
    main()