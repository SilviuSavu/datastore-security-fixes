"""
Build Historical CIK Mapping
============================

PURPOSE:
    Creates the authoritative CIK → Ticker mapping file used throughout the pipeline.
    This is the SOURCE OF TRUTH for linking SEC Central Index Keys (CIK) to trading symbols.

INPUT:
    - SEC Bulk Archive ZIPs: bronze/sec_bulk/*.zip
    - Each ZIP contains sub.txt (submission metadata with CIK, company name, and instance filename)

OUTPUT:
    - bronze/universe/historical_ciks.csv
    - Columns: ticker, cik (10-digit zero-padded), name
    - One row per unique CIK (14,846 as of Dec 2024)

DOWNSTREAM DEPENDENCIES (scripts that consume this file):
    - standardize_eodhd_market.py: Maps Bronze price files (named by CIK) to tickers
    - standardize_sec.py: Links SEC filings to tickers for fundamental data

HOW TICKER IS DERIVED:
    - SEC instance filenames follow pattern: {ticker}-{date}.xml (e.g., aapl-20230930.xml)
    - Script extracts the prefix before the first hyphen as the ticker
    - Filter: Only 1-5 char tickers, excludes 'FORM*' prefixes (form type artifacts)

IMPORTANT: CIK is UNIQUE per company. This script enforces one ticker per CIK.
"""

import os
import zipfile
import pandas as pd
import glob
import re
from tqdm import tqdm

# Constants
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
SEC_BULK_DIR = os.path.join(BASE_DIR, "bronze", "sec_bulk")
UNIVERSE_DIR = os.path.join(BASE_DIR, "bronze", "universe")
OUTPUT_FILE = os.path.join(UNIVERSE_DIR, "historical_ciks.csv")

def extract_tickers_from_zip(zip_path):
    """
    Reads sub.txt from a zip archive and extracts (cik, ticker, name).
    Ticker is derived from the 'instance' filename (e.g. 'aapl-2023...xml').
    """
    mappings = []
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            if 'sub.txt' in z.namelist():
                # Read sub.txt line by line or with pandas
                # sub.txt is tab separated
                # Columns we need: adsh, cik, name, instance
                # We use chunks to avoid memory issues (though sub.txt is usually <100MB)
                
                with z.open('sub.txt') as f:
                    # Parse efficiently
                    df = pd.read_csv(f, sep='\t', usecols=['cik', 'name', 'instance'], on_bad_lines='skip', encoding='latin-1', dtype=str)
                    
                    # Drop rows without instance
                    df = df.dropna(subset=['instance'])
                    
                    # Derive Ticker
                    # Instance format: 'ticker-date.xml' OR 'form-date.xml'
                    # We accept everything before the first hyphen as a POTENTIAL ticker.
                    # We will filter out generic ones later (like 'form', 'def', '10k').
                    
                    for _, row in df.iterrows():
                        instance = str(row['instance']).lower()
                        parts = instance.split('-')
                        if len(parts) > 1:
                            ticker = parts[0].upper()
                            # Filter heuristic: Tickers are typically 1-5 chars.
                            # 'form10k' is 7 chars. 'ck000...' is garbage.
                            if 1 <= len(ticker) <= 5 and not ticker.startswith('FORM'):
                                mappings.append({
                                    "ticker": ticker,
                                    "cik": str(row['cik']).zfill(10),
                                    "name": row['name']
                                })
    except Exception as e:
        print(f"Error reading {zip_path}: {e}")
        
    return mappings

def main():
    if not os.path.exists(UNIVERSE_DIR):
        os.makedirs(UNIVERSE_DIR)

    zip_files = sorted(glob.glob(os.path.join(SEC_BULK_DIR, "*.zip")))
    print(f"Found {len(zip_files)} SEC Bulk archives.")
    
    all_mappings = {} # Key by Ticker to keep latest/best? Or CIK?
    # We want Ticker -> CIK map.
    # A ticker might change CIKs? Rare. A CIK might change Tickers? Yes.
    # We want the *latest* CIK for a Ticker? Or *all*?
    # Let's collect all unique (Ticker, CIK) pairs.
    
    records = []
    
    for zf in tqdm(zip_files, desc="Processing Archives"):
        # We assume recent zips have recent tickers.
        # We can process in reverse order maybe?
        # Actually, aggregating unique pairs is safer.
        extracted = extract_tickers_from_zip(zf)
        records.extend(extracted)
        
    # Create DataFrame
    df = pd.DataFrame(records)
    
    # Drop duplicates
    df = df.drop_duplicates(subset=['cik'], keep='first')
    
    # Save
    print(f"Extracted {len(df)} historical ticker mappings.")
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
