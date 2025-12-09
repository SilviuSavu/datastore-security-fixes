import zipfile
import pandas as pd
import sys

zip_path = 'bronze/sec_bulk/2024q3.zip'

try:
    with zipfile.ZipFile(zip_path, 'r') as z:
        if 'sub.txt' in z.namelist():
            with z.open('sub.txt') as f:
                # Read just the header
                header = pd.read_csv(f, sep='\t', nrows=0)
                print(f"Columns in sub.txt: {list(header.columns)}")
                
                # Check for 'ticker' or similar
                potential_ticker_cols = [c for c in header.columns if 'tick' in c.lower() or 'symbol' in c.lower()]
                print(f"Potential ticker columns: {potential_ticker_cols}")
        else:
            print("sub.txt not found in zip")
except Exception as e:
    print(f"Error: {e}")
