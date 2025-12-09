import zipfile
import pandas as pd
import sys

zip_path = 'bronze/sec_bulk/2024q3.zip'

try:
    with zipfile.ZipFile(zip_path, 'r') as z:
        if 'sub.txt' in z.namelist():
            with z.open('sub.txt') as f:
                # Read first 20 rows
                df = pd.read_csv(f, sep='\t', nrows=20, usecols=['instance', 'name', 'cik'])
                print(df)
        else:
            print("sub.txt not found in zip")
except Exception as e:
    print(f"Error: {e}")
