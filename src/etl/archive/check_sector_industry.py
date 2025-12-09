import zipfile
import pandas as pd

zip_path = 'bronze/sec_bulk/2024q3.zip'

try:
    with zipfile.ZipFile(zip_path, 'r') as z:
        # Check files in zip
        print(f"Files in zip: {z.namelist()}")
        
        # Check sub.txt for SIC
        if 'sub.txt' in z.namelist():
            with z.open('sub.txt') as f:
                df = pd.read_csv(f, sep='\t', nrows=20, usecols=['name', 'sic'])
                print("\nSample SIC data:")
                print(df.head())
        
        # Check tag.txt for GICS related tags?
        if 'tag.txt' in z.namelist():
             with z.open('tag.txt') as f:
                # Read header
                header = pd.read_csv(f, sep='\t', nrows=0)
                print(f"\nColumns in tag.txt: {list(header.columns)}")
                
                # Search for GICS in tags - reading chunks to be safe
                found_gics = False
                for chunk in pd.read_csv(f, sep='\t', chunksize=10000):
                    matches = chunk[chunk['tag'].astype(str).str.contains('GICS', case=False, na=False)]
                    if not matches.empty:
                        print("\nFound GICS related tags:")
                        print(matches[['tag', 'tlabel', 'doc']].head())
                        found_gics = True
                        break
                if not found_gics:
                    print("\nNo 'GICS' string found in tag.txt tags")

except Exception as e:
    print(f"Error: {e}")
