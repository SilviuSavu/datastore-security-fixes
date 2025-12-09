import pandas as pd
import logging

# Mock pipeline logic
try:
    metadata_df = pd.read_csv('master_cik_list.csv', dtype={'cik': str})
    metadata_map = metadata_df.set_index('cik')[['ticker', 'sic']].to_dict('index')
    print(f"Loaded {len(metadata_map)} items in map.")
    
    # Sample key check
    keys = list(metadata_map.keys())
    print("Sample keys:", keys[:5])
    
    # Check a known foreign CIK
    # From debug output: 1951222
    target = "0001951222"
    if target in metadata_map:
        print(f"Found {target}: {metadata_map[target]}")
    else:
        print(f"{target} NOT found in map.")
        
    # Check unpadded
    if "1951222" in metadata_map:
        print(f"Found 1951222: {metadata_map['1951222']}")
        
except Exception as e:
    print(f"Error: {e}")
