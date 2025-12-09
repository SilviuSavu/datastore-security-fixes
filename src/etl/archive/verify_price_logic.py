import os
import sys
import pandas as pd
import shutil
from src.etl.bronze_data_pipeline import PriceDataModule

# Setup
module = PriceDataModule()
# Unset API key to force YF fallback
module.eodhd_api_key = None 

# 1. Test Local Priority
print("\n--- Testing Local Priority ---")
dummy_cik = "9999999999"
dummy_path = os.path.join(module.eodhd_data_dir, f"{dummy_cik}.csv")
start_content = "dummy_content"

# Create dummy file
with open(dummy_path, "w") as f:
    f.write(start_content)

try:
    # Run fetch - should return True and NOT change content
    result = module.fetch_ticker_with_fallback("NON_EXISTENT_TICKER", dummy_cik, "primary")
    
    with open(dummy_path, "r") as f:
        end_content = f.read()
        
    if result and end_content == start_content:
        print("✅ Local Priority Test Passed: File used and not modified.")
    else:
        print(f"❌ Local Priority Test Failed: Result={result}, Content Changed={end_content != start_content}")

finally:
    if os.path.exists(dummy_path):
        os.remove(dummy_path)

# 2. Test YFinance Backfill & Format
print("\n--- Testing YFinance Backfill & Format ---")
real_cik = "0000320193" # Apple
real_ticker = "AAPL"
real_path = os.path.join(module.eodhd_data_dir, f"{real_cik}.csv")

# Ensure clean state
if os.path.exists(real_path):
    os.remove(real_path)

try:
    # Run fetch - should download from YF
    result = module.fetch_ticker_with_fallback(real_ticker, real_cik, "primary")
    
    if result and os.path.exists(real_path):
        df = pd.read_csv(real_path)
        print(f"File created at {real_path}")
        print("Columns:", list(df.columns))
        print("Head:")
        print(df.head(2))
        
        expected_cols = ['date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume']
        if list(df.columns) == expected_cols:
             print("✅ YFinance Backfill Test Passed: File created with correct schema.")
        else:
             print(f"❌ YFinance Backfill Test Failed: Incorrect columns. Expected {expected_cols}, got {list(df.columns)}")
    else:
        print("❌ YFinance Backfill Test Failed: File not created or return False.")

except Exception as e:
    print(f"❌ Error during test: {e}")
