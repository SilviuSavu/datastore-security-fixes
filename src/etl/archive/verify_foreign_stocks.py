import pandas as pd

# Load foreign stocks list
df = pd.read_csv('foreign_stocks.csv')

print(f"Columns: {list(df.columns)}")
print(f"Total rows: {len(df)}")
print(f"Tickers found: {df['ticker'].notna().sum()}")
print(f"SIC codes found: {df['sic'].notna().sum()}")

# Sample some with data
print("\nSample with Ticker and SIC:")
print(df[df['ticker'].notna() & df['sic'].notna()].head())
