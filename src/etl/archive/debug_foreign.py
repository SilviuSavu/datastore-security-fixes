import pandas as pd

foreign = pd.read_csv('foreign_stocks.csv')
master = pd.read_csv('master_cik_list.csv', dtype={'cik': str})

print("Foreign Stocks CIKs (head):", foreign['cik'].head().tolist())

# Check overlap
foreign_ciks = set(foreign['cik'].astype(str))
master_ciks = set(master['cik'].astype(str))

missing_in_master = foreign_ciks - master_ciks
print(f"Foreign CIKs missing from Master: {len(missing_in_master)}")

# Check metadata for foreign ciks in master
master_foreign = master[master['cik'].isin(foreign_ciks)]
print(f"Master records for foreign stocks: {len(master_foreign)}")
print(f"Tickers for foreign stocks in master: {master_foreign['ticker'].notna().sum()}")
print(f"SIC for foreign stocks in master: {master_foreign['sic'].notna().sum()}")

# Print sample
print("\nSample Master data for foreign stocks:")
print(master_foreign.head())
