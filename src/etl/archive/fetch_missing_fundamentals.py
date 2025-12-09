"""
Fetch Missing CIK Fundamentals & Create Master CIK Universe
===========================================================

PURPOSE:
    1. Fetch SEC filings (including 20-F, 40-F) for CIKs missing from bulk data
    2. Create comprehensive master CIK universe from all XBRL data sources
    3. Filter to equities only and identify foreign stocks

INPUT:
    - List of CIKs missing from silver/fundamentals/sec

OUTPUT:
    - Appends missing facts to bronze/fundamentals/edgartools (raw facts)
    - Creates master_cik_universe.csv with filtered equity CIKs
    - Creates master_cik_list.csv with all CIKs from XBRL data
    - Creates foreign_stocks.csv with foreign filers
    - Creates cik_universe_summary.json with statistics
"""

import os
import sys
import polars as pl
import pandas as pd
from pathlib import Path
from typing import Set
from edgar import Company, set_identity
from tqdm import tqdm
import time
import json

# Constants
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SILVER_MARKET = os.path.join(BASE_DIR, "silver", "market_data", "eodhd")
SILVER_FUNDS = os.path.join(BASE_DIR, "silver", "fundamentals", "sec")
OUTPUT_DIR = os.path.join(BASE_DIR, "bronze", "fundamentals", "edgartools")

# Configure identity for SEC rate limiting
set_identity('DataStore silviu.savu@example.com')

def get_missing_ciks():
    """Find CIKs in market data but not in fundamentals."""
    market_ciks = set(pl.scan_delta(SILVER_MARKET).select('cik').unique().collect()['cik'].to_list())
    fund_ciks = set(pl.scan_delta(SILVER_FUNDS).select('cik').unique().collect()['cik'].to_list())
    return list(market_ciks - fund_ciks)

def fetch_company_facts(cik):
    """Fetch all XBRL facts for a company."""
    try:
        company = Company(str(int(cik)))  # Remove leading zeros for API
        facts = company.facts
        
        if facts is None or len(facts._facts) == 0:
            return None
        
        # Convert facts to DataFrame
        records = []
        for fact in facts._facts:
            records.append({
                'cik': cik,
                'concept': fact.concept,
                'taxonomy': fact.taxonomy,
                'value': fact.value,
                'unit': fact.unit,
                'period_start': fact.period_start,
                'period_end': fact.period_end,
                'fiscal_year': fact.fiscal_year,
                'fiscal_period': fact.fiscal_period,
                'filing_date': fact.filing_date,
                'form_type': fact.form_type,
            })
        
        return pd.DataFrame(records)
        
    except Exception as e:
        print(f"Error fetching CIK {cik}: {e}")
        return None

def main():
    # Get missing CIKs
    missing = get_missing_ciks()
    print(f"Found {len(missing)} CIKs missing fundamentals.")
    
    if not missing:
        print("No missing CIKs found.")
        return
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Process in batches to save progress
    BATCH_SIZE = 100
    success_count = 0
    
    for i in range(0, len(missing), BATCH_SIZE):
        batch = missing[i:i+BATCH_SIZE]
        batch_dfs = []
        
        for cik in tqdm(batch, desc=f"Batch {i//BATCH_SIZE + 1}"):
            df = fetch_company_facts(cik)
            if df is not None and not df.empty:
                batch_dfs.append(df)
                success_count += 1
            
            # Rate limiting - SEC allows 10 requests/sec
            time.sleep(0.15)
        
        # Save batch
        if batch_dfs:
            df_batch = pd.concat(batch_dfs, ignore_index=True)
            output_file = os.path.join(OUTPUT_DIR, f"batch_{i:05d}.parquet")
            df_batch.to_parquet(output_file)
            print(f"Saved batch to {output_file}")
    
    print(f"\nComplete. Fetched facts for {success_count}/{len(missing)} CIKs.")

def create_master_cik_list() -> Set[str]:
   """Create comprehensive CIK list from all XBRL sources"""

   print("\n🔍 Creating master CIK list from XBRL data...")

   # 1. Load from SEC bulk data (2009-2024)
   sec_ciks = set()
   sec_path = Path('bronze/fundamentals/sec_bulk/parquet')
   sec_files = list(sec_path.glob('*.parquet'))

   print(f"📊 Processing {len(sec_files)} SEC bulk files...")
   for i, parquet_file in enumerate(sec_files):
       try:
           df = pd.read_parquet(parquet_file, columns=['cik'])
           file_ciks = set(df['cik'].astype(str).str.zfill(10))
           sec_ciks.update(file_ciks)
           if (i + 1) % 10 == 0:
               print(f"  Processed {i+1}/{len(sec_files)} files...")
       except Exception as e:
           print(f'⚠️  Error reading {parquet_file}: {e}')

   print(f"✅ Found {len(sec_ciks)} CIKs in SEC bulk data")

   # 2. Load from EdgarTools data
   edgar_ciks = set()
   edgar_path = Path('bronze/fundamentals/edgartools')
   edgar_files = list(edgar_path.glob('*.parquet'))

   print(f"📊 Processing {len(edgar_files)} EdgarTools files...")
   for i, parquet_file in enumerate(edgar_files):
       try:
           df = pd.read_parquet(parquet_file, columns=['cik'])
           file_ciks = set(df['cik'].astype(str).str.zfill(10))
           edgar_ciks.update(file_ciks)
           if (i + 1) % 10 == 0:
               print(f"  Processed {i+1}/{len(edgar_files)} files...")
       except Exception as e:
           print(f'⚠️  Error reading {parquet_file}: {e}')

   print(f"✅ Found {len(edgar_ciks)} CIKs in EdgarTools data")

   # 3. Combine all CIKs
   all_ciks = sec_ciks.union(edgar_ciks)
   print(f"🎯 Total unique CIKs found: {len(all_ciks)}")

   # Save raw master list
   master_df = pd.DataFrame({'cik': sorted(all_ciks)})
   master_df.to_csv('master_cik_list.csv', index=False)
   print(f"💾 Saved master CIK list to master_cik_list.csv")

   return all_ciks

def filter_equities_only(all_ciks: Set[str]) -> Set[str]:
   """Filter CIK list to only include common stocks on major exchanges"""

   print("\n🔍 Filtering to equities only...")

   # Load EODHD universe for exchange/type filtering
   eodhd_file = 'bronze/universe/eodhd_master_universe.csv'
   if not os.path.exists(eodhd_file):
       print(f"⚠️  EODHD universe file not found: {eodhd_file}")
       return set()

   eodhd_df = pd.read_csv(eodhd_file, dtype=str)

   # Valid security types and exchanges
   valid_types = {'Common Stock'}
   valid_exchanges = {'NYSE', 'NASDAQ', 'NYSE MKT', 'NYSE ARCA', 'AMEX', 'BATS'}

   # Filter to equities only
   equity_df = eodhd_df[
       (eodhd_df['Type'].isin(valid_types)) &
       (eodhd_df['Exchange'].isin(valid_exchanges))
   ]

   # Get CIKs for these equities (ensure proper formatting)
   equity_ciks = set(equity_df['cik'].astype(str).str.zfill(10))

   # Filter our master list to only include equity CIKs
   filtered_ciks = all_ciks.intersection(equity_ciks)

   print(f"✅ Found {len(filtered_ciks)} equity CIKs (from {len(all_ciks)} total)")
   print(f"🗑️  Excluded {len(all_ciks) - len(filtered_ciks)} non-equity securities")

   return filtered_ciks

def find_foreign_stocks(all_ciks: Set[str]) -> Set[str]:
   """Identify foreign stocks that file 20-F/40-F instead of 10-K"""

   print("\n🔍 Identifying foreign stocks...")

   # Load all filings data to find foreign filers
   foreign_forms = {'20-F', '40-F', '6-K'}
   foreign_ciks = set()

   sec_path = Path('bronze/fundamentals/sec_bulk/parquet')
   sec_files = list(sec_path.glob('*.parquet'))

   print(f"📊 Scanning {len(sec_files)} files for foreign filers...")

   for i, parquet_file in enumerate(sec_files):
       try:
           df = pd.read_parquet(parquet_file, columns=['cik', 'form'])
           # Filter to foreign forms and get CIKs
           file_foreign_ciks = set(df[df['form'].isin(foreign_forms)]['cik'].astype(str).str.zfill(10))
           foreign_ciks.update(file_foreign_ciks)

           if (i + 1) % 10 == 0:
               print(f"  Scanned {i+1}/{len(sec_files)} files...")

       except Exception as e:
           print(f'⚠️  Error reading {parquet_file}: {e}')

   # Filter to only CIKs that are in our master list
   master_foreign_ciks = all_ciks.intersection(foreign_ciks)

   print(f"✅ Found {len(master_foreign_ciks)} foreign filers in master list")
   print(f"🌍 These are non-US companies listed on US exchanges")

   return master_foreign_ciks

def create_final_universe():
   """Create the final master CIK universe"""

   print("\n🚀 Creating final master CIK universe...")

   # 1. Get all CIKs from XBRL data
   all_ciks = create_master_cik_list()

   # 2. Filter to equities only
   equity_ciks = filter_equities_only(all_ciks)

   # 3. Identify foreign stocks
   foreign_ciks = find_foreign_stocks(all_ciks)

   # 4. Create final universe classification
   domestic_equities = equity_ciks - foreign_ciks
   foreign_equities = equity_ciks & foreign_ciks

   print(f"\n📊 Final Universe Summary:")
   print(f"  🏢 Domestic Equities: {len(domestic_equities)}")
   print(f"  🌍 Foreign Equities: {len(foreign_equities)}")
   print(f"  🏦 Total Equity Universe: {len(equity_ciks)}")

   # Create final DataFrame with classification
   final_data = []
   for cik in equity_ciks:
       cik_type = 'foreign' if cik in foreign_ciks else 'domestic'
       final_data.append({
           'cik': cik,
           'type': cik_type,
           'description': 'Foreign filer (20-F/40-F)' if cik in foreign_ciks else 'Domestic filer (10-K)'
       })

   # Save final results
   final_df = pd.DataFrame(final_data)
   final_df.to_csv('master_cik_universe.csv', index=False)
   print(f"💾 Saved final master universe to master_cik_universe.csv")

   # Save foreign stocks separately
   foreign_df = final_df[final_df['type'] == 'foreign']
   foreign_df.to_csv('foreign_stocks.csv', index=False)
   print(f"💾 Saved foreign stocks list to foreign_stocks.csv")

   # Create summary report
   summary = {
       'total_ciks': len(all_ciks),
       'equity_ciks': len(equity_ciks),
       'domestic_equities': len(domestic_equities),
       'foreign_equities': len(foreign_equities),
       'non_equity_ciks': len(all_ciks) - len(equity_ciks),
       'foreign_filers': len(foreign_ciks),
       'created_files': ['master_cik_list.csv', 'master_cik_universe.csv', 'foreign_stocks.csv']
   }

   with open('cik_universe_summary.json', 'w') as f:
       json.dump(summary, f, indent=2)

   print(f"💾 Saved summary report to cik_universe_summary.json")
   print("\n🎉 Master CIK universe creation complete!")

   return summary

if __name__ == "__main__":
   main()
   # Always create master CIK universe after fetching missing data
   create_final_universe()
