import sys
import os
import pandas as pd
import json

# Force add project root to path
sys.path.append(os.getcwd())

from src.etl.archive.sec_mapper import SECMapper

def main():
    # 1. Get SEC Data for Allstate (CIK 899051)
    try:
        df = pd.read_parquet('bronze/fundamentals/sec_bulk/parquet/2024q3.parquet')
        df_all = df[df['cik'] == 899051]
        
        if df_all.empty:
            print('Allstate (899051) not found in 2024q3.parquet')
            return
            
        std_all = SECMapper.standardize(df_all)
        # Sort by period to get latest
        sec_row = std_all.sort_values('period').iloc[-1]
        
        sec_ni = sec_row['NetIncome']
        sec_rev = sec_row['TotalRevenue']

        print('--- SEC BULK (Standardized) ---')
        print(f'Period: {sec_row["period"]}')
        print(f'NetIncome: {sec_ni}')
        print(f'Revenue:   {sec_rev}')

        # 2. Get EODHD Data
        with open('bronze/fundamentals/eodhd/json/ALL.US.json') as f:
            eod_data = json.load(f)

        target_date = str(sec_row['period']) # YYYYMMDD
        target_dash = f'{target_date[:4]}-{target_date[4:6]}-{target_date[6:]}'

        print('\n--- EODHD JSON ---')
        q_data = eod_data.get('Financials', {}).get('Income_Statement', {}).get('quarterly', {})
        
        match = q_data.get(target_dash)
        if not match:
             # Try finding ANY match in 2024 for visual check if exact date fails
             print(f'Date {target_dash} not found. Available keys: {list(q_data.keys())[:5]}')
        else:
            eod_ni = float(match.get('netIncome'))
            eod_rev = float(match.get('totalRevenue'))
            print(f'Period: {target_dash}')
            print(f'NetIncome: {eod_ni}')
            print(f'Revenue:   {eod_rev}')
            
            # DIFF
            if sec_ni is not None and eod_ni is not None:
                diff_ni = abs(sec_ni - eod_ni)
                print(f'\nDiff NetIncome: {diff_ni} ({(diff_ni/eod_ni)*100:.6f}%)')
                
            if sec_rev is not None and eod_rev is not None:
                 diff_rev = abs(sec_rev - eod_rev)
                 print(f'Diff Revenue:   {diff_rev} ({(diff_rev/eod_rev)*100:.6f}%)')

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
