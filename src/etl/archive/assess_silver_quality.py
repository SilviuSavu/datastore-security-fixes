import pandas as pd
import os
from pathlib import Path
import logging
from typing import Dict, Set

# Configuration
SILVER_PATH = Path("/Users/savusilviu/Desktop/SilviuCorneliuSavu/DataStore/silver")
MARKET_DATA_PATH = SILVER_PATH / "market_data/eodhd"
FUNDAMENTALS_PATH = SILVER_PATH / "fundamentals/sec"
TICKERS_FILE = Path("/Users/savusilviu/Desktop/SilviuCorneliuSavu/DataStore/silver_tickers.txt")
CIK_MAP_FILE = Path("/Users/savusilviu/Desktop/SilviuCorneliuSavu/DataStore/bronze/universe/historical_ciks.csv")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_universe() -> Set[str]:
    """Loads target universe tickers."""
    if not TICKERS_FILE.exists():
        logger.error(f"Tickers file not found: {TICKERS_FILE}")
        return set()
    with open(TICKERS_FILE, 'r') as f:
        # Extract ticker from "TICKER.EXCHANGE" format
        tickers = {line.strip().split('.')[0] for line in f if line.strip()}
    logger.info(f"Loaded {len(tickers)} tickers from universe file.")
    return tickers

def load_cik_map() -> pd.DataFrame:
    """Loads Ticker -> CIK mapping"""
    if not CIK_MAP_FILE.exists():
        logger.error(f"CIK map not found: {CIK_MAP_FILE}")
        return pd.DataFrame()
    
    # Read CSV: ticker, cik, name
    # CIK should be string to preserve leading zeros
    df = pd.read_csv(CIK_MAP_FILE, dtype={'cik': str})
    # Normalize tickers
    df['ticker'] = df['ticker'].str.upper()
    return df

def check_market_data(universe_tickers: Set[str]) -> Dict:
    logger.info("Checking Market Data (by Ticker)...")
    market_tickers_found = set()
    years_found = []

    if not MARKET_DATA_PATH.exists():
        return {}

    for year_dir in sorted(MARKET_DATA_PATH.glob("year=*")):
        year = year_dir.name.split('=')[1]
        years_found.append(year)
        try:
            df = pd.read_parquet(year_dir, columns=['ticker'])
            found = set(df['ticker'].unique())
            market_tickers_found.update(found)
        except Exception:
            pass

    missing = universe_tickers - market_tickers_found
    
    return {
        "years": years_found,
        "found": len(market_tickers_found),
        "missing": len(missing),
        "missing_sample": list(missing)[:10]
    }

def check_fundamentals_data(universe_tickers: Set[str], cik_map_df: pd.DataFrame) -> Dict:
    logger.info("Checking Fundamentals Data (by CIK)...")
    
    # 1. Map Universe Tickers -> CIKs
    # Filter CIK map to only include universe tickers
    universe_map = cik_map_df[cik_map_df['ticker'].isin(universe_tickers)]
    universe_ciks = set(universe_map['cik'].unique())
    
    logger.info(f"Mapped {len(universe_tickers)} universe tickers to {len(universe_ciks)} unique CIKs.")
    
    # Identify tickers that failed to map to a CIK
    mapped_tickers = set(universe_map['ticker'])
    unmapped_tickers = universe_tickers - mapped_tickers

    # 2. Scan SEC data for these CIKs
    found_ciks = set()
    years_found = []

    if not FUNDAMENTALS_PATH.exists():
        return {}
        
    for year_dir in sorted(FUNDAMENTALS_PATH.glob("year=*")):
        year = year_dir.name.split('=')[1]
        years_found.append(year)
        try:
            # SEC data has 'cik' column
            df = pd.read_parquet(year_dir, columns=['cik'])
            # Ensure CIKs are strings padded to 10 chars to match map
            current_ciks = set(df['cik'].astype(str).str.zfill(10))
            found_ciks.update(current_ciks)
        except Exception:
            pass

    # 3. Calculate coverage based on CIKs
    missing_ciks = universe_ciks - found_ciks
    
    # Coverage logic:
    # A ticker is covered if its mapped CIK is found in the SEC data.
    covered_tickers = universe_map[universe_map['cik'].isin(found_ciks)]['ticker'].unique()
    covered_count = len(covered_tickers)
    
    return {
        "years": years_found,
        "universe_ciks_count": len(universe_ciks),
        "found_ciks_count": len(found_ciks.intersection(universe_ciks)),
        "missing_ciks_count": len(missing_ciks),
        "covered_tickers_count": covered_count,
        "missing_sample_ciks": list(missing_ciks)[:10],
        "unmapped_tickers_count": len(unmapped_tickers),
        "total_coverage_pct": (covered_count / len(universe_tickers) * 100) if universe_tickers else 0
    }

def main():
    universe = load_universe()
    cik_map = load_cik_map()
    
    market_stats = check_market_data(universe)
    fund_stats = check_fundamentals_data(universe, cik_map)
    
    print("\n" + "="*50)
    print("SILVER LAYER QUALITY ASSESSMENT (REVISED)")
    print("="*50)
    print(f"Target Universe Size: {len(universe)}")
    print("-" * 30)
    print("MARKET DATA (Ticker Match):")
    print(f"  Missing: {market_stats['missing']}")
    print("-" * 30)
    print("FUNDAMENTALS (SEC - CIK Match):")
    print(f"  Mapped Tickers to CIKs: {len(universe) - fund_stats['unmapped_tickers_count']} / {len(universe)}")
    print(f"  Unmapped Tickers: {fund_stats['unmapped_tickers_count']}")
    if fund_stats['unmapped_tickers_count'] > 0:
        print("    (These tickers have no CIK in historical_ciks.csv)")
    
    print(f"  Universe CIKs with Data: {fund_stats['found_ciks_count']} / {fund_stats['universe_ciks_count']}")
    print(f"  Missing CIKs: {fund_stats['missing_ciks_count']}")
    
    # Calculate effective coverage
    # Effective Coverage = (Covered Tickers + Unmapped but confirmed missing externally?)
    # For now, just raw data presence.
    print(f"  Final Ticker Coverage (Data Present): {fund_stats['total_coverage_pct']:.2f}%")
    
    if fund_stats['missing_ciks_count'] > 0:
        print(f"  Sample Missing CIKs: {fund_stats['missing_sample_ciks']}")

if __name__ == "__main__":
    main()
