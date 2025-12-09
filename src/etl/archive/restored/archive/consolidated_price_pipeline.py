"""
Consolidated Price Pipeline
===========================

Combines functionality from both price_pipeline.py (YFinance) and eodhd_price_pipeline.py
into a unified, flexible price data collection system.

Features:
- Multi-source support (EODHD primary, YFinance fallback)
- Threaded parallel processing for performance
- CIK-ticker mapping integration
- Configurable batch sizes and worker counts
- Comprehensive error handling and logging
"""

import os
import requests
import pandas as pd
import yfinance as yf
import time
from dotenv import load_dotenv
import concurrent.futures
from typing import Dict, List, Optional, Tuple
import logging

# Load Environment
load_dotenv()
EODHD_API_KEY = os.getenv("EODHD_API_KEY")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
MASTER_UNIVERSE_FILE = os.path.join(BASE_DIR, "bronze", "universe", "eodhd_master_universe.csv")
EODHD_DATA_DIR = os.path.join(BASE_DIR, "bronze", "market_data", "eodhd", "daily")
YFINANCE_DATA_DIR = os.path.join(BASE_DIR, "bronze", "market_data", "yfinance", "daily")

class PricePipeline:
    """Unified price data collection pipeline with multi-source support."""
    
    def __init__(self, primary_source: str = "eodhd", fallback_source: str = "yfinance"):
        self.primary_source = primary_source
        self.fallback_source = fallback_source
        self.eodhd_api_key = EODHD_API_KEY
        self.ticker_cik_map = {}
        self.stats = {
            'total_attempted': 0,
            'primary_success': 0,
            'fallback_success': 0,
            'complete_failures': 0,
            'start_time': None,
            'end_time': None
        }
    
    def setup_directories(self):
        """Ensure all required directories exist."""
        for directory in [EODHD_DATA_DIR, YFINANCE_DATA_DIR]:
            if not os.path.exists(directory):
                os.makedirs(directory)
                logger.info(f"Created directory: {directory}")
    
    def load_universe(self) -> Dict[str, str]:
        """
        Loads the Master Universe and creates ticker->CIK mapping.
        Handles both EODHD and YFinance ticker formats.
        """
        if not os.path.exists(MASTER_UNIVERSE_FILE):
            logger.error("Master universe not found. Run universe_pipeline.py first.")
            return {}
        
        df = pd.read_csv(MASTER_UNIVERSE_FILE)
        final_map = {}
        
        # Process EODHD format (Code column)
        for code, cik, type_ in zip(df['Code'], df['cik'], df['Type']):
            if pd.isna(code):
                continue
            
            # Filter for Common Stock only
            if str(type_).strip() != 'Common Stock':
                continue
            
            code_str = str(code).upper()
            cik_str = str(cik).strip()
            
            if cik_str and cik_str != 'NAN':
                final_map[code_str] = cik_str
            else:
                # Fallback to ticker itself
                final_map[code_str] = code_str
        
        # Create YFinance-compatible versions (replace dots with hyphens)
        yf_compatible = {}
        for ticker, cik in final_map.items():
            yf_ticker = ticker.replace('.', '-')
            yf_compatible[yf_ticker] = cik
        
        logger.info(f"Loaded {len(final_map)} tickers from Master Universe")
        return {'eodhd': final_map, 'yfinance': yf_compatible}
    
    def fetch_eodhd_ticker_history(self, ticker: str, cik: str) -> bool:
        """Fetch price history from EODHD API."""
        if not self.eodhd_api_key:
            return False
            
        url = f"https://eodhd.com/api/eod/{ticker}.US"
        params = {
            "api_token": self.eodhd_api_key,
            "fmt": "json",
            "period": "d"
        }
        
        try:
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
            
            if not data or not isinstance(data, list):
                return False
            
            df = pd.DataFrame(data)
            # Save to EODHD directory
            filepath = os.path.join(EODHD_DATA_DIR, f"{cik}.csv")
            df.to_csv(filepath, index=False)
            return True
            
        except Exception as e:
            logger.warning(f"EODHD failed for {ticker}: {e}")
            return False
    
    def fetch_yfinance_ticker_history(self, ticker: str, cik: str) -> bool:
        """Fetch price history from YFinance."""
        try:
            # YFinance uses ticker format with hyphens
            ticker_obj = yf.Ticker(ticker)
            hist = ticker_obj.history(period="max")
            
            if hist.empty:
                return False
            
            # Standardize and save
            hist = hist.reset_index()
            filepath = os.path.join(YFINANCE_DATA_DIR, f"{cik}.csv")
            hist.to_csv(filepath, index=False)
            return True
            
        except Exception as e:
            logger.warning(f"YFinance failed for {ticker}: {e}")
            return False
    
    def fetch_ticker_with_fallback(self, ticker: str, cik: str, source_map: str) -> bool:
        """Fetch data with automatic fallback to secondary source."""
        success = False
        
        # Try primary source first
        if self.primary_source == "eodhd":
            success = self.fetch_eodhd_ticker_history(ticker, cik)
        elif self.primary_source == "yfinance":
            success = self.fetch_yfinance_ticker_history(ticker, cik)
        
        # Try fallback if primary failed
        if not success and self.fallback_source:
            if self.fallback_source == "eodhd":
                success = self.fetch_eodhd_ticker_history(ticker, cik)
            elif self.fallback_source == "yfinance":
                success = self.fetch_yfinance_ticker_history(ticker, cik)
        
        if success:
            if source_map == "primary":
                self.stats['primary_success'] += 1
            else:
                self.stats['fallback_success'] += 1
        else:
            self.stats['complete_failures'] += 1
        
        return success
    
    def run_pipeline(self, max_tickers: Optional[int] = None, max_workers: int = 50):
        """Run the complete price data collection pipeline."""
        self.setup_directories()
        universe_data = self.load_universe()
        
        if not universe_data:
            logger.error("No universe data loaded. Aborting.")
            return False
        
        # Use appropriate ticker mapping based on primary source
        ticker_map = universe_data[self.primary_source]
        tickers_to_process = list(ticker_map.items())
        
        if max_tickers:
            tickers_to_process = tickers_to_process[:max_tickers]
            logger.info(f"Limiting to first {max_tickers} tickers")
        
        self.stats['total_attempted'] = len(tickers_to_process)
        self.stats['start_time'] = time.time()
        
        logger.info(f"Starting price pipeline for {len(tickers_to_process)} tickers "
                    f"using {max_workers} workers (Primary: {self.primary_source}, "
                    f"Fallback: {self.fallback_source})")
        
        success_count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(self.fetch_ticker_with_fallback, ticker, cik, "primary"):
                (ticker, cik) for ticker, cik in tickers_to_process
            }
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    success_count += 1
        
        self.stats['end_time'] = time.time()
        duration = self.stats['end_time'] - self.stats['start_time']
        
        logger.info(f"Pipeline completed in {duration:.1f} seconds")
        logger.info(f"Success: {success_count}/{len(tickers_to_process)} tickers")
        logger.info(f"Primary source success: {self.stats['primary_success']}")
        logger.info(f"Fallback source success: {self.stats['fallback_success']}")
        logger.info(f"Complete failures: {self.stats['complete_failures']}")
        
        return True
    
    def get_statistics(self) -> Dict:
        """Return pipeline execution statistics."""
        return self.stats

def main():
    """Command-line entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Consolidated Price Data Pipeline")
    parser.add_argument('--primary', choices=['eodhd', 'yfinance'], default='eodhd',
                        help="Primary data source")
    parser.add_argument('--fallback', choices=['eodhd', 'yfinance', 'none'], default='yfinance',
                        help="Fallback data source")
    parser.add_argument('--max-tickers', type=int, default=None,
                        help="Limit number of tickers to process")
    parser.add_argument('--workers', type=int, default=50,
                        help="Number of worker threads")
    
    args = parser.parse_args()
    
    # Handle 'none' fallback case
    fallback_source = args.fallback if args.fallback != 'none' else None
    
    pipeline = PricePipeline(
        primary_source=args.primary,
        fallback_source=fallback_source
    )
    
    success = pipeline.run_pipeline(
        max_tickers=args.max_tickers,
        max_workers=args.workers
    )
    
    if not success:
        logger.error("Pipeline failed")
        return 1
    
    logger.info("Pipeline completed successfully")
    return 0

if __name__ == "__main__":
    exit(main())