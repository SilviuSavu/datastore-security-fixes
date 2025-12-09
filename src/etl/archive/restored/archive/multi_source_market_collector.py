import pandas as pd
import yfinance as yf
import requests
import time
from pathlib import Path
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MultiSourceMarketCollector:
    """
    Collects market data from multiple sources:
    1. YFinance (primary free source)
    2. Alpha Vantage (backup)
    3. Polygon.io (premium backup)
    4. EODHD (existing source)
    """
    
    def __init__(self, alpha_vantage_key: Optional[str] = None, polygon_key: Optional[str] = None):
        import os
        self.alpha_vantage_key = alpha_vantage_key or os.getenv('ALPHA_VANTAGE_API_KEY')
        self.polygon_key = polygon_key or os.getenv('POLYGON_API_KEY')
        self.eodhd_api_key = os.getenv('EODHD_API_KEY')
        self.databento_api_key = os.getenv('DATABENTO_API_KEY')
        self.cache_dir = Path('bronze/market_data/cache')
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.rate_limiter = threading.Lock()
        
    def get_yfinance_data(self, ticker: str, period: str = "max") -> Optional[pd.DataFrame]:
        """Get market data from YFinance"""
        try:
            ticker_obj = yf.Ticker(ticker.upper())
            
            # Get historical data
            hist = ticker_obj.history(period=period)
            
            if hist.empty:
                logger.warning(f"No data found for ticker {ticker}")
                return None
            
            # Standardize column names
            hist = hist.reset_index()
            hist.columns = [col.lower().replace(' ', '_') for col in hist.columns]
            
            # Ensure required columns
            required_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
            if 'adj_close' in hist.columns:
                hist['adjusted_close'] = hist['adj_close']
            elif 'close' in hist.columns:
                hist['adjusted_close'] = hist['close']
            
            # Rename columns to match EODHD format
            column_mapping = {
                'date': 'date',
                'open': 'open',
                'high': 'high', 
                'low': 'low',
                'close': 'close',
                'adjusted_close': 'adjusted_close',
                'volume': 'volume'
            }
            
            hist = hist[[col for col in column_mapping.keys() if col in hist.columns]]
            hist = hist.rename(columns=column_mapping)
            
            logger.info(f"Retrieved {len(hist)} records for {ticker} from YFinance")
            return hist
            
        except Exception as e:
            logger.error(f"YFinance failed for {ticker}: {e}")
            return None
    
    def get_alpha_vantage_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Get market data from Alpha Vantage"""
        if not self.alpha_vantage_key:
            return None
            
        try:
            # Alpha Vantage has rate limits (5 calls/minute for free tier)
            with self.rate_limiter:
                time.sleep(12)  # Respect rate limits
                
            url = f"https://www.alphavantage.co/query"
            params = {
                'function': 'TIME_SERIES_DAILY_ADJUSTED',
                'symbol': ticker,
                'outputsize': 'full',
                'apikey': self.alpha_vantage_key
            }
            
            response = requests.get(url, params=params, timeout=30)
            if response.status_code != 200:
                logger.error(f"Alpha Vantage API error: {response.status_code}")
                return None
            
            data = response.json()
            
            if 'Time Series (Daily)' not in data:
                logger.warning(f"No data found for {ticker} in Alpha Vantage")
                return None
            
            # Convert to DataFrame
            time_series = data['Time Series (Daily)']
            records = []
            
            for date_str, values in time_series.items():
                records.append({
                    'date': date_str,
                    'open': float(values['1. open']),
                    'high': float(values['2. high']),
                    'low': float(values['3. low']),
                    'close': float(values['4. close']),
                    'adjusted_close': float(values['5. adjusted close']),
                    'volume': int(values['6. volume'])
                })
            
            df = pd.DataFrame(records)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            
            logger.info(f"Retrieved {len(df)} records for {ticker} from Alpha Vantage")
            return df
            
        except Exception as e:
            logger.error(f"Alpha Vantage failed for {ticker}: {e}")
            return None
    
    def get_polygon_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Get market data from Polygon.io"""
        if not self.polygon_key:
            return None
            
        try:
            # Polygon.io requires paid subscription
            # This is a placeholder implementation
            logger.warning("Polygon.io integration not implemented - requires paid subscription")
            return None
            
        except Exception as e:
            logger.error(f"Polygon.io failed for {ticker}: {e}")
            return None
    
    def get_cached_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Check if we have cached data for this ticker"""
        cache_file = self.cache_dir / f"{ticker.upper()}_cache.parquet"
        
        if cache_file.exists():
            try:
                df = pd.read_parquet(cache_file)
                # Check if data is recent (less than 1 day old)
                latest_date = pd.to_datetime(df['date']).max()
                if latest_date >= datetime.now() - timedelta(days=1):
                    logger.info(f"Using cached data for {ticker}")
                    return df
            except Exception as e:
                logger.warning(f"Failed to read cache for {ticker}: {e}")
        
        return None
    
    def cache_data(self, ticker: str, df: pd.DataFrame):
        """Cache market data for future use"""
        try:
            cache_file = self.cache_dir / f"{ticker.upper()}_cache.parquet"
            df.to_parquet(cache_file, index=False)
            logger.info(f"Cached data for {ticker}")
        except Exception as e:
            logger.error(f"Failed to cache data for {ticker}: {e}")
    
    def get_market_data(self, ticker: str, use_cache: bool = True) -> Optional[pd.DataFrame]:
        """
        Get market data from multiple sources with fallback
        """
        ticker = ticker.upper()
        
        # Check cache first
        if use_cache:
            cached_data = self.get_cached_data(ticker)
            if cached_data is not None:
                return cached_data
        
        # Try sources in order
        data = None
        
        # 1. YFinance (primary)
        data = self.get_yfinance_data(ticker)
        
        # 2. Alpha Vantage (backup)
        if data is None:
            data = self.get_alpha_vantage_data(ticker)
        
        # 3. Polygon.io (premium backup)
        if data is None:
            data = self.get_polygon_data(ticker)
        
        # Cache successful results
        if data is not None:
            self.cache_data(ticker, data)
        else:
            logger.error(f"Failed to get market data for {ticker} from all sources")
        
        return data
    
    def batch_collect_data(self, tickers: List[str], max_workers: int = 5) -> Dict[str, pd.DataFrame]:
        """
        Collect data for multiple tickers in parallel
        """
        results = {}
        
        logger.info(f"Collecting market data for {len(tickers)} tickers using {max_workers} workers")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_ticker = {
                executor.submit(self.get_market_data, ticker): ticker 
                for ticker in tickers
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    data = future.result()
                    if data is not None:
                        results[ticker] = data
                        logger.info(f"Successfully collected data for {ticker}")
                    else:
                        logger.warning(f"No data available for {ticker}")
                except Exception as e:
                    logger.error(f"Error collecting data for {ticker}: {e}")
        
        logger.info(f"Successfully collected data for {len(results)}/{len(tickers)} tickers")
        return results
    
    def save_to_bronze(self, ticker: str, df: pd.DataFrame, source: str = "multi_source"):
        """Save market data to bronze layer"""
        try:
            # Save as CSV to match existing EODHD structure
            bronze_dir = Path('bronze/market_data/multi_source/daily')
            bronze_dir.mkdir(parents=True, exist_ok=True)
            
            csv_file = bronze_dir / f"{ticker}.csv"
            df.to_csv(csv_file, index=False)
            
            logger.info(f"Saved {len(df)} records for {ticker} to {csv_file}")
            
        except Exception as e:
            logger.error(f"Failed to save data for {ticker}: {e}")
    
    def get_collection_statistics(self) -> Dict:
        """Get statistics about the collection process"""
        cache_files = list(self.cache_dir.glob("*.parquet"))
        bronze_files = list(Path('bronze/market_data/multi_source/daily').glob("*.csv"))
        
        return {
            'cached_tickers': len(cache_files),
            'bronze_files': len(bronze_files),
            'cache_directory': str(self.cache_dir),
            'bronze_directory': str(Path('bronze/market_data/multi_source/daily'))
        }

if __name__ == "__main__":
    # Test the collector
    collector = MultiSourceMarketCollector()
    
    # Test with some known tickers
    test_tickers = ['AAPL', 'MSFT', 'GOOGL']
    
    print("Multi-Source Market Collector Test")
    print("=" * 50)
    print(collector.get_collection_statistics())
    
    for ticker in test_tickers:
        data = collector.get_market_data(ticker)
        if data is not None:
            print(f"\n{ticker}: {len(data)} records")
            print(f"Date range: {data['date'].min()} to {data['date'].max()}")
            print(f"Sample data:")
            print(data.head(2))
        else:
            print(f"\n{ticker}: No data available")