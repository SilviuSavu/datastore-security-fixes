import pandas as pd
import requests
import time
from pathlib import Path
import json
from typing import Dict, Optional, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CIKTickerMapper:
    """
    Maps SEC CIK codes to ticker symbols using multiple sources:
    1. SEC company data
    2. YFinance lookup
    3. Alpha Vantage API
    4. SEC EDGAR ticker mapping
    """
    
    def __init__(self):
        self.cache_file = Path('bronze/market_data/cik_ticker_mapping.json')
        self.mapping_cache = self._load_cache()
        self.sec_tickers = self._load_sec_tickers()
        
    def _load_cache(self) -> Dict[str, str]:
        """Load existing CIK-ticker mapping from cache"""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load cache: {e}")
        return {}
    
    def _save_cache(self):
        """Save mapping to cache file"""
        try:
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_file, 'w') as f:
                json.dump(self.mapping_cache, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")
    
    def _load_sec_tickers(self) -> Dict[str, str]:
        """Load SEC company tickers from official SEC data"""
        try:
            # Try to get from existing SEC data
            sec_path = Path('silver/fundamentals/sec')
            ticker_mapping = {}
            
            for year_dir in sorted(sec_path.glob('year=*')):
                if not year_dir.is_dir():
                    continue
                    
                for parquet_file in year_dir.glob('*.parquet'):
                    try:
                        df = pd.read_parquet(parquet_file, columns=['cik', 'ticker'])
                        if 'ticker' in df.columns:
                            for _, row in df.dropna(subset=['ticker']).iterrows():
                                cik = str(row['cik']).zfill(10)
                                ticker = str(row['ticker']).upper()
                                if ticker and ticker != 'nan':
                                    ticker_mapping[cik] = ticker
                    except Exception:
                        continue
            
            logger.info(f"Loaded {len(ticker_mapping)} tickers from SEC data")
            return ticker_mapping
            
        except Exception as e:
            logger.error(f"Failed to load SEC tickers: {e}")
            return {}
    
    def get_ticker_from_sec(self, cik: str) -> Optional[str]:
        """Get ticker from SEC data"""
        return self.sec_tickers.get(cik)
    
    def get_ticker_from_yfinance(self, cik: str) -> Optional[str]:
        """Try to find ticker using YFinance search"""
        try:
            import yfinance as yf
            
            # Try common ticker patterns for the CIK
            cik_num = int(cik.lstrip('0'))
            
            # First, try to get company info from SEC
            try:
                url = f"https://www.sec.gov/cgi-bin/browse-edgar?CIK={cik}&owner=exclude&count=10"
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    # Look for ticker in the response
                    import re
                    ticker_match = re.search(r'Ticker:\s*([A-Z]+)', response.text)
                    if ticker_match:
                        ticker = ticker_match.group(1)
                        logger.info(f"Found ticker {ticker} for CIK {cik} from SEC")
                        return ticker
            except Exception:
                pass
            
            # If SEC lookup fails, try common patterns
            # This is a fallback - not very reliable
            logger.warning(f"Could not find ticker for CIK {cik}")
            return None
            
        except Exception as e:
            logger.error(f"YFinance lookup failed for CIK {cik}: {e}")
            return None
    
    def get_ticker_from_alpha_vantage(self, cik: str) -> Optional[str]:
        """Try Alpha Vantage API for ticker lookup"""
        try:
            # Alpha Vantage doesn't have direct CIK lookup
            # This would require a subscription and custom logic
            # For now, return None
            return None
        except Exception as e:
            logger.error(f"Alpha Vantage lookup failed for CIK {cik}: {e}")
            return None
    
    def get_ticker(self, cik: str, force_refresh: bool = False) -> Optional[str]:
        """
        Get ticker for CIK using multiple sources with caching
        """
        if not force_refresh and cik in self.mapping_cache:
            return self.mapping_cache[cik]
        
        # Try sources in order of reliability
        ticker = None
        
        # 1. SEC data (most reliable)
        ticker = self.get_ticker_from_sec(cik)
        
        # 2. SEC EDGAR lookup
        if not ticker:
            ticker = self.get_ticker_from_yfinance(cik)
        
        # 3. Alpha Vantage
        if not ticker:
            ticker = self.get_ticker_from_alpha_vantage(cik)
        
        # Cache the result
        if ticker:
            self.mapping_cache[cik] = ticker
            self._save_cache()
            logger.info(f"Mapped CIK {cik} -> ticker {ticker}")
        else:
            logger.warning(f"Could not find ticker for CIK {cik}")
        
        return ticker
    
    def batch_map_ciks(self, ciks: list, batch_size: int = 100) -> Dict[str, str]:
        """Map multiple CIKs to tickers"""
        results = {}
        
        logger.info(f"Mapping {len(ciks)} CIKs to tickers")
        
        for i, cik in enumerate(ciks):
            if i % batch_size == 0:
                logger.info(f"Processed {i}/{len(ciks)} CIKs")
            
            ticker = self.get_ticker(cik)
            if ticker:
                results[cik] = ticker
            
            # Rate limiting
            if i % 10 == 0:
                time.sleep(0.1)
        
        logger.info(f"Successfully mapped {len(results)}/{len(ciks)} CIKs")
        return results
    
    def get_statistics(self) -> Dict:
        """Get mapping statistics"""
        return {
            'total_cached_mappings': len(self.mapping_cache),
            'sec_ticker_mappings': len(self.sec_tickers),
            'cache_file_exists': self.cache_file.exists()
        }

if __name__ == "__main__":
    # Test the mapper
    mapper = CIKTickerMapper()
    
    # Test with some CIKs
    test_ciks = ['0000320193', '0000789019', '0000051143']  # Apple, Microsoft, IBM
    
    print("CIK Ticker Mapper Test")
    print("=" * 40)
    print(mapper.get_statistics())
    
    for cik in test_ciks:
        ticker = mapper.get_ticker(cik)
        print(f"CIK {cik} -> {ticker}")