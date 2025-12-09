import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Dict, List, Optional, Set
import time
from datetime import datetime, timedelta
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from .cik_ticker_mapper import CIKTickerMapper
from .multi_source_market_collector import MultiSourceMarketCollector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ComprehensiveMarketPipeline:
    """
    Comprehensive pipeline to collect market data for all SEC companies:
    1. Identify missing market data
    2. Map CIKs to tickers
    3. Collect market data from multiple sources
    4. Save to bronze and update silver layer
    """
    
    def __init__(self, alpha_vantage_key: Optional[str] = None, polygon_key: Optional[str] = None):
        import os
        self.mapper = CIKTickerMapper()
        self.collector = MultiSourceMarketCollector(alpha_vantage_key, polygon_key)
        self.pipeline_log = Path('logs/comprehensive_market_pipeline.log')
        self.pipeline_log.parent.mkdir(exist_ok=True)
        
        # Setup logging
        file_handler = logging.FileHandler(self.pipeline_log)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)
    
    def get_missing_ciks(self) -> Set[str]:
        """Get CIKs that are missing market data"""
        logger.info("Identifying missing market data...")
        
        # Get SEC CIKs
        sec_path = Path('silver/fundamentals/sec')
        sec_ciks = set()
        
        for year_dir in sorted(sec_path.glob('year=*')):
            if not year_dir.is_dir():
                continue
                
            for parquet_file in year_dir.glob('*.parquet'):
                try:
                    df = pd.read_parquet(parquet_file, columns=['cik'])
                    file_ciks = set(df['cik'].astype(str).str.zfill(10))
                    sec_ciks.update(file_ciks)
                except Exception:
                    continue
        
        # Get existing EODHD CIKs
        eodhd_path = Path('bronze/market_data/eodhd/daily')
        csv_files = list(eodhd_path.glob('*.csv'))
        
        cik_files = [f for f in csv_files if f.name.replace('.csv', '').isdigit()]
        eodhd_ciks = set()
        for f in cik_files:
            cik = f.name.replace('.csv', '').lstrip('0') or '0'
            eodhd_ciks.add(cik.zfill(10))
        
        # Get existing multi-source CIKs
        multi_source_path = Path('bronze/market_data/multi_source/daily')
        multi_source_files = list(multi_source_path.glob('*.csv')) if multi_source_path.exists() else []
        multi_source_ciks = set()
        for f in multi_source_files:
            if f.name.replace('.csv', '').isdigit():
                cik = f.name.replace('.csv', '').lstrip('0') or '0'
                multi_source_ciks.add(cik.zfill(10))
        
        # Combine existing sources
        existing_ciks = eodhd_ciks.union(multi_source_ciks)
        missing_ciks = sec_ciks - existing_ciks
        
        logger.info(f"SEC CIKs: {len(sec_ciks)}")
        logger.info(f"EODHD CIKs: {len(eodhd_ciks)}")
        logger.info(f"Multi-source CIKs: {len(multi_source_ciks)}")
        logger.info(f"Missing CIKs: {len(missing_ciks)}")
        
        return missing_ciks
    
    def map_ciks_to_tickers(self, ciks: List[str]) -> Dict[str, str]:
        """Map CIKs to tickers"""
        logger.info(f"Mapping {len(ciks)} CIKs to tickers...")
        
        # Use the mapper's batch functionality
        ticker_mapping = self.mapper.batch_map_ciks(ciks)
        
        logger.info(f"Successfully mapped {len(ticker_mapping)}/{len(ciks)} CIKs to tickers")
        
        # Save mapping results
        mapping_file = Path('logs/cik_ticker_mapping_results.json')
        with open(mapping_file, 'w') as f:
            json.dump(ticker_mapping, f, indent=2)
        
        return ticker_mapping
    
    def collect_market_data_for_ciks(self, cik_ticker_mapping: Dict[str, str], 
                                  batch_size: int = 50, max_workers: int = 5) -> Dict[str, pd.DataFrame]:
        """Collect market data for mapped CIKs"""
        logger.info(f"Collecting market data for {len(cik_ticker_mapping)} companies...")
        
        tickers = list(cik_ticker_mapping.values())
        
        # Collect data in batches to avoid overwhelming APIs
        all_results = {}
        
        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(tickers)-1)//batch_size + 1}")
            
            try:
                batch_results = self.collector.batch_collect_data(batch_tickers, max_workers)
                
                # Map results back to CIKs
                for ticker, data in batch_results.items():
                    # Find the CIK for this ticker
                    cik = None
                    for c, t in cik_ticker_mapping.items():
                        if t == ticker:
                            cik = c
                            break
                    
                    if cik and data is not None:
                        all_results[cik] = data
                        # Save to bronze layer immediately
                        self.collector.save_to_bronze(ticker, data)
                
                # Rate limiting between batches
                if i + batch_size < len(tickers):
                    time.sleep(5)
                    
            except Exception as e:
                logger.error(f"Error processing batch {i//batch_size + 1}: {e}")
                continue
        
        logger.info(f"Successfully collected market data for {len(all_results)} companies")
        return all_results
    
    def update_silver_layer(self, market_data: Dict[str, pd.DataFrame]):
        """Update silver layer with new market data"""
        logger.info("Updating silver layer with new market data...")
        
        # Create silver directory structure
        silver_path = Path('silver/market_data/comprehensive')
        silver_path.mkdir(parents=True, exist_ok=True)
        
        # Process each company's data
        for cik, df in market_data.items():
            try:
                # Add CIK column
                df['cik'] = int(cik.lstrip('0'))
                
                # Extract year for partitioning
                df['year'] = pd.to_datetime(df['date']).dt.year
                
                # Save by year partitions (similar to existing structure)
                for year, year_data in df.groupby('year'):
                    year_dir = silver_path / f'year={year}'
                    year_dir.mkdir(exist_ok=True)
                    
                    # Remove year column before saving
                    year_data = year_data.drop('year', axis=1)
                    
                    # Save as parquet
                    output_file = year_dir / f'{cik}_{year}.parquet'
                    year_data.to_parquet(output_file, index=False)
                
                logger.info(f"Updated silver layer for CIK {cik}")
                
            except Exception as e:
                logger.error(f"Failed to update silver layer for CIK {cik}: {e}")
        
        logger.info("Silver layer update completed")
    
    def generate_coverage_report(self, original_missing: Set[str], 
                             successful_collections: Dict[str, pd.DataFrame]) -> Dict:
        """Generate coverage report"""
        successful_ciks = set(successful_collections.keys())
        still_missing = original_missing - successful_ciks
        
        report = {
            'pipeline_run': datetime.now().isoformat(),
            'originally_missing': len(original_missing),
            'successfully_collected': len(successful_ciks),
            'still_missing': len(still_missing),
            'success_rate': len(successful_ciks) / len(original_missing) * 100 if original_missing else 0,
            'successful_ciks': list(successful_ciks)[:50],  # First 50 for brevity
            'still_missing_ciks': list(still_missing)[:50]  # First 50 for brevity
        }
        
        # Save report
        report_file = Path('logs/market_data_coverage_report.json')
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Coverage report saved to {report_file}")
        return report
    
    def run_full_pipeline(self, batch_size: int = 50, max_workers: int = 5):
        """Run the complete market data collection pipeline"""
        logger.info("Starting comprehensive market data collection pipeline...")
        start_time = datetime.now()
        
        try:
            # Step 1: Identify missing CIKs
            missing_ciks = self.get_missing_ciks()
            
            if not missing_ciks:
                logger.info("No missing market data found. Pipeline complete.")
                return
            
            # Step 2: Map CIKs to tickers
            cik_ticker_mapping = self.map_ciks_to_tickers(list(missing_ciks))
            
            if not cik_ticker_mapping:
                logger.error("No CIKs could be mapped to tickers. Pipeline failed.")
                return
            
            # Step 3: Collect market data
            market_data = self.collect_market_data_for_ciks(
                cik_ticker_mapping, batch_size, max_workers
            )
            
            # Step 4: Update silver layer
            if market_data:
                self.update_silver_layer(market_data)
            
            # Step 5: Generate coverage report
            report = self.generate_coverage_report(missing_ciks, market_data)
            
            # Log summary
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info("=" * 60)
            logger.info("PIPELINE SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Duration: {duration}")
            logger.info(f"Originally missing: {report['originally_missing']}")
            logger.info(f"Successfully collected: {report['successfully_collected']}")
            logger.info(f"Still missing: {report['still_missing']}")
            logger.info(f"Success rate: {report['success_rate']:.1f}%")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
    
    def get_pipeline_status(self) -> Dict:
        """Get current pipeline status"""
        missing_ciks = self.get_missing_ciks()
        
        return {
            'missing_cik_count': len(missing_ciks),
            'mapper_stats': self.mapper.get_statistics(),
            'collector_stats': self.collector.get_collection_statistics(),
            'pipeline_log': str(self.pipeline_log)
        }

if __name__ == "__main__":
    # Run the pipeline
    pipeline = ComprehensiveMarketPipeline()
    
    print("Comprehensive Market Data Pipeline")
    print("=" * 50)
    print(pipeline.get_pipeline_status())
    
    # Uncomment to run the full pipeline
    # pipeline.run_full_pipeline(batch_size=20, max_workers=3)