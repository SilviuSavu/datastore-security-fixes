#!/usr/bin/env python3
"""
Comprehensive Market Data Collection Pipeline Runner

This script runs the complete market data collection pipeline to fill gaps
for all SEC companies that are missing market data.

Usage:
    python3 run_comprehensive_market_pipeline.py [--batch-size SIZE] [--max-workers N] [--test-run]

Options:
    --batch-size SIZE    Number of CIKs to process in each batch (default: 50)
    --max-workers N     Number of parallel workers (default: 5)
    --test-run         Run with small sample for testing (default: False)
"""

import sys
import argparse
from datetime import datetime

# Add src to path
sys.path.append('src')

from etl.comprehensive_market_pipeline import ComprehensiveMarketPipeline
from etl.market_data_monitor import MarketDataMonitor

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Comprehensive Market Data Collection Pipeline')
    
    parser.add_argument('--batch-size', type=int, default=50,
                       help='Number of CIKs to process in each batch (default: 50)')
    parser.add_argument('--max-workers', type=int, default=5,
                       help='Number of parallel workers (default: 5)')
    parser.add_argument('--test-run', action='store_true',
                       help='Run with small sample for testing')
    parser.add_argument('--alpha-vantage-key', type=str,
                       help='Alpha Vantage API key (optional)')
    parser.add_argument('--polygon-key', type=str,
                       help='Polygon.io API key (optional)')
    
    return parser.parse_args()

def main():
    """Main pipeline runner"""
    args = parse_arguments()
    
    print("=" * 80)
    print("COMPREHENSIVE MARKET DATA COLLECTION PIPELINE")
    print("=" * 80)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Batch Size: {args.batch_size}")
    print(f"Max Workers: {args.max_workers}")
    print(f"Test Run: {args.test_run}")
    print()
    
    # Initialize pipeline
    pipeline = ComprehensiveMarketPipeline(
        alpha_vantage_key=args.alpha_vantage_key,
        polygon_key=args.polygon_key
    )
    
    # Get initial status
    print("Getting initial pipeline status...")
    status = pipeline.get_pipeline_status()
    
    print(f"Missing CIKs: {status['missing_cik_count']}")
    print(f"Mapper Stats: {status['mapper_stats']}")
    print(f"Collector Stats: {status['collector_stats']}")
    print()
    
    if status['missing_cik_count'] == 0:
        print("✅ No missing market data found. All SEC companies have market data!")
        return
    
    # Run pipeline
    try:
        if args.test_run:
            print("🧪 RUNNING TEST MODE (small sample)...")
            # Run with small sample for testing
            missing_ciks = pipeline.get_missing_ciks()
            test_ciks = list(missing_ciks)[:20]  # Small sample
            
            print(f"Testing with {len(test_ciks)} CIKs...")
            
            # Map to tickers
            ticker_mapping = pipeline.map_ciks_to_tickers(test_ciks)
            print(f"Mapped {len(ticker_mapping)} CIKs to tickers")
            
            if ticker_mapping:
                # Collect market data
                market_data = pipeline.collect_market_data_for_ciks(
                    ticker_mapping, batch_size=10, max_workers=2
                )
                print(f"Collected market data for {len(market_data)} companies")
                
                if market_data:
                    # Update silver layer
                    pipeline.update_silver_layer(market_data)
                    print("✅ Updated silver layer successfully")
                    
                    # Generate report
                    report = pipeline.generate_coverage_report(set(test_ciks), market_data)
                    print(f"Success Rate: {report['success_rate']:.1f}%")
        else:
            print("🚀 RUNNING FULL PIPELINE...")
            print(f"Processing {status['missing_cik_count']} missing CIKs...")
            print("This may take several hours depending on API limits.")
            print()
            
            # Run full pipeline
            pipeline.run_full_pipeline(
                batch_size=args.batch_size,
                max_workers=args.max_workers
            )
        
        print()
        print("=" * 80)
        print("PIPELINE COMPLETED")
        print("=" * 80)
        
        # Final monitoring report
        print("Generating final monitoring report...")
        monitor = MarketDataMonitor()
        final_report = monitor.generate_comprehensive_report()
        
        print(f"Final Coverage Rate: {final_report['coverage_analysis']['coverage_rate']:.1f}%")
        print(f"Total Companies with Market Data: {final_report['coverage_analysis']['total_market_coverage']}")
        print(f"Still Missing: {final_report['coverage_analysis']['missing_market_data']}")
        print(f"Data Quality Score: {final_report['data_quality']['quality_score']:.1f}%")
        
        if final_report['recommendations']:
            print()
            print("📋 RECOMMENDATIONS:")
            for i, rec in enumerate(final_report['recommendations'], 1):
                print(f"  {i}. {rec}")
        
        print()
        print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("📊 Detailed reports saved to logs/")
        
    except KeyboardInterrupt:
        print("\n⚠️  Pipeline interrupted by user")
        print("Progress has been saved. Run again to continue.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()