import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Dict, List, Tuple
import json
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MarketDataMonitor:
    """
    Monitors and validates market data quality:
    1. Coverage analysis
    2. Data quality checks
    3. Completeness validation
    4. Performance monitoring
    """
    
    def __init__(self):
        self.report_dir = Path('logs/market_data_quality')
        self.report_dir.mkdir(parents=True, exist_ok=True)
        
    def get_comprehensive_coverage_report(self) -> Dict:
        """Generate comprehensive coverage report"""
        logger.info("Generating comprehensive coverage report...")
        
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
        
        # Get EODHD CIKs
        eodhd_path = Path('bronze/market_data/eodhd/daily')
        eodhd_ciks = set()
        if eodhd_path.exists():
            csv_files = list(eodhd_path.glob('*.csv'))
            cik_files = [f for f in csv_files if f.name.replace('.csv', '').isdigit()]
            for f in cik_files:
                cik = f.name.replace('.csv', '').lstrip('0') or '0'
                eodhd_ciks.add(cik.zfill(10))
        
        # Get Multi-source CIKs
        multi_source_path = Path('bronze/market_data/multi_source/daily')
        multi_source_ciks = set()
        if multi_source_path.exists():
            csv_files = list(multi_source_path.glob('*.csv'))
            for f in csv_files:
                if f.name.replace('.csv', '').isdigit():
                    cik = f.name.replace('.csv', '').lstrip('0') or '0'
                    multi_source_ciks.add(cik.zfill(10))
        
        # Get Silver Market Data CIKs
        silver_path = Path('silver/market_data')
        silver_ciks = set()
        if silver_path.exists():
            for source_dir in silver_path.iterdir():
                if source_dir.is_dir() and source_dir.name.startswith('year='):
                    for parquet_file in source_dir.glob('*.parquet'):
                        try:
                            df = pd.read_parquet(parquet_file, columns=['cik'])
                            cik = str(df['cik'].iloc[0]).zfill(10) if len(df) > 0 else None
                            if cik:
                                silver_ciks.add(cik)
                        except Exception:
                            continue
        
        # Calculate coverage metrics
        total_market_ciks = eodhd_ciks.union(multi_source_ciks).union(silver_ciks)
        coverage_rate = len(total_market_ciks) / len(sec_ciks) * 100 if sec_ciks else 0
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'sec_fundamentals_ciks': len(sec_ciks),
            'eodhd_market_ciks': len(eodhd_ciks),
            'multi_source_market_ciks': len(multi_source_ciks),
            'silver_market_ciks': len(silver_ciks),
            'total_market_coverage': len(total_market_ciks),
            'coverage_rate': coverage_rate,
            'missing_market_data': len(sec_ciks) - len(total_market_ciks),
            'coverage_breakdown': {
                'eodhd_coverage': len(eodhd_ciks),
                'multi_source_coverage': len(multi_source_ciks),
                'silver_coverage': len(silver_ciks)
            }
        }
        
        return report
    
    def validate_data_quality(self, sample_size: int = 100) -> Dict:
        """Validate data quality for sample of companies"""
        logger.info(f"Validating data quality for {sample_size} companies...")
        
        quality_results = {
            'total_companies_checked': 0,
            'data_quality_issues': {},
            'quality_score': 0,
            'issues_found': []
        }
        
        # Check silver market data
        silver_path = Path('silver/market_data/comprehensive')
        if not silver_path.exists():
            return quality_results
        
        companies_checked = 0
        total_issues = 0
        
        for year_dir in sorted(silver_path.glob('year=*'))[:10]:  # Limit to 10 years for performance
            for parquet_file in year_dir.glob('*.parquet')[:sample_size//10]:  # Sample files
                try:
                    df = pd.read_parquet(parquet_file)
                    companies_checked += 1
                    
                    issues = self._check_dataframe_quality(df, parquet_file.name)
                    if issues:
                        total_issues += len(issues)
                        quality_results['issues_found'].extend(issues)
                        
                except Exception as e:
                    logger.error(f"Error checking {parquet_file}: {e}")
                    continue
        
        quality_results['total_companies_checked'] = companies_checked
        quality_results['quality_score'] = max(0, 100 - (total_issues / companies_checked * 100)) if companies_checked > 0 else 0
        
        return quality_results
    
    def _check_dataframe_quality(self, df: pd.DataFrame, filename: str) -> List[Dict]:
        """Check quality of individual dataframe"""
        issues = []
        
        # Check for required columns
        required_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            issues.append({
                'file': filename,
                'type': 'missing_columns',
                'details': f"Missing columns: {missing_cols}",
                'severity': 'high'
            })
        
        # Check for null values in critical columns
        if not df.empty:
            for col in ['open', 'high', 'low', 'close']:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    issues.append({
                        'file': filename,
                        'type': 'null_values',
                        'details': f"Column {col} has {null_count} null values",
                        'severity': 'medium'
                    })
        
        # Check for negative prices
        price_cols = ['open', 'high', 'low', 'close']
        for col in price_cols:
            if col in df.columns:
                negative_count = (df[col] < 0).sum()
                if negative_count > 0:
                    issues.append({
                        'file': filename,
                        'type': 'negative_prices',
                        'details': f"Column {col} has {negative_count} negative values",
                        'severity': 'high'
                    })
        
        # Check for data continuity (gaps in dates)
        if 'date' in df.columns and len(df) > 1:
            df_sorted = df.sort_values('date')
            date_gaps = (df_sorted['date'].diff() > timedelta(days=10)).sum()
            if date_gaps > 0:
                issues.append({
                    'file': filename,
                    'type': 'date_gaps',
                    'details': f"Found {date_gaps} gaps larger than 10 days",
                    'severity': 'medium'
                })
        
        return issues
    
    def generate_performance_metrics(self) -> Dict:
        """Generate performance metrics for the pipeline"""
        logger.info("Generating performance metrics...")
        
        # Check log files for performance data
        log_file = Path('logs/comprehensive_market_pipeline.log')
        metrics = {
            'pipeline_runs': 0,
            'avg_processing_time': 0,
            'success_rate': 0,
            'error_rate': 0
        }
        
        if log_file.exists():
            try:
                with open(log_file, 'r') as f:
                    log_content = f.read()
                    
                # Simple parsing of log for metrics
                lines = log_content.split('\n')
                info_lines = [line for line in lines if 'INFO:' in line]
                error_lines = [line for line in lines if 'ERROR:' in line]
                
                metrics['pipeline_runs'] = len([line for line in info_lines if 'PIPELINE SUMMARY' in line])
                metrics['error_rate'] = len(error_lines) / len(lines) * 100 if lines else 0
                
            except Exception as e:
                logger.error(f"Error parsing log file: {e}")
        
        return metrics
    
    def create_coverage_visualization(self, report: Dict):
        """Create visualization of coverage data"""
        try:
            # Create coverage pie chart
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
            
            # Coverage breakdown
            coverage_data = [
                report['coverage_breakdown']['eodhd_coverage'],
                report['coverage_breakdown']['multi_source_coverage'],
                report['coverage_breakdown']['silver_coverage'],
                report['missing_market_data']
            ]
            coverage_labels = ['EODHD', 'Multi-Source', 'Silver', 'Missing']
            colors = ['#2ecc71', '#3498db', '#9b59b6', '#e74c3c']
            
            ax1.pie(coverage_data, labels=coverage_labels, colors=colors, autopct='%1.1f%%')
            ax1.set_title('Market Data Coverage Breakdown')
            
            # Coverage rate gauge
            coverage_rate = report['coverage_rate']
            ax2.bar(['Coverage Rate'], [coverage_rate], color=['#2ecc71'])
            ax2.set_ylim(0, 100)
            ax2.set_title(f'Overall Coverage: {coverage_rate:.1f}%')
            ax2.set_ylabel('Percentage')
            
            plt.tight_layout()
            
            # Save visualization
            viz_file = self.report_dir / 'coverage_visualization.png'
            plt.savefig(viz_file, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Coverage visualization saved to {viz_file}")
            
        except Exception as e:
            logger.error(f"Error creating visualization: {e}")
    
    def generate_comprehensive_report(self) -> Dict:
        """Generate comprehensive monitoring report"""
        logger.info("Generating comprehensive monitoring report...")
        
        # Get all reports
        coverage_report = self.get_comprehensive_coverage_report()
        quality_report = self.validate_data_quality()
        performance_report = self.generate_performance_metrics()
        
        # Combine into comprehensive report
        comprehensive_report = {
            'report_timestamp': datetime.now().isoformat(),
            'coverage_analysis': coverage_report,
            'data_quality': quality_report,
            'performance_metrics': performance_report,
            'recommendations': self._generate_recommendations(coverage_report, quality_report)
        }
        
        # Save report
        report_file = self.report_dir / f'market_data_monitoring_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(report_file, 'w') as f:
            json.dump(comprehensive_report, f, indent=2, default=str)
        
        # Create visualization
        self.create_coverage_visualization(coverage_report)
        
        logger.info(f"Comprehensive report saved to {report_file}")
        return comprehensive_report
    
    def _generate_recommendations(self, coverage_report: Dict, quality_report: Dict) -> List[str]:
        """Generate actionable recommendations"""
        recommendations = []
        
        # Coverage recommendations
        if coverage_report['coverage_rate'] < 90:
            recommendations.append(
                f"Coverage rate is {coverage_report['coverage_rate']:.1f}%. "
                "Consider expanding data sources or improving CIK-to-ticker mapping."
            )
        
        if coverage_report['missing_market_data'] > 1000:
            recommendations.append(
                f"{coverage_report['missing_market_data']} companies still lack market data. "
                "Prioritize high-volume companies for data collection."
            )
        
        # Quality recommendations
        if quality_report['quality_score'] < 95:
            recommendations.append(
                f"Data quality score is {quality_report['quality_score']:.1f}%. "
                "Review and clean data quality issues."
            )
        
        if quality_report['issues_found']:
            high_severity_issues = [issue for issue in quality_report['issues_found'] if issue.get('severity') == 'high']
            if high_severity_issues:
                recommendations.append(
                    f"Found {len(high_severity_issues)} high-severity data quality issues. "
                    "Immediate attention required."
                )
        
        return recommendations

if __name__ == "__main__":
    # Run monitoring
    monitor = MarketDataMonitor()
    
    print("Market Data Monitoring System")
    print("=" * 50)
    
    # Generate comprehensive report
    report = monitor.generate_comprehensive_report()
    
    # Print summary
    print("\nMonitoring Summary:")
    print(f"Coverage Rate: {report['coverage_analysis']['coverage_rate']:.1f}%")
    print(f"Companies with Market Data: {report['coverage_analysis']['total_market_coverage']}")
    print(f"Missing Market Data: {report['coverage_analysis']['missing_market_data']}")
    print(f"Data Quality Score: {report['data_quality']['quality_score']:.1f}%")
    print(f"Issues Found: {len(report['data_quality']['issues_found'])}")
    print(f"Recommendations: {len(report['recommendations'])}")
    
    print("\nTop Recommendations:")
    for i, rec in enumerate(report['recommendations'][:3], 1):
        print(f"{i}. {rec}")