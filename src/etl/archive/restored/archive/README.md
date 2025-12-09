# ETL Pipeline Refactoring Archive

This directory contains the original ETL pipeline files that have been refactored and integrated into the comprehensive `sec_data_pipeline.py` system.

## 🗃️ Archived Files

### 1. `fetch_missing_fundamentals.py`
- **Status**: Integrated into `DownloadModule` class
- **Functionality**: CIK universe creation and missing fundamentals download
- **Replacement**: `sec_data_pipeline.py` - `DownloadModule` class

### 2. `finra_pipeline.py`
- **Status**: Integrated into `FINRAModule` class
- **Functionality**: FINRA margin statistics, short interest, and Reg SHO data collection
- **Replacement**: `sec_data_pipeline.py` - `FINRAModule` class

### 3. `fred_pipeline.py`
- **Status**: Integrated into `FREDModule` class
- **Functionality**: 50+ macroeconomic indicators collection
- **Replacement**: `sec_data_pipeline.py` - `FREDModule` class

### 4. `global_macro_pipeline.py`
- **Status**: Integrated into `GlobalMacroModule` class
- **Functionality**: International economic data via DBnomics
- **Replacement**: `sec_data_pipeline.py` - `GlobalMacroModule` class

### 5. `eodhd_fundamentals_pipeline.py`
- **Status**: Partially integrated into `PriceDataModule` class
- **Functionality**: EODHD price data collection
- **Replacement**: `sec_data_pipeline.py` - `PriceDataModule` class

### 6. `multi_source_market_collector.py`
- **Status**: Partially integrated into `PriceDataModule` class
- **Functionality**: Multi-source market data collection
- **Replacement**: `sec_data_pipeline.py` - `PriceDataModule` class

### 7. `consolidated_price_pipeline.py`
- **Status**: Partially integrated into `PriceDataModule` class
- **Functionality**: Price data consolidation with CIK-ticker mapping
- **Replacement**: `sec_data_pipeline.py` - `PriceDataModule` class

## 🚀 Migration Guide

### Before (Multiple Files)
```bash
python3 src/etl/fetch_missing_fundamentals.py
python3 src/etl/finra_pipeline.py
python3 src/etl/fred_pipeline.py
python3 src/etl/global_macro_pipeline.py
python3 src/etl/eodhd_fundamentals_pipeline.py
python3 src/etl/consolidated_price_pipeline.py
```

### After (Unified System)
```bash
# Run comprehensive data collection
python3 src/etl/sec_data_pipeline.py comprehensive

# Run specific data sources
python3 src/etl/sec_data_pipeline.py finra
python3 src/etl/sec_data_pipeline.py fred
python3 src/etl/sec_data_pipeline.py global
python3 src/etl/sec_data_pipeline.py price

# Use enhanced options
python3 src/etl/sec_data_pipeline.py complete --batch-size 50 --force --verbose
python3 src/etl/sec_data_pipeline.py price --max-tickers 1000 --workers 25
```

## 📊 Benefits of Refactoring

1. **Unified Interface**: Single entry point for all data collection
2. **Enhanced CLI**: Comprehensive argument parsing with help system
3. **Performance Optimization**: Fixed critical O(n²) bottleneck
4. **Better Organization**: Modular design with clear separation of concerns
5. **Advanced Features**: Dry-run mode, verbose logging, force processing
6. **Backward Compatibility**: All original functionality preserved

## 🔧 Files to Keep

The following files remain in the main `src/etl/` directory as they serve different purposes:

- `assess_silver_quality.py` - Data quality assessment
- `build_historical_cik_map.py` - Historical CIK mapping
- `cik_ticker_mapper.py` - CIK to ticker mapping utility
- `market_data_monitor.py` - Market data monitoring
- `sec_data_pipeline.py` - **Main unified pipeline** (refactored)
- `sec_mapper.py` - SEC data mapping utilities
- `universe_pipeline.py` - Universe creation pipeline
- `standardization/` - Data standardization modules

## 📁 Archive Purpose

These files are kept for reference and historical purposes. They represent the evolution of the ETL system and may be useful for:

- Understanding the original implementation
- Reference for specific algorithms or approaches
- Historical debugging or comparison
- Documentation of the refactoring process

**Note**: These archived files should not be used in production. All functionality has been integrated into the new unified `sec_data_pipeline.py` system.