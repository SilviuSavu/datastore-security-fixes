#!/usr/bin/env python3
"""
Unified SEC Data Pipeline
=========================

A comprehensive refactored solution that combines the functionality of:
- fetch_missing_fundamentals.py (CIK universe creation)
- sec_bulk_pipeline.py (SEC bulk processing)

This unified pipeline provides:
1. SEC bulk data processing (ZIP files)
2. Missing CIK fundamentals download
3. Master CIK universe creation
4. Equity filtering and foreign detection
5. Comprehensive error handling and logging

Technical Debt Addressed:
- Centralized configuration system for paths and settings
- Standardized CIK data type handling
- Refactored complex logic into focused helper functions
- Removed dead code and improved documentation

Key Features:
- Internal configuration system with centralized path management
- CIK standardization functions for consistent 10-digit zero-padded string format
- Financial data processing helpers for complex logic refactoring
- Comprehensive error handling and logging throughout
- Memory-efficient processing with chunk-based data handling
- Type safety with comprehensive type hints and validation
"""

import os
import sys
import zipfile
import polars as pl
import pandas as pd
from pathlib import Path
from typing import Set, List, Dict, Optional, Tuple, Any
from edgar import Company, set_identity
from deltalake import write_deltalake
from tqdm import tqdm
import time
import json
import logging
import glob
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Configure identity for SEC rate limiting
set_identity('DataStore silviu.savu@example.com')

# Additional imports for new modules
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import dbnomics
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables
load_dotenv()

# ============================================================================
# INTERNAL CONFIGURATION SYSTEM
# ============================================================================
class _PipelineConfig:
    """
    Internal configuration manager for bronze data pipeline.

    Centralizes all hardcoded paths, magic strings, and settings to address
    technical debt issue #1: Hardcoded paths and magic strings.
    """

    def __init__(self):
        # Calculate project root: src/etl/bronze_data_pipeline.py -> DataStore/
        self.PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        sys.path.insert(0, self.PROJECT_ROOT)

        # ===== PATH CONFIGURATION =====
        # Silver layer paths
        self.SILVER_MARKET = os.path.join(self.PROJECT_ROOT, "silver", "market_data", "eodhd")
        self.SILVER_FUNDS = os.path.join(self.PROJECT_ROOT, "silver", "fundamentals", "sec")

        # Bronze layer paths
        self.SEC_BULK_DIR = os.path.join(self.PROJECT_ROOT, "bronze", "landing", "sec_raw")
        self.RAW_OUTPUT_DIR = os.path.join(self.PROJECT_ROOT, "bronze", "delta", "sec_bulk")
        self.STD_OUTPUT_DIR = os.path.join(self.PROJECT_ROOT, "silver", "fundamentals", "sec_batch")
        self.EDGARTOOLS_DIR = os.path.join(self.PROJECT_ROOT, "bronze", "fundamentals", "edgartools")

        # Reference files
        self.MASTER_CIK_LIST = os.path.join(self.PROJECT_ROOT, "references", "master_cik_list.parquet")
        self.EODHD_UNIVERSE = os.path.join(self.PROJECT_ROOT, "references", "eodhd_master_universe.parquet")
        self.CIK_MAP_JSON = os.path.join(self.PROJECT_ROOT, "bronze", "landing", "market_raw", "cik_ticker_mapping.json")

        # ===== CIK CONFIGURATION =====
        # Standardize CIK handling to address technical debt issue #2
        self.CIK_FORMAT = "string"  # Standard format: 10-digit zero-padded string
        self.DEFAULT_CIK_LENGTH = 10
        self.CIK_PADDING_CHAR = "0"

        # ===== PROCESSING CONFIGURATION =====
        self.DEFAULT_BATCH_SIZE = 100
        self.MAX_WORKERS = 50
        self.MIN_FISCAL_YEAR = 2009  # XBRL data availability starts here
        self.DEFAULT_LIMIT = 10_000_000  # Default market cap threshold

        # ===== LOGGING CONFIGURATION =====
        self.LOGGING_LEVEL = logging.INFO
        self.LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        self.LOG_FILE = os.path.join(self.PROJECT_ROOT, "logs", "bronze_pipeline.log")

        # ===== FILE FORMAT CONSTANTS =====
        self.FILE_FORMAT_PARQUET = "parquet"
        self.FILE_FORMAT_CSV = "csv"
        self.FILE_FORMAT_DELTA = "delta"

        # ===== SEC FORM TYPES =====
        self.DOMESTIC_FORMS = {'10-K', '10-Q'}
        self.FOREIGN_FORMS = {'20-F', '40-F', '6-K'}
        self.ALL_VALID_FORMS = self.DOMESTIC_FORMS.union(self.FOREIGN_FORMS)

        # Initialize logging
        self._setup_logging()

        # Ensure output directories exist
        self._ensure_directories()

    def _setup_logging(self):
        """Configure logging system with file and console handlers"""
        # Ensure logs directory exists
        os.makedirs(os.path.dirname(self.LOG_FILE), exist_ok=True)

        # Remove basicConfig to avoid duplicate logging
        # The SECLogger class will handle all logging configuration
        # logging.basicConfig(
        #     level=self.LOGGING_LEVEL,
        #     format=self.LOG_FORMAT,
        #     handlers=[
        #         logging.StreamHandler(),
        #         logging.FileHandler(self.LOG_FILE)
        #     ]
        # )

    def _ensure_directories(self):
        """Ensure all required directories exist"""
        directories = [
            self.RAW_OUTPUT_DIR,
            self.STD_OUTPUT_DIR,
            self.EDGARTOOLS_DIR,
            self.SILVER_MARKET,
            self.SILVER_FUNDS
        ]

        for directory in directories:
            os.makedirs(directory, exist_ok=True)

    def validate_paths(self) -> bool:
        """Validate that critical paths exist and are accessible"""
        critical_paths = [
            self.PROJECT_ROOT,
            self.SEC_BULK_DIR,
            self.RAW_OUTPUT_DIR
        ]

        missing_paths = []
        for path in critical_paths:
            if not os.path.exists(path):
                missing_paths.append(path)

        if missing_paths:
            logging.warning(f"Missing critical paths: {missing_paths}")
            return False

        return True

    def get_financial_years_range(self) -> range:
        """Get range of financial years for processing based on configuration"""
        current_year = datetime.now().year
        return range(self.MIN_FISCAL_YEAR, current_year + 1)

# Initialize configuration
config = _PipelineConfig()

# ======================
# 1. CUSTOM EXCEPTIONS
# ======================
class SECDataError(Exception):
    """Base exception for SEC data processing"""
    pass

class DownloadError(SECDataError):
    """Download-related errors"""
    pass

class ProcessingError(SECDataError):
    """Data processing errors"""
    pass

class ValidationError(SECDataError):
    """Data validation errors"""
    pass

class CIKValidationError(ValidationError):
    """CIK-specific validation errors"""
    pass

# ======================
# 2. CIK STANDARDIZATION FUNCTIONS
# ======================
def _normalize_cik(cik: str | int | float | None) -> str:
    """
    Standardize CIK to 10-digit zero-padded string format.

    Addresses technical debt issue #2: Inconsistent CIK data type handling.

    Args:
        cik: CIK value (can be string, int, float, or have leading/trailing whitespace)

    Returns:
        Standardized 10-digit zero-padded string CIK

    Raises:
        CIKValidationError: If CIK cannot be normalized
    """
    try:
        # Handle None or empty values
        if cik is None:
            raise CIKValidationError("CIK cannot be None")

        # Convert to string and strip whitespace
        cik_str = str(cik).strip()

        # Handle empty string after stripping
        if not cik_str:
            raise CIKValidationError("CIK cannot be empty")

        # Remove any non-digit characters
        cik_digits = ''.join(c for c in cik_str if c.isdigit())

        # Validate we have exactly 10 digits after processing
        if len(cik_digits) != 10:
            raise CIKValidationError(f"CIK must be exactly 10 digits after normalization, got {len(cik_digits)}: {cik}")

        return cik_digits

    except (ValueError, AttributeError) as e:
        raise CIKValidationError(f"Invalid CIK format: {cik} - {str(e)}")

def _validate_cik_dataframe(df: pd.DataFrame, cik_column: str = 'cik') -> bool:
    """
    Validate that all CIKs in a DataFrame are properly formatted.

    Args:
        df: DataFrame containing CIK data
        cik_column: Name of column containing CIK values

    Returns:
        True if all CIKs are valid, False otherwise
    """
    if cik_column not in df.columns:
        logger.log_error("validate_cik_dataframe", f"CIK column '{cik_column}' not found in DataFrame")
        return False

    invalid_ciks = []
    for cik in df[cik_column].unique():
        try:
            _normalize_cik(cik)
        except CIKValidationError as e:
            invalid_ciks.append(f"{cik} ({str(e)})")

    if invalid_ciks:
        logger.log_error("validate_cik_dataframe", f"Invalid CIKs found: {', '.join(invalid_ciks[:10])}")
        return False

    return True

def _batch_cik_normalization(df: pd.DataFrame, cik_column: str = 'cik') -> pd.DataFrame:
    """
    Apply CIK normalization to an entire DataFrame column.

    Args:
        df: DataFrame containing CIK data
        cik_column: Name of column containing CIK values

    Returns:
        DataFrame with normalized CIK values

    Raises:
        CIKValidationError: If any CIK cannot be normalized
    """
    # Create a copy to avoid modifying original
    result_df = df.copy()

    # Apply normalization to the CIK column
    try:
        result_df[cik_column] = result_df[cik_column].apply(_normalize_cik)
        return result_df
    except Exception as e:
        raise CIKValidationError(f"Batch CIK normalization failed: {str(e)}")

# ======================
# 3. HELPER FUNCTIONS FOR COMPLEX LOGIC
# ======================
def _process_financial_data_chunk(df_chunk: pd.DataFrame, config: _PipelineConfig) -> pd.DataFrame:
    """
    Process a chunk of financial data with standardized transformations.

    Addresses technical debt issue #3: Complex nested logic by breaking down
    financial data processing into focused helper functions.

    Args:
        df_chunk: DataFrame chunk containing raw financial data
        config: Pipeline configuration object

    Returns:
        Processed DataFrame with standardized financial metrics
    """
    try:
        # Apply CIK normalization to ensure consistency
        if 'cik' in df_chunk.columns:
            df_chunk = _batch_cik_normalization(df_chunk, 'cik')

        # Standardize date formats
        date_columns = ['period_start', 'period_end', 'filing_date']
        for col in date_columns:
            if col in df_chunk.columns:
                df_chunk[col] = pd.to_datetime(df_chunk[col], errors='coerce')

        # Calculate derived financial metrics
        if 'value' in df_chunk.columns and 'unit' in df_chunk.columns:
            df_chunk['standardized_value'] = df_chunk.apply(
                lambda row: _standardize_financial_value(row['value'], row['unit']),
                axis=1
            )

        return df_chunk

    except Exception as e:
        logger.log_error("_process_financial_data_chunk", f"Financial data processing failed: {str(e)}")
        raise ProcessingError(f"Financial data processing failed: {str(e)}")

def _standardize_financial_value(value: Any, unit: str) -> float:
    """
    Standardize financial values with unit conversion.

    Args:
        value: Raw financial value
        unit: Unit of measurement

    Returns:
        Standardized float value
    """
    try:
        # Handle None or missing values
        if pd.isna(value) or value is None:
            return 0.0

        # Convert to float
        numeric_value = float(value)

        # Apply unit conversion if needed
        unit_multipliers = {
            'USD': 1.0,
            'shares': 1.0,
            'pure': 1.0,
            'USD/share': 1.0,
            'percent': 0.01  # Convert percentage to decimal
        }

        multiplier = unit_multipliers.get(unit.lower(), 1.0)
        return numeric_value * multiplier

    except (ValueError, TypeError) as e:
        logger.log_warning("_standardize_financial_value", f"Value standardization failed: {value} {unit} - {str(e)}")
        return 0.0

def _validate_financial_dataframe(df: pd.DataFrame) -> bool:
    """
    Validate financial DataFrame structure and content.

    Args:
        df: DataFrame to validate

    Returns:
        True if DataFrame is valid, False otherwise
    """
    required_columns = ['cik', 'concept', 'value', 'period_end']
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        logger.log_error("_validate_financial_dataframe", f"Missing required columns: {missing_columns}")
        return False

    # Validate CIK format
    if not _validate_cik_dataframe(df):
        return False

    return True

# ======================
# 4. LOGGING SYSTEM
# ======================
class SECLogger:
    """Comprehensive logging system for SEC data pipeline"""

    def __init__(self, name: str = "sec_pipeline"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Create console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)

        # Create file handler
        log_file = os.path.join(config.PROJECT_ROOT, "logs", "sec_pipeline.log")
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)

        # Add handlers
        self.logger.addHandler(ch)
        self.logger.addHandler(fh)

    def log_download(self, cik: str, success: bool, error: Optional[str] = None):
        """Log download operations"""
        if success:
            self.logger.info(f"✅ Download successful: CIK {cik}")
        else:
            self.logger.error(f"❌ Download failed: CIK {cik} - {error}")

    def log_processing(self, file: str, records: int, duration: float):
        """Log processing operations"""
        self.logger.info(f"📊 Processed {file}: {records} records in {duration:.2f}s")

    def log_error(self, operation: str, error: str):
        """Log general errors"""
        self.logger.error(f"🚨 {operation} error: {error}")

    def log_info(self, message: str):
        """Log informational messages"""
        self.logger.info(message)
    
    def log_warning(self, operation: str, message: str):
        """Log warning messages"""
        self.logger.warning(f"⚠️ {operation}: {message}")

# Initialize logger
logger = SECLogger()

# ======================
# 3. CORE UTILITIES
# ======================
class CIKUtils:
    """Utility functions for CIK processing"""

    @staticmethod
    def normalize_cik(cik: str) -> str:
        """Ensure 10-digit zero-padded CIK format"""
        return str(int(cik)).zfill(10)

    @staticmethod
    def extract_metadata_from_parquet(file_path: str) -> List[Dict]:
        """Extract CIKs, tickers, and SIC codes from parquet file"""
        try:
            # Check available columns first
            # We want 'cik', 'ticker', 'sic' if available
            columns_needed = ['cik']
            
            # Use pyarrow to peek at schema if needed, or just try reading with optional columns
            # Polars scan might be better for schema check, but pandas read_parquet is used elsewhere
            # Let's read and see what we have
            df = pd.read_parquet(file_path)
            
            records = []
            
            has_ticker = 'ticker' in df.columns
            has_sic = 'sic' in df.columns
            
            # Select relevant columns + unique CIKs (or keeping best row per CIK?)
            # Usually one filing per CIK per quarter, but can be multiple. 
            # We'll take unique CIKs and their associated metadata. 
            # If multiple tickers/SICs for one CIK in a file, we take the last one or mode? 
            # Simplest: take the one from the most recent filing or just drop duplicates keeping last.
            
            cols = ['cik']
            if has_ticker: cols.append('ticker')
            if has_sic: cols.append('sic')
            
            # Drop duplicates by CIK to get unique metadata per file
            unique_df = df[cols].drop_duplicates(subset=['cik'], keep='last')
            
            for _, row in unique_df.iterrows():
                record = {'cik': CIKUtils.normalize_cik(row['cik'])}
                if has_ticker and pd.notna(row['ticker']):
                    record['ticker'] = str(row['ticker']).upper()
                if has_sic and pd.notna(row['sic']):
                    try:
                        record['sic'] = int(row['sic'])
                    except (ValueError, TypeError) as e:
                        logger.log_warning("SIC parsing", f"SIC parsing failed for CIK {row['cik']}: {str(e)}")
                        record['sic'] = None
                records.append(record)
                
            return records
            
        except Exception as e:
            logger.log_error(f"extract_metadata_from_parquet({file_path})", str(e))
            return []

    @staticmethod
    def get_file_list(directory: str, pattern: str = "*.parquet") -> List[str]:
        """Get list of files matching pattern"""
        return sorted(glob.glob(os.path.join(directory, pattern)))

# ======================
# 4. DOWNLOAD MODULE
# ======================
class DownloadModule:
    """Handles all SEC data download operations"""

    def __init__(self):
        self.config = config

    def get_missing_ciks(self) -> List[str]:
        """Find CIKs in Master Universe (XBRL Filers) but not in fundamentals"""
        try:
            target_ciks = set()

            # 1. Master Universe (XBRL Filers 2009+)
            # The user explicitly wants to train only on data available since XBRL.
            # This file is generated from the SEC Bulk ZIPs which contain XBRL data.
            universe_path = self.config.MASTER_CIK_LIST

            # Check if the expected file exists
            if not os.path.exists(universe_path):
                logger.log_error("get_missing_ciks", f"Master Universe not found at expected path: {self.config.MASTER_CIK_LIST}")
                logger.log_error("get_missing_ciks", "Please ensure master_cik_list.parquet exists in the references directory.")
                return []

            try:
                universe_df = pd.read_parquet(universe_path)
                master_ciks = set(universe_df['cik'].astype(str).str.zfill(10))
                target_ciks.update(master_ciks)
                logger.log_info(f"📊 Loaded {len(master_ciks)} CIKs from Master Universe (XBRL Filers)")
            except Exception as e:
                logger.log_error("get_missing_ciks", f"Failed to read Master Universe: {e}")
                return []

            logger.log_info(f"🎯 Target CIK Universe (XBRL Only): {len(target_ciks)}")

            # 2. Existing Fundamentals (Check Bronze Delta - what we just processed from Zips)
            # We should only download from API if we didn't find it in the Bulk Zips
            fund_ciks = set()

            # First try the actual location where XBRL processing saves data
            actual_bronze_delta = os.path.join(self.config.PROJECT_ROOT, "bronze", "delta", "sec_bulk")
            if os.path.exists(actual_bronze_delta):
                try:
                    # Try to scan as Delta table first
                    fund_ciks = set(pl.scan_delta(actual_bronze_delta).select('cik').unique().collect()['cik'].to_list())
                    logger.log_info(f"📊 Found {len(fund_ciks)} CIKs already in Bronze Delta (from Bulk Zips)")
                except Exception as e:
                    logger.log_warning("get_missing_ciks", f"Could not read Bronze Delta as Delta table ({str(e)}). Trying direct parquet scan...")

                    # Fallback: Scan parquet files directly if Delta scan fails
                    try:
                        # Get all parquet files recursively
                        parquet_files = []
                        for root, dirs, files in os.walk(actual_bronze_delta):
                            for file in files:
                                if file.endswith('.parquet') or file.endswith('.snappy.parquet'):
                                    parquet_files.append(os.path.join(root, file))

                        logger.log_info(f"🔍 Found {len(parquet_files)} parquet files to scan for existing CIKs")

                        # Read CIKs from each parquet file
                        for i, parquet_file in enumerate(parquet_files):
                            try:
                                df = pd.read_parquet(parquet_file, columns=['cik'])
                                if 'cik' in df.columns:
                                    file_ciks = set(df['cik'].astype(str).str.zfill(10))
                                    fund_ciks.update(file_ciks)
                                    logger.log_info(f"📊 Added {len(file_ciks)} CIKs from {parquet_file} (total so far: {len(fund_ciks)})")
                            except Exception as inner_e:
                                logger.log_warning(f"get_missing_ciks", f"Could not read {parquet_file}: {str(inner_e)}")
                                continue

                        if fund_ciks:
                            logger.log_info(f"✅ Successfully scanned parquet files directly: found {len(fund_ciks)} CIKs with fundamentals")
                        else:
                            logger.log_warning("get_missing_ciks", "No CIKs found in any parquet files")

                    except Exception as outer_e:
                        logger.log_error("get_missing_ciks", f"Failed to scan parquet files directly: {str(outer_e)}. Assuming empty.")
            else:
                logger.log_warning("get_missing_ciks", f"Bronze Delta directory does not exist at actual location: {actual_bronze_delta}")

            # Fallback to the configured location if nothing found in actual location
            if not fund_ciks and os.path.exists(self.config.RAW_OUTPUT_DIR):
                try:
                    # Try Delta scan first
                    fund_ciks = set(pl.scan_delta(self.config.RAW_OUTPUT_DIR).select('cik').unique().collect()['cik'].to_list())
                    logger.log_info(f"📊 Found {len(fund_ciks)} CIKs already in Bronze Delta (from configured location)")
                except Exception as e:
                    logger.log_warning("get_missing_ciks", f"Could not read Bronze Delta from configured location ({str(e)}). Assuming empty.")

            # 3. Calculate Missing with enhanced diagnostics
            missing = list(target_ciks - fund_ciks)
            only_in_bulk = list(fund_ciks - target_ciks)

            # Enhanced logging to understand the data situation
            logger.log_info(f"🔍 CIK Analysis Summary:")
            logger.log_info(f"  🎯 Target CIKs (Master Universe): {len(target_ciks)}")
            logger.log_info(f"  📊 Found in Bulk Data: {len(fund_ciks)}")
            logger.log_info(f"  🔍 Missing from Bulk: {len(missing)}")
            logger.log_info(f"  🆕 Only in Bulk Data: {len(only_in_bulk)}")

            # Add XBRL availability context
            if len(fund_ciks) > len(target_ciks):
                logger.log_info(f"💡 Insight: Bulk data contains {len(only_in_bulk)} more CIKs than expected target")
                logger.log_info(f"   This suggests the Master Universe may be outdated or incomplete")

            return missing

        except Exception as e:
            logger.log_error("get_missing_ciks", str(e))
            return []

    def fetch_company_facts(self, cik: str) -> Optional[pd.DataFrame]:
        """Fetch all XBRL facts for a company via EdgarTools API"""
        try:
            # Enhanced diagnostic logging
            logger.log_info(f"🔍 Attempting to fetch facts for CIK {cik}")

            # Test direct SEC API first for better error diagnosis
            test_url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
            headers = {'User-Agent': 'DataStore silviu.savu@example.com'}

            try:
                response = requests.get(test_url, headers=headers, timeout=10)
                if response.status_code == 404:
                    logger.log_info(f"📊 CIK {cik}: No XBRL data available (404 - expected for pre-XBRL filings)")
                    return None
                elif response.status_code == 403:
                    logger.log_warning(f"⚠️ CIK {cik}: Access forbidden (403 - rate limiting or permission issue)")
                    return None
                elif response.status_code == 429:
                    logger.log_warning(f"⚠️ CIK {cik}: Rate limited (429 - too many requests)")
                    time.sleep(5)  # Wait longer before retry
                    return None
                elif response.status_code != 200:
                    logger.log_warning(f"⚠️ CIK {cik}: HTTP error {response.status_code}")
                    return None
                else:
                    # If direct API works, proceed with EdgarTools for consistency
                    pass
            except requests.RequestException as api_error:
                logger.log_warning(f"⚠️ CIK {cik}: API test failed - {str(api_error)}")

            # Proceed with EdgarTools
            company = Company(str(int(cik)))  # Remove leading zeros for API
            facts = company.facts

            if facts is None:
                logger.log_info(f"📊 CIK {cik}: EdgarTools returned None (no XBRL data)")
                return None
            elif len(facts._facts) == 0:
                logger.log_info(f"📊 CIK {cik}: EdgarTools returned empty facts list (no XBRL data)")
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

            logger.log_info(f"✅ CIK {cik}: Successfully fetched {len(records)} XBRL facts")
            return pd.DataFrame(records)

        except Exception as e:
            logger.log_download(cik, False, str(e))
            return None

    def download_missing_ciks(self, batch_size: int = 100, limit: Optional[int] = None) -> int:
        """Download missing CIK fundamentals in batches with enhanced XBRL availability tracking"""
        missing = self.get_missing_ciks()
        logger.log_info(f"🔍 Found {len(missing)} CIKs missing fundamentals")

        if not missing:
            logger.log_info("✅ No missing CIKs found")
            return 0

        # Apply limit if specified
        if limit is not None:
            missing = missing[:limit]
            logger.log_info(f"📈 Limited to {len(missing)} CIKs due to --limit parameter")

        # Create output directory
        os.makedirs(self.config.EDGARTOOLS_DIR, exist_ok=True)

        # Initialize statistics
        success_count = 0
        no_xbrl_count = 0
        api_error_count = 0
        xbrl_stats = {
            'total_attempted': 0,
            'successful_fetches': 0,
            'no_xbrl_data': 0,
            'api_errors': 0,
            'xbrl_availability_rate': 0.0
        }

        for i in range(0, len(missing), batch_size):
            batch = missing[i:i+batch_size]
            batch_dfs = []

            for cik in tqdm(batch, desc=f"Batch {i//batch_size + 1}"):
                xbrl_stats['total_attempted'] += 1
                df = self.fetch_company_facts(cik)

                if df is not None and not df.empty:
                    batch_dfs.append(df)
                    success_count += 1
                    xbrl_stats['successful_fetches'] += 1
                else:
                    # Check what type of failure occurred based on logs
                    # This would be more robust with direct error tracking
                    no_xbrl_count += 1
                    xbrl_stats['no_xbrl_data'] += 1

                # Rate limiting - SEC allows 10 requests/sec
                time.sleep(0.15)

            # Save batch
            if batch_dfs:
                df_batch = pd.concat(batch_dfs, ignore_index=True)
                output_file = os.path.join(self.config.EDGARTOOLS_DIR, f"batch_{i:05d}.parquet")
                df_batch.to_parquet(output_file)
                logger.log_info(f"💾 Saved batch to {output_file}")

        # Calculate XBRL availability rate
        if xbrl_stats['total_attempted'] > 0:
            xbrl_stats['xbrl_availability_rate'] = xbrl_stats['successful_fetches'] / xbrl_stats['total_attempted']

        # Enhanced summary with XBRL availability insights
        logger.log_info(f"🎉 Download Complete. Summary:")
        logger.log_info(f"  📊 Attempted: {xbrl_stats['total_attempted']} CIKs")
        logger.log_info(f"  ✅ Success: {success_count} ({xbrl_stats['xbrl_availability_rate']*100:.1f}% availability)")
        logger.log_info(f"  ❌ No XBRL Data: {xbrl_stats['no_xbrl_data']}")
        logger.log_info(f"  ⚠️ API Errors: {xbrl_stats['api_errors']}")

        # Provide context about expected XBRL availability
        if xbrl_stats['xbrl_availability_rate'] < 0.5:
            logger.log_info(f"💡 Note: Low XBRL availability ({xbrl_stats['xbrl_availability_rate']*100:.1f}%) is expected for pre-2009 data")
            logger.log_info(f"   XBRL adoption was phased in starting 2009. Many older filings don't have XBRL data.")
        elif xbrl_stats['xbrl_availability_rate'] < 0.8:
            logger.log_info(f"💡 Note: Moderate XBRL availability ({xbrl_stats['xbrl_availability_rate']*100:.1f}%) suggests mixed vintage data")
        else:
            logger.log_info(f"💡 Note: High XBRL availability ({xbrl_stats['xbrl_availability_rate']*100:.1f}%) indicates mostly post-2009 data")

        # Save XBRL statistics for reference
        stats_file = os.path.join(self.config.EDGARTOOLS_DIR, "xbrl_availability_stats.json")
        with open(stats_file, 'w') as f:
            json.dump(xbrl_stats, f, indent=2)
        logger.log_info(f"💾 Saved XBRL availability statistics to {stats_file}")

        return success_count

# ======================
# 5. PROCESSING MODULE
# ======================
class ProcessingModule:
    """Handles SEC data processing from various sources"""

    def __init__(self):
        self.config = config

    def list_sec_zips(self) -> List[str]:
        """Returns list of Q1-Q4 zip files in chronological order"""
        # First try the configured directory
        zips = sorted(glob.glob(os.path.join(self.config.SEC_BULK_DIR, "*.zip")))

        # If no ZIPs found in configured directory, check alternative location
        if not zips:
            alternative_dir = os.path.join(self.config.PROJECT_ROOT, "bronze", "sec_bulk")
            alt_zips = sorted(glob.glob(os.path.join(alternative_dir, "*.zip")))
            if alt_zips:
                logger.log_info(f"ℹ️ Using alternative SEC bulk directory: {alternative_dir}")
                return alt_zips

        return zips

    def process_quarter(self, zip_path: str, force: bool = False) -> bool:
        """Process a single SEC Bulk Zip file"""
        quarter_name = os.path.basename(zip_path).replace(".zip", "")
        logger.log_info(f"📦 Processing {quarter_name}...")

        # Check if already processed and force is False
        raw_output = os.path.join(self.config.RAW_OUTPUT_DIR, f"{quarter_name}.parquet")
        if not force and os.path.exists(raw_output):
            logger.log_info(f"📦 Skipping {quarter_name} - already processed (use --force to re-process)")
            return True

        try:
            start_time = time.time()

            with zipfile.ZipFile(zip_path, 'r') as z:
                # 1. Load Submissions (Company Info)
                with z.open('sub.txt') as f:
                    df_sub = pd.read_csv(f, sep='\t', encoding='utf-8', low_memory=False)
                    # Include foreign forms (20-F, 40-F, 6-K) to capture foreign issuers
                    df_sub = df_sub[df_sub['form'].isin(['10-K', '10-Q', '20-F', '40-F', '6-K'])].copy()

                    # Extract Ticker from Instance Filename (e.g., "aapl-20230930_htm.xml" -> "AAPL")
                    # Assuming standard naming convention: ticker-date.xml or similar
                    # Some instances might not follow this, so we handle gracefully
                    if 'instance' in df_sub.columns:
                        df_sub['ticker'] = df_sub['instance'].astype(str).str.split('-').str[0].str.upper().str.slice(0, 5)
                    else:
                        df_sub['ticker'] = None


                if df_sub.empty:
                    logger.log_info(f"[{quarter_name}] No 10-K/10-Q filings found")
                    return False

                # 2. Load Numbers (The Data)
                with z.open('num.txt') as f:
                    # Enforce coreg as string to avoid float inference/collision with actual strings
                    df_num = pd.read_csv(f, sep='\t', encoding='utf-8', low_memory=False, dtype={'coreg': object, 'value': float})

                # 3. Data Merge Strategy
                valid_adsh = set(df_sub['adsh'])
                df_num = df_num[df_num['adsh'].isin(valid_adsh)]

                # Merge NUM with SUB (to get CIK/Date/Ticker/SIC)
                # Ensure we select 'ticker' and 'sic' from df_sub if they exist
                sub_cols = ['adsh', 'cik', 'name', 'period', 'fy', 'fp', 'form', 'filed']
                if 'ticker' in df_sub.columns:
                    sub_cols.append('ticker')
                if 'sic' in df_sub.columns:
                    sub_cols.append('sic')

                df_merged = df_num.merge(
                    df_sub[sub_cols],
                    on='adsh',
                    how='left'
                )

                # --- SAVE BRONZE (DELTA) ---
                # Add source metadata
                df_merged['source_file'] = quarter_name
                
                # Write to Delta
                delta_path = self.config.RAW_OUTPUT_DIR
                write_deltalake(
                    delta_path,
                    df_merged,
                    mode='append',
                    partition_by=['fy', 'form'], # Partitioning by Fiscal Year and Form helps retrieval
                    schema_mode='merge'
                )
                logger.log_processing(delta_path, len(df_merged), time.time() - start_time)

                logger.log_info(f"[{quarter_name}] Bronze Ingestion (Delta) Complete")
                return True

        except Exception as e:
            logger.log_error(f"process_quarter({quarter_name})", str(e))
            import traceback
            traceback.print_exc()
            return False

    def process_all_quarters(self, quarters: Optional[str] = None, force: bool = False) -> int:
        """Process all available SEC bulk ZIP files"""
        zips = self.list_sec_zips()

        # Diagnostic logging to understand the directory structure
        logger.log_info(f"🔍 DEBUG: Looking for SEC ZIP files in: {self.config.SEC_BULK_DIR}")
        logger.log_info(f"🔍 DEBUG: Current SEC_BULK_DIR configuration: {self.config.SEC_BULK_DIR}")

        # Check if the expected directory exists
        if not os.path.exists(self.config.SEC_BULK_DIR):
            logger.log_warning(f"⚠️ Expected SEC bulk directory does not exist: {self.config.SEC_BULK_DIR}")

        # Check if alternative directory exists
        alternative_dir = os.path.join(self.config.PROJECT_ROOT, "bronze", "sec_bulk")
        if os.path.exists(alternative_dir):
            logger.log_info(f"ℹ️ Alternative directory found: {alternative_dir}")
            # Check if there are ZIP files in the alternative directory
            alt_zips = glob.glob(os.path.join(alternative_dir, "*.zip"))
            if alt_zips:
                logger.log_info(f"📁 Found {len(alt_zips)} ZIP files in alternative directory: {alternative_dir}")

        if not zips:
            logger.log_info("⚠️ No SEC Bulk Data found. Please download data to bronze/landing/sec_raw/")
            return 0

        # Filter by specific quarters if provided
        if quarters:
            quarter_list = [q.strip() for q in quarters.split(",")]
            zips = [z for z in zips if any(q in z for q in quarter_list)]
            if not zips:
                logger.log_info(f"⚠️ No matching quarters found for: {quarters}")
                return 0

        logger.log_info(f"📊 Found {len(zips)} quarters. Starting sequential processing...")

        success_count = 0
        for z in zips:
            if self.process_quarter(z, force=force):
                success_count += 1

        logger.log_info(f"🎉 Processed {success_count}/{len(zips)} quarters successfully")
        return success_count

# ======================
# 6. CIK UNIVERSE MODULE
# ======================
class CIKUniverseModule:
    """Handles master CIK universe creation and filtering"""

    def __init__(self):
        self.config = config

    def fetch_sec_tickers(self) -> Dict[str, Dict[str, Any]]:
        """Fetch official SEC company tickers JSON"""
        url = "https://www.sec.gov/files/company_tickers.json"
        try:
            headers = {'User-Agent': 'DataStore silviu.savu@example.com'} 
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            # Format: { "0": { "cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc." }, ... }
            # Convert to: { "0000320193": { "ticker": "AAPL", "title": "Apple Inc." } }
            
            formatted_data = {}
            for _, item in data.items():
                cik = str(item['cik_str']).zfill(10)
                formatted_data[cik] = {
                    'ticker': item['ticker'],
                    'title': item['title']
                }
            
            logger.log_info(f"✅ Fetched {len(formatted_data)} tickers from SEC")
            return formatted_data
            
        except Exception as e:
            logger.log_error("fetch_sec_tickers", f"Failed to fetch SEC tickers: {str(e)}")
            return {}

    def create_master_cik_list(self) -> Set[str]:
        """Create comprehensive CIK list from all XBRL sources with metadata"""
        logger.log_info("🔍 Creating master CIK list from XBRL data...")

        # Structure: cik -> {'cik': ..., 'ticker': ..., 'sic': ...}
        all_metadata = {}

        # 1. Load from SEC bulk data (2009-Present)
        self._load_sec_bulk_metadata(all_metadata)

        # 2. Load from EdgarTools data (if it has metadata, otherwise just CIKs)
        # EdgarTools data usually has concept/value, less likely to have ticker in the same way 
        # unless we parse it from somewhere else. For now we just add CIKs if missing.
        self._load_edgartools_ciks(all_metadata)

        # 3. Load from official SEC company_tickers.json (Best source for tickers)
        sec_tickers = self.fetch_sec_tickers()
        for cik, info in sec_tickers.items():
            if cik not in all_metadata:
                all_metadata[cik] = {'cik': cik, 'ticker': None, 'sic': None}
            
            # Always prefer official SEC ticker if available
            all_metadata[cik]['ticker'] = info['ticker']
            # We could also save title if we wanted, but schema is cik,ticker,sic
            # If we want to extend schema, we can, but let's stick to requested first.

        # 4. Enrich with EODHD Master Universe (The user's provided complete map)
        eodhd_file = 'references/eodhd_master_universe.parquet'
        if os.path.exists(eodhd_file):
            try:
                logger.log_info(f"📚 Enriching with EODHD Master Universe: {eodhd_file}")
                eodhd_df = pd.read_parquet(eodhd_file)
                
                # Filter for rows with valid CIK and Code
                eodhd_df = eodhd_df.dropna(subset=['cik', 'Code'])
                
                # Create a lookup map: CIK (normalized str) -> Ticker
                cik_to_ticker = {}
                for _, row in eodhd_df.iterrows():
                    try:
                        c = str(int(float(row['cik']))).zfill(10) # "0000320193"
                        code = row['Code']
                        if code:
                            cik_to_ticker[c] = code
                    except (ValueError, TypeError, AttributeError) as e:
                        # Use row['cik'] directly since c might not be defined if the exception occurs early
                        cik_value = row['cik'] if 'cik' in row else 'unknown'
                        logger.log_warning("CIK-ticker mapping", f"CIK-ticker mapping failed for {cik_value}: {str(e)}")
                        continue
                
                enrich_count = 0
                for cik in all_metadata:
                    # Normalize our key to match lookup
                    norm_cik = str(cik).zfill(10)
                    
                    if norm_cik in cik_to_ticker:
                        # Update ticker if missing
                        if not all_metadata[cik]['ticker']:
                            all_metadata[cik]['ticker'] = cik_to_ticker[norm_cik]
                            enrich_count += 1
                
                logger.log_info(f"✅ Enriched {enrich_count} CIKs with tickers from EODHD")
                
            except Exception as e:
                logger.log_error("enrich_eodhd", f"Failed to enrich from EODHD: {e}")

        # 5. Save raw master list with metadata
        self._save_master_list(all_metadata)

        all_ciks = set(all_metadata.keys())
        logger.log_info(f"🎯 Total unique CIKs found: {len(all_ciks)}")
        return all_ciks

    def _load_sec_bulk_metadata(self, all_metadata: Dict[str, Dict]):
        """Load CIKs and metadata from SEC bulk data (Delta Table) by processing YEARLY chunks to avoid OOM"""
        if not os.path.exists(self.config.RAW_OUTPUT_DIR) or not os.path.exists(os.path.join(self.config.RAW_OUTPUT_DIR, "_delta_log")):
            logger.log_warning("load_sec_bulk_metadata", f"Delta table not found at {self.config.RAW_OUTPUT_DIR}. Skipping.")
            return

        logger.log_info(f"📊 Scanning SEC Bulk Delta Table at {self.config.RAW_OUTPUT_DIR} (Year-by-Year)...")

        total_processed = 0
        from datetime import datetime
        current_year = datetime.now().year
        # Iterate from 2009 to Current Year + 1
        for year in range(2009, current_year + 2):
            try:
                # We filter by partition 'year' if it exists. 
                # Note: Delta Table directory structure usually implies partitions, but let's query efficiently.
                # If partition column 'year' is not exposed in scan (sometimes it is virtual), we might check path presence?
                # Actually, let's try scanning with filter. Polars scan_delta handles partition pruning.
                
                # Check if year folder exists to avoid empty scans (optimization)
                year_path = os.path.join(self.config.RAW_OUTPUT_DIR, f"year={year}")
                if not os.path.exists(year_path):
                    continue

                logger.log_info(f"  > Processing Year {year}...")

                # Fetch only this year's data
                agg_df = pl.scan_delta(self.config.RAW_OUTPUT_DIR).filter(
                    pl.col('year') == str(year)
                ).select([
                    pl.col('cik').cast(pl.Utf8).str.zfill(10).alias('cik'),
                    pl.col('ticker').cast(pl.Utf8),
                    pl.col('sic').cast(pl.Int64)
                ]).filter(
                    pl.col('cik').is_not_null()
                ).unique().collect()

                # Merge into main dictionary
                count = 0
                for row in agg_df.iter_rows(named=True):
                    cik = row['cik']
                    if cik not in all_metadata:
                        all_metadata[cik] = {'cik': cik, 'ticker': None, 'sic': None}
                    
                    if row['ticker']:
                        all_metadata[cik]['ticker'] = row['ticker']
                    if row['sic']:
                        all_metadata[cik]['sic'] = row['sic']
                    count += 1
                
                total_processed += count
                # Force garbage collection / memory release
                del agg_df
                
            except Exception as e:
                # Log but continue to next year (don't break whole pipeline for one bad year)
                logger.log_error(f"Failed to process year {year} in SEC bulk metadata", str(e))

        logger.log_info(f"✅ Loaded compacted metadata for {len(all_metadata)} unique CIKs (Processed {total_processed} yearly records)")

    def _load_edgartools_ciks(self, all_metadata: Dict[str, Dict]):
        """Load CIKs from EdgarTools data"""
        edgar_ciks = set()
        edgar_path = Path('bronze/fundamentals/edgartools')
        edgar_files = CIKUtils.get_file_list(str(edgar_path))

        logger.log_info(f"📊 Processing {len(edgar_files)} EdgarTools files...")

        for i, parquet_file in enumerate(edgar_files):
            try:
                # EdgarTools parquet structure is different, usually just concepts
                # We typically only get CIKs from here
                file_metadata = CIKUtils.extract_metadata_from_parquet(parquet_file)
                for record in file_metadata:
                    cik = record['cik']
                    if cik not in all_metadata:
                        all_metadata[cik] = {'cik': cik, 'ticker': None, 'sic': None}
                    # No ticker/sic update here typically unless we added it to edgar pipeline
                    
                if (i + 1) % 10 == 0:
                    logger.log_info(f"  Processed {i+1}/{len(edgar_files)} files...")
            except Exception as e:
                logger.log_error(f"load_edgartools_ciks({parquet_file})", str(e))

        logger.log_info(f"✅ Processed EdgarTools data (Total CIKs now: {len(all_metadata)})")


    def _save_master_list(self, metadata: Dict[str, Dict]):
        """Save raw master CIK list with metadata"""
        # Convert dict to DataFrame
        data = list(metadata.values())
        master_df = pd.DataFrame(data)
        
        # Ensure columns order
        cols = ['cik', 'ticker', 'sic']
        for c in cols:
            if c not in master_df.columns:
                master_df[c] = None
        
        master_df = master_df[cols].sort_values('cik')
        os.makedirs('references', exist_ok=True)
        master_df.to_parquet('references/master_cik_list_raw.parquet', index=False)
        logger.log_info("💾 Saved RAW master CIK list to references/master_cik_list_raw.parquet")
        logger.log_info("   Note: Curated list (master_cik_list.parquet) is NOT overwritten.")

    def filter_equities_only(self, all_ciks: Set[str], filter_foreign: bool = False, min_market_cap: float = 10_000_000) -> Set[str]:
        """Filter CIK list to only include common stocks on major exchanges with market cap filtering"""
        logger.log_info("🔍 Filtering to equities only with market cap...")

        # Load master CIK list for exchange/type filtering
        eodhd_file = 'references/master_cik_list.parquet'
        if not os.path.exists(eodhd_file):
            logger.log_error("filter_equities_only", f"EODHD universe file not found: {eodhd_file}")
            return set()

        try:
            eodhd_df = pd.read_parquet(eodhd_file)

            # Add diagnostic logging to understand the data structure
            available_columns = list(eodhd_df.columns)
            logger.log_info(f"🔍 Master CIK list columns: {available_columns}")

            # Since the master CIK list is generated internally from XBRL files,
            # it won't have the 'Type' column that was expected from external EODHD sources.
            # For internally generated lists, we assume all CIKs are potential equities
            # since they come from SEC filings (10-K, 10-Q, etc.) which are typically equity issuers.

            if 'Type' not in eodhd_df.columns:
                 logger.log_info("Column 'Type' not found in master list - this is expected for internally generated XBRL-based lists")
                 logger.log_info("💡 Using conservative approach: retaining all CIKs from XBRL filers as potential equities")
                 # Initialize with all CIKs from master list
                 filtered_ciks = set(eodhd_df['cik'].astype(str).str.zfill(10))
            else:
                 # Valid security types (Keep Common Stock only, but allow ALL exchanges to include Delisted/OTC)
                 valid_types = {'Common Stock'}

                 # Filter to equities only - IGNORING EXCHANGE to keep Delisted/OTC history
                 equity_df = eodhd_df[eodhd_df['Type'].isin(valid_types)]
                 filtered_ciks = set(equity_df['cik'].astype(str).str.zfill(10))

            # Intersection with our provided set
            filtered_ciks = all_ciks.intersection(filtered_ciks)

            # SKIP Market Cap filtering to avoid excluding delisted companies (which often have 0 cap now)
            # filtered_ciks = self.filter_by_market_cap(filtered_ciks, min_market_cap=min_market_cap)

            logger.log_info(f"✅ Found {len(filtered_ciks)} equity CIKs (from {len(all_ciks)} total)")
            logger.log_info(f"🗑️  Excluded {len(all_ciks) - len(filtered_ciks)} non-equity securities (Funds/ETFs/etc)")
            logger.log_info(f"⚠️ Skipped Market Cap & Exchange filtering to include Delisted/Historical stocks")

            return filtered_ciks

        except Exception as e:
            logger.log_error("filter_equities_only", str(e))
            return set()

    def filter_by_market_cap(self, equity_ciks: Set[str], min_market_cap: float = 10_000_000) -> Set[str]:
        """Filter equities by market capitalization using EdgarTools data and market prices"""
        logger.log_info("💰 Applying market cap filtering...")

        if not equity_ciks:
            logger.log_warning("filter_by_market_cap", "No equity CIKs provided for market cap filtering")
            return set()

        try:
            # Get shares outstanding from EdgarTools data
            shares_data = self._get_shares_outstanding(equity_ciks)
            if not shares_data:
                logger.log_warning("filter_by_market_cap", "No shares data found")
                return equity_ciks  # Return original if no shares data

            # Get current market prices
            price_data = self._get_current_prices(equity_ciks)
            if not price_data:
                logger.log_warning("filter_by_market_cap", "No price data found")
                return equity_ciks  # Return original if no price data

            # Calculate market caps and filter
            market_cap_data = []
            for cik in equity_ciks:
                shares = shares_data.get(cik, 0)
                price = price_data.get(cik, 0)

                if shares > 0 and price > 0:
                    market_cap = shares * price
                    market_cap_data.append((cik, market_cap))

            # Filter by the specified market cap threshold
            filtered_ciks = set()
            for cik, market_cap in market_cap_data:
                if market_cap >= min_market_cap:
                    filtered_ciks.add(cik)

            logger.log_info(f"📊 Market cap filtering: {len(filtered_ciks)} CIKs passed (>= ${min_market_cap/1_000_000}M)")
            logger.log_info(f"🗑️  Excluded {len(equity_ciks) - len(filtered_ciks)} low market cap securities")

            return filtered_ciks

        except Exception as e:
            logger.log_error("filter_by_market_cap", f"Market cap filtering failed: {str(e)}")
            return equity_ciks  # Return original on error

    def _get_shares_outstanding(self, ciks: Set[str]) -> Dict[str, float]:
        """Get shares outstanding from EdgarTools data"""
        shares_data = {}

        try:
            # Look for CommonStockSharesOutstanding in EdgarTools files
            for file in glob.glob('bronze/fundamentals/edgartools/batch_*.parquet'):
                try:
                    df = pd.read_parquet(file)
                    # Filter for shares outstanding data
                    shares_df = df[df['concept'] == 'us-gaap:CommonStockSharesOutstanding']
                    if not shares_df.empty:
                        for _, row in shares_df.iterrows():
                            cik = str(row['cik']).zfill(10)
                            if cik in ciks:
                                try:
                                    shares = float(row['value'])
                                    shares_data[cik] = shares
                                except (ValueError, TypeError):
                                    continue
                except Exception as e:
                    logger.log_warning("_get_shares_outstanding", f"Error reading {file}: {str(e)}")
                    continue

            logger.log_info(f"📄 Found shares data for {len(shares_data)} CIKs")

        except Exception as e:
            logger.log_error("_get_shares_outstanding", str(e))

        return shares_data

    def _get_current_prices(self, ciks: Set[str]) -> Dict[str, float]:
        """Get current prices from market data"""
        price_data = {}
        start_time = time.time()

        try:
            # Try to get prices from silver market data
            try:
                logger.log_info(f"🔍 Starting price search for {len(ciks)} CIKs...")
                # Read all market data files
                market_files = glob.glob('silver/market_data/eodhd/**/*.parquet', recursive=True)
                logger.log_info(f"📁 Found {len(market_files)} market data files to scan...")
                
                files_processed = 0
                for i, file in enumerate(market_files):
                    try:
                        file_start = time.time()
                        df = pd.read_parquet(file)
                        
                        if 'cik' in df.columns and 'close' in df.columns:
                            # More efficient: check which CIKs are in this file first
                            file_ciks = set(df['cik'].astype(str).str.zfill(10))
                            matching_ciks = ciks.intersection(file_ciks)
                            
                            if matching_ciks:
                                # Only process matching CIKs
                                for cik in matching_ciks:
                                    if cik not in price_data:  # Skip if we already have price
                                        cik_data = df[df['cik'] == cik]  # Compare as string since both are strings
                                        if not cik_data.empty:
                                            latest_price = cik_data['close'].iloc[-1]
                                            price_data[cik] = float(latest_price)
                        
                        files_processed += 1
                        file_time = time.time() - file_start
                        
                        # Log progress every 10 files or if file takes > 1 second
                        if (i + 1) % 10 == 0 or file_time > 1.0:
                            logger.log_info(f"  Processed {i+1}/{len(market_files)} files ({files_processed} with data) - {len(price_data)} prices found - Current file: {file_time:.2f}s")
                    
                    except Exception as e:
                        logger.log_warning("_get_current_prices", f"Error reading {file}: {str(e)}")
                        continue
                
                total_time = time.time() - start_time
                logger.log_info(f"✅ Market data scan complete: {len(market_files)} files in {total_time:.2f}s")
                        
            except Exception as e:
                logger.log_warning("_get_current_prices", f"Error reading market data: {str(e)}")

            # Fallback: try to get from EODHD universe if available
            if len(price_data) < len(ciks):
                try:
                    logger.log_info(f"🔄 Using master CIK list fallback for missing prices...")
                    eodhd_df = pd.read_parquet('references/master_cik_list.parquet')
                    fallback_start = time.time()
                    
                    for _, row in eodhd_df.iterrows():
                        cik = str(row['cik']).zfill(10)
                        if cik in ciks and cik not in price_data:
                            try:
                                # Try to extract price from available fields
                                price = float(row.get('close', 0)) or float(row.get('price', 0))
                                if price > 0:
                                    price_data[cik] = price
                            except (ValueError, TypeError):
                                continue
                    
                    fallback_time = time.time() - fallback_start
                    logger.log_info(f"✅ EODHD fallback complete: {fallback_time:.2f}s")
                    
                except Exception as e:
                    logger.log_warning("_get_current_prices", f"Error reading EODHD data: {str(e)}")

            total_time = time.time() - start_time
            logger.log_info(f"💵 Found price data for {len(price_data)}/{len(ciks)} CIKs in {total_time:.2f}s")

        except Exception as e:
            logger.log_error("_get_current_prices", str(e))

        return price_data

    def find_foreign_stocks(self, all_ciks: Set[str]) -> Set[str]:
        """Identify foreign stocks that file 20-F/40-F instead of 10-K"""
        logger.log_info("🔍 Identifying foreign stocks...")

        foreign_forms = {'20-F', '40-F', '6-K'}
        foreign_ciks = set()

        # 1. Check SEC bulk data for foreign filers
        sec_path = Path('bronze/fundamentals/sec_bulk/parquet')
        sec_files = CIKUtils.get_file_list(str(sec_path))

        logger.log_info(f"📊 Scanning {len(sec_files)} SEC bulk files for foreign filers...")

        for i, parquet_file in enumerate(sec_files):
            try:
                df = pd.read_parquet(parquet_file, columns=['cik', 'form'])
                # Filter to foreign forms and get CIKs
                file_foreign_ciks = set(df[df['form'].isin(foreign_forms)]['cik'].astype(str).str.zfill(10))
                foreign_ciks.update(file_foreign_ciks)

                if (i + 1) % 10 == 0:
                    logger.log_info(f"  Scanned {i+1}/{len(sec_files)} SEC files...")

            except Exception as e:
                logger.log_error(f"find_foreign_stocks({parquet_file})", str(e))

        # 2. Check EdgarTools data for foreign filers (enhanced detection)
        edgar_path = Path('bronze/fundamentals/edgartools')
        edgar_files = CIKUtils.get_file_list(str(edgar_path))

        logger.log_info(f"📊 Scanning {len(edgar_files)} EdgarTools files for foreign filers...")

        for i, parquet_file in enumerate(edgar_files):
            try:
                df = pd.read_parquet(parquet_file, columns=['cik', 'form_type'])
                # Filter to foreign forms and get CIKs
                file_foreign_ciks = set(df[df['form_type'].isin(foreign_forms)]['cik'].astype(str).str.zfill(10))
                foreign_ciks.update(file_foreign_ciks)

                if (i + 1) % 10 == 0:
                    logger.log_info(f"  Scanned {i+1}/{len(edgar_files)} EdgarTools files...")

            except Exception as e:
                logger.log_error(f"find_foreign_stocks({parquet_file})", str(e))

        # Filter to only CIKs that are in our master list
        master_foreign_ciks = all_ciks.intersection(foreign_ciks)

        logger.log_info(f"✅ Found {len(master_foreign_ciks)} foreign filers in master list")
        logger.log_info("🌍 These are non-US companies listed on US exchanges")

        return master_foreign_ciks

    def create_final_universe(self, filter_foreign: bool = False, min_market_cap: float = 10_000_000) -> Dict:
        """Create the final master CIK universe"""
        logger.log_info("🚀 Creating final master CIK universe...")

        # 1. Get all CIKs from XBRL data
        all_ciks = self.create_master_cik_list()

        # 2. Filter to equities only
        equity_ciks = self.filter_equities_only(all_ciks, filter_foreign=filter_foreign, min_market_cap=min_market_cap)

        # 3. Identify foreign stocks
        foreign_ciks = self.find_foreign_stocks(all_ciks)

        # Load metadata map for enrichment
        try:
            # Load metadata from master list (created in previous step)
            # This list has Ticker and SIC
            metadata_df = pd.read_parquet('references/master_cik_list.parquet')
            
            # Create lookup dictionary: CIK -> {ticker, sic}
            metadata_map = metadata_df.set_index('cik')[['ticker', 'sic']].to_dict('index')
        except Exception as e:
            logger.log_warning("create_final_universe", f"Could not load metadata for enrichment: {e}")
            metadata_map = {}

        # 4. Create final universe classification
        domestic_equities = equity_ciks - foreign_ciks
        foreign_equities = equity_ciks & foreign_ciks

        logger.log_info(f"\n📊 Final Universe Summary:")
        logger.log_info(f"  🏢 Domestic Equities: {len(domestic_equities)}")
        logger.log_info(f"  🌍 Foreign Equities: {len(foreign_equities)}")
        logger.log_info(f"  🏦 Total Equity Universe: {len(equity_ciks)}")

        # Create final DataFrame with classification
        final_data = []
        for cik in equity_ciks:
            cik_type = 'foreign' if cik in foreign_ciks else 'domestic'
            normalized_cik = str(cik).zfill(10) # Ensure key matches
            
            # Lookup metadata (try both int and string key formats if needed, but we normalized to string)
            # master_cik_list.csv usually has int CIKs unless we forced string. 
            # Let's ensure we handle the lookup robustly.
            # The keys in metadata_map are strings because we loaded with dtype={'cik': str} matches
            
            meta = metadata_map.get(normalized_cik.lstrip('0'), {}) # Try without leading zeros if int based
            if not meta:
                 meta = metadata_map.get(normalized_cik, {}) # Try with normalized

            final_data.append({
                'cik': cik,
                'type': cik_type,
                'description': 'Foreign filer (20-F/40-F)' if cik in foreign_ciks else 'Domestic filer (10-K)',
                'ticker': meta.get('ticker'),
                'sic': meta.get('sic')
            })

        # Save final results
        final_df = pd.DataFrame(final_data)
        
        # Reorder columns
        cols = ['cik', 'ticker', 'sic', 'type', 'description']
        final_df = final_df[cols]
        
        final_df.to_parquet('references/master_cik_universe.parquet', index=False)
        logger.log_info("💾 Saved final master universe to references/master_cik_universe.parquet")

        # Save foreign stocks separately
        foreign_df = final_df[final_df['type'] == 'foreign']
        foreign_df.to_parquet('references/foreign_stocks.parquet', index=False)
        logger.log_info("💾 Saved foreign stocks list to references/foreign_stocks.parquet")

        # Create summary report
        summary = {
            'total_ciks': len(all_ciks),
            'equity_ciks': len(equity_ciks),
            'domestic_equities': len(domestic_equities),
            'foreign_equities': len(foreign_equities),
            'non_equity_ciks': len(all_ciks) - len(equity_ciks),
            'foreign_filers': len(foreign_ciks),
            'filter_foreign': filter_foreign,
            'min_market_cap': min_market_cap,
            'created_files': ['references/master_cik_list.parquet', 'references/master_cik_universe.parquet', 'references/foreign_stocks.parquet'],
            'timestamp': datetime.now().isoformat()
        }

        with open('references/cik_universe_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)

        logger.log_info("💾 Saved summary report to cik_universe_summary.json")
        logger.log_info("🎉 Master CIK universe creation complete!")

        return summary

# ======================
# 7. NEW DATA MODULES
# ======================

# ======================
# 7.1 FINRA MODULE
# ======================
class FINRAModule:
    """Handles FINRA data collection"""

    def __init__(self):
        # FINRA API Configuration
        self.finra_client_id = os.getenv("FINRA_CLIENT_ID")
        self.finra_client_password = os.getenv("FINRA_CLIENT_PASSWORD")
        self.finra_token_url = "https://ews.fip.finra.org/fip/rest/ews/oauth2/access_token"

        # Directories
        self.finra_dir = os.path.join(config.PROJECT_ROOT, "bronze", "finra")
        self.margin_dir = os.path.join(self.finra_dir, "margin")
        self.short_interest_dir = os.path.join(self.finra_dir, "short_interest")
        self.reg_sho_dir = os.path.join(self.finra_dir, "reg_sho")

        # Setup directories
        self._setup_directories()

    def _setup_directories(self):
        """Ensure all required directories exist"""
        for directory in [self.finra_dir, self.margin_dir, self.short_interest_dir, self.reg_sho_dir]:
            os.makedirs(directory, exist_ok=True)

    def get_finra_token(self) -> Optional[str]:
        """Authenticate with FINRA API"""
        if not self.finra_client_id:
            logger.log_error("get_finra_token", "Missing FINRA Client ID in .env")
            return None

        try:
            # Standard FINRA OAuth flow
            pwd = self.finra_client_password if self.finra_client_password else ""
            auth = (self.finra_client_id, pwd)
            data = {"grant_type": "client_credentials"}

            r = requests.post(self.finra_token_url, auth=auth, data=data)
            r.raise_for_status()

            token = r.json().get("access_token")
            return token

        except Exception as e:
            logger.log_error("get_finra_token", f"Authentication Failed: {str(e)}")
            return None

    def download_margin_statistics(self) -> bool:
        """Download FINRA Margin Statistics"""
        logger.log_info("📊 [FINRA] Fetching Margin Statistics...")

        try:
            url = "https://www.finra.org/rules-guidance/key-topics/margin-accounts/margin-statistics"
            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
            }

            response = requests.get(url, headers=headers)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")
            tables = pd.read_html(str(soup))

            if not tables:
                logger.log_error("download_margin_statistics", "No tables found for Margin Statistics")
                return False

            # Process first table
            df = tables[0]
            timestamp = datetime.now().strftime("%Y%m%d")
            filepath = os.path.join(self.margin_dir, f"margin_statistics_{timestamp}.csv")
            df.to_csv(filepath, index=False)

            logger.log_info(f"💾 [FINRA] Saved Margin Statistics to {filepath} ({len(df)} rows)")
            return True

        except Exception as e:
            logger.log_error("download_margin_statistics", f"Error fetching Margin Statistics: {str(e)}")
            return False

    def download_equity_short_interest(self, token: str) -> bool:
        """Download Equity Short Interest data using authenticated token"""
        logger.log_info("📊 [FINRA] Fetching Equity Short Interest Data...")

        if not token:
            logger.log_error("download_equity_short_interest", "No FINRA token provided")
            return False

        try:
            url = "https://api.finra.org/data/group/otcMarket/name/EquityShortInterest"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            payload = {"limit": 5000}  # Large batch

            r = requests.post(url, headers=headers, json=payload)
            r.raise_for_status()

            # Save as CSV
            timestamp = datetime.now().strftime("%Y%m%d")
            output_file = os.path.join(self.short_interest_dir, f"equity_short_interest_{timestamp}.csv")

            with open(output_file, "w") as f:
                f.write(r.text)

            line_count = len(r.text.splitlines())
            logger.log_info(f"💾 [FINRA] Saved Equity Short Interest to {output_file} ({line_count} lines)")
            return True

        except Exception as e:
            logger.log_error("download_equity_short_interest", f"Error downloading Equity Short Interest: {str(e)}")
            return False

    def download_reg_sho_daily(self, token: str) -> bool:
        """Download Reg SHO Daily Short Sale Volume data"""
        logger.log_info("📊 [FINRA] Fetching Reg SHO Daily Volume Data...")

        if not token:
            logger.log_error("download_reg_sho_daily", "No FINRA token provided")
            return False

        try:
            url = "https://api.finra.org/data/group/otcMarket/name/RegShoDaily"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            payload = {"limit": 5000}

            r = requests.post(url, headers=headers, json=payload)
            r.raise_for_status()

            # Save as CSV
            timestamp = datetime.now().strftime("%Y%m%d")
            output_file = os.path.join(self.reg_sho_dir, f"reg_sho_daily_{timestamp}.csv")

            with open(output_file, "w") as f:
                f.write(r.text)

            line_count = len(r.text.splitlines())
            logger.log_info(f"💾 [FINRA] Saved Reg SHO Daily to {output_file} ({line_count} lines)")
            return True

        except Exception as e:
            logger.log_error("download_reg_sho_daily", f"Error downloading Reg SHO Daily: {str(e)}")
            return False

    def run_finra_pipeline(self) -> Dict:
        """Run complete FINRA data collection"""
        logger.log_info("🚀 [FINRA] Starting FINRA data collection...")

        results = {
            'margin_statistics': False,
            'equity_short_interest': False,
            'reg_sho_daily': False,
            'timestamp': datetime.now().isoformat()
        }

        # Test Authentication
        token = self.get_finra_token()
        if token:
            logger.log_info(f"✅ [FINRA] Authentication successful: {token[:10]}...")
            results['equity_short_interest'] = self.download_equity_short_interest(token)
            results['reg_sho_daily'] = self.download_reg_sho_daily(token)
        else:
            logger.log_warning("run_finra_pipeline", "Authentication Failed. Skipping authenticated downloads.")

        # Public Data (Always run)
        results['margin_statistics'] = self.download_margin_statistics()

        logger.log_info("🎉 [FINRA] FINRA data collection complete")
        return results

# ======================
# 7.2 FRED MODULE
# ======================
class FREDModule:
    """Handles FRED macroeconomic data collection"""

    def __init__(self):
        self.fred_api_key = os.getenv("FRED_API_KEY")

        # Directories
        self.fred_dir = os.path.join(config.PROJECT_ROOT, "bronze", "macro", "fred")
        self.fred_landing_dir = os.path.join(config.PROJECT_ROOT, "bronze", "landing", "fred_raw")
        os.makedirs(self.fred_dir, exist_ok=True)
        os.makedirs(self.fred_landing_dir, exist_ok=True)

        # Core FRED Series Map
        self.series_map = {
            "GDP": "Gross Domestic Product (Quarterly)",
            "GDPC1": "Real Gross Domestic Product (Quarterly)",
            "CPIAUCSL": "Consumer Price Index for All Urban Consumers (Monthly)",
            "FEDFUNDS": "Federal Funds Effective Rate (Monthly)",
            "DGS10": "Market Yield on U.S. Treasury Securities at 10-Year Constant Maturity (Daily)",
            "DGS2": "Market Yield on U.S. Treasury Securities at 2-Year Constant Maturity (Daily)",
            "T10Y2Y": "10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity (Daily)",
            "UNRATE": "Unemployment Rate (Monthly)",
            "VIXCLS": "CBOE Volatility Index (VIX) (Daily)",
            "M2SL": "M2 Money Stock (Monthly)",
            "BAMLC0A0CM": "ICE BofA US Corp Master Option-Adjusted Spread (Daily)",
            "BAMLH0A0HYM2": "ICE BofA US High Yield Index Option-Adjusted Spread (Daily)",
            "INDPRO": "Industrial Production: Total Index (Monthly)",
            "TCU": "Capacity Utilization: Total Industry (Monthly)",
            "HOUST": "Housing Starts: Total: New Privately Owned Housing Units Started (Monthly)",
            "UMCSENT": "University of Michigan: Consumer Sentiment (Monthly)",
            "RRSFS": "Real Retail and Food Services Sales (Monthly)",
            "T10YIE": "10-Year Breakeven Inflation Rate (Daily)",
            "T5YIE": "5-Year Breakeven Inflation Rate (Daily)",
            "WALCL": "Assets: Total Assets: Total Assets (Weekly - Fed Balance Sheet)",
            "DTWEXBGS": "Nominal Broad U.S. Dollar Index (Daily)",
            "DCOILWTICO": "Crude Oil Prices: West Texas Intermediate (WTI) (Daily)",
            "STLFSI3": "St. Louis Fed Financial Stress Index (Weekly)",
            "TEDRATE": "TED Spread (Daily)",
            "BAA10Y": "Moody's Seasoned Baa Corporate Bond Yield Relative to Yield on 10-Year Treasury Constant Maturity (Daily)",
            "SAHMREALTIME": "Real-time Sahm Rule Recession Indicator (Monthly)",
            "WEI": "Weekly Economic Index (Lewis-Mertens-Stock) (Weekly)",
            "GOLDAMGBD228NLBM": "Gold Fixing Price 10:30 A.M. (London time) in London Bullion Market, based in U.S. Dollars (Daily)",
            "PCOPPUSDM": "Global price of Copper (Monthly)",
            "DEXJPUS": "Japanese Yen to U.S. Dollar Spot Exchange Rate (Daily)",
            "DTB3": "3-Month Treasury Bill Secondary Market Rate, Discount Basis (Daily)",
            "MORTGAGE30US": "30-Year Fixed Rate Mortgage Average in the United States (Weekly)"
        }

    def fetch_fred_series(self, series_id: str, description: str) -> bool:
        """Fetch individual FRED series"""
        if not self.fred_api_key:
            logger.log_error("fetch_fred_series", "FRED_API_KEY not found in .env")
            return False

        try:
            url = "https://api.stlouisfed.org/fred/series/observations"
            params = {
                "series_id": series_id,
                "api_key": self.fred_api_key,
                "file_type": "json",
                "observation_start": "1776-07-04"  # Get everything
            }

            logger.log_info(f"📊 [FRED] Fetching {series_id} ({description})...")

            r = requests.get(url, params=params)
            r.raise_for_status()
            data = r.json()

            observations = data.get("observations", [])
            if not observations:
                logger.log_warning("fetch_fred_series", f"No observations found for {series_id}")
                return False

            # Parse into DataFrame
            df = pd.DataFrame(observations)
            df = df[["date", "value"]]

            # FRED returns '.' for missing values
            df = df[df["value"] != "."]

            # Save to landing zone (CSV format)
            os.makedirs(self.fred_landing_dir, exist_ok=True)
            filepath = os.path.join(self.fred_landing_dir, f"{series_id}.csv")
            df.to_csv(filepath, index=False)

            logger.log_info(f"💾 [FRED] Saved {series_id} to landing zone {filepath} ({len(df)} rows)")
            return True

        except Exception as e:
            logger.log_error("fetch_fred_series", f"Error fetching {series_id}: {str(e)}")
            return False

    def run_fred_pipeline(self) -> Dict:
        """Run complete FRED data collection"""
        logger.log_info(f"🚀 [FRED] Starting FRED Macro Download ({len(self.series_map)} series)...")

        results = {}
        success_count = 0

        for series_id, description in self.series_map.items():
            try:
                success = self.fetch_fred_series(series_id, description)
                results[series_id] = success
                if success:
                    success_count += 1
            except Exception as e:
                logger.log_error("run_fred_pipeline", f"Failed to process series {series_id}: {str(e)}")
                results[series_id] = False

        logger.log_info(f"🎉 [FRED] FRED Download Complete. Success: {success_count}/{len(self.series_map)}")
        return {
            'total_series': len(self.series_map),
            'success_count': success_count,
            'results': results,
            'timestamp': datetime.now().isoformat()
        }

# ======================
# 7.3 GLOBAL MACRO MODULE
# ======================
class GlobalMacroModule:
    """Handles global macroeconomic data collection"""

    def __init__(self):
        # Directories
        self.global_macro_dir = os.path.join(config.PROJECT_ROOT, "bronze", "macro", "global")
        self.global_landing_dir = os.path.join(config.PROJECT_ROOT, "bronze", "landing", "global_raw")
        os.makedirs(self.global_macro_dir, exist_ok=True)
        os.makedirs(self.global_landing_dir, exist_ok=True)

        # Global Series Map
        self.series_map = {
            "EURUSD": {
                "provider": "ECB",
                "dataset": "EXR",
                "series": "D.USD.EUR.SP00.A",
                "desc": "Euro vs USD (ECB Reference Rate)"
            }
        }

    def fetch_global_series(self, key: str, info: Dict) -> bool:
        """Fetch global macro series"""
        logger.log_info(f"📊 [GLOBAL] Fetching {key} ({info['desc']})...")

        try:
            df = dbnomics.fetch_series(info['provider'], info['dataset'], info['series'])

            # Standardize
            if 'period' in df.columns:
                df['date'] = pd.to_datetime(df['period'])
            elif 'original_period' in df.columns:
                df['date'] = pd.to_datetime(df['original_period'])

            # Clean
            df = df.sort_values('date')
            df = df[['date', 'value']].dropna()

            # Save to landing zone (CSV format)
            os.makedirs(self.global_landing_dir, exist_ok=True)
            filepath = os.path.join(self.global_landing_dir, f"{key}.csv")
            df.to_csv(filepath, index=False)

            logger.log_info(f"💾 [GLOBAL] Saved {key} to landing zone {filepath} ({len(df)} rows)")
            return True

        except Exception as e:
            logger.log_error("fetch_global_series", f"Failed {key}: {str(e)}")
            return False

    def run_global_pipeline(self) -> Dict:
        """Run complete global macro data collection"""
        logger.log_info(f"🚀 [GLOBAL] Starting Global Macro Download (DB.nomics)...")

        results = {}
        success_count = 0

        for key, info in self.series_map.items():
            try:
                success = self.fetch_global_series(key, info)
                results[key] = success
                if success:
                    success_count += 1
            except Exception as e:
                logger.log_error("run_global_pipeline", f"Failed to process series {key}: {str(e)}")
                results[key] = False

        logger.log_info(f"🎉 [GLOBAL] Global Download Complete. Success: {success_count}/{len(self.series_map)}")
        return {
            'total_series': len(self.series_map),
            'success_count': success_count,
            'results': results,
            'timestamp': datetime.now().isoformat()
        }

# ======================
# 7.4 PRICE DATA MODULE
# ======================
class PriceDataModule:
    """Handles price data collection with CIK-ticker mapping"""

    def __init__(self):
        self.eodhd_api_key = os.getenv("EODHD_API_KEY")
        self.ticker_cik_map = {}
        self.stats = {
            'total_attempted': 0,
            'primary_success': 0,
            'fallback_success': 0,
            'complete_failures': 0,
            'start_time': None,
            'end_time': None
        }

        # Directories
        self.eodhd_data_dir = os.path.join(config.PROJECT_ROOT, "bronze", "market_data", "eodhd", "daily")
        self.yfinance_data_dir = os.path.join(config.PROJECT_ROOT, "bronze", "market_data", "yfinance", "daily")
        os.makedirs(self.eodhd_data_dir, exist_ok=True)
        os.makedirs(self.yfinance_data_dir, exist_ok=True)

    def load_universe(self) -> Dict:
        """
        Loads the Master Universe and creates ticker->CIK mapping.
        Handles both EODHD and YFinance ticker formats.
        """
        master_universe_file = self.config.MASTER_CIK_LIST

        if not os.path.exists(master_universe_file):
            logger.log_error("load_universe", "Master universe not found. Run universe_pipeline.py first.")
            return {}

        try:
            df = pd.read_parquet(master_universe_file)
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

            logger.log_info(f"📊 [PRICE] Loaded {len(final_map)} tickers from Master Universe")
            return {'eodhd': final_map, 'yfinance': yf_compatible}

        except Exception as e:
            logger.log_error("load_universe", f"Error loading universe: {str(e)}")
            return {}

    def fetch_eodhd_ticker_history(self, ticker: str, cik: str) -> bool:
        """Fetch price history from EODHD API"""
        if not self.eodhd_api_key:
            logger.log_warning("fetch_eodhd_ticker_history", "EODHD_API_KEY not configured")
            return False

        try:
            url = f"https://eodhd.com/api/eod/{ticker}.US"
            params = {
                "api_token": self.eodhd_api_key,
                "fmt": "json",
                "period": "d"
            }

            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()

            if not data or not isinstance(data, list):
                return False

            df = pd.DataFrame(data)
            # Save to EODHD directory
            filepath = os.path.join(self.eodhd_data_dir, f"{cik}.csv")
            df.to_csv(filepath, index=False)
            return True

        except Exception as e:
            logger.log_warning("fetch_eodhd_ticker_history", f"EODHD failed for {ticker}: {str(e)}")
            return False

    def fetch_yfinance_ticker_history(self, ticker: str, cik: str) -> bool:
        """Fetch price history from YFinance"""
        try:
            # YFinance uses ticker format with hyphens
            # EODHD Schema: date,open,high,low,close,adjusted_close,volume
            ticker_obj = yf.Ticker(ticker)
            
            # Use auto_adjust=False to get both Close and Adj Close
            hist = ticker_obj.history(period="max", auto_adjust=False)

            if hist.empty:
                return False

            # Standardize columns to match EODHD format
            hist = hist.reset_index()
            
            # Rename columns
            column_map = {
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Adj Close': 'adjusted_close',
                'Volume': 'volume'
            }
            hist = hist.rename(columns=column_map)
            
            # Select and order columns
            target_cols = ['date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume']
            # Ensure all columns exist
            for col in target_cols:
                if col not in hist.columns:
                    hist[col] = None 
                    
            hist = hist[target_cols]
            
            # Save to EODHD directory (as it is the unified store)
            filepath = os.path.join(self.eodhd_data_dir, f"{cik}.csv")
            hist.to_csv(filepath, index=False)
            logger.log_info(f"💾 [PRICE] Saved YFinance backfill for {ticker} (CIK {cik}) to {filepath}")
            return True

        except Exception as e:
            logger.log_warning("fetch_yfinance_ticker_history", f"YFinance failed for {ticker}: {str(e)}")
            return False

    def fetch_ticker_with_fallback(self, ticker: str, cik: str, source_map: str) -> bool:
        """Fetch data with automatic fallback to secondary source"""
        
        # 0. Check Priority: Local EODHD file
        local_path = os.path.join(self.eodhd_data_dir, f"{cik}.csv")
        if os.path.exists(local_path):
            # If file exists, we assume we have data (or at least attempted it)
            # You might want to check file size or age here if strict update needed
            # logger.log_info(f"✅ [PRICE] Local data found for {ticker} (CIK {cik})")
            # We count this as primary success/skipped
            # self.stats['primary_success'] += 1  # Or track as 'skipped_local'? 
            return True

        success = False

        # 1. Try EODHD API first (only if configured and strict fetching is desired, 
        # but user implied backfill priority. We will KEEP EODHD API call if key exists
        # so we get fresh data if missing, but typically we might just skip to YF if key is missing)
        # Note: If we want to strictly ONLY use YF for missing data to save API calls, we could comment this out.
        # But 'fallback' implies trying primary first. I will keep it as is, but it runs after local check.
        if self.eodhd_api_key:
             success = self.fetch_eodhd_ticker_history(ticker, cik)

        # 2. Try YFinance fallback if EODHD failed (or was skipped)
        if not success:
            success = self.fetch_yfinance_ticker_history(ticker, cik)

        if success:
            if source_map == "primary":
                self.stats['primary_success'] += 1
            else:
                self.stats['fallback_success'] += 1
        else:
            self.stats['complete_failures'] += 1

        return success

    def run_price_pipeline(self, max_tickers: Optional[int] = None, max_workers: int = 50) -> Dict:
        """Run complete price data collection pipeline"""
        self.stats['start_time'] = time.time()

        universe_data = self.load_universe()
        if not universe_data:
            logger.log_error("run_price_pipeline", "No universe data loaded. Aborting.")
            return {'error': 'No universe data'}

        # Use EODHD mapping as primary
        ticker_map = universe_data['eodhd']
        tickers_to_process = list(ticker_map.items())

        if max_tickers:
            tickers_to_process = tickers_to_process[:max_tickers]
            logger.log_info(f"📊 [PRICE] Limiting to first {max_tickers} tickers")

        self.stats['total_attempted'] = len(tickers_to_process)

        logger.log_info(f"🚀 [PRICE] Starting price pipeline for {len(tickers_to_process)} tickers "
                       f"using {max_workers} workers (Primary: EODHD, Fallback: YFinance)")

        success_count = 0
        import concurrent.futures
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

        logger.log_info(f"🎉 [PRICE] Pipeline completed in {duration:.1f} seconds")
        logger.log_info(f"📊 [PRICE] Success: {success_count}/{len(tickers_to_process)} tickers")
        logger.log_info(f"📊 [PRICE] Primary source success: {self.stats['primary_success']}")
        logger.log_info(f"📊 [PRICE] Fallback source success: {self.stats['fallback_success']}")
        logger.log_info(f"📊 [PRICE] Complete failures: {self.stats['complete_failures']}")

        return {
            'success_count': success_count,
            'total_attempted': len(tickers_to_process),
            'duration_seconds': duration,
            'stats': self.stats,
            'timestamp': datetime.now().isoformat()
        }

# ======================
# 8. MAIN PIPELINE CLASS
# ======================
class SECDataPipeline:
    """Unified SEC Data Pipeline"""

    def __init__(self):
        self.download_module = DownloadModule()
        self.processing_module = ProcessingModule()
        self.universe_module = CIKUniverseModule()

    def run_complete_pipeline(self, batch_size: int = 100, force: bool = False, filter_foreign: bool = False, min_market_cap: float = 10_000_000):
        """Run the complete SEC data pipeline"""
        logger.log_info("🚀 Starting complete SEC data pipeline...")

        # 1. Process all SEC bulk data
        self.processing_module.process_all_quarters(force=force)

        # 2. Create master CIK universe with filtering options
        self.universe_module.create_final_universe(filter_foreign=filter_foreign, min_market_cap=min_market_cap)

        # 3. Download missing CIK fundamentals - DISABLED as per user request
        # self.download_module.download_missing_ciks(batch_size=batch_size)
        logger.log_info("📋 Skipping missing CIK download as per user configuration")

        logger.log_info("🎉 Complete SEC data pipeline finished!")

    def run_bulk_processing_only(self, quarters: Optional[str] = None, force: bool = False):
        """Run only SEC bulk data processing"""
        logger.log_info("📦 Starting SEC bulk data processing...")
        self.processing_module.process_all_quarters(quarters=quarters, force=force)
        logger.log_info("✅ SEC bulk processing complete")

    def run_universe_creation_only(self, filter_foreign: bool = False, min_market_cap: float = 10_000_000):
        """Run only master CIK universe creation"""
        logger.log_info("🌍 Starting master CIK universe creation...")
        self.universe_module.create_final_universe(filter_foreign=filter_foreign, min_market_cap=min_market_cap)
        logger.log_info("✅ Master CIK universe creation complete")

    def run_missing_download_only(self, batch_size: int = 100, limit: Optional[int] = None):
        """Run only missing CIK download"""
        logger.log_info("🔍 Starting missing CIK download...")
        # self.download_module.download_missing_ciks(batch_size=batch_size, limit=limit)
        logger.log_info("📋 Skipping missing CIK download as per user configuration")
        logger.log_info("✅ Missing CIK download complete")

# ======================
# 9. COMPREHENSIVE PIPELINE CLASS
# ======================
class ComprehensiveDataPipeline:
    """Unified Comprehensive Data Pipeline"""

    def __init__(self):
        # Existing SEC modules
        self.sec_pipeline = SECDataPipeline()
        self.download_module = DownloadModule()
        self.processing_module = ProcessingModule()
        self.universe_module = CIKUniverseModule()

        # New data modules
        self.finra_module = FINRAModule()
        self.fred_module = FREDModule()
        self.global_module = GlobalMacroModule()
        self.price_module = PriceDataModule()

    def run_comprehensive_pipeline(self, **kwargs):
        """Run complete comprehensive data collection workflow"""
        logger.log_info("🚀 Starting comprehensive data collection pipeline...")

        results = {
            'sec': {},
            'finra': {},
            'fred': {},
            'global': {},
            'price': {}
        }

        # 1. Run SEC data pipeline (existing)
        logger.log_info("📦 Processing SEC data...")
        results['sec'] = self.sec_pipeline.run_complete_pipeline(**kwargs)

        # 2. Run FINRA data collection
        logger.log_info("🏢 Collecting FINRA data...")
        results['finra'] = self.finra_module.run_finra_pipeline()

        # 3. Run FRED macro data collection
        logger.log_info("📊 Downloading FRED macroeconomic data...")
        results['fred'] = self.fred_module.run_fred_pipeline()

    def run_full_backfill(self, batch_size=100, workers=50, force=False):
        """
        Run the specific backfill sequence:
        1. Process SEC Bulk (to Bronze Delta)
        2. Create Master Universe (from Bronze Delta)
        3. Backfill Prices (using Universe)
        4. Backfill Fundamentals (using Universe)
        """
        results = {}
        logger.log_info("\n" + "="*50)

        logger.log_info("🚀 STARTING FULL BACKFILL ORCHESTRATION")
        logger.log_info("="*50 + "\n")

        # 1. Process all SEC Bulk data
        logger.log_info("STEP 1: Processing SEC Bulk ZIPs -> Bronze Delta")
        self.sec_pipeline.run_bulk_processing_only(force=force)

        # 2. Create Master CIK Universe
        logger.log_info("\nSTEP 2: Creating Master CIK Universe")
        self.sec_pipeline.run_universe_creation_only(filter_foreign=False, min_market_cap=10_000_000)

        # 3. Run External Data Backfills (Price, FRED, FINRA, Global)
        logger.log_info("\nSTEP 3: Backfilling External Data (Price, FRED, FINRA, Macro)")

        logger.log_info("  > Backfilling Prices...")
        self.price_module.run_price_pipeline(max_workers=workers)

        logger.log_info("  > Backfilling FRED Data...")
        self.fred_module.run_fred_pipeline()

        logger.log_info("  > Backfilling FINRA Data...")
        self.finra_module.run_finra_pipeline()

        logger.log_info("  > Backfilling Global Macro Data...")
        self.global_module.run_global_pipeline()

        logger.log_info("\n" + "="*50)
        logger.log_info("🎉 BACKFILL COMPLETE")
        logger.log_info("="*50 + "\n")

        # 4. Run global macro data collection
        logger.log_info("🌍 Fetching global macro data...")
        results['global'] = self.global_module.run_global_pipeline()

        # 5. Run price data collection (with limit to prevent resource issues)
        logger.log_info("💰 Collecting price data...")
        results['price'] = self.price_module.run_price_pipeline(max_tickers=1000, max_workers=20)

        logger.log_info("🎉 Comprehensive data collection complete!")
        return results

    def run_finra_only(self):
        """Run only FINRA data collection"""
        logger.log_info("🏢 Starting FINRA data collection...")
        result = self.finra_module.run_finra_pipeline()
        logger.log_info("✅ FINRA data collection complete")
        return result

    def run_fred_only(self):
        """Run only FRED data collection"""
        logger.log_info("📊 Starting FRED macroeconomic data collection...")
        result = self.fred_module.run_fred_pipeline()
        logger.log_info("✅ FRED data collection complete")
        return result

    def run_global_only(self):
        """Run only global macro data collection"""
        logger.log_info("🌍 Starting global macro data collection...")
        result = self.global_module.run_global_pipeline()
        logger.log_info("✅ Global macro data collection complete")
        return result

    def run_price_only(self, max_tickers: Optional[int] = None, max_workers: int = 50):
        """Run only price data collection"""
        logger.log_info("💰 Starting price data collection...")
        result = self.price_module.run_price_pipeline(max_tickers=max_tickers, max_workers=max_workers)
        logger.log_info("✅ Price data collection complete")
        return result

# ======================
# 10. MAIN EXECUTION
# ======================
def main():
    """Main entry point with enhanced CLI argument parsing"""
    import argparse

    # Initialize both pipelines for backward compatibility
    sec_pipeline = SECDataPipeline()
    comprehensive_pipeline = ComprehensiveDataPipeline()

    # Enhanced argument parser
    parser = argparse.ArgumentParser(
        description="Comprehensive Data Pipeline - Unified data processing (SEC, FINRA, FRED, Global Macro, Price)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Main command argument
    parser.add_argument(
        "command",
        nargs="?",
        default="comprehensive",
        choices=["complete", "bulk", "universe", "missing", "status",
                 "comprehensive", "finra", "fred", "global", "price", "backfill"],
        help="Command to execute"
    )

    # Global options
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without executing")
    parser.add_argument("--force", action="store_true", help="Force re-processing even if data exists")

    # Command-specific options
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for downloads (complete/missing/price commands)")
    parser.add_argument("--quarters", help="Specific quarters to process (bulk command)")
    parser.add_argument("--filter-foreign", action="store_true", help="Filter out foreign filers (universe command)")
    parser.add_argument("--min-market-cap", type=float, default=10_000_000, help="Minimum market cap in USD (universe command)")
    parser.add_argument("--limit", type=int, help="Limit number of missing CIKs to process (missing command)")
    parser.add_argument("--detailed", action="store_true", help="Show detailed status (status command)")
    parser.add_argument("--max-tickers", type=int, help="Limit number of tickers to process (price command)")
    parser.add_argument("--workers", type=int, default=50, help="Number of worker threads (price command)")

    args = parser.parse_args()

    # Set verbose logging if requested
    if args.verbose:
        logger.logger.setLevel(logging.DEBUG)
        for handler in logger.logger.handlers:
            handler.setLevel(logging.DEBUG)

    if args.dry_run:
        logger.log_info("🔍 DRY RUN MODE: No actual processing will occur")
        _show_dry_run_plan(args)
        return

    # Execute the requested command
    try:
        if args.command == "comprehensive":
            logger.log_info("🚀 Starting comprehensive data collection pipeline...")
            comprehensive_pipeline.run_comprehensive_pipeline(
                batch_size=args.batch_size,
                force=args.force,
                filter_foreign=args.filter_foreign,
                min_market_cap=args.min_market_cap
            )
        elif args.command == "finra":
            logger.log_info("🏢 Starting FINRA data collection...")
            comprehensive_pipeline.run_finra_only()
        elif args.command == "fred":
            logger.log_info("📊 Starting FRED macroeconomic data collection...")
            comprehensive_pipeline.run_fred_only()
        elif args.command == "global":
            logger.log_info("🌍 Starting global macro data collection...")
            comprehensive_pipeline.run_global_only()
        elif args.command == "price":
            logger.log_info("💰 Starting price data collection...")
            comprehensive_pipeline.run_price_only(
                max_tickers=args.max_tickers,
                max_workers=args.workers
            )

        elif args.command == "backfill":
            logger.log_info("🚀 Starting full backfill sequence...")
            comprehensive_pipeline.run_full_backfill(
                batch_size=args.batch_size,
                workers=args.workers,
                force=args.force
            )
        elif args.command == "complete":
            logger.log_info("🚀 Starting complete SEC data pipeline...")
            sec_pipeline.run_complete_pipeline(
                batch_size=args.batch_size,
                force=args.force,
                filter_foreign=args.filter_foreign,
                min_market_cap=args.min_market_cap
            )
        elif args.command == "bulk":
            logger.log_info("📦 Starting SEC bulk data processing...")
            sec_pipeline.run_bulk_processing_only(
                quarters=args.quarters,
                force=args.force
            )
        elif args.command == "universe":
            logger.log_info("🌍 Starting master CIK universe creation...")
            sec_pipeline.run_universe_creation_only(
                filter_foreign=args.filter_foreign,
                min_market_cap=args.min_market_cap
            )
        elif args.command == "missing":
            logger.log_info("🔍 Starting missing CIK download...")
            sec_pipeline.run_missing_download_only(
                batch_size=args.batch_size,
                limit=args.limit
            )
        elif args.command == "status":
            _show_status(detailed=args.detailed)
        else:
            parser.print_help()
    except Exception as e:
        logger.log_error("main", f"Pipeline execution failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def _show_dry_run_plan(args):
    """Show what would be executed in dry-run mode"""
    logger.log_info("📋 DRY RUN PLAN")
    logger.log_info(f"Command: {args.command}")

    if args.command in ["complete", "missing", "price"]:
        logger.log_info(f"Batch Size: {args.batch_size}")
    if args.command == "bulk":
        logger.log_info(f"Quarters: {args.quarters or 'All available'}")
    if args.command in ["complete", "universe"]:
        logger.log_info(f"Filter Foreign: {args.filter_foreign}")
        logger.log_info(f"Min Market Cap: ${args.min_market_cap:,.0f}")
    if args.command == "missing" and args.limit:
        logger.log_info(f"Limit: {args.limit}")
    if args.command == "price":
        logger.log_info(f"Max Tickers: {args.max_tickers or 'All available'}")
        logger.log_info(f"Workers: {args.workers}")
    if args.force:
        logger.log_info("Force Processing: Yes")

    if args.command in ["comprehensive", "finra", "fred", "global", "price"]:
        logger.log_info("\nThis would execute the comprehensive data pipeline with the specified parameters")
    else:
        logger.log_info("\nThis would execute the SEC data pipeline with the specified parameters")

def _show_status(detailed=False):
    """Show current SEC data status"""
    logger.log_info("📊 SEC Data Status")

    # Check for SEC bulk data
    sec_bulk_dir = os.path.join(config.PROJECT_ROOT, "bronze", "landing", "sec_raw")
    sec_zip_files = list(Path(sec_bulk_dir).glob("*.zip")) if os.path.exists(sec_bulk_dir) else []

    # Check for processed data
    raw_output_dir = os.path.join(config.PROJECT_ROOT, "bronze", "fundamentals", "sec_bulk", "parquet")
    parquet_files = list(Path(raw_output_dir).glob("*.parquet")) if os.path.exists(raw_output_dir) else []

    # Remove legacy CSVs if they exist
    for f in ["master_cik_list.csv", "master_cik_universe.csv", "foreign_stocks.csv"]:
        legacy_path = os.path.join('references', f) # Assuming legacy CSVs were in 'references'
        if os.path.exists(legacy_path):
            logger.log_info(f"🗑️ Removing legacy CSV: {legacy_path}")
            os.remove(legacy_path)

    # Check for master universe files (now parquet)
    master_files = []
    for f in ["master_cik_list.parquet", "master_cik_universe.parquet", "foreign_stocks.parquet", "cik_universe_summary.json"]:
        if os.path.exists(os.path.join('references', f)):
            master_files.append(f)

    logger.log_info(f"SEC Bulk ZIPs: {len(sec_zip_files)} files")
    logger.log_info(f"Processed Parquet: {len(parquet_files)} files")
    logger.log_info(f"Master Universe Files: {len(master_files)} files")

    if detailed:
        _show_detailed_status()

def _show_detailed_status():
    """Show detailed status breakdown"""
    logger.log_info("📊 Detailed SEC Data Breakdown")

    # Show quarter files
    sec_bulk_dir = os.path.join(config.PROJECT_ROOT, "bronze", "landing", "sec_raw")
    if os.path.exists(sec_bulk_dir):
        zip_files = sorted(Path(sec_bulk_dir).glob("*.zip"))
        if zip_files:
            logger.log_info("SEC Bulk ZIP Files:")
            for i, zip_file in enumerate(zip_files, 1):
                size_mb = zip_file.stat().st_size / (1024 * 1024)
                logger.log_info(f"  {i}. {zip_file.name} ({size_mb:.1f} MB)")

    # Show processed parquet files
    raw_output_dir = os.path.join(config.PROJECT_ROOT, "bronze", "fundamentals", "sec_bulk", "parquet")
    if os.path.exists(raw_output_dir):
        parquet_files = sorted(Path(raw_output_dir).glob("*.parquet"))
        if parquet_files:
            logger.log_info("Processed Parquet Files:")
            for i, parquet_file in enumerate(parquet_files, 1):
                size_mb = parquet_file.stat().st_size / (1024 * 1024)
                logger.log_info(f"  {i}. {parquet_file.name} ({size_mb:.1f} MB)")

if __name__ == "__main__":
    main()