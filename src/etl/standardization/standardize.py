"""
Refactored Unified Standardization Module

Transforms Bronze layer data into Silver layer Delta Tables with
modular, maintainable architecture.

Key Improvements:
- Modular processor design for different statement types
- Standardized CIK handling using CIKHandler utility
- Memory-efficient chunking with ChunkProcessor
- Comprehensive validation with FinancialDataValidator
- Clear separation of concerns across components
"""

import os
import sys
import pandas as pd
import numpy as np
import polars as pl
import glob
import json
import logging
from typing import Union, List, Any, Optional, Tuple, Dict
from deltalake import write_deltalake
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Import refactored components
from src.utils.delta_lake_compatibility import DeltaLakeCompatibility
from src.utils.logging import DataStoreLogger
from src.utils.cik_handler import CIKHandler

# ============================================================================
# INTERNAL CONFIGURATION SYSTEM
# ============================================================================
class _Config:
    """Internal configuration manager for standardization module."""

    def __init__(self):
        self.PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        sys.path.insert(0, self.PROJECT_ROOT)

    # Path Configuration - All paths now centralized
    BRONZE_FUNDAMENTALS = os.path.join(self.PROJECT_ROOT, "bronze/fundamentals/sec_bulk")
    SILVER_FUNDAMENTALS = os.path.join(self.PROJECT_ROOT, "silver/fundamentals")
    
    # File Patterns
    PARQUET_PATTERN = "*.parquet"

# Initialize global configuration
config = _Config()
logger = DataStoreLogger()


# ============================================================================
# REFACTORED SEC MAPPER WITH MODULAR DESIGN
# ============================================================================
class SECMapper:
    """
    Standardizes raw SEC XBRL tags into common financial metrics.

    Refactored to use modular processors for improved maintainability:
    - IncomeStatementProcessor: Handles income statement data
    - BalanceSheetProcessor: Handles balance sheet data  
    - CashFlowProcessor: Handles cash flow data
    - FinancialDataValidator: Validates processed data
    - ChunkProcessor: Manages memory-efficient processing
    """
    
    # Priority Maps: List of tags in order of preference
    MAP_REVENUE = ['RevenueFromContractWithCustomerExcludingAssessedTax', 'Revenues', 'SalesRevenueNet']
    MAP_COST_OF_REVENUE = ['CostOfRevenue', 'CostOfGoodsAndServicesSold']
    MAP_GROSS_PROFIT = ['GrossProfit']
    MAP_R_AND_D = ['ResearchAndDevelopmentExpense']
    MAP_SGA = ['SellingGeneralAndAdministrativeExpense']
    MAP_OP_EXPENSE = ['OperatingExpenses']
    MAP_OP_INCOME = ['OperatingIncomeLoss']
    MAP_INTEREST_EXPENSE = ['InterestExpense']
    MAP_TOTAL_ASSETS = ['Assets']
    MAP_CURRENT_ASSETS = ['AssetsCurrent']
    MAP_CASH_AND_EQUIV = ['CashAndCashEquivalentsAtCarryingValue']
    MAP_TOTAL_LIABILITIES = ['Liabilities']
    MAP_CURRENT_LIABILITIES = ['LiabilitiesCurrent']
    MAP_TOTAL_EQUITY = ['StockholdersEquity']
    MAP_CF_OPERATING = ['NetCashProvidedByUsedInOperatingActivities']
    MAP_DEPRECIATION = ['DepreciationDepletionAndAmortization']
    MAP_CAPEX = ['PaymentsToAcquirePropertyPlantAndEquipment']

    @staticmethod
    def standardize(df_raw: pd.DataFrame, chunk_size: int = 1000) -> pd.DataFrame:
        """
        Standardize raw SEC XBRL financial data into a consistent format.

        Refactored to use modular processors for improved maintainability
        and reduced complexity. Addresses technical debt from monolithic design.

        Args:
            df_raw: Raw DataFrame containing financial data from SEC filings
            chunk_size: Number of unique CIKs to process in each batch

        Returns:
            pd.DataFrame: Standardized DataFrame with comprehensive financial metrics
        """
        # Validate input DataFrame
        if df_raw.empty:
            logger.warning("Empty DataFrame provided to standardize()")
            return pd.DataFrame()

        # Standardize CIK format using new utility function
        if 'cik' in df_raw.columns:
            df_raw['cik'] = CIKHandler.normalize_cik(df_raw['cik'])

        # Filter for consolidated data
        if 'segments' in df_raw.columns:
            df_clean = df_raw[df_raw['segments'].isna() | (df_raw['segments'] == '')].copy()
        else:
            df_clean = df_raw.copy()

        # Validate CIK data after filtering
        if not CIKHandler.validate_cik_consistency(df_clean):
            logger.warning("CIK validation failed - some CIKs may be invalid")

        # Import processors (lazy import to avoid circular dependency)
        from src.etl.standardization.financial_processor import (
            FinancialDataStandardizer, 
            ChunkProcessor
        )

        # Initialize processors
        standardizer = FinancialDataStandardizer(SECMapper)
        chunk_processor = ChunkProcessor(chunk_size)

        # Process in chunks using refactored architecture
        standardized_chunks = chunk_processor.process_data_in_chunks(
            df_clean, standardizer
        )

        # Concatenate all chunks
        if not standardized_chunks:
            return pd.DataFrame()

        df_std = pd.DataFrame(standardized_chunks)
        logger.info(f"Successfully standardized {len(df_std)} rows across {len(standardized_chunks)} chunks")
        
        return df_std.reset_index()


# ============================================================================
# VALIDATION HELPER FUNCTIONS
# ============================================================================
def validate_standardized_output(df_std: pd.DataFrame) -> bool:
    """
    Validate the output of standardization process.
    
    Args:
        df_std: Standardized DataFrame to validate
        
    Returns:
        True if validation passes, False otherwise
    """
    try:
        # Check for required columns
        required_columns = ['cik', 'period', 'totalRevenue', 'netIncome']
        missing_columns = [col for col in required_columns if col not in df_std.columns]
        
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return False
        
        # Check data consistency
        if df_std.empty:
            logger.warning("Standardized DataFrame is empty")
            return False
        
        # Validate CIK format
        if not CIKHandler.validate_cik_consistency(df_std):
            logger.error("CIK consistency validation failed")
            return False
        
        # Check for reasonable financial values
        if 'totalRevenue' in df_std.columns:
            negative_revenue = (df_std['totalRevenue'] < 0).sum()
            if negative_revenue > 0:
                logger.warning(f"Found {negative_revenue} rows with negative revenue")
        
        logger.info("Standardization output validation passed")
        return True
        
    except Exception as e:
        logger.error(f"Standardization validation error: {e}")
        return False


# ============================================================================
# CONVENIENCE FUNCTIONS FOR EXTERNAL CALLING
# ============================================================================
def standardize_fundamentals() -> None:
    """Standardize SEC fundamentals data using refactored architecture."""
    try:
        logger.info("Starting SEC fundamentals standardization...")
        
        # Read all Parquet files
        parquet_files = glob.glob(os.path.join(config.BRONZE_FUNDAMENTALS, config.PARQUET_PATTERN))
        logger.info(f"Found {len(parquet_files)} Parquet files to process")
        
        all_data = []
        
        for file in parquet_files:
            try:
                df_raw = pd.read_parquet(file)
                logger.info(f"Processing {os.path.basename(file)}: {len(df_raw)} rows")
                
                # Standardize using refactored method
                df_std = SECMapper.standardize(df_raw)
                
                if not df_std.empty:
                    all_data.append(df_std)
                    logger.info(f"✅ Standardized {os.path.basename(file)}: {len(df_std)} rows")
                else:
                    logger.warning(f"⚠️  Empty result for {os.path.basename(file)}")
                    
            except Exception as e:
                logger.error(f"❌ Failed to process {os.path.basename(file)}: {e}")
        
        if all_data:
            # Combine all standardized data
            df_final = pd.concat(all_data, ignore_index=True)
            logger.info(f"Combined data: {len(df_final)} total rows")
            
            # Validate output
            if validate_standardized_output(df_final):
                # Write to Delta Lake with transaction safety
                compatibility = DeltaLakeCompatibility(config.SILVER_FUNDAMENTALS, enable_transactions=True)
                compatibility.safe_write_dataframe(df_final, mode="overwrite")
                logger.info("✅ Fundamentals data written to Silver layer")
            else:
                logger.error("❌ Standardization validation failed")
        else:
            logger.warning("⚠️  No data processed")
            
    except Exception as e:
        logger.error(f"Critical error in fundamentals standardization: {e}")
        raise


# ============================================================================
# MAIN EXECUTION BLOCK
# ============================================================================
if __name__ == "__main__":
    standardize_fundamentals()
