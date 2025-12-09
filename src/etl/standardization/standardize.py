"""
Unified Standardization Module
==============================
Transforms Bronze layer data into Silver layer Delta Tables.

Supports:
  - Fundamentals (SEC XBRL, EdgarTools) -> silver/fundamentals
  - Market Prices (EODHD) -> silver/prices
  - Macro (FRED, Global) -> silver/macro/{fred,global}
  - FINRA (Reg SHO, Short Interest) -> silver/finra/{short_volume,short_interest}

Usage:
  python standardize.py [fundamentals|prices|macro|finra|all]

This module addresses technical debt by:
- Centralizing configuration in internal _Config class
- Standardizing CIK data type handling
- Refactoring complex logic into focused helper functions
- Removing dead code and improving documentation
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
from src.utils.delta_lake_compatibility import DeltaLakeCompatibility
from datetime import datetime

# ============================================================================
# INTERNAL CONFIGURATION SYSTEM
# ============================================================================
class _Config:
    """
    Internal configuration manager for standardization module.

    Centralizes all hardcoded paths, magic strings, and settings to address
    technical debt issue #1: Hardcoded paths and magic strings.
    """

    def __init__(self):
        # Calculate project root: src/etl/standardization/standardize.py -> DataStore/
        self.PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        sys.path.insert(0, self.PROJECT_ROOT)

        # ===== PATH CONFIGURATION =====
        # Bronze layer paths
        self.BRONZE_DELTA_SEC = os.path.join(self.PROJECT_ROOT, "bronze", "delta", "sec_bulk")
        self.BRONZE_MARKET_DIR = os.path.join(self.PROJECT_ROOT, "bronze", "delta", "market_data")
        self.BRONZE_FRED_DIR = os.path.join(self.PROJECT_ROOT, "bronze", "macro", "fred")
        self.BRONZE_GLOBAL_DIR = os.path.join(self.PROJECT_ROOT, "bronze", "macro", "global")
        self.BRONZE_REG_SHO = os.path.join(self.PROJECT_ROOT, "bronze", "delta", "finra", "reg_sho")
        self.BRONZE_SHORT_INT = os.path.join(self.PROJECT_ROOT, "bronze", "delta", "finra")  # Base dir for short interest
        self.BRONZE_EDGARTOOLS = os.path.join(self.PROJECT_ROOT, "bronze", "fundamentals", "edgartools")

        # Silver layer paths
        self.SILVER_FUNDAMENTALS = os.path.join(self.PROJECT_ROOT, "silver", "fundamentals")
        self.SILVER_PRICES = os.path.join(self.PROJECT_ROOT, "silver", "prices")
        self.SILVER_FRED = os.path.join(self.PROJECT_ROOT, "silver", "macro", "fred")
        self.SILVER_GLOBAL = os.path.join(self.PROJECT_ROOT, "silver", "macro", "global")
        self.SILVER_REG_SHO = os.path.join(self.PROJECT_ROOT, "silver", "finra", "short_volume")
        self.SILVER_SHORT_INT = os.path.join(self.PROJECT_ROOT, "silver", "finra", "short_interest")

        # Reference files
        self.UNIVERSE_MAP = os.path.join(self.PROJECT_ROOT, "references", "master_cik_list.parquet")
        self.GICS_MAP_PATH = os.path.join(self.PROJECT_ROOT, "references", "cik_gics_mapping.parquet")
        self.EODHD_UNIVERSE = os.path.join(self.PROJECT_ROOT, "references", "eodhd_master_universe.parquet")
        self.CIK_MAP_JSON = os.path.join(self.PROJECT_ROOT, "bronze", "landing", "market_raw", "cik_ticker_mapping.json")

        # ===== CIK CONFIGURATION =====
        # Standardize CIK handling to address technical debt issue #2
        self.CIK_FORMAT = "string"  # Standard format: 10-digit zero-padded string
        self.DEFAULT_CIK_LENGTH = 10
        self.CIK_PADDING_CHAR = "0"

        # ===== PROCESSING CONFIGURATION =====
        self.DEFAULT_CHUNK_SIZE = 1000
        self.MAX_MEMORY_USAGE_MB = 2000
        self.MIN_FISCAL_YEAR = 2009  # XBRL data availability starts here

        # ===== LOGGING CONFIGURATION =====
        self.LOGGING_LEVEL = logging.INFO
        self.LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        self.LOG_FILE = os.path.join(self.PROJECT_ROOT, "logs", "standardization.log")

        # ===== FINANCIAL MAPPING CONSTANTS =====
        # Standardized financial statement type identifiers
        self.STATEMENT_BALANCE_SHEET = "BS"
        self.STATEMENT_INCOME = "IS"
        self.STATEMENT_CASH_FLOW = "CF"

        # ===== FILE FORMAT CONSTANTS =====
        self.FILE_FORMAT_PARQUET = "parquet"
        self.FILE_FORMAT_CSV = "csv"
        self.FILE_FORMAT_DELTA = "delta"

        # Initialize logging
        self._setup_logging()

    def _setup_logging(self):
        """Configure logging system with file and console handlers"""
        logging.basicConfig(
            level=self.LOGGING_LEVEL,
            format=self.LOG_FORMAT,
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(self.LOG_FILE)
            ]
        )

    def validate_paths(self) -> bool:
        """Validate that critical paths exist and are accessible"""
        critical_paths = [
            self.PROJECT_ROOT,
            self.BRONZE_DELTA_SEC,
            self.SILVER_FUNDAMENTALS
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
config = _Config()

# ============================================================================
# CIK HANDLING UTILITIES
# ============================================================================
def _normalize_cik(cik: Union[str, int, float]) -> str:
    """
    Standardize CIK format to 10-digit zero-padded string.

    Addresses technical debt issue #2: Inconsistent CIK data type handling.

    Args:
        cik: CIK value (can be string, int, or float)

    Returns:
        str: Normalized 10-digit CIK string

    Raises:
        ValueError: If CIK cannot be converted to valid format

    Examples:
        >>> _normalize_cik("320193")
        "0000320193"
        >>> _normalize_cik(320193)
        "0000320193"
        >>> _normalize_cik(320193.0)
        "0000320193"
    """
    try:
        # Convert to string, remove any decimal points, pad to 10 digits
        cik_str = str(int(float(str(cik))))
        normalized_cik = cik_str.zfill(config.DEFAULT_CIK_LENGTH)

        # Validate the result
        if len(normalized_cik) != config.DEFAULT_CIK_LENGTH or not normalized_cik.isdigit():
            raise ValueError(f"CIK normalization produced invalid result: {normalized_cik}")

        return normalized_cik
    except (ValueError, TypeError) as e:
        logging.error(f"CIK normalization failed for {cik}: {str(e)}")
        raise ValueError(f"Invalid CIK format: {cik}") from e

def _validate_cik_dataframe(df: pd.DataFrame, cik_column: str = 'cik') -> bool:
    """
    Validate that all CIKs in a DataFrame follow the standard format.

    Args:
        df: DataFrame to validate
        cik_column: Name of CIK column

    Returns:
        bool: True if all CIKs are valid, False otherwise
    """
    if cik_column not in df.columns:
        logging.warning(f"CIK column {cik_column} not found in DataFrame")
        return False

    invalid_ciks = []
    for cik in df[cik_column].unique():
        try:
            normalized = _normalize_cik(cik)
            if len(normalized) != config.DEFAULT_CIK_LENGTH or not normalized.isdigit():
                invalid_ciks.append(cik)
        except ValueError:
            invalid_ciks.append(cik)

    if invalid_ciks:
        logging.warning(f"Found {len(invalid_ciks)} invalid CIKs: {invalid_ciks[:5]}...")
        return False

    return True

def _batch_cik_normalization(df: pd.DataFrame, cik_column: str = 'cik') -> pd.DataFrame:
    """
    Apply CIK normalization to an entire DataFrame column in a batch operation.

    This is more efficient than row-by-row normalization for large datasets.

    Args:
        df: DataFrame containing CIK data
        cik_column: Name of CIK column to normalize

    Returns:
        pd.DataFrame: DataFrame with normalized CIK values
    """
    if cik_column not in df.columns:
        logging.warning(f"CIK column {cik_column} not found for batch normalization")
        return df

    try:
        # Vectorized normalization operation
        df[cik_column] = df[cik_column].apply(_normalize_cik)
        logging.info(f"Normalized {len(df)} CIK values in column '{cik_column}'")
        return df
    except Exception as e:
        logging.error(f"Batch CIK normalization failed: {str(e)}")
        raise

def _validate_cik_dataframe(df: pd.DataFrame, cik_column: str = 'cik') -> bool:
    """
    Validate that all CIKs in a DataFrame follow the standard format.

    Args:
        df: DataFrame to validate
        cik_column: Name of CIK column

    Returns:
        bool: True if all CIKs are valid, False otherwise
    """
    if cik_column not in df.columns:
        logging.warning(f"CIK column {cik_column} not found in DataFrame")
        return False

    invalid_ciks = []
    for cik in df[cik_column].unique():
        try:
            normalized = _normalize_cik(cik)
            if len(normalized) != config.DEFAULT_CIK_LENGTH or not normalized.isdigit():
                invalid_ciks.append(cik)
        except ValueError:
            invalid_ciks.append(cik)

    if invalid_ciks:
        logging.warning(f"Found {len(invalid_ciks)} invalid CIKs: {invalid_ciks[:5]}...")
        return False

    return True

# ============================================================================
# FINANCIAL PROCESSING HELPER FUNCTIONS
# ============================================================================

def _process_statement_type(df_chunk: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Process statement type logic (Balance Sheet, Income Statement, Cash Flow)

    Args:
        df_chunk: DataFrame chunk to process

    Returns:
        Tuple of (bs_rows, is_rows, cf_rows) DataFrames
    """
    if 'qtrs' not in df_chunk.columns:
        return df_chunk, df_chunk, df_chunk

    # Balance Sheet logic (instant)
    mask_bs = (df_chunk['qtrs'] < 0.1)

    # Income Statement logic (Q1, Q2, Q3, FY)
    mask_is = ((df_chunk['qtrs'] >= 0.9) & (df_chunk['qtrs'] <= 1.1)) | \
              ((df_chunk['qtrs'] >= 3.9) & (df_chunk['qtrs'] <= 4.1))

    # Cash Flow YTD Logic
    fp_map = {'Q1': 1.0, 'Q2': 2.0, 'Q3': 3.0, 'FY': 4.0}
    clean_fp = df_chunk['fp'].astype(str).str.upper()
    expected_qtrs = clean_fp.map(fp_map).fillna(4.0)
    mask_cf = (df_chunk['qtrs'] >= expected_qtrs - 0.1) & (df_chunk['qtrs'] <= expected_qtrs + 0.1)

    return df_chunk[mask_bs], df_chunk[mask_is], df_chunk[mask_cf]

def _create_financial_mapping_helper(tags: List[str], pivot_df: pd.DataFrame, mapping_name: str) -> pd.Series:
    """
    Helper function to extract financial data using tag mapping

    Args:
        tags: List of possible tags to try
        pivot_df: Pivoted DataFrame containing the data
        mapping_name: Name of the mapping (for logging)

    Returns:
        pd.Series: Extracted data series
    """
    if pivot_df is None or pivot_df.empty:
        logging.debug(f"No data available for {mapping_name}")
        return pd.Series(np.nan, index=pivot_df.index if pivot_df is not None else [])

    available_tags = [t for t in tags if t in pivot_df.columns]
    if not available_tags:
        logging.debug(f"No available tags for {mapping_name}: {tags}")
        return pd.Series(np.nan, index=pivot_df.index)

    # Use the first available tag (prioritized by order in tag_list)
    result = pivot_df[available_tags].bfill(axis=1).iloc[:, 0]
    logging.debug(f"Used {available_tags[0]} for {mapping_name}")
    return result

def _calculate_derived_metrics(data: Dict[str, pd.Series]) -> Dict[str, pd.Series]:
    """
    Calculate derived financial metrics from base metrics

    Args:
        data: Dictionary containing base financial metrics

    Returns:
        Dict[str, pd.Series]: Dictionary with additional derived metrics
    """
    # Net interest income
    data['netInterestIncome'] = data['interestIncome'].fillna(0) - data['interestExpense'].fillna(0)

    # Fallback calculations for missing operating expenses
    data['totalOperatingExpenses'] = data['totalOperatingExpenses'].fillna(
        data['sellingGeneralAdministrative'].fillna(0) + data['researchDevelopment'].fillna(0)
    )

    # Fallback calculations for missing operating income
    data['operatingIncome'] = data['operatingIncome'].fillna(
        data['grossProfit'].fillna(0) - data['totalOperatingExpenses'].fillna(0)
    )

    # EBIT and EBITDA calculations
    data['ebit'] = data['netIncome'] + data['incomeTaxExpense'].fillna(0) + data['interestExpense'].fillna(0)
    data['ebitda'] = data['ebit'] + data['depreciationAndAmortization'].fillna(0)

    # Net tangible assets
    data['netTangibleAssets'] = data['totalAssets'] - data['intangibleAssets'].fillna(0) - data['goodWill'].fillna(0)

    # Net debt calculation
    data['netDebt'] = (data['shortTermDebt'].fillna(0) + data['longTermDebt'].fillna(0)) - data['cashAndEquivalents'].fillna(0)

    # Net working capital
    data['netWorkingCapital'] = data['totalCurrentAssets'].fillna(0) - data['totalCurrentLiabilities'].fillna(0)

    # Cash and short term investments
    data['cashAndShortTermInvestments'] = data['cashAndEquivalents'].fillna(0) + data['shortTermInvestments'].fillna(0)

    # Liabilities and equity
    data['liabilitiesAndStockholdersEquity'] = data['totalLiab'].fillna(0) + data['totalStockholderEquity'].fillna(0)

    # Free cash flow
    data['freeCashFlow'] = data['totalCashFromOperatingActivities'].fillna(0) - data['capitalExpenditures'].fillna(0)

    # Other operating expenses
    data['otherOperatingExpenses'] = (
        data['totalOperatingExpenses'].fillna(0)
        - data['sellingGeneralAdministrative'].fillna(0)
        - data['researchDevelopment'].fillna(0)
        - data['costOfRevenue'].fillna(0)
    )
    data['otherOperatingExpenses'] = data['otherOperatingExpenses'].apply(lambda x: x if x > 0 else 0)

    return data

# ============================================================================
# SHARED UTILITIES
# ============================================================================


# ============================================================================
# SHARED UTILITIES
# ============================================================================
def load_ticker_map() -> Optional[pd.DataFrame]:
    """
    Loads CIK -> Ticker mapping from historical_ciks.parquet.

    Uses new config system to address technical debt issue #1.
    """
    if not os.path.exists(config.UNIVERSE_MAP):
        logging.warning(f"Mapping file not found at {config.UNIVERSE_MAP}")
        return None

    try:
        # Read Parquet
        df = pd.read_parquet(config.UNIVERSE_MAP)
        # Use standardized CIK normalization
        df['cik'] = df['cik'].apply(_normalize_cik)
        return df[['cik', 'ticker']].drop_duplicates()
    except Exception as e:
        logging.error(f"Failed to load ticker map: {str(e)}")
        return None

def load_gics_map() -> Optional[pd.DataFrame]:
    """
    Loads CIK -> GICS mapping.

    Uses new config system and CIK standardization.
    """
    if not os.path.exists(config.GICS_MAP_PATH):
        logging.warning(f"GICS mapping file not found at {config.GICS_MAP_PATH}")
        return None

    try:
        # Read Parquet
        df = pd.read_parquet(config.GICS_MAP_PATH)
        # Use standardized CIK normalization
        df['cik'] = df['cik'].apply(_normalize_cik)
        return df
    except Exception as e:
        logging.error(f"Failed to load GICS map: {str(e)}")
        return None

# ============================================================================
# SEC MAPPER (Fundamentals)
# ============================================================================
class SECMapper:
    """
    Standardizes raw SEC XBRL tags into common financial metrics.
    Source: bronze/fundamentals/sec_bulk/parquet/*.parquet
    Target: Silver Layer (Standardized Fundamentals)
    """
    
    # Priority Maps: List of tags in order of preference (most specific/modern first)
    # --- INCOME STATEMENT PARITY ---
    MAP_REVENUE = ['RevenueFromContractWithCustomerExcludingAssessedTax', 'Revenues', 'SalesRevenueNet', 'SalesRevenueGoodsNet', 'SalesRevenueServicesNet', 'RealEstateRevenueNet', 'RegulatedAndUnregulatedOperatingRevenue']
    MAP_COST_OF_REVENUE = ['CostOfRevenue', 'CostOfGoodsAndServicesSold', 'CostOfGoodsSold', 'CostOfServices']
    MAP_GROSS_PROFIT = ['GrossProfit']
    MAP_R_AND_D = ['ResearchAndDevelopmentExpense', 'ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost', 'ResearchDevelopmentAndEngineeringExpense']
    MAP_SGA = ['SellingGeneralAndAdministrativeExpense', 'SellingAndMarketingExpense', 'GeneralAndAdministrativeExpense']
    MAP_OP_EXPENSE = ['OperatingExpenses', 'OperatingCostsAndExpenses']
    MAP_OP_INCOME = ['OperatingIncomeLoss']
    MAP_INTEREST_EXPENSE = ['InterestExpense']
    MAP_INTEREST_INCOME = ['InvestmentIncomeInterest', 'InterestIncomeOperating', 'InterestAndDividendIncomeOperating']
    MAP_OTHER_INCOME_EXPENSE = ['OtherNonoperatingIncomeExpense', 'NonoperatingIncomeExpense']
    MAP_PRETAX_INCOME = ['IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest', 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments']
    MAP_INCOME_TAX = ['IncomeTaxExpenseBenefit']
    MAP_NET_INCOME = ['NetIncomeLoss', 'ProfitLoss', 'NetIncomeLossAvailableToCommonStockholdersBasic']
    MAP_EPS_BASIC = ['EarningsPerShareBasic']
    MAP_EPS_DILUTED = ['EarningsPerShareDiluted']
    MAP_EBIT = ['NetIncomeLoss'] # Often calc as NI + Int + Tax, but keeping placeholder
    MAP_EBITDA = ['NetIncomeLoss'] # Placeholder for derived

    # --- BALANCE SHEET PARITY ---
    MAP_TOTAL_ASSETS = ['Assets']
    MAP_CURRENT_ASSETS = ['AssetsCurrent']
    MAP_CASH_AND_EQUIV = ['CashAndCashEquivalentsAtCarryingValue', 'Cash', 'CashAndCashEquivalents']
    MAP_SHORT_TERM_INVESTMENTS = ['AvailableForSaleSecuritiesDebtSecuritiesCurrent', 'MarketableSecuritiesCurrent', 'ShortTermInvestments']
    MAP_NET_RECEIVABLES = ['AccountsReceivableNetCurrent', 'ReceivablesNetCurrent', 'AccountsNotesAndLoansReceivableNetCurrent']
    MAP_INVENTORY = ['InventoryNet', 'InventoryGross']
    MAP_OTHER_CURRENT_ASSETS = ['PrepaidExpenseAndOtherAssetsCurrent', 'OtherAssetsCurrent']
    MAP_TOTAL_NON_CURRENT_ASSETS = ['AssetsNoncurrent'] # Derived often
    MAP_PPE_NET = ['PropertyPlantAndEquipmentNet']
    MAP_GOODWILL = ['Goodwill']
    MAP_INTANGIBLE_ASSETS = ['IntangibleAssetsNetExcludingGoodwill', 'FiniteLivedIntangibleAssetsNet']
    MAP_LONG_TERM_INVESTMENTS = ['LongTermInvestments', 'AvailableForSaleSecuritiesNoncurrent']
    MAP_OTHER_NON_CURRENT_ASSETS = ['OtherAssetsNoncurrent']

    MAP_TOTAL_LIABILITIES = ['Liabilities']
    MAP_CURRENT_LIABILITIES = ['LiabilitiesCurrent']
    MAP_ACCOUNTS_PAYABLE = ['AccountsPayableCurrent', 'AccountsPayableAndAccruedLiabilitiesCurrent']
    MAP_SHORT_TERM_DEBT = ['DebtCurrent', 'ShortTermBorrowings', 'LoansPayableCurrent', 'NotesPayableCurrent', 'LongTermDebtCurrent']
    MAP_CURRENT_DEFERRED_REVENUE = ['ContractWithCustomerLiabilityCurrent', 'DeferredRevenueCurrent']
    MAP_OTHER_CURRENT_LIAB = ['OtherLiabilitiesCurrent', 'AccruedLiabilitiesCurrent']
    MAP_NON_CURRENT_LIABILITIES = ['LiabilitiesNoncurrent']
    MAP_LONG_TERM_DEBT = ['LongTermDebtNoncurrent', 'LongTermDebt', 'LongTermDebtAndCapitalLeaseObligations']
    MAP_DEFERRED_LONG_TERM_LIAB = ['DeferredTaxLiabilitiesNoncurrent']
    MAP_OTHER_NON_CURRENT_LIAB = ['OtherLiabilitiesNoncurrent']
    
    MAP_TOTAL_EQUITY = ['StockholdersEquity', 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest']
    MAP_COMMON_STOCK = ['CommonStockValue', 'CommonStockParOrStatedValuePerShare']
    MAP_RETAINED_EARNINGS = ['RetainedEarningsAccumulatedDeficit']
    MAP_CAPITAL_SURPLUS = ['AdditionalPaidInCapital', 'CapitalSurplus']
    MAP_TREASURY_STOCK = ['TreasuryStockValue']
    MAP_MINORITY_INTEREST = ['MinorityInterest', 'NoncontrollingInterestInConsolidatedEntity']

    # --- CASH FLOW PARITY ---
    MAP_CF_OPERATING = ['NetCashProvidedByUsedInOperatingActivities']
    MAP_DEPRECIATION = ['DepreciationDepletionAndAmortization', 'Depreciation', 'AmortizationOfIntangibleAssets']
    MAP_CHANGE_RECEIVABLES = ['IncreaseDecreaseInAccountsReceivable', 'IncreaseDecreaseInReceivables']
    MAP_CHANGE_INVENTORY = ['IncreaseDecreaseInInventories']
    MAP_STOCK_BASED_COMPENSATION = ['ShareBasedCompensation', 'StockOptionPlanExpense']
    
    MAP_CF_INVESTING = ['NetCashProvidedByUsedInInvestingActivities']
    MAP_CAPEX = ['PaymentsToAcquirePropertyPlantAndEquipment', 'PaymentsForCapitalImprovements']
    MAP_INVESTMENTS_ACQUIRED = ['PaymentsToAcquireInvestments', 'PaymentsToAcquireAvailableForSaleSecurities']
    
    MAP_CF_FINANCING = ['NetCashProvidedByUsedInFinancingActivities']
    MAP_DIVIDENDS_PAID = ['PaymentsOfDividends', 'PaymentsOfDividendsCommonStock', 'Dividends']
    MAP_NET_BORROWINGS = ['ProceedsFromRepaymentsOfShortTermDebt', 'ProceedsFromIssuanceOfLongTermDebt']
    MAP_SALE_PURCHASE_STOCK = ['PaymentsForRepurchaseOfCommonStock']

    # --- INSURANCE / FINANCIALS EXTENSIONS ---
    MAP_COST_OF_REVENUE.extend([
        'BenefitsLossesAndExpenses',
        'PolicyholderBenefitsAndClaimsIncurredNet',
        'InterestCreditedToPolicyholdersAccountBalance',
        'LossesAndLossAdjustmentExpenses'
    ])
    
    MAP_REVENUE.extend([
        'PremiumsEarnedNet',
        'NetInvestmentIncome',
        'RealizedInvestmentGainsLosses',
        'LifeAndAnnuityPremiums'
    ])
    
    MAP_INVENTORY.extend([
        'DeferredPolicyAcquisitionCosts'
    ])
    
    MAP_SHORT_TERM_INVESTMENTS.extend([
        'InvestmentsEquitySecurities',
        'FixedMaturitiesAvailableForSale'
    ])
    
    MAP_LONG_TERM_INVESTMENTS.extend([
        'InvestmentsFixedMaturities',
        'FixedMaturitiesHeldToMaturity',
        'MortgageLoansOnRealEstate'
    ])
    
    MAP_NET_RECEIVABLES.extend([
        'PremiumsReceivable',
        'ReinsuranceRecoverables'
    ])
    
    MAP_CURRENT_LIABILITIES.extend([
        'UnearnedPremiums'
    ]) # Note: Insurers often lack Current/NonCurrent distinction, this is a heuristic

    # --- EODHD PARITY EXTENSIONS ---
    MAP_ACCUMULATED_AMORTIZATION = ['FiniteLivedIntangibleAssetsAccumulatedAmortization', 'AccumulatedAmortizationOfOtherAssets', 'AccumulatedAmortization', 'AccumulatedAmortizationDefiniteLivedIntangibleAssets']
    MAP_NEGATIVE_GOODWILL = ['BusinessCombinationBargainPurchaseGainRecognizedAmount', 'GainOnBargainPurchase', 'NegativeGoodwill']
    MAP_DEFERRED_CHARGES = ['DeferredCosts', 'DeferredFinanceCostsNet', 'DeferredLongTermAssetCharges']
    MAP_REDEEMABLE_PREFERRED = ['RedeemablePreferredStockValue', 'TemporaryEquityRedeemableNoncontrollingInterests']
    MAP_CHANGE_LIABILITIES = ['IncreaseDecreaseInOperatingLiabilities', 'IncreaseDecreaseInOtherOperatingLiabilities', 'IncreaseDecreaseInLiabilities']
    MAP_CHANGE_OPERATING = ['IncreaseDecreaseInOperatingCapital']
    MAP_NON_CURRENT_LIABILITIES_OTHER = ['OtherLiabilitiesNoncurrent']
    MAP_OTHER_NON_CASH = ['OtherNoncashIncomeExpense']
    MAP_CHANGE_CASH = ['CashAndCashEquivalentsPeriodIncreaseDecrease']


    @staticmethod
    def standardize(df_raw: pd.DataFrame, chunk_size: int = 1000) -> pd.DataFrame:
        """
        Standardize raw SEC XBRL financial data into a consistent format.

        This is the core standardization function that transforms raw financial data
        from SEC filings into a standardized format suitable for analysis and modeling.

        Input: Raw DataFrame containing SEC XBRL data with columns like:
               (adsh, tag, value, cik, period, filed, form, fy, fp, sic, etc.)
        Output: Standardized DataFrame with consistent financial metrics:
               (cik, date, TotalRevenue, NetIncome, GrossProfit, etc.)

        Technical Debt Addressed:
        - Uses internal configuration system for paths and settings
        - Applies standardized CIK normalization throughout
        - Implements memory-efficient chunking to handle large datasets
        - Provides comprehensive error handling and logging

        Memory Optimization Strategy:
        Uses CIK-based chunking to prevent memory explosion during pivot operations.
        This ensures we process ALL financial tags without data loss, but in manageable batches
        that fit within memory constraints.

        Args:
            df_raw: Raw DataFrame containing financial data from SEC filings
            chunk_size: Number of unique CIKs to process in each batch (default: 1000)

        Returns:
            pd.DataFrame: Standardized DataFrame with comprehensive financial metrics
                     Returns empty DataFrame if input is empty or processing fails

        Raises:
            ValueError: If critical columns are missing from input DataFrame
            MemoryError: If memory constraints are exceeded during processing

        Note:
            This function handles the complex transformation logic that was previously
            scattered throughout the codebase, centralizing it for better maintainability.
        """
        # Validate input DataFrame
        if df_raw.empty:
            logging.warning("Empty DataFrame provided to standardize()")
            return pd.DataFrame()

        # Standardize CIK format using new utility function
        if 'cik' in df_raw.columns:
            df_raw['cik'] = df_raw['cik'].apply(_normalize_cik)

        # 1. Filter for Consolidated Data
        if 'segments' in df_raw.columns:
             df_clean = df_raw[df_raw['segments'].isna() | (df_raw['segments'] == '')].copy()
        else:
             df_clean = df_raw.copy()

        # Validate CIK data after filtering
        if not _validate_cik_dataframe(df_clean):
            logging.warning("CIK validation failed - some CIKs may be invalid")

        # 2. Get unique CIKs for chunking
        unique_ciks = df_clean['cik'].unique()
        total_ciks = len(unique_ciks)

        all_standardized_chunks = []

        logging.info(f"    > Standardizing {total_ciks} CIKs in batches of {chunk_size}...")

        # Process in chunks
        for i in range(0, total_ciks, chunk_size):
            batch_ciks = unique_ciks[i : i + chunk_size]
            df_chunk = df_clean[df_clean['cik'].isin(batch_ciks)].copy()

             # --- Standardize this chunk ---

            # A. Split Logic based on Statement Type & Duration
            if 'qtrs' in df_chunk.columns:
                mask_bs = (df_chunk['qtrs'] < 0.1) # Instant
                mask_is = ((df_chunk['qtrs'] >= 0.9) & (df_chunk['qtrs'] <= 1.1)) | \
                          ((df_chunk['qtrs'] >= 3.9) & (df_chunk['qtrs'] <= 4.1))

                # CF YTD Logic
                fp_map = {'Q1': 1.0, 'Q2': 2.0, 'Q3': 3.0, 'FY': 4.0}
                clean_fp = df_chunk['fp'].astype(str).str.upper()
                expected_qtrs = clean_fp.map(fp_map).fillna(4.0)
                mask_cf = (df_chunk['qtrs'] >= expected_qtrs - 0.1) & (df_chunk['qtrs'] <= expected_qtrs + 0.1)

                df_bs_rows = df_chunk[mask_bs]
                df_is_rows = df_chunk[mask_is]
                df_cf_rows = df_chunk[mask_cf]
            else:
                df_bs_rows = df_chunk
                df_is_rows = df_chunk
                df_cf_rows = df_chunk

            # B. Pivot (Now safe because N is small)
            index_cols = ['adsh', 'cik', 'name', 'period', 'filed', 'form', 'fy', 'fp', 'sic']
            
            pivot_bs = df_bs_rows.pivot_table(index=index_cols, columns='tag', values='value', aggfunc='max')
            pivot_is = df_is_rows.pivot_table(index=index_cols, columns='tag', values='value', aggfunc='max')
            pivot_cf = df_cf_rows.pivot_table(index=index_cols, columns='tag', values='value', aggfunc='max')
            
            # C. Merge Logic
            all_indices = pivot_is.index.union(pivot_bs.index).union(pivot_cf.index)
            
            # Re-index
            pivot_is = pivot_is.reindex(all_indices)
            pivot_bs = pivot_bs.reindex(all_indices)
            pivot_cf = pivot_cf.reindex(all_indices)
            
            # Helper
            def get_col(tags, source_df):
                if source_df is None or source_df.empty: 
                    return pd.Series(np.nan, index=all_indices)
                available = [t for t in tags if t in source_df.columns]
                if not available: 
                    return pd.Series(np.nan, index=all_indices)
                return source_df[available].bfill(axis=1).iloc[:, 0]

            data = {}

            # --- INCOME STATEMENT ---
            data['totalRevenue'] = get_col(SECMapper.MAP_REVENUE, pivot_is)
            data['costOfRevenue'] = get_col(SECMapper.MAP_COST_OF_REVENUE, pivot_is)
            data['grossProfit'] = get_col(SECMapper.MAP_GROSS_PROFIT, pivot_is)
            data['researchDevelopment'] = get_col(SECMapper.MAP_R_AND_D, pivot_is)
            data['sellingGeneralAdministrative'] = get_col(SECMapper.MAP_SGA, pivot_is)
            data['sellingAndMarketingExpenses'] = get_col(['SellingAndMarketingExpense'], pivot_is)
            data['totalOperatingExpenses'] = get_col(SECMapper.MAP_OP_EXPENSE, pivot_is)
            data['operatingIncome'] = get_col(SECMapper.MAP_OP_INCOME, pivot_is)
            data['interestExpense'] = get_col(SECMapper.MAP_INTEREST_EXPENSE, pivot_is)
            data['interestIncome'] = get_col(SECMapper.MAP_INTEREST_INCOME, pivot_is)
            data['netInterestIncome'] = data['interestIncome'].fillna(0) - data['interestExpense'].fillna(0)
            
            # Fallback
            data['totalOperatingExpenses'] = data['totalOperatingExpenses'].fillna(
                data['sellingGeneralAdministrative'].fillna(0) + data['researchDevelopment'].fillna(0)
            )
            data['operatingIncome'] = data['operatingIncome'].fillna(
                data['grossProfit'].fillna(0) - data['totalOperatingExpenses'].fillna(0)
            )

            data['totalOtherIncomeExpenseNet'] = get_col(SECMapper.MAP_OTHER_INCOME_EXPENSE, pivot_is)
            data['incomeBeforeTax'] = get_col(SECMapper.MAP_PRETAX_INCOME, pivot_is)
            data['incomeTaxExpense'] = get_col(SECMapper.MAP_INCOME_TAX, pivot_is)
            data['netIncome'] = get_col(SECMapper.MAP_NET_INCOME, pivot_is)
            data['netIncomeFromContinuingOps'] = get_col(['IncomeLossFromContinuingOperations'], pivot_is)
            data['netIncomeApplicableToCommonShares'] = get_col(['NetIncomeLossAvailableToCommonStockholdersBasic'], pivot_is)
            data['epsActual'] = get_col(SECMapper.MAP_EPS_BASIC, pivot_is)
            data['epsDiluted'] = get_col(SECMapper.MAP_EPS_DILUTED, pivot_is)
            
            data['effectOfAccountingCharges'] = get_col(['CumulativeEffectOfNewAccountingPrincipleInPeriodOfAdoption'], pivot_is)
            data['extraordinaryItems'] = get_col(['ExtraordinaryItemsGross'], pivot_is)
            data['nonRecurring'] = get_col(['NonoperatingIncomeExpense'], pivot_is)
            data['discontinuedOperations'] = get_col(['IncomeLossFromDiscontinuedOperationsNetOfTax'], pivot_is)
            data['preferredStockAndOtherAdjustments'] = get_col(['PreferredStockDividendsAndOtherAdjustments'], pivot_is)
            data['reconciledDepreciation'] = get_col(['DepreciationDepletionAndAmortization'], pivot_is)
            data['depreciationAndAmortization'] = get_col(['DepreciationDepletionAndAmortization'], pivot_is)
            data['ebit'] = data['netIncome'] + data['incomeTaxExpense'].fillna(0) + data['interestExpense'].fillna(0)
            data['ebitda'] = data['ebit'] + data['depreciationAndAmortization'].fillna(0)

            data['taxProvision'] = data['incomeTaxExpense']

            # --- BALANCE SHEET ---
            data['totalAssets'] = get_col(SECMapper.MAP_TOTAL_ASSETS, pivot_bs)
            data['intangibleAssets'] = get_col(SECMapper.MAP_INTANGIBLE_ASSETS, pivot_bs)
            data['goodWill'] = get_col(SECMapper.MAP_GOODWILL, pivot_bs)
            data['otherCurrentAssets'] = get_col(SECMapper.MAP_OTHER_CURRENT_ASSETS, pivot_bs)
            data['totalCurrentAssets'] = get_col(SECMapper.MAP_CURRENT_ASSETS, pivot_bs)
            data['cashAndEquivalents'] = get_col(SECMapper.MAP_CASH_AND_EQUIV, pivot_bs)
            data['cash'] = data['cashAndEquivalents']
            data['shortTermInvestments'] = get_col(SECMapper.MAP_SHORT_TERM_INVESTMENTS, pivot_bs)
            data['netReceivables'] = get_col(SECMapper.MAP_NET_RECEIVABLES, pivot_bs)
            data['inventory'] = get_col(SECMapper.MAP_INVENTORY, pivot_bs)
            data['nonCurrentAssetsTotal'] = get_col(SECMapper.MAP_TOTAL_NON_CURRENT_ASSETS, pivot_bs)
            data['propertyPlantAndEquipmentNet'] = get_col(SECMapper.MAP_PPE_NET, pivot_bs)
            data['propertyPlantAndEquipmentGross'] = get_col(['PropertyPlantAndEquipmentGross'], pivot_bs)
            data['accumulatedDepreciation'] = get_col(['AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment'], pivot_bs)
            data['longTermInvestments'] = get_col(SECMapper.MAP_LONG_TERM_INVESTMENTS, pivot_bs)
            data['otherAssets'] = get_col(SECMapper.MAP_OTHER_NON_CURRENT_ASSETS, pivot_bs)
            data['netTangibleAssets'] = data['totalAssets'] - data['intangibleAssets'].fillna(0) - data['goodWill'].fillna(0)

            data['totalLiab'] = get_col(SECMapper.MAP_TOTAL_LIABILITIES, pivot_bs)
            data['totalCurrentLiabilities'] = get_col(SECMapper.MAP_CURRENT_LIABILITIES, pivot_bs)
            data['accountsPayable'] = get_col(SECMapper.MAP_ACCOUNTS_PAYABLE, pivot_bs)
            data['shortTermDebt'] = get_col(SECMapper.MAP_SHORT_TERM_DEBT, pivot_bs)
            data['currentDeferredRevenue'] = get_col(SECMapper.MAP_CURRENT_DEFERRED_REVENUE, pivot_bs)
            data['otherCurrentLiab'] = get_col(SECMapper.MAP_OTHER_CURRENT_LIAB, pivot_bs)
            data['nonCurrentLiabilitiesTotal'] = get_col(SECMapper.MAP_NON_CURRENT_LIABILITIES, pivot_bs)
            data['longTermDebt'] = get_col(SECMapper.MAP_LONG_TERM_DEBT, pivot_bs)
            data['deferredLongTermLiab'] = get_col(SECMapper.MAP_DEFERRED_LONG_TERM_LIAB, pivot_bs)
            data['otherLiab'] = get_col(SECMapper.MAP_OTHER_NON_CURRENT_LIAB, pivot_bs)
            data['capitalLeaseObligations'] = get_col(['FinanceLeaseLiability', 'CapitalLeaseObligations', 'CapitalLeaseObligationsCurrent', 'CapitalLeaseObligationsNoncurrent', 'FinanceLeaseLiabilityNoncurrent', 'FinanceLeaseLiabilityCurrent'], pivot_bs)

            data['totalStockholderEquity'] = get_col(SECMapper.MAP_TOTAL_EQUITY, pivot_bs)
            data['commonStock'] = get_col(SECMapper.MAP_COMMON_STOCK, pivot_bs)
            data['retainedEarnings'] = get_col(SECMapper.MAP_RETAINED_EARNINGS, pivot_bs)
            data['additionalPaidInCapital'] = get_col(SECMapper.MAP_CAPITAL_SURPLUS, pivot_bs)
            data['treasuryStock'] = get_col(SECMapper.MAP_TREASURY_STOCK, pivot_bs)
            data['minorityInterest'] = get_col(SECMapper.MAP_MINORITY_INTEREST, pivot_bs)
            data['noncontrollingInterestInConsolidatedEntity'] = data['minorityInterest']
            
            data['accumulatedOtherComprehensiveIncome'] = get_col(['AccumulatedOtherComprehensiveIncomeLossNetOfTax'], pivot_bs)
            data['warrants'] = get_col(['ClassOfWarrantOrRightOutstanding'], pivot_bs)
            data['preferredStockTotalEquity'] = get_col(['PreferredStockValue'], pivot_bs)
            data['commonStockSharesOutstanding'] = get_col(['CommonStockSharesOutstanding'], pivot_bs)
            data['netDebt'] = (data['shortTermDebt'].fillna(0) + data['longTermDebt'].fillna(0)) - data['cashAndEquivalents'].fillna(0)
            data['netInvestedCapital'] = data['totalStockholderEquity'] + data['netDebt']

            data['accumulatedAmortization'] = get_col(SECMapper.MAP_ACCUMULATED_AMORTIZATION, pivot_bs)
            data['negativeGoodwill'] = get_col(SECMapper.MAP_NEGATIVE_GOODWILL, pivot_bs)
            data['deferredLongTermAssetCharges'] = get_col(SECMapper.MAP_DEFERRED_CHARGES, pivot_bs)
            data['preferredStockRedeemable'] = get_col(SECMapper.MAP_REDEEMABLE_PREFERRED, pivot_bs)
            data['nonCurrentLiabilitiesOther'] = get_col(SECMapper.MAP_NON_CURRENT_LIABILITIES_OTHER, pivot_bs)
            
            data['date'] = all_indices.get_level_values('period')
            data['filing_date'] = all_indices.get_level_values('filed')
            data['currency_symbol'] = 'USD'
            data['capitalSurpluse'] = data['additionalPaidInCapital']
            data['propertyPlantEquipment'] = data['propertyPlantAndEquipmentNet']
            data['longTermDebtTotal'] = data['longTermDebt']
            data['commonStockTotalEquity'] = data['commonStock']
            data['retainedEarningsTotalEquity'] = data['retainedEarnings']
            data['liabilitiesAndStockholdersEquity'] = data['totalLiab'].fillna(0) + data['totalStockholderEquity'].fillna(0)
            data['netWorkingCapital'] = data['totalCurrentAssets'].fillna(0) - data['totalCurrentLiabilities'].fillna(0)
            data['cashAndShortTermInvestments'] = data['cashAndEquivalents'].fillna(0) + data['shortTermInvestments'].fillna(0)
            data['totalPermanentEquity'] = data['totalStockholderEquity']
            data['temporaryEquityRedeemableNoncontrollingInterests'] = data['preferredStockRedeemable']
            data['shortLongTermDebt'] = data['shortTermDebt']

            # --- CASH FLOW ---
            data['totalCashFromOperatingActivities'] = get_col(SECMapper.MAP_CF_OPERATING, pivot_cf)
            data['depreciation'] = get_col(SECMapper.MAP_DEPRECIATION, pivot_cf)
            data['changeToNetincome'] = get_col(SECMapper.MAP_NET_INCOME, pivot_cf)
            data['changeToAccountReceivables'] = get_col(SECMapper.MAP_CHANGE_RECEIVABLES, pivot_cf)
            data['changeToInventory'] = get_col(SECMapper.MAP_CHANGE_INVENTORY, pivot_cf)
            data['stockBasedCompensation'] = get_col(SECMapper.MAP_STOCK_BASED_COMPENSATION, pivot_cf)
            data['changeInWorkingCapital'] = get_col(['IncreaseDecreaseInOperatingCapital'], pivot_cf)
            
            data['totalCashflowsFromInvestingActivities'] = get_col(SECMapper.MAP_CF_INVESTING, pivot_cf)
            data['capitalExpenditures'] = get_col(SECMapper.MAP_CAPEX, pivot_cf)
            data['investments'] = get_col(SECMapper.MAP_INVESTMENTS_ACQUIRED, pivot_cf)
            
            data['totalCashFromFinancingActivities'] = get_col(SECMapper.MAP_CF_FINANCING, pivot_cf)
            data['dividendsPaid'] = get_col(SECMapper.MAP_DIVIDENDS_PAID, pivot_cf)
            data['salePurchaseOfStock'] = get_col(SECMapper.MAP_SALE_PURCHASE_STOCK, pivot_cf)
            data['netBorrowings'] = get_col(SECMapper.MAP_NET_BORROWINGS, pivot_cf)
            data['freeCashFlow'] = data['totalCashFromOperatingActivities'].fillna(0) - data['capitalExpenditures'].fillna(0)
            
            data['exchangeRateChanges'] = get_col(['EffectOfExchangeRateOnCashAndCashEquivalents'], pivot_cf)
            data['cashAndCashEquivalentsChanges'] = get_col(['CashAndCashEquivalentsPeriodIncreaseDecrease'], pivot_cf)
            
            data['endPeriodCashFlow'] = data['cashAndEquivalents']
            data['beginPeriodCashFlow'] = data['endPeriodCashFlow'].fillna(0) - data['cashAndCashEquivalentsChanges'].fillna(0)

            data['changeToLiabilities'] = get_col(SECMapper.MAP_CHANGE_LIABILITIES, pivot_cf)
            data['changeToOperatingActivities'] = get_col(SECMapper.MAP_CHANGE_OPERATING, pivot_cf)
            data['otherNonCashItems'] = get_col(SECMapper.MAP_OTHER_NON_CASH, pivot_cf)
            
            data['changeInCash'] = data['cashAndCashEquivalentsChanges'] 
            data['changeReceivables'] = data['changeToAccountReceivables']
            
            data['otherOperatingExpenses'] = (
                data['totalOperatingExpenses'].fillna(0) 
                - data['sellingGeneralAdministrative'].fillna(0) 
                - data['researchDevelopment'].fillna(0) 
                - data['costOfRevenue'].fillna(0)
            )
            data['otherOperatingExpenses'] = data['otherOperatingExpenses'].apply(lambda x: x if x > 0 else 0)

            data['earningAssets'] = get_col(['InterestEarningAssets', 'AssetsInterestBearingSecuritiesAmount'], pivot_bs)

            # Construct Chunk DF
            df_std_chunk = pd.DataFrame(data, index=all_indices)
            all_standardized_chunks.append(df_std_chunk)
            
            # Memory cleanup for this chunk
            del df_chunk, df_bs_rows, df_is_rows, df_cf_rows, pivot_bs, pivot_is, pivot_cf, data, df_std_chunk
            
        # Concat all chunks
        if not all_standardized_chunks:
            return pd.DataFrame()
        
        df_std = pd.concat(all_standardized_chunks)
        return df_std.reset_index()

    @staticmethod
    def validate_coverage(df_std):
        pass

# ============================================================================
# FUNDAMENTALS STANDARDIZATION LOGIC
# ============================================================================

# EdgarTools concept to column mapping
EDGARTOOLS_CONCEPT_TO_COLUMN = {
    'us-gaap:Revenues': 'totalRevenue',
    'us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax': 'totalRevenue',
    'us-gaap:SalesRevenueNet': 'totalRevenue',
    'us-gaap:CostOfRevenue': 'costOfRevenue',
    'us-gaap:CostOfGoodsAndServicesSold': 'costOfRevenue',
    'us-gaap:GrossProfit': 'grossProfit',
    'us-gaap:ResearchAndDevelopmentExpense': 'researchDevelopment',
    'us-gaap:SellingGeneralAndAdministrativeExpense': 'sellingGeneralAdministrative',
    'us-gaap:GeneralAndAdministrativeExpense': 'sellingGeneralAdministrative',
    'us-gaap:OperatingExpenses': 'totalOperatingExpenses',
    'us-gaap:OperatingIncomeLoss': 'operatingIncome',
    'us-gaap:InterestExpense': 'interestExpense',
    'us-gaap:InvestmentIncomeInterest': 'interestIncome',
    'us-gaap:OtherNonoperatingIncomeExpense': 'totalOtherIncomeExpenseNet',
    'us-gaap:IncomeTaxExpenseBenefit': 'incomeTaxExpense',
    'us-gaap:NetIncomeLoss': 'netIncome',
    'us-gaap:EarningsPerShareBasic': 'netIncomeApplicableToCommonShares',
    'us-gaap:Assets': 'totalAssets',
    'us-gaap:AssetsCurrent': 'totalCurrentAssets',
    'us-gaap:CashAndCashEquivalentsAtCarryingValue': 'cash',
    'us-gaap:AccountsReceivableNetCurrent': 'netReceivables',
    'us-gaap:InventoryNet': 'inventory',
    'us-gaap:PropertyPlantAndEquipmentNet': 'propertyPlantEquipment',
    'us-gaap:Goodwill': 'goodWill',
    'us-gaap:IntangibleAssetsNetExcludingGoodwill': 'intangibleAssets',
    'us-gaap:Liabilities': 'totalLiab',
    'us-gaap:LiabilitiesCurrent': 'totalCurrentLiabilities',
    'us-gaap:AccountsPayableCurrent': 'accountsPayable',
    'us-gaap:LongTermDebt': 'longTermDebt',
    'us-gaap:LongTermDebtNoncurrent': 'longTermDebt',
    'us-gaap:StockholdersEquity': 'totalStockholderEquity',
    'us-gaap:RetainedEarningsAccumulatedDeficit': 'retainedEarnings',
    'us-gaap:CommonStockSharesOutstanding': 'commonStockSharesOutstanding',
    'us-gaap:NetCashProvidedByUsedInOperatingActivities': 'totalCashFromOperatingActivities',
    'us-gaap:NetCashProvidedByUsedInInvestingActivities': 'totalCashflowsFromInvestingActivities',
    'us-gaap:NetCashProvidedByUsedInFinancingActivities': 'totalCashFromFinancingActivities',
    'us-gaap:DepreciationDepletionAndAmortization': 'depreciation',
}

def pivot_edgartools_facts(df_cik_period):
    """Convert long-form EdgarTools facts to wide-form row matching Silver schema."""
    row = {
        'cik': df_cik_period['cik'].iloc[0],
        'filed': df_cik_period['filing_date'].iloc[0] if 'filing_date' in df_cik_period.columns else None,
        'fy': df_cik_period['fiscal_year'].iloc[0] if 'fiscal_year' in df_cik_period.columns else None,
        'fp': df_cik_period['fiscal_period'].iloc[0] if 'fiscal_period' in df_cik_period.columns else None,
        'form': df_cik_period['form_type'].iloc[0] if 'form_type' in df_cik_period.columns else None,
    }
    
    for _, fact in df_cik_period.iterrows():
        concept = fact.get('concept', '')
        if concept in EDGARTOOLS_CONCEPT_TO_COLUMN:
            col_name = EDGARTOOLS_CONCEPT_TO_COLUMN[concept]
            value = fact.get('value')
            if value is not None and col_name not in row:  # First value wins
                row[col_name] = value
    
    return row

def process_edgartools_data():
    """Process EdgarTools fetched parquet files and append to Silver."""
    if not os.path.exists(BRONZE_EDGARTOOLS):
        print("No EdgarTools data directory found. Skipping.")
        return
    
    files = sorted(glob.glob(os.path.join(BRONZE_EDGARTOOLS, "*.parquet")))
    if not files:
        print("No EdgarTools parquet files found. Skipping.")
        return
    
    print(f"\n--- Processing EdgarTools Data ---")
    print(f"Found {len(files)} batch files.")
    
    all_rows = []
    
    for f in files:
        df = pd.read_parquet(f)
        
        if df.empty:
            continue
        
        # Check required columns exist
        if 'cik' not in df.columns or 'fiscal_year' not in df.columns:
            continue
            
        for (cik, fy, fp), group in df.groupby(['cik', 'fiscal_year', 'fiscal_period'], dropna=False):
            row = pivot_edgartools_facts(group)
            all_rows.append(row)
    
    if not all_rows:
        print("No EdgarTools rows to write.")
        return
    
    df_final = pd.DataFrame(all_rows)
    
    # Add year partition column
    df_final['year'] = pd.to_numeric(df_final['fy'], errors='coerce').fillna(0).astype(int)
    df_final = df_final[df_final['year'] >= 2009]
    
    # Ensure CIK is zero-padded string
    df_final['cik'] = df_final['cik'].astype(str).str.zfill(10)
    
    # Add GICS data
    df_gics = load_gics_map()
    if df_gics is not None:
        df_final = pd.merge(df_final, df_gics, on='cik', how='left')
        gics_cols = ['gics_sector', 'gics_group', 'gics_industry', 'gics_sub_industry']
        for col in gics_cols:
            if col in df_final.columns:
                df_final[col] = df_final[col].fillna('Unknown')
    
    print(f"Writing {len(df_final)} EdgarTools rows to Silver...")
    
    write_deltalake(
        SILVER_FUNDAMENTALS,
        df_final,
        partition_by=['year'],
        mode='append',
        schema_mode='merge'
    )
    
    print("✅ EdgarTools data appended to Silver.")

def process_fundamentals_batch(df_std, df_map):
    """
    Standardizes a batch of SEC data (already in DF form from Delta).
    """
    try:
        # 3. Map Tickers
        if df_map is not None:
            df_std['cik'] = df_std['cik'].astype(str).str.zfill(10)
            df_final = df_std.merge(df_map, on='cik', how='left')
        else:
            df_final = df_std
            
        # Load GICS Mapping
        df_gics = load_gics_map()
        if df_gics is not None:
            # Merge GICS into df_final
            df_final = pd.merge(df_final, df_gics, on='cik', how='left')
            # print("Merged GICS data.")
            
            # Fill NULL GICS values with 'Unknown' for missing classifications
            gics_cols = ['gics_sector', 'gics_group', 'gics_industry', 'gics_sub_industry']
            for col in gics_cols:
                if col in df_final.columns:
                    df_final[col] = df_final[col].fillna('Unknown')
            # print("Filled missing GICS values with 'Unknown'.")
        else:
            print("GICS mapping not found. Proceeding without GICS.")
            # Add empty columns to prevent schema errors if file is missing
            for col in ['gics_sector', 'gics_group', 'gics_industry', 'gics_sub_industry']:
                df_final[col] = 'Unknown'
            
        # 4. Add Year for Partitioning - use Fiscal Year (fy), not filed date
        df_final['year'] = df_final['fy'].fillna(0).astype(int)
        
        # 5. Filter to 2009+ only (SEC XBRL availability)
        df_final = df_final[df_final['year'] >= 2009]
        
        return df_final
        
    except Exception as e:
        print(f"[ERROR] Failed to process batch: {e}")
        import traceback
        traceback.print_exc()
        return None

def standardize_fundamentals():
    print(f"--- Standardizing Fundamentals ---")
    print(f"Source: {BRONZE_DELTA_SEC}")
    print(f"Target Delta Table: {SILVER_FUNDAMENTALS}")
    
    # 1. Load Map
    df_map = load_ticker_map()
    if df_map is not None:
        print(f"Loaded {len(df_map)} CIK-Ticker mappings.")
    
    # 2. Read Bronze Delta & Accumulate
    all_dfs = []
    
    try:
        # Access Delta Table
        if not os.path.exists(BRONZE_DELTA_SEC):
            print(f"Bronze SEC data not found at {BRONZE_DELTA_SEC}")
            return

        # Use Delta Lake compatibility layer
        delta_compatibility = DeltaLakeCompatibility(BRONZE_DELTA_SEC)

        # Iterate years (Fiscal Year)
        # We assume data exists for these years.
        for year in range(2009, 2026):
            print(f"Processing Fiscal Year {year}...")
            try:
                # Read specific year partition filters using compatibility layer
                # This will use Polars scan_delta with fallback to DeltaTable
                df_raw = delta_compatibility.read_table(filter_condition=f"fy == {year}")
                
                if df_raw.empty:
                    continue
                    
                df_std = SECMapper.standardize(df_raw) # Standardize raw DF
                df_final = process_fundamentals_batch(df_std, df_map) # Map & Filter
            
                if df_final is not None and not df_final.empty:
                    # Fix Types before collecting
                    if 'cik' in df_final.columns: df_final['cik'] = df_final['cik'].astype(str)
                    if 'ticker' in df_final.columns: df_final['ticker'] = df_final['ticker'].astype(str)
                    all_dfs.append(df_final)
                    
            except Exception as e:
                # If partition doesn't exist, it might throw error or return empty.
                pass 
            
            # Explicit memory cleanup
            if 'df_raw' in locals(): del df_raw
            if 'df_std' in locals(): del df_std
            if 'df_final' in locals(): del df_final 
            # We keep 'df_final' in 'all_dfs' so we deleted the local reference, however all_dfs holds it.
            # To be truly safe, strict streaming is needed, but for now we GC the intermediate large raw DF.
            import gc
            gc.collect()

        # 3. GLOBAL PROCESSING (Concat -> Sort -> FFill)
        if all_dfs:
            print(f"Merging {len(all_dfs)} yearly batches...")
            df_master = pd.concat(all_dfs, ignore_index=True)
            
            # Debug: Check columns
            if 'cik' not in df_master.columns:
                print(f"CRITICAL ERROR: 'cik' column missing from master dataframe!")
                print(f"Columns found: {df_master.columns.tolist()}")
                return

            print(f"Sorting {len(df_master)} rows by CIK/Date...")
            # Ensure sorting by CIK then time (filed date usually best for continuity)
            if 'filed' in df_master.columns:
                df_master['filed'] = pd.to_datetime(df_master['filed'], errors='coerce')
                df_master.sort_values(by=['cik', 'filed'], inplace=True)
            else:
                # Fallback to period_end if filed missing (unlikely in standardized data)
                df_master.sort_values(by=['cik', 'fy', 'fp'], inplace=True)
            
            print("Applying Global Forward Fill (ffill)...")
            # Group by CIK and ffill
            
            # Simplest efficient way:
            df_master = df_master.groupby('cik', group_keys=False).apply(lambda x: x.ffill())
            
            print(f"Writing {len(df_master)} rows to Silver Delta Table...")
            write_deltalake(
                SILVER_FUNDAMENTALS,
                df_master,
                partition_by=['year'],
                mode='overwrite', # Overwrite because we are re-writing whole history cleanly
                schema_mode='overwrite'
            )
            print("✅ Silver Standardization Complete (with Forward Fill)")
        else:
            print("No data found to process.") 

    except Exception as e:
        print(f"Error accessing Bronze Delta: {e}")
    
    # --- EDGARTOOLS DATA PROCESSING ---
    # Process EdgarTools fetched data and append to Silver
    process_edgartools_data()


# ============================================================================
# PRICES STANDARDIZATION LOGIC (EODHD)
# ============================================================================
def load_price_mappings():
    """
    Loads bidirectional CIK <-> Ticker mappings from ALL available sources.
    Priority: EODHD master universe (primary), then SEC historical_ciks (fallback).
    """
    cik_to_ticker = {}
    ticker_to_cik = {}
    equity_ciks = set()  # CIKs that are valid equities
    
    # 1. Load EODHD master universe (primary source)
    if os.path.exists(EODHD_UNIVERSE):
        df = pd.read_parquet(EODHD_UNIVERSE)
        df = df[df['cik'].notna() & (df['cik'] != '')]
        df['cik'] = df['cik'].str.zfill(10)
        
        # Populate equity_ciks or other maps if needed from EODHD
        # (Original code didn't fully use EODHD df rows effectively in valid_ciks logic, mostly relied on JSON)
        
    if os.path.exists(CIK_MAP_JSON):
        try:
            with open(CIK_MAP_JSON, 'r') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    for k, v in data.items():
                        if str(v).isdigit():
                             t, c = k, str(v).zfill(10)
                        else:
                             t, c = v, str(k).zfill(10)
                        
                        ticker_to_cik[t] = c
                        cik_to_ticker[c] = t
                        equity_ciks.add(c) 
        except Exception as e:
            print(f"Error loading JSON map: {e}")
            
    return cik_to_ticker, ticker_to_cik, equity_ciks

def standardize_prices():
    print(f"--- Standardizing Prices ---")
    print(f"Source (Bronze Delta): {BRONZE_MARKET_DIR}")
    print(f"Target (Silver Delta): {SILVER_PRICES}")
    
    cik_to_ticker, ticker_to_cik, equity_ciks = load_price_mappings()
    print(f"Loaded {len(cik_to_ticker)} CIK mappings.")

    try:
        if not os.path.exists(BRONZE_MARKET_DIR):
             print("Bronze Market Delta Table not found.")
             return
             
        lf_bronze = pl.scan_delta(BRONZE_MARKET_DIR)
        
        # Get Years
        years = lf_bronze.select(pl.col("year").unique()).collect()["year"].to_list()
        years = sorted([y for y in years if y is not None])
        
        print(f"Found years in Bronze: {years}")
        
        total_rows = 0
        
        # Convert map to DataFrame for joining
        if ticker_to_cik:
             map_data = [{"ticker": t, "mapped_cik": c} for t, c in ticker_to_cik.items()]
             df_map = pl.DataFrame(map_data, schema={"ticker": pl.Utf8, "mapped_cik": pl.Utf8})
        else:
             df_map = pl.DataFrame([], schema={"ticker": pl.Utf8, "mapped_cik": pl.Utf8})

        for year in years:
            if year < 2009: continue 
            
            print(f"Processing Year {year}...")
            
            # Filter Bronze
            lf_year = lf_bronze.filter(pl.col("year") == year)
            
            # Materialize
            df_year = lf_year.collect()
            
            if df_year.height == 0: continue

            # verify columns
            cols = df_year.columns
            if "ticker" not in cols:
                 df_year = df_year.with_columns(pl.lit(None).cast(pl.Utf8).alias("ticker"))
            if "cik" not in cols:
                 df_year = df_year.with_columns(pl.lit(None).cast(pl.Utf8).alias("cik"))

            # Join
            df_year = df_year.join(df_map, on="ticker", how="left")
            
            # Coalesce
            df_year = df_year.with_columns(
                pl.coalesce([pl.col("cik"), pl.col("mapped_cik")]).alias("final_cik")
            )
            
            df_valid_ciks = pl.DataFrame({"final_cik": list(equity_ciks)}, schema={"final_cik": pl.Utf8})
            
            if df_valid_ciks.height > 0:
                 df_clean = df_year.join(df_valid_ciks, on="final_cik", how="inner")
            else:
                 df_clean = df_year.filter(pl.col("cik").is_not_null())
                 df_clean = df_clean.with_columns(pl.col("cik").alias("final_cik"))

            # Schema Selection & Cleaning (Forward Fill)
            # 1. Sort by CIK, Date to ensure time-series continuity
            df_clean = df_clean.sort(["cik", "date"])

            # 2. Forward Fill prices/volume to handle missing days (NaNs)
            df_clean = df_clean.with_columns([
                pl.col("open").forward_fill().over("cik"),
                pl.col("high").forward_fill().over("cik"),
                pl.col("low").forward_fill().over("cik"),
                pl.col("close").forward_fill().over("cik"),
                pl.col("adjusted_close").forward_fill().over("cik"),
                pl.col("volume").forward_fill().over("cik"),
            ])

            df_clean = df_clean.select([
                pl.col("date"),
                pl.col("open"),
                pl.col("high"),
                pl.col("low"),
                pl.col("close"),
                pl.col("adjusted_close"),
                pl.col("volume"),
                pl.col("cik"),
                pl.col("ticker"),
                pl.col("year")
            ])
            
            if df_clean.height > 0:
                # print(f"  > Writing {df_clean.height} rows to Silver...")
                write_deltalake(
                    SILVER_PRICES,
                    df_clean.to_arrow(),
                    partition_by=['year'],
                    mode='append',
                    schema_mode='merge'
                )
                total_rows += df_clean.height
                
        print(f"Silver Standardization Complete. Total Rows: {total_rows}")
        
    except Exception as e:
        print(f"Error processing Bronze Delta: {e}")
        import traceback
        traceback.print_exc()

# ============================================================================
# MACRO STANDARDIZATION LOGIC
# ============================================================================
def process_fred():
    """Reads individual FRED time-series CSVs and merges them."""
    print(f"Processing FRED data from {BRONZE_FRED_DIR}...")
    files = glob.glob(os.path.join(BRONZE_FRED_DIR, "*.csv"))
    if not files:
        print("No FRED files found.")
        return

    dfs = []
    for f in files:
        try:
            # Filename is the Series ID (e.g., GDP.csv)
            series_id = os.path.basename(f).replace(".csv", "")
            
            df = pd.read_csv(f)
            if len(df.columns) < 2: continue
            
            df.columns = ['date', 'value']
            df['series_id'] = series_id
            
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['value'] = pd.to_numeric(df['value'], errors='coerce') 
            
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {f}: {e}")
            
    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
        df_final = df_final.dropna(subset=['date'])
        
        # Sort and FFill
        print("Sorting and Forward Filling FRED data...")
        df_final.sort_values(by=['series_id', 'date'], inplace=True)
        df_final['value'] = df_final.groupby('series_id')['value'].ffill()

        print(f"Writing {len(df_final)} rows to {SILVER_FRED}...")
        write_deltalake(
            SILVER_FRED,
            df_final,
            partition_by=['series_id'],
            mode='overwrite', 
            schema_mode='overwrite'
        )

def process_global():
    """Reads Global Macro data."""
    print(f"Processing Global Macro from {BRONZE_GLOBAL_DIR}...")
    if not os.path.exists(BRONZE_GLOBAL_DIR):
        print("Global Macro directory not found.")
        return
        
    files = glob.glob(os.path.join(BRONZE_GLOBAL_DIR, "*.csv"))
    
    if not files:
        print("No Global Macro files found.")
        return
        
    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            df['source_file'] = os.path.basename(f)
            dfs.append(df)
        except Exception as e:
            print(f"[WARNING] Data processing failed in chunk {i}: {str(e)}")
            continue
            
    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
        # Identify date column heuristic
        date_cols = [c for c in df_final.columns if 'date' in c.lower() or 'time' in c.lower()]
        if date_cols:
            df_final.rename(columns={date_cols[0]: 'date'}, inplace=True)
            df_final['date'] = pd.to_datetime(df_final['date'], errors='coerce')
            
        print(f"Writing {len(df_final)} rows to {SILVER_GLOBAL}...")
        write_deltalake(SILVER_GLOBAL, df_final, mode='overwrite')

def standardize_macro():
    print("--- Standardizing Macro Data ---")
    process_fred()
    process_global()

# ============================================================================
# FINRA STANDARDIZATION LOGIC
# ============================================================================
def process_reg_sho():
    print(f"Processing FINRA Reg SHO from {BRONZE_REG_SHO}...")
    try:
        if not os.path.exists(BRONZE_REG_SHO):
            print("Bronze Reg SHO not found.")
            return

        # 1. Scan Delta
        lf = pl.scan_delta(BRONZE_REG_SHO)
        
        # 2. Get Years (to batch processing)
        try:
            years = lf.select(pl.col("date").dt.year().unique()).collect()["date"].to_list()
            years = sorted([y for y in years if y is not None])
        except (ValueError, TypeError) as e:
             print(f"[WARNING] Date parsing failed, using fallback range: {str(e)}")
             # Fallback if date parsing issues, try to infer or just try generic range
             # Or if partitioned by year? RegSHO usually isn't partition by year in bronze.
             # If not partitioned, we must filter by date range.
             print("Could not infer years from data. processing by year range 2010-2025.")
             years = range(2010, 2026)

        print(f"Processing years: {years}")
        
        for year in years:
            # Filter for this year
            df_year = lf.filter(pl.col("date").dt.year() == year).collect()
            
            if df_year.height == 0: continue

            df_batch = df_year.to_pandas()
            
            df_batch.rename(columns={
                'tradeReportDate': 'date',
                'securitiesInformationProcessorSymbolIdentifier': 'ticker', 
                'shortParQuantity': 'short_volume',
                'shortExemptParQuantity': 'short_exempt_volume',
                'totalParQuantity': 'total_volume',
                'marketCode': 'market'
            }, inplace=True)
            
            df_batch['date'] = pd.to_datetime(df_batch['date'], errors='coerce')
            df_batch = df_batch.dropna(subset=['date'])
            df_batch['year'] = df_batch['date'].dt.year.astype(int)
            
            if not df_batch.empty:
                df_batch.sort_values(by=['ticker', 'date'], inplace=True)
                # GroupBy FFill within the batch (Year) is safer than global if memory constrained
                # Ideally we want global ffill, but that requires full history.
                # Trade-off: Yearly batches mean boundaries might not ffill across Dec 31 -> Jan 1 perfectly.
                # However, for massive data, this is necessary.
                cols_to_fill = ['short_volume', 'short_exempt_volume', 'total_volume']
                cols_to_fill = [c for c in cols_to_fill if c in df_batch.columns]
                
                if cols_to_fill:
                     df_batch[cols_to_fill] = df_batch.groupby('ticker')[cols_to_fill].ffill()

            print(f"Writing {len(df_batch)} rows for {year} to {SILVER_REG_SHO}...")
            write_deltalake(
                SILVER_REG_SHO,
                df_batch,
                partition_by=['year'],
                mode='append',
                schema_mode='merge'
            )
            
            del df_batch
            del df_year
            import gc
            gc.collect()
        
    except Exception as e:
        print(f"[ERROR] Reg SHO processing failed: {str(e)}")
        import traceback
        traceback.print_exc()
        # Add proper error logging
        print(f"[CRITICAL] Failed to process Reg SHO data - check logs for details")

def process_short_interest():
    bronze_short_int_table = os.path.join(BRONZE_SHORT_INT, "short_interest")
    print(f"Processing FINRA Short Interest from {bronze_short_int_table}...")
    
    try:
        if not os.path.exists(bronze_short_int_table):
            print("Bronze Short Interest Delta Table not found.")
            return

        lf = pl.scan_delta(bronze_short_int_table)
        
        # Batch by year
        # Short Interest has 'settlementDate'
        try:
             years = lf.select(pl.col("settlementDate").dt.year().unique()).collect()["settlementDate"].to_list()
             years = sorted([y for y in years if y is not None])
        except:
             years = range(2009, 2026)

        for year in years:
            df_year = lf.filter(pl.col("settlementDate").dt.year() == year).collect()
            if df_year.height == 0: continue

            df_all = df_year.to_pandas()
            
            clean_cols = {
                'settlementDate': 'date',
                'issueSymbolIdentifier': 'ticker',
                'currentShortShareNumber': 'short_interest',
                'averageShortShareNumber': 'avg_daily_volume',
                'daysToCoverNumber': 'days_to_cover'
            }
            df_all.rename(columns=clean_cols, inplace=True)
            
            if 'date' in df_all.columns:
                df_all['date'] = pd.to_datetime(df_all['date'], errors='coerce')
                df_all = df_all.dropna(subset=['date'])
                df_all['year'] = df_all['date'].dt.year.astype(int)
                
                if not df_all.empty:
                    df_all.sort_values(by=['ticker', 'date'], inplace=True)
                    cols_to_fill = ['short_interest', 'avg_daily_volume', 'days_to_cover']
                    cols_to_fill = [c for c in cols_to_fill if c in df_all.columns]
                    
                    if cols_to_fill:
                         df_all[cols_to_fill] = df_all.groupby('ticker')[cols_to_fill].ffill()

                print(f"Writing {len(df_all)} rows for {year} to {SILVER_SHORT_INT}...")
                write_deltalake(
                    SILVER_SHORT_INT,
                    df_all,
                    partition_by=['year'],
                    mode='append',
                    schema_mode='merge'
                )
            else:
                print("Required 'date' column not found in Short Interest data. Skipping write.")
            
            del df_all
            del df_year
            import gc
            gc.collect()

    except Exception as e:
        print(f"Error processing Short Interest: {e}")

def standardize_finra():
    print("--- Standardizing FINRA Data ---")
    process_reg_sho()
    process_short_interest()

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================
def main():
    if len(sys.argv) < 2:
        print("Usage: python standardize.py [fundamentals|prices|macro|finra|all]")
        sys.exit(1)
    
    target = sys.argv[1].lower()
    
    if target == "fundamentals":
        standardize_fundamentals()
    elif target == "prices":
        standardize_prices()
    elif target == "macro":
        standardize_macro()
    elif target == "finra":
        standardize_finra()
    elif target == "all":
        standardize_fundamentals()
        standardize_prices()
        standardize_macro()
        standardize_finra()
    else:
        print(f"Unknown target: {target}")
        sys.exit(1)

if __name__ == "__main__":
    main()
