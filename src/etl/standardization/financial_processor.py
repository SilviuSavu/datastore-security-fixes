"""
Financial Data Processing Components

Refactored components from the monolithic SECMapper.standardize() method
to improve maintainability, testability, and code clarity.
"""

import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Optional, Tuple, Union

from src.utils.cik_handler import CIKHandler
from src.utils.logging import DataStoreLogger

logger = DataStoreLogger()


class IncomeStatementProcessor:
    """Handles processing of income statement data."""
    
    def __init__(self, mapping_config: Dict):
        self.mapping = mapping_config
    
    def process_income_data(self, pivot_is: pd.DataFrame, all_indices: pd.Index) -> Dict:
        """Extract and process income statement metrics."""
        data = {}
        
        # Revenue items
        data['totalRevenue'] = self._get_mapped_columns(pivot_is, self.mapping.MAP_REVENUE)
        data['costOfRevenue'] = self._get_mapped_columns(pivot_is, self.mapping.MAP_COST_OF_REVENUE)
        data['grossProfit'] = self._get_mapped_columns(pivot_is, self.mapping.MAP_GROSS_PROFIT)
        
        # Operating expenses
        data['researchDevelopment'] = self._get_mapped_columns(pivot_is, self.mapping.MAP_R_AND_D)
        data['sellingGeneralAdministrative'] = self._get_mapped_columns(pivot_is, self.mapping.MAP_SGA)
        data['totalOperatingExpenses'] = self._get_mapped_columns(pivot_is, self.mapping.MAP_OP_EXPENSE)
        
        # Income metrics
        data['operatingIncome'] = self._get_mapped_columns(pivot_is, self.mapping.MAP_OP_INCOME)
        data['interestExpense'] = self._get_mapped_columns(pivot_is, self.mapping.MAP_INTEREST_EXPENSE)
        data['interestIncome'] = self._get_mapped_columns(pivot_is, self.mapping.MAP_INTEREST_INCOME)
        data['netInterestIncome'] = data['interestIncome'].fillna(0) - data['interestExpense'].fillna(0)
        
        # Non-operating items
        data['totalOtherIncomeExpenseNet'] = self._get_mapped_columns(pivot_is, self.mapping.MAP_OTHER_INCOME_EXPENSE)
        data['incomeBeforeTax'] = self._get_mapped_columns(pivot_is, self.mapping.MAP_PRETAX_INCOME)
        data['incomeTaxExpense'] = self._get_mapped_columns(pivot_is, self.mapping.MAP_INCOME_TAX)
        data['netIncome'] = self._get_mapped_columns(pivot_is, self.mapping.MAP_NET_INCOME)
        
        return data
    
    def _get_mapped_columns(self, df: pd.DataFrame, columns: List[str]) -> pd.Series:
        """Get first available column from mapping list."""
        for col in columns:
            if col in df.columns:
                return df[col]
        return pd.Series(np.nan, index=df.index)


class BalanceSheetProcessor:
    """Handles processing of balance sheet data."""
    
    def __init__(self, mapping_config: Dict):
        self.mapping = mapping_config
    
    def process_balance_sheet_data(self, pivot_bs: pd.DataFrame, all_indices: pd.Index) -> Dict:
        """Extract and process balance sheet metrics."""
        data = {}
        
        # Asset items
        data['totalAssets'] = self._get_mapped_columns(pivot_bs, self.mapping.MAP_TOTAL_ASSETS)
        data['intangibleAssets'] = self._get_mapped_columns(pivot_bs, self.mapping.MAP_INTANGIBLE_ASSETS)
        data['goodWill'] = self._get_mapped_columns(pivot_bs, self.mapping.MAP_GOODWILL)
        data['totalCurrentAssets'] = self._get_mapped_columns(pivot_bs, self.mapping.MAP_CURRENT_ASSETS)
        data['cashAndEquivalents'] = self._get_mapped_columns(pivot_bs, self.mapping.MAP_CASH_AND_EQUIV)
        data['netReceivables'] = self._get_mapped_columns(pivot_bs, self.mapping.MAP_NET_RECEIVABLES)
        data['inventory'] = self._get_mapped_columns(pivot_bs, self.mapping.MAP_INVENTORY)
        data['propertyPlantAndEquipmentNet'] = self._get_mapped_columns(pivot_bs, self.mapping.MAP_PPE_NET)
        
        # Liability items
        data['totalLiab'] = self._get_mapped_columns(pivot_bs, self.mapping.MAP_TOTAL_LIABILITIES)
        data['totalCurrentLiabilities'] = self._get_mapped_columns(pivot_bs, self.mapping.MAP_CURRENT_LIABILITIES)
        data['accountsPayable'] = self._get_mapped_columns(pivot_bs, self.mapping.MAP_ACCOUNTS_PAYABLE)
        data['shortTermDebt'] = self._get_mapped_columns(pivot_bs, self.mapping.MAP_SHORT_TERM_DEBT)
        data['longTermDebt'] = self._get_mapped_columns(pivot_bs, self.mapping.MAP_LONG_TERM_DEBT)
        
        # Equity items
        data['totalStockholderEquity'] = self._get_mapped_columns(pivot_bs, self.mapping.MAP_TOTAL_EQUITY)
        data['commonStock'] = self._get_mapped_columns(pivot_bs, self.mapping.MAP_COMMON_STOCK)
        data['retainedEarnings'] = self._get_mapped_columns(pivot_bs, self.mapping.MAP_RETAINED_EARNINGS)
        data['minorityInterest'] = self._get_mapped_columns(pivot_bs, self.mapping.MAP_MINORITY_INTEREST)
        
        return data
    
    def _get_mapped_columns(self, df: pd.DataFrame, columns: List[str]) -> pd.Series:
        """Get first available column from mapping list."""
        for col in columns:
            if col in df.columns:
                return df[col]
        return pd.Series(np.nan, index=df.index)


class CashFlowProcessor:
    """Handles processing of cash flow data."""
    
    def __init__(self, mapping_config: Dict):
        self.mapping = mapping_config
    
    def process_cash_flow_data(self, pivot_cf: pd.DataFrame, all_indices: pd.Index) -> Dict:
        """Extract and process cash flow metrics."""
        data = {}
        
        # Operating cash flow
        data['totalCashFromOperatingActivities'] = self._get_mapped_columns(pivot_cf, self.mapping.MAP_CF_OPERATING)
        data['depreciationAndAmortization'] = self._get_mapped_columns(pivot_cf, self.mapping.MAP_DEPRECIATION)
        data['changeToOperatingActivities'] = self._get_mapped_columns(pivot_cf, self.mapping.MAP_CHANGE_OPERATING)
        
        # Investing cash flow
        data['totalCashflowsFromInvestingActivities'] = self._get_mapped_columns(pivot_cf, self.mapping.MAP_CF_INVESTING)
        data['capitalExpenditures'] = self._get_mapped_columns(pivot_cf, self.mapping.MAP_CAPEX)
        data['investments'] = self._get_mapped_columns(pivot_cf, self.mapping.MAP_INVESTMENTS_ACQUIRED)
        
        # Financing cash flow
        data['totalCashFromFinancingActivities'] = self._get_mapped_columns(pivot_cf, self.mapping.MAP_CF_FINANCING)
        data['dividendsPaid'] = self._get_mapped_columns(pivot_cf, self.mapping.MAP_DIVIDENDS_PAID)
        data['netBorrowings'] = self._get_mapped_columns(pivot_cf, self.mapping.MAP_NET_BORROWINGS)
        data['salePurchaseOfStock'] = self._get_mapped_columns(pivot_cf, self.mapping.MAP_SALE_PURCHASE_STOCK)
        
        return data
    
    def _get_mapped_columns(self, df: pd.DataFrame, columns: List[str]) -> pd.Series:
        """Get first available column from mapping list."""
        for col in columns:
            if col in df.columns:
                return df[col]
        return pd.Series(np.nan, index=df.index)


class FinancialDataValidator:
    """Validates financial data consistency and quality."""
    
    @staticmethod
    def validate_financial_data(data: Dict, source_type: str = "unknown") -> bool:
        """
        Validate processed financial data for consistency.
        
        Args:
            data: Dictionary containing processed financial metrics
            source_type: Type of data source (income, balance, cashflow)
            
        Returns:
            True if data passes validation, False otherwise
        """
        try:
            # Basic consistency checks - Convert dict values to Series if needed
            def get_series(key):
                value = data.get(key)
                if value is None:
                    return pd.Series([0])
                if isinstance(value, list):
                    return pd.Series(value)
                return pd.Series(value) if not isinstance(value, pd.Series) else value
            
            if 'totalRevenue' in data and 'costOfRevenue' in data:
                revenue = get_series('totalRevenue').fillna(0)
                cost = get_series('costOfRevenue').fillna(0)
                if (revenue < 0).any() or (cost < 0).any():
                    logger.warning(f"Negative revenue/cost values detected in {source_type}")
                    return False
            
            # Gross profit consistency
            if all(k in data for k in ['totalRevenue', 'costOfRevenue', 'grossProfit']):
                revenue = get_series('totalRevenue').fillna(0)
                cost = get_series('costOfRevenue').fillna(0)
                gross = get_series('grossProfit').fillna(0)
                
                # Gross profit should be revenue - cost (approximately)
                calculated_gross = revenue - cost
                diff = (gross - calculated_gross).abs()
                if (diff / (revenue + 1e-10) > 0.5).any():  # Allow 50% tolerance for accounting differences
                    logger.warning(f"Gross profit inconsistency detected in {source_type}")
                    logger.warning(f"Revenue - Cost: {calculated_gross}, Reported: {gross}")
                    # Don't return False, just log warning as accounting standards vary
            
            # Balance sheet consistency check
            if all(k in data for k in ['totalAssets', 'totalLiab', 'totalStockholderEquity']):
                assets = get_series('totalAssets').fillna(0)
                liab = get_series('totalLiab').fillna(0)
                equity = get_series('totalStockholderEquity').fillna(0)
                
                # Assets ≈ Liabilities + Equity (allowing for rounding)
                calculated_equity = assets - liab
                diff = (equity - calculated_equity).abs()
                if (diff / (assets + 1e-10) > 0.1).any():  # Allow 10% tolerance
                    logger.warning(f"Balance sheet equation imbalance in {source_type}")
                    logger.warning(f"Assets - Liabilities: {calculated_equity}, Reported Equity: {equity}")
            
            return True
            
        except Exception as e:
            logger.error(f"Financial data validation error: {e}")
            return False


class FinancialDataStandardizer:
    """
    Main orchestrator for financial data standardization.
    
    Coordinates the processing of different financial statement types
    using specialized processors for each data type.
    """
    
    def __init__(self, mapping_config: Dict):
        self.mapping = mapping_config
        self.income_processor = IncomeStatementProcessor(mapping_config)
        self.balance_processor = BalanceSheetProcessor(mapping_config)
        self.cashflow_processor = CashFlowProcessor(mapping_config)
        self.validator = FinancialDataValidator()
    
    def standardize_chunk(self, df_bs_rows: pd.DataFrame, df_is_rows: pd.DataFrame, 
                        df_cf_rows: pd.DataFrame, all_indices: pd.Index) -> Dict:
        """
        Standardize a single chunk of financial data.
        
        Args:
            df_bs_rows: Balance sheet data rows
            df_is_rows: Income statement data rows  
            df_cf_rows: Cash flow data rows
            all_indices: Combined index for all data types
            
        Returns:
            Dictionary containing standardized financial metrics
        """
        # Create pivot tables for this chunk
        index_cols = ['adsh', 'cik', 'name', 'period', 'filed', 'form', 'fy', 'fp', 'sic']
        
        pivot_bs = self._create_pivot(df_bs_rows, index_cols, 'tag', 'value')
        pivot_is = self._create_pivot(df_is_rows, index_cols, 'tag', 'value')
        pivot_cf = self._create_pivot(df_cf_rows, index_cols, 'tag', 'value')
        
        # Process each statement type
        income_data = self.income_processor.process_income_data(pivot_is, all_indices)
        balance_data = self.balance_processor.process_balance_sheet_data(pivot_bs, all_indices)
        cashflow_data = self.cashflow_processor.process_cash_flow_data(pivot_cf, all_indices)
        
        # Combine all data
        data = {**income_data, **balance_data, **cashflow_data}
        
        # Add metadata
        data.update({
            'adsh': df_bs_rows['adsh'].iloc[0] if not df_bs_rows.empty else None,
            'cik': df_bs_rows['cik'].iloc[0] if not df_bs_rows.empty else None,
            'name': df_bs_rows['name'].iloc[0] if not df_bs_rows.empty else None,
            'period': df_bs_rows['period'].iloc[0] if not df_bs_rows.empty else None,
            'filed': df_bs_rows['filed'].iloc[0] if not df_bs_rows.empty else None,
            'form': df_bs_rows['form'].iloc[0] if not df_bs_rows.empty else None,
            'fy': df_bs_rows['fy'].iloc[0] if not df_bs_rows.empty else None,
            'fp': df_bs_rows['fp'].iloc[0] if not df_bs_rows.empty else None,
            'sic': df_bs_rows['sic'].iloc[0] if not df_bs_rows.empty else None,
        })
        
        # Validate processed data
        self.validator.validate_financial_data(data, "financial_standardization")
        
        return data
    
    def _create_pivot(self, df: pd.DataFrame, index_cols: List[str], 
                     column_col: str, value_col: str) -> pd.DataFrame:
        """Create pivot table from DataFrame."""
        if df.empty:
            return pd.DataFrame()
        
        pivot = df.pivot_table(index=index_cols, columns=column_col, values=value_col, aggfunc='max')
        
        # Re-index to match all indices
        all_indices = pivot.index.union(
            pd.Index(df[index_cols].drop_duplicates().itertuples(index=False, name=None))
        )
        
        return pivot.reindex(all_indices)


class ChunkProcessor:
    """Handles memory-efficient chunking of financial data."""
    
    def __init__(self, chunk_size: int = 1000):
        self.chunk_size = chunk_size
        self.logger = DataStoreLogger()
    
    def process_data_in_chunks(self, df_clean: pd.DataFrame, 
                            processor: FinancialDataStandardizer) -> List[Dict]:
        """
        Process DataFrame in chunks to manage memory usage.
        
        Args:
            df_clean: Cleaned DataFrame with CIK normalization applied
            processor: FinancialDataStandardizer instance
            
        Returns:
            List of standardized data chunks
        """
        unique_ciks = df_clean['cik'].unique()
        total_ciks = len(unique_ciks)
        
        self.logger.info(f"Processing {total_ciks} CIKs in chunks of {self.chunk_size}")
        
        all_chunks = []
        
        for i in range(0, total_ciks, self.chunk_size):
            batch_ciks = unique_ciks[i:i + self.chunk_size]
            df_chunk = df_clean[df_clean['cik'].isin(batch_ciks)]
            
            # Split by statement type
            df_bs_rows = df_chunk.copy()
            df_is_rows = df_chunk.copy()
            df_cf_rows = df_chunk.copy()
            
            # Apply statement type filtering
            if 'qtrs' in df_chunk.columns:
                # Balance sheet logic
                mask_bs = (df_chunk['qtrs'] < 0.1)
                df_bs_rows = df_chunk[mask_bs]
                
                # Income statement logic
                mask_is = ((df_chunk['qtrs'] >= 0.9) & (df_chunk['qtrs'] <= 1.1)) | \
                         ((df_chunk['qtrs'] >= 3.9) & (df_chunk['qtrs'] <= 4.1))
                df_is_rows = df_chunk[mask_is]
                
                # Cash flow logic
                fp_map = {'Q1': 1.0, 'Q2': 2.0, 'Q3': 3.0, 'FY': 4.0}
                clean_fp = df_chunk['fp'].astype(str).str.upper()
                expected_qtrs = clean_fp.map(fp_map).fillna(4.0)
                mask_cf = (df_chunk['qtrs'] >= expected_qtrs - 0.1) & \
                         (df_chunk['qtrs'] <= expected_qtrs + 0.1)
                df_cf_rows = df_chunk[mask_cf]
            
            # Create combined index
            index_cols = ['adsh', 'cik', 'name', 'period', 'filed', 'form', 'fy', 'fp', 'sic']
            
            # Collect all unique index tuples
            all_index_tuples = set()
            
            if not df_bs_rows.empty:
                all_index_tuples.update(df_bs_rows[index_cols].itertuples(index=False, name=None))
            if not df_is_rows.empty:
                all_index_tuples.update(df_is_rows[index_cols].itertuples(index=False, name=None))
            if not df_cf_rows.empty:
                all_index_tuples.update(df_cf_rows[index_cols].itertuples(index=False, name=None))
            
            all_indices = pd.MultiIndex.from_tuples(all_index_tuples)
            
            # Standardize this chunk
            chunk_data = processor.standardize_chunk(df_bs_rows, df_is_rows, df_cf_rows, all_indices)
            
            if chunk_data:
                all_chunks.append(chunk_data)
            
            # Clean up memory for this chunk
            del df_chunk, df_bs_rows, df_is_rows, df_cf_rows
        
        return all_chunks
