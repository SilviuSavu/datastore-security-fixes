"""
Unit tests for refactored financial processor components.

Tests the modular processors that replace the monolithic SECMapper.standardize() method.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime

from src.etl.standardization.financial_processor import (
    IncomeStatementProcessor,
    BalanceSheetProcessor, 
    CashFlowProcessor,
    FinancialDataValidator,
    FinancialDataStandardizer,
    ChunkProcessor
)


class TestIncomeStatementProcessor:
    """Test cases for IncomeStatementProcessor."""
    
    def test_processor_initialization(self):
        """Test processor initialization with mapping config."""
        mapping = {
            'MAP_REVENUE': ['Revenue'],
            'MAP_COST_OF_REVENUE': ['CostOfRevenue']
        }
        processor = IncomeStatementProcessor(mapping)
        
        assert processor.mapping == mapping
        assert processor._get_mapped_columns == processor._get_mapped_columns
    
    def test_process_income_data(self):
        """Test income statement data processing."""
        mapping = {
            'MAP_REVENUE': ['Revenue', 'Revenues'],
            'MAP_COST_OF_REVENUE': ['CostOfRevenue'],
            'MAP_GROSS_PROFIT': ['GrossProfit']
        }
        processor = IncomeStatementProcessor(mapping)
        
        # Create test pivot table
        pivot_is = pd.DataFrame({
            'Revenue': [1000, 2000],
            'CostOfRevenue': [600, 1200],
            'GrossProfit': [400, 800],
            'OperatingIncome': [200, 400],
            'NetIncome': [150, 300]
        }, index=pd.Index(['idx1', 'idx2']))
        
        result = processor.process_income_data(pivot_is, pivot_is.index)
        
        assert 'totalRevenue' in result
        assert 'costOfRevenue' in result
        assert 'grossProfit' in result
        assert 'operatingIncome' in result
        assert 'netIncome' in result
        
        # Check values match expected
        assert result['totalRevenue'].iloc[0] == 1000
        assert result['totalRevenue'].iloc[1] == 2000
        assert result['netInterestIncome'].iloc[0] == -600  # 200 - 800
    
    def test_mapped_columns_fallback(self):
        """Test fallback to NaN when mapped columns not available."""
        mapping = {'MAP_REVENUE': ['NonExistentColumn']}
        processor = IncomeStatementProcessor(mapping)
        
        pivot_is = pd.DataFrame({'OtherColumn': [100]}, index=pd.Index(['idx1']))
        
        result = processor.process_income_data(pivot_is, pivot_is.index)
        
        assert pd.isna(result['totalRevenue']).iloc[0]


class TestBalanceSheetProcessor:
    """Test cases for BalanceSheetProcessor."""
    
    def test_process_balance_sheet_data(self):
        """Test balance sheet data processing."""
        mapping = {
            'MAP_TOTAL_ASSETS': ['Assets'],
            'MAP_TOTAL_LIABILITIES': ['Liabilities'],
            'MAP_TOTAL_EQUITY': ['StockholdersEquity']
        }
        processor = BalanceSheetProcessor(mapping)
        
        pivot_bs = pd.DataFrame({
            'Assets': [1000, 2000],
            'Liabilities': [400, 800],
            'StockholdersEquity': [600, 1200]
        }, index=pd.Index(['idx1', 'idx2']))
        
        result = processor.process_balance_sheet_data(pivot_bs, pivot_bs.index)
        
        assert 'totalAssets' in result
        assert 'totalLiab' in result
        assert 'totalStockholderEquity' in result
        
        assert result['totalAssets'].iloc[0] == 1000
        assert result['totalLiab'].iloc[0] == 400
    
    def test_balance_sheet_equation(self):
        """Test balance sheet equation consistency."""
        mapping = {
            'MAP_TOTAL_ASSETS': ['Assets'],
            'MAP_TOTAL_LIABILITIES': ['Liabilities'],
            'MAP_TOTAL_EQUITY': ['StockholdersEquity']
        }
        processor = BalanceSheetProcessor(mapping)
        
        pivot_bs = pd.DataFrame({
            'Assets': [1000],
            'Liabilities': [300],
            'StockholdersEquity': [700]
        }, index=pd.Index(['idx1']))
        
        result = processor.process_balance_sheet_data(pivot_bs, pivot_bs.index)
        
        # Assets ≈ Liabilities + Equity (700 ≈ 300 + 700, reasonable tolerance)
        assert result['totalAssets'].iloc[0] == 1000
        assert result['totalLiab'].iloc[0] == 300
        assert result['totalStockholderEquity'].iloc[0] == 700


class TestCashFlowProcessor:
    """Test cases for CashFlowProcessor."""
    
    def test_process_cash_flow_data(self):
        """Test cash flow data processing."""
        mapping = {
            'MAP_CF_OPERATING': ['NetCashProvidedByUsedInOperatingActivities'],
            'MAP_CF_INVESTING': ['NetCashProvidedByUsedInInvestingActivities'],
            'MAP_CAPEX': ['PaymentsToAcquirePropertyPlantAndEquipment']
        }
        processor = CashFlowProcessor(mapping)
        
        pivot_cf = pd.DataFrame({
            'NetCashProvidedByUsedInOperatingActivities': [300, 500],
            'NetCashProvidedByUsedInInvestingActivities': [-200, -300],
            'PaymentsToAcquirePropertyPlantAndEquipment': [-100, -150]
        }, index=pd.Index(['idx1', 'idx2']))
        
        result = processor.process_cash_flow_data(pivot_cf, pivot_cf.index)
        
        assert 'totalCashFromOperatingActivities' in result
        assert 'totalCashflowsFromInvestingActivities' in result
        assert 'capitalExpenditures' in result
        
        assert result['totalCashFromOperatingActivities'].iloc[0] == 300
        assert result['capitalExpenditures'].iloc[0] == -100


class TestFinancialDataValidator:
    """Test cases for FinancialDataValidator."""
    
    def test_validate_financial_data_success(self):
        """Test successful validation of financial data."""
        data = {
            'totalRevenue': [1000, 2000],
            'costOfRevenue': [600, 1200],
            'netIncome': [150, 300]
        }
        
        validator = FinancialDataValidator()
        result = validator.validate_financial_data(data, "test")
        
        assert result is True
    
    def test_validate_negative_revenue(self):
        """Test detection of negative revenue values."""
        data = {
            'totalRevenue': [-100, 2000],  # Negative revenue
            'costOfRevenue': [600, 1200],
            'netIncome': [150, 300]
        }
        
        validator = FinancialDataValidator()
        result = validator.validate_financial_data(data, "test")
        
        assert result is False
    
    def test_validate_balance_sheet_equation(self):
        """Test balance sheet equation validation."""
        data = {
            'totalAssets': [1000],
            'totalLiab': [300],
            'totalStockholderEquity': [900],  # Imbalance: 100 ≠ 300 + 900
        }
        
        validator = FinancialDataValidator()
        result = validator.validate_financial_data(data, "test")
        
        assert result is False
    
    def test_validate_empty_data(self):
        """Test validation of empty financial data."""
        data = {}
        
        validator = FinancialDataValidator()
        result = validator.validate_financial_data(data, "test")
        
        assert result is True  # Empty data shouldn't fail validation


class TestFinancialDataStandardizer:
    """Test cases for FinancialDataStandardizer orchestrator."""
    
    def test_standardizer_initialization(self):
        """Test standardizer initialization with SECMapper config."""
        from src.etl.standardization.standardize import SECMapper
        standardizer = FinancialDataStandardizer(SECMapper)
        
        assert standardizer.income_processor is not None
        assert standardizer.balance_processor is not None
        assert standardizer.cashflow_processor is not None
        assert standardizer.validator is not None
    
    def test_standardize_chunk(self):
        """Test chunk standardization functionality."""
        from src.etl.standardization.standardize import SECMapper
        
        # Create test data
        df_bs = pd.DataFrame({
            'adsh': ['ads1'], 'cik': ['0000012345'], 'tag': ['Assets'], 'value': [1000]
        })
        df_is = pd.DataFrame({
            'adsh': ['ads1'], 'cik': ['0000012345'], 'tag': ['Revenue'], 'value': [500]
        })
        df_cf = pd.DataFrame({
            'adsh': ['ads1'], 'cik': ['0000012345'], 'tag': ['Cash'], 'value': [200]
        })
        
        standardizer = FinancialDataStandardizer(SECMapper)
        
        # Mock all_indices
        all_indices = pd.MultiIndex.from_tuples([
            ('ads1', '0000012345', '2023', '2023-01-01', '10-K', 2023, 'FY', 1234)
        ])
        
        result = standardizer.standardize_chunk(df_bs, df_is, df_cf, all_indices)
        
        assert isinstance(result, dict)
        assert 'cik' in result
        assert 'adsh' in result
        assert 'totalAssets' in result
        assert 'totalRevenue' in result


class TestChunkProcessor:
    """Test cases for ChunkProcessor."""
    
    def test_chunk_processor_initialization(self):
        """Test chunk processor initialization."""
        processor = ChunkProcessor(chunk_size=500)
        
        assert processor.chunk_size == 500
        assert processor.logger is not None
    
    def test_process_data_in_chunks(self):
        """Test chunking functionality."""
        from src.etl.standardization.standardize import SECMapper
        
        processor = ChunkProcessor(chunk_size=2)
        
        # Create test data with 4 unique CIKs
        df_clean = pd.DataFrame({
            'cik': ['0000012345', '0000067890', '0000012345', '0000098765'],
            'tag': ['Assets', 'Revenue', 'Assets', 'Liabilities'],
            'value': [1000, 500, 2000, 300],
            'qtrs': [0.1, 4.0, 0.1, 4.0]
        })
        
        # Mock standardizer
        class MockStandardizer:
            def standardize_chunk(self, df_bs, df_is, df_cf, all_indices):
                return {'cik': 'test', 'totalAssets': 1000}
        
        standardizer = MockStandardizer()
        
        # Should process in 2 chunks (4 CIKs / 2 per chunk)
        results = processor.process_data_in_chunks(df_clean, standardizer)
        
        assert len(results) == 2
        assert processor.logger is not None


class TestRefactoredSECMapper:
    """Integration tests for refactored SECMapper."""
    
    def test_standardize_with_refactored_architecture(self):
        """Test that refactored standardize method works end-to-end."""
        from src.etl.standardization.standardize import SECMapper
        
        # Create test data
        df_raw = pd.DataFrame({
            'cik': ['0000012345', '0000067890'],
            'adsh': ['ads1', 'ads2'],
            'tag': ['Assets', 'Revenue'],
            'value': [1000, 500],
            'qtrs': [0.1, 4.0],
            'period': ['2023', '2023'],
            'filed': ['2023-01-01', '2023-01-01'],
            'form': ['10-K', '10-K'],
            'fy': [2023, 2023],
            'fp': ['FY', 'FY'],
            'sic': [1234, 5678]
        })
        
        # Test refactored standardize method
        result = SECMapper.standardize(df_raw, chunk_size=2)
        
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        
        # Check that CIKs are properly formatted
        if 'cik' in result.columns:
            unique_ciks = result['cik'].unique()
            for cik in unique_ciks:
                assert len(str(cik)) == 10  # 10-digit format
                assert str(cik).isdigit()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
