#!/usr/bin/env python3
"""
Comprehensive Unit Tests for Refactored Code
===========================================

Unit tests for the refactored standardize.py and bronze_data_pipeline.py files.
These tests validate the technical debt refactoring implementation.
"""

import unittest
import os
import sys
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from datetime import datetime

# Add the project root to Python path for imports
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Import the refactored modules
from src.etl.standardization.standardize import (
    _Config,
    _normalize_cik,
    _batch_cik_normalization,
    _process_financial_statement,
    _map_financial_concepts,
    _calculate_derived_metrics,
    _validate_cik_dataframe
)

from src.etl.bronze_data_pipeline import (
    _PipelineConfig,
    _normalize_cik as _normalize_cik_bronze,
    _batch_cik_normalization as _batch_cik_normalization_bronze,
    _validate_cik_dataframe as _validate_cik_dataframe_bronze,
    _process_financial_data_chunk,
    _standardize_financial_value,
    _validate_financial_dataframe
)

class TestStandardizeRefactoring(unittest.TestCase):
    """Test cases for standardize.py refactoring"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = _Config()
        self.test_data_dir = os.path.join(project_root, "test_data")
        os.makedirs(self.test_data_dir, exist_ok=True)

    def test_config_initialization(self):
        """Test configuration system initialization"""
        self.assertIsNotNone(self.config.PROJECT_ROOT)
        self.assertTrue(os.path.exists(self.config.SILVER_MARKET))
        self.assertTrue(os.path.exists(self.config.SILVER_FUNDS))
        self.assertEqual(self.config.CIK_FORMAT, "string")
        self.assertEqual(self.config.DEFAULT_CIK_LENGTH, 10)

    def test_cik_normalization(self):
        """Test CIK normalization function"""
        # Test valid CIKs
        self.assertEqual(_normalize_cik("0000320193"), "0000320193")
        self.assertEqual(_normalize_cik("320193"), "0000320193")
        self.assertEqual(_normalize_cik(320193), "0000320193")
        self.assertEqual(_normalize_cik(320193.0), "0000320193")

        # Test edge cases
        self.assertEqual(_normalize_cik("  320193  "), "0000320193")
        self.assertEqual(_normalize_cik("A320193B"), "0000320193")

        # Test invalid CIKs
        with self.assertRaises(Exception):
            _normalize_cik(None)
        with self.assertRaises(Exception):
            _normalize_cik("")
        with self.assertRaises(Exception):
            _normalize_cik("123")  # Too short

    def test_batch_cik_normalization(self):
        """Test batch CIK normalization"""
        test_df = pd.DataFrame({
            'cik': ['320193', '0000320193', '1318605', None, 'invalid'],
            'name': ['Apple', 'Apple', 'Tesla', 'Unknown', 'Invalid']
        })

        result_df = _batch_cik_normalization(test_df)
        self.assertEqual(result_df.loc[0, 'cik'], '0000320193')
        self.assertEqual(result_df.loc[1, 'cik'], '0000320193')
        self.assertEqual(result_df.loc[2, 'cik'], '0001318605')

    def test_financial_processing_helpers(self):
        """Test financial processing helper functions"""
        # Test statement processing
        statement_data = {
            'Assets': 1000000,
            'Liabilities': 500000,
            'Revenue': 2000000,
            'NetIncome': 300000
        }
        processed = _process_financial_statement(statement_data)
        self.assertIn('working_capital', processed)
        self.assertIn('profit_margin', processed)

        # Test concept mapping
        concepts = ['Revenue', 'NetIncome', 'Assets', 'Liabilities', 'UnknownConcept']
        mapped = _map_financial_concepts(concepts)
        self.assertEqual(mapped['Revenue'], 'revenue')
        self.assertEqual(mapped['UnknownConcept'], 'other')

        # Test derived metrics
        financials = {
            'revenue': 2000000,
            'net_income': 300000,
            'total_assets': 1000000,
            'total_liabilities': 500000
        }
        metrics = _calculate_derived_metrics(financials)
        self.assertAlmostEqual(metrics['profit_margin'], 0.15)
        self.assertAlmostEqual(metrics['debt_to_equity'], 1.0)

    def test_dataframe_validation(self):
        """Test DataFrame validation functions"""
        # Test valid DataFrame
        valid_df = pd.DataFrame({
            'cik': ['0000320193', '0001318605'],
            'concept': ['Revenue', 'NetIncome'],
            'value': [2000000, 300000]
        })
        self.assertTrue(_validate_cik_dataframe(valid_df))

        # Test invalid DataFrame
        invalid_df = pd.DataFrame({
            'cik': ['320193', 'invalid_cik'],
            'concept': ['Revenue', 'NetIncome']
        })
        self.assertFalse(_validate_cik_dataframe(invalid_df))

class TestBronzePipelineRefactoring(unittest.TestCase):
    """Test cases for bronze_data_pipeline.py refactoring"""

    def setUp(self):
        """Set up test fixtures"""
        self.config = _PipelineConfig()
        self.test_data_dir = os.path.join(project_root, "test_data")
        os.makedirs(self.test_data_dir, exist_ok=True)

    def test_pipeline_config_initialization(self):
        """Test pipeline configuration system initialization"""
        self.assertIsNotNone(self.config.PROJECT_ROOT)
        self.assertTrue(os.path.exists(self.config.SEC_BULK_DIR))
        self.assertTrue(os.path.exists(self.config.RAW_OUTPUT_DIR))
        self.assertEqual(self.config.CIK_FORMAT, "string")
        self.assertEqual(self.config.DEFAULT_CIK_LENGTH, 10)

    def test_cik_normalization_bronze(self):
        """Test CIK normalization function in bronze pipeline"""
        # Test valid CIKs
        self.assertEqual(_normalize_cik_bronze("0000320193"), "0000320193")
        self.assertEqual(_normalize_cik_bronze("320193"), "0000320193")
        self.assertEqual(_normalize_cik_bronze(320193), "0000320193")

        # Test edge cases
        self.assertEqual(_normalize_cik_bronze("  320193  "), "0000320193")
        self.assertEqual(_normalize_cik_bronze("A320193B"), "0000320193")

        # Test invalid CIKs
        with self.assertRaises(Exception):
            _normalize_cik_bronze(None)
        with self.assertRaises(Exception):
            _normalize_cik_bronze("")
        with self.assertRaises(Exception):
            _normalize_cik_bronze("123")  # Too short

    def test_batch_cik_normalization_bronze(self):
        """Test batch CIK normalization in bronze pipeline"""
        test_df = pd.DataFrame({
            'cik': ['320193', '0000320193', '1318605', None, 'invalid'],
            'name': ['Apple', 'Apple', 'Tesla', 'Unknown', 'Invalid']
        })

        result_df = _batch_cik_normalization_bronze(test_df)
        self.assertEqual(result_df.loc[0, 'cik'], '0000320193')
        self.assertEqual(result_df.loc[1, 'cik'], '0000320193')
        self.assertEqual(result_df.loc[2, 'cik'], '0001318605')

    def test_financial_processing_helpers_bronze(self):
        """Test financial processing helper functions in bronze pipeline"""
        # Test financial data chunk processing
        test_df = pd.DataFrame({
            'cik': ['320193', '1318605'],
            'concept': ['Revenue', 'NetIncome'],
            'value': [2000000, 300000],
            'unit': ['USD', 'USD'],
            'period_start': ['2023-01-01', '2023-01-01'],
            'period_end': ['2023-12-31', '2023-12-31'],
            'filing_date': ['2024-02-15', '2024-02-15']
        })

        processed_df = _process_financial_data_chunk(test_df, self.config)
        self.assertTrue('standardized_value' in processed_df.columns)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(processed_df['period_start']))

        # Test financial value standardization
        self.assertEqual(_standardize_financial_value(2000000, 'USD'), 2000000.0)
        self.assertEqual(_standardize_financial_value(15.5, 'percent'), 0.155)
        self.assertEqual(_standardize_financial_value(None, 'USD'), 0.0)

        # Test financial DataFrame validation
        self.assertTrue(_validate_financial_dataframe(processed_df))

    def test_dataframe_validation_bronze(self):
        """Test DataFrame validation functions in bronze pipeline"""
        # Test valid DataFrame
        valid_df = pd.DataFrame({
            'cik': ['0000320193', '0001318605'],
            'concept': ['Revenue', 'NetIncome'],
            'value': [2000000, 300000],
            'period_end': ['2023-12-31', '2023-12-31']
        })
        self.assertTrue(_validate_cik_dataframe_bronze(valid_df))

        # Test invalid DataFrame
        invalid_df = pd.DataFrame({
            'cik': ['320193', 'invalid_cik'],
            'concept': ['Revenue', 'NetIncome']
        })
        self.assertFalse(_validate_cik_dataframe_bronze(invalid_df))

class TestIntegration(unittest.TestCase):
    """Integration tests for both refactored modules"""

    def test_cik_consistency_across_modules(self):
        """Test that CIK normalization is consistent between modules"""
        test_ciks = ['320193', '0000320193', '1318605', 1318605, 320193.0]

        for cik in test_ciks:
            std_result = _normalize_cik(cik)
            bronze_result = _normalize_cik_bronze(cik)
            self.assertEqual(std_result, bronze_result,
                           f"CIK normalization inconsistent for {cik}")

    def test_batch_processing_consistency(self):
        """Test that batch processing is consistent between modules"""
        test_df = pd.DataFrame({
            'cik': ['320193', '0000320193', '1318605', None, 'invalid'],
            'name': ['Apple', 'Apple', 'Tesla', 'Unknown', 'Invalid']
        })

        std_result = _batch_cik_normalization(test_df.copy())
        bronze_result = _batch_cik_normalization_bronze(test_df.copy())

        # Compare normalized CIK values
        for i in range(len(test_df)):
            if pd.notna(test_df.loc[i, 'cik']):
                std_cik = std_result.loc[i, 'cik']
                bronze_cik = bronze_result.loc[i, 'cik']
                self.assertEqual(std_cik, bronze_cik,
                               f"Batch normalization inconsistent at index {i}")

if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)