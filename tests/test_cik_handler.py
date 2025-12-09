"""
Unit tests for CIK Handler module.

Tests the centralized CIK handling utilities to ensure data consistency
and type safety throughout the DataStore project.
"""

import pytest
import pandas as pd
import polars as pl
from src.utils.cik_handler import CIKHandler, normalize_cik, validate_cik_format, convert_to_standard_format
from src.utils.logging import DataValidationError


class TestCIKHandler:
    """Test cases for CIKHandler class methods."""
    
    def test_normalize_cik_string(self):
        """Test CIK normalization with string input."""
        # Test various string formats
        assert CIKHandler.normalize_cik("123456") == "0000123456"
        assert CIKHandler.normalize_cik("7890123") == "0007890123"
        assert CIKHandler.normalize_cik("0000012345") == "0000012345"  # Already 10 digits
    
    def test_normalize_cik_integer(self):
        """Test CIK normalization with integer input."""
        assert CIKHandler.normalize_cik(123456) == "0000123456"
        assert CIKHandler.normalize_cik(7890123) == "0007890123"
    
    def test_normalize_cik_float(self):
        """Test CIK normalization with float input."""
        assert CIKHandler.normalize_cik(123456.0) == "0000123456"
        assert CIKHandler.normalize_cik(7890123.789) == "0007890123"  # Decimal part removed
    
    def test_normalize_cik_series_pandas(self):
        """Test CIK normalization with pandas Series."""
        series = pd.Series([123456, "7890123", "0000012345"])
        normalized = CIKHandler.normalize_cik(series)
        expected = pd.Series(["0000123456", "0007890123", "0000012345"])
        pd.testing.assert_series_equal(normalized, expected)
    
    def test_normalize_cik_series_polars(self):
        """Test CIK normalization with polars Series."""
        series = pl.Series(["123456", "7890123", "0000012345"])
        normalized = CIKHandler.normalize_cik(series)
        expected = pl.Series(["0000123456", "0007890123", "0000012345"])
        assert normalized.to_list() == expected.to_list()
    
    def test_normalize_cik_invalid_format(self):
        """Test CIK normalization with invalid formats."""
        with pytest.raises(DataValidationError):
            CIKHandler.normalize_cik("abc123")
        
        with pytest.raises(DataValidationError):
            CIKHandler.normalize_cik("123456789012")  # Too long
        
        with pytest.raises(DataValidationError):
            CIKHandler.normalize_cik("")
        
        with pytest.raises(DataValidationError):
            CIKHandler.normalize_cik(None)
    
    def test_validate_cik_format_valid(self):
        """Test CIK format validation with valid inputs."""
        assert CIKHandler.validate_cik_format("0000123456")
        assert CIKHandler.validate_cik_format("123456")
        assert CIKHandler.validate_cik_format(123456)
        assert CIKHandler.validate_cik_format("7890123")
    
    def test_validate_cik_format_invalid(self):
        """Test CIK format validation with invalid inputs."""
        assert not CIKHandler.validate_cik_format("abc123")
        assert not CIKHandler.validate_cik_format("")
        assert not CIKHandler.validate_cik_format(None)
        assert not CIKHandler.validate_cik_format("123456789012")  # Too long
    
    def test_convert_dataframe_to_standard_format_pandas(self):
        """Test DataFrame CIK column normalization with pandas."""
        df = pd.DataFrame({
            'cik': [123456, "7890123", "0000012345"],
            'value': [100, 200, 300]
        })
        normalized = CIKHandler.convert_to_standard_format(df)
        expected_cik = pd.Series(["0000123456", "0007890123", "0000012345"], name='cik')
        pd.testing.assert_series_equal(normalized['cik'], expected_cik)
    
    def test_convert_dataframe_to_standard_format_polars(self):
        """Test DataFrame CIK column normalization with polars."""
        df = pl.DataFrame({
            'cik': ["123456", "7890123", "0000012345"],
            'value': [100, 200, 300]
        })
        normalized = CIKHandler.convert_to_standard_format(df)
        expected_cik = ["0000123456", "0007890123", "0000012345"]
        assert normalized['cik'].to_list() == expected_cik
    
    def test_convert_dataframe_missing_cik_column(self):
        """Test DataFrame normalization when CIK column is missing."""
        df = pd.DataFrame({'value': [100, 200, 300]})
        normalized = CIKHandler.convert_to_standard_format(df)
        pd.testing.assert_frame_equal(normalized, df)
        
        df = pl.DataFrame({'value': [100, 200, 300]})
        normalized = CIKHandler.convert_to_standard_format(df)
        assert normalized.equals(df)
    
    def test_ensure_string_type(self):
        """Test ensure_string_type method."""
        assert CIKHandler.ensure_string_type(123456) == "0000123456"
        assert CIKHandler.ensure_string_type("7890123") == "0007890123"
        
        with pytest.raises(DataValidationError):
            CIKHandler.ensure_string_type("abc123")
    
    def test_validate_cik_consistency_pandas(self):
        """Test CIK consistency validation with pandas DataFrame."""
        # Valid DataFrame
        df_valid = pd.DataFrame({
            'cik': ["0000123456", "0007890123", "0000001234"],
            'value': [100, 200, 300]
        })
        assert CIKHandler.validate_cik_consistency(df_valid)
        
        # DataFrame with invalid CIK
        df_invalid = pd.DataFrame({
            'cik': ["abc123", "0007890123", "0000001234"],
            'value': [100, 200, 300]
        })
        assert not CIKHandler.validate_cik_consistency(df_invalid)
        
        # DataFrame with null CIK values
        df_null = pd.DataFrame({
            'cik': [None, "0007890123", "0000001234"],
            'value': [100, 200, 300]
        })
        assert not CIKHandler.validate_cik_consistency(df_null)
    
    def test_validate_cik_consistency_polars(self):
        """Test CIK consistency validation with polars DataFrame."""
        # Valid DataFrame
        df_valid = pl.DataFrame({
            'cik': ["0000123456", "0007890123", "0000001234"],
            'value': [100, 200, 300]
        })
        assert CIKHandler.validate_cik_consistency(df_valid)
        
        # DataFrame with invalid CIK
        df_invalid = pl.DataFrame({
            'cik': ["abc123", "0007890123", "0000001234"],
            'value': [100, 200, 300]
        })
        assert not CIKHandler.validate_cik_consistency(df_invalid)
        
        # DataFrame with null CIK values
        df_null = pl.DataFrame({
            'cik': [None, "0007890123", "0000001234"],
            'value': [100, 200, 300]
        })
        assert not CIKHandler.validate_cik_consistency(df_null)


class TestConvenienceFunctions:
    """Test cases for convenience functions."""
    
    def test_normalize_cik_convenience_function(self):
        """Test normalize_cik convenience function."""
        assert normalize_cik(123456) == "0000123456"
        assert normalize_cik("7890123") == "0007890123"
    
    def test_validate_cik_format_convenience_function(self):
        """Test validate_cik_format convenience function."""
        assert validate_cik_format("0000123456")
        assert not validate_cik_format("abc123")
    
    def test_convert_to_standard_format_convenience_function(self):
        """Test convert_to_standard_format convenience function."""
        df = pd.DataFrame({
            'cik': [123456, "7890123"],
            'value': [100, 200]
        })
        normalized = convert_to_standard_format(df)
        expected_cik = pd.Series(["0000123456", "0007890123"], name='cik')
        pd.testing.assert_series_equal(normalized['cik'], expected_cik)


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_single_digit_cik(self):
        """Test normalization of single-digit CIK."""
        assert CIKHandler.normalize_cik(1) == "0000000001"
        assert CIKHandler.normalize_cik("9") == "0000000009"
    
    def test_maximum_length_cik(self):
        """Test normalization of maximum length CIK."""
        assert CIKHandler.normalize_cik("1234567890") == "1234567890"
        assert CIKHandler.normalize_cik(1234567890) == "1234567890"
    
    def test_leading_zeros_preservation(self):
        """Test that leading zeros are handled correctly."""
        # Input with leading zeros should be preserved
        assert CIKHandler.normalize_cik("0000123456") == "0000123456"  # Preserved
        assert CIKHandler.normalize_cik("000123456") == "0000123456"
    
    def test_negative_numbers(self):
        """Test handling of negative numbers."""
        with pytest.raises(DataValidationError):
            CIKHandler.normalize_cik(-123456)
    
    def test_empty_series(self):
        """Test normalization of empty Series."""
        pandas_series = pd.Series([], dtype=object)
        normalized = CIKHandler.normalize_cik(pandas_series)
        assert len(normalized) == 0
        
        polars_series = pl.Series([], dtype=pl.Utf8)
        normalized = CIKHandler.normalize_cik(polars_series)
        assert len(normalized) == 0
    
    def test_mixed_type_series(self):
        """Test normalization of Series with mixed types."""
        series = pd.Series([123456, "7890123", None, 456789])
        normalized = CIKHandler.normalize_cik(series)
        
        # Check that valid values are normalized and None becomes None
        assert normalized[0] == "0000123456"
        assert normalized[1] == "0007890123"
        assert pd.isna(normalized[2])
        assert normalized[3] == "0000456789"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
