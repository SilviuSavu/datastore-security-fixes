"""
Unit tests for Delta Lake compatibility module.

Tests the transaction management, schema validation, and safe operations
for Delta Lake integration.
"""

import pytest
import os
import tempfile
import shutil
import polars as pl
from src.utils.delta_lake_compatibility import (
    DeltaLakeCompatibility, 
    create_delta_compatibility_layer,
    safe_write_delta_table,
    create_delta_table_with_schema
)
from src.utils.logging import DataProcessingError, DataValidationError
from src.utils.cik_handler import CIKHandler


class TestDeltaLakeCompatibility:
    """Test cases for DeltaLakeCompatibility class."""
    
    @pytest.fixture
    def temp_delta_table(self):
        """Create a temporary Delta Lake table for testing."""
        temp_dir = tempfile.mkdtemp()
        table_path = os.path.join(temp_dir, "test_table")
        
        # Create initial table
        df = pl.DataFrame({
            'cik': ['0000123456', '0000789012', '0000345678'],
            'value': [100, 200, 300],
            'date': ['2023-01-01', '2023-01-02', '2023-01-03']
        })
        
        try:
            create_delta_table_with_schema(table_path, df)
            yield table_path
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_initialization_with_valid_path(self, temp_delta_table):
        """Test initialization with valid Delta Lake table path."""
        compatibility = DeltaLakeCompatibility(temp_delta_table)
        assert compatibility.table_path == temp_delta_table
        assert compatibility.enable_transactions is True
    
    def test_initialization_with_transactions_disabled(self, temp_delta_table):
        """Test initialization with transactions disabled."""
        compatibility = DeltaLakeCompatibility(temp_delta_table, enable_transactions=False)
        assert compatibility.enable_transactions is False
    
    def test_initialization_with_invalid_path(self):
        """Test initialization with invalid path raises error."""
        with pytest.raises(FileNotFoundError):
            DeltaLakeCompatibility("/nonexistent/path")
    
    def test_initialization_without_delta_log(self):
        """Test initialization without Delta Lake log directory raises error."""
        temp_dir = tempfile.mkdtemp()
        try:
            with pytest.raises(ValueError):
                DeltaLakeCompatibility(temp_dir)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_get_schema(self, temp_delta_table):
        """Test schema retrieval."""
        compatibility = DeltaLakeCompatibility(temp_delta_table)
        schema = compatibility.get_schema()
        
        assert 'cik' in schema.names()
        assert 'value' in schema.names()
        assert 'date' in schema.names()
    
    def test_read_table(self, temp_delta_table):
        """Test table reading."""
        compatibility = DeltaLakeCompatibility(temp_delta_table)
        df = compatibility.read_table()
        
        assert len(df) == 3
        assert 'cik' in df.columns
        assert 'value' in df.columns
        assert 'date' in df.columns
    
    def test_read_table_with_limit(self, temp_delta_table):
        """Test table reading with limit."""
        compatibility = DeltaLakeCompatibility(temp_delta_table)
        df = compatibility.read_table(limit=2)
        
        assert len(df) == 2
    
    def test_table_exists(self, temp_delta_table):
        """Test table existence check."""
        compatibility = DeltaLakeCompatibility(temp_delta_table)
        assert compatibility.table_exists() is True
    
    def test_table_not_exists(self):
        """Test table existence check with non-existent table."""
        compatibility = DeltaLakeCompatibility("/nonexistent/path", enable_transactions=False)
        assert compatibility.table_exists() is False
    
    def test_transaction_context_manager(self, temp_delta_table):
        """Test transaction context manager."""
        compatibility = DeltaLakeCompatibility(temp_delta_table)
        
        with compatibility.transaction() as tx:
            assert tx is compatibility
            assert compatibility._transaction_active is True
        
        assert compatibility._transaction_active is False
    
    def test_transaction_with_transactions_disabled(self, temp_delta_table):
        """Test transaction context manager with transactions disabled."""
        compatibility = DeltaLakeCompatibility(temp_delta_table, enable_transactions=False)
        
        with compatibility.transaction() as tx:
            assert tx is compatibility
            # Should not raise any errors
    
    def test_validate_schema_compatibility(self, temp_delta_table):
        """Test schema validation."""
        compatibility = DeltaLakeCompatibility(temp_delta_table)
        
        # Compatible DataFrame
        df_compatible = pl.DataFrame({
            'cik': ['0000987654'],
            'value': [400],
            'date': ['2023-01-04']
        })
        
        assert compatibility.validate_schema_compatibility(df_compatible) is True
    
    def test_validate_schema_with_invalid_cik(self, temp_delta_table):
        """Test schema validation with invalid CIK column."""
        compatibility = DeltaLakeCompatibility(temp_delta_table)
        
        # DataFrame with invalid CIK
        df_invalid_cik = pl.DataFrame({
            'cik': ['abc123'],
            'value': [400],
            'date': ['2023-01-04']
        })
        
        assert compatibility.validate_schema_compatibility(df_invalid_cik) is False
    
    def test_safe_write_dataframe(self, temp_delta_table):
        """Test safe DataFrame writing."""
        compatibility = DeltaLakeCompatibility(temp_delta_table)
        
        df_new = pl.DataFrame({
            'cik': ['0000987654'],
            'value': [400],
            'date': ['2023-01-04']
        })
        
        initial_count = len(compatibility.read_table())
        result = compatibility.safe_write_dataframe(df_new)
        
        assert result is True
        final_count = len(compatibility.read_table())
        assert final_count == initial_count + 1
    
    def test_safe_write_dataframe_with_invalid_cik(self, temp_delta_table):
        """Test safe writing with invalid CIK raises error."""
        compatibility = DeltaLakeCompatibility(temp_delta_table)
        
        df_invalid = pl.DataFrame({
            'cik': ['abc123'],
            'value': [400],
            'date': ['2023-01-04']
        })
        
        with pytest.raises(DataValidationError):
            compatibility.safe_write_dataframe(df_invalid)
    
    def test_safe_write_empty_dataframe(self, temp_delta_table):
        """Test writing empty DataFrame returns False."""
        compatibility = DeltaLakeCompatibility(temp_delta_table)
        
        df_empty = pl.DataFrame()
        
        result = compatibility.safe_write_dataframe(df_empty)
        assert result is False
    
    def test_dtype_compatibility_check(self, temp_delta_table):
        """Test data type compatibility checking."""
        compatibility = DeltaLakeCompatibility(temp_delta_table)
        
        # Same types should be compatible
        str_type = pl.String()
        assert compatibility._are_dtypes_compatible(str_type, str_type) is True
        
        # Different string types should be compatible
        str_type1 = pl.String()
        str_type2 = pl.String()
        assert compatibility._are_dtypes_compatible(str_type1, str_type2) is True
        
        # Numeric types should be compatible
        int_type = pl.Int64()
        float_type = pl.Float64()
        assert compatibility._are_dtypes_compatible(int_type, float_type) is True


class TestUtilityFunctions:
    """Test cases for utility functions."""
    
    @pytest.fixture
    def temp_table_path(self):
        """Create temporary path for Delta Lake table."""
        temp_dir = tempfile.mkdtemp()
        table_path = os.path.join(temp_dir, "test_table")
        try:
            yield table_path
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_create_delta_compatibility_layer(self, temp_table_path):
        """Test factory function for creating compatibility layer."""
        # First create a table
        df = pl.DataFrame({
            'cik': ['0000123456'],
            'value': [100]
        })
        create_delta_table_with_schema(temp_table_path, df)
        
        # Then test factory function
        compatibility = create_delta_compatibility_layer(temp_table_path)
        assert isinstance(compatibility, DeltaLakeCompatibility)
        assert compatibility.table_path == temp_table_path
    
    def test_create_delta_table_with_schema(self, temp_table_path):
        """Test creating Delta Lake table with schema."""
        df = pl.DataFrame({
            'cik': ['0000123456', '0000789012'],
            'value': [100, 200],
            'date': ['2023-01-01', '2023-01-02']
        })
        
        result = create_delta_table_with_schema(temp_table_path, df)
        assert result is True
        
        # Verify table exists and has data
        compatibility = DeltaLakeCompatibility(temp_table_path)
        assert compatibility.table_exists()
        
        read_df = compatibility.read_table()
        assert len(read_df) == 2
        assert CIKHandler.validate_cik_consistency(read_df)
    
    def test_create_delta_table_with_invalid_cik(self, temp_table_path):
        """Test creating table with invalid CIK raises error."""
        df = pl.DataFrame({
            'cik': ['abc123'],
            'value': [100]
        })
        
        with pytest.raises(DataProcessingError):
            create_delta_table_with_schema(temp_table_path, df)
    
    def test_create_delta_table_with_partitioning(self, temp_table_path):
        """Test creating Delta Lake table with partitioning."""
        df = pl.DataFrame({
            'cik': ['0000123456', '0000789012'],
            'value': [100, 200],
            'date': ['2023-01-01', '2023-01-02']
        })
        
        result = create_delta_table_with_schema(
            temp_table_path, df, 
            partition_by=['date']
        )
        assert result is True
    
    def test_safe_write_delta_table_utility(self, temp_table_path):
        """Test safe write utility function."""
        # First create table
        df_initial = pl.DataFrame({
            'cik': ['0000123456'],
            'value': [100]
        })
        create_delta_table_with_schema(temp_table_path, df_initial)
        
        # Then write additional data
        df_new = pl.DataFrame({
            'cik': ['0000789012'],
            'value': [200]
        })
        
        result = safe_write_delta_table(temp_table_path, df_new)
        assert result is True
        
        # Verify data was written
        compatibility = DeltaLakeCompatibility(temp_table_path)
        final_df = compatibility.read_table()
        assert len(final_df) == 2


class TestTransactionHandling:
    """Test cases for transaction handling."""
    
    @pytest.fixture
    def temp_delta_table(self):
        """Create a temporary Delta Lake table for transaction testing."""
        temp_dir = tempfile.mkdtemp()
        table_path = os.path.join(temp_dir, "test_table")
        
        df = pl.DataFrame({
            'cik': ['0000123456'],
            'value': [100]
        })
        
        try:
            create_delta_table_with_schema(table_path, df)
            yield table_path
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_successful_transaction(self, temp_delta_table):
        """Test successful transaction commits properly."""
        compatibility = DeltaLakeCompatibility(temp_delta_table)
        
        df_new = pl.DataFrame({
            'cik': ['0000789012'],
            'value': [200]
        })
        
        with compatibility.transaction():
            compatibility.safe_write_dataframe(df_new)
        
        # Verify data was committed
        final_df = compatibility.read_table()
        assert len(final_df) == 2
    
    def test_failed_transaction_rollback(self, temp_delta_table):
        """Test failed transaction rolls back properly."""
        compatibility = DeltaLakeCompatibility(temp_delta_table)
        
        initial_df = compatibility.read_table()
        initial_count = len(initial_df)
        
        df_valid = pl.DataFrame({
            'cik': ['0000789012'],
            'value': [200]
        })
        
        df_invalid = pl.DataFrame({
            'cik': ['abc123'],  # Invalid CIK
            'value': [300]
        })
        
        try:
            with compatibility.transaction():
                # First write succeeds
                compatibility.safe_write_dataframe(df_valid)
                # Second write fails and should trigger rollback
                compatibility.safe_write_dataframe(df_invalid)
        except DataValidationError:
            pass  # Expected error
        
        # Verify rollback occurred - should be back to initial state
        final_df = compatibility.read_table()
        assert len(final_df) == initial_count


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
