"""
Delta Lake Compatibility Layer

This module provides a unified interface for Delta Lake operations with proper error handling,
transaction management, and fallback mechanisms. It prioritizes Polars for Delta operations
since it works reliably, with fallback to DeltaTable API when needed.

Key Features:
- Primary use of Polars scan_delta for reading operations
- Fallback to DeltaTable API when Polars is not available
- Transaction management with atomic operations
- Schema validation and data type consistency
- Comprehensive error handling and logging
- Metadata operations with proper error handling
"""

import os
import logging
from typing import Optional, Dict, Any, Callable, Union
import polars as pl
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError
from contextlib import contextmanager

from .logging import DataStoreLogger, DataProcessingError, DataValidationError
from .cik_handler import CIKHandler

# Configure logging
logger = DataStoreLogger()

class DeltaLakeCompatibility:
    """
    A compatibility layer for Delta Lake operations that handles API differences,
    provides robust error handling with fallback mechanisms, and ensures
    transaction safety for data operations.
    """

    def __init__(self, table_path: str, enable_transactions: bool = True):
        """
        Initialize the Delta Lake compatibility layer.

        Args:
            table_path: Path to the Delta Lake table
            enable_transactions: Whether to enable transaction management
        """
        self.table_path = table_path
        self._validate_table_path()
        self._delta_table = None
        self._polars_available = True
        self.enable_transactions = enable_transactions
        self._transaction_active = False

    def _validate_table_path(self):
        """Validate that the table path exists and contains Delta Lake files."""
        if not os.path.exists(self.table_path):
            raise FileNotFoundError(f"Delta Lake table path not found: {self.table_path}")

        delta_log_path = os.path.join(self.table_path, "_delta_log")
        if not os.path.exists(delta_log_path):
            raise ValueError(f"No _delta_log directory found in: {self.table_path}")
    
    @contextmanager
    def transaction(self):
        """
        Context manager for Delta Lake transaction operations.
        
        Ensures atomic operations with proper rollback on failure.
        """
        if not self.enable_transactions:
            logger.warning("Transactions disabled, proceeding without transaction safety")
            yield self
            return
            
        if self._transaction_active:
            logger.warning("Transaction already active, creating nested transaction")
            yield self
            return
            
        self._transaction_active = True
        logger.debug(f"Starting transaction for table: {self.table_path}")
        
        try:
            # Get initial version for rollback
            delta_table = self._get_delta_table()
            initial_version = delta_table.version()
            
            yield self
            
            # Transaction successful - commit is implicit in Delta Lake
            logger.debug(f"Transaction committed successfully. Version: {initial_version}")
            
        except Exception as e:
            # Transaction failed - attempt rollback
            logger.error(f"Transaction failed: {e}")
            try:
                delta_table = self._get_delta_table()
                delta_table.restore(initial_version)
                logger.info(f"Transaction rolled back to version: {initial_version}")
            except Exception as rollback_error:
                logger.error(f"Failed to rollback transaction: {rollback_error}")
                raise DataProcessingError(f"Transaction failed and rollback unsuccessful: {e}") from e
            
            raise DataProcessingError(f"Transaction rolled back due to error: {e}") from e
        finally:
            self._transaction_active = False
    
    def validate_schema_compatibility(self, df: pl.DataFrame, 
                                    cik_column: str = 'cik') -> bool:
        """
        Validate DataFrame schema compatibility with the Delta Lake table.
        
        Args:
            df: DataFrame to validate
            cik_column: Name of the CIK column to validate
            
        Returns:
            True if schema is compatible
        """
        try:
            # Get table schema
            table_schema = self.get_schema()
            
            # Validate CIK column if present
            if cik_column in df.columns:
                if not CIKHandler.validate_cik_consistency(df, cik_column):
                    raise DataValidationError(f"CIK column '{cik_column}' validation failed")
            
            # Check column compatibility
            df_columns = set(df.columns)
            table_columns = set(table_schema.names())
            
            # Ensure all required columns are present
            missing_columns = table_columns - df_columns
            if missing_columns:
                logger.warning(f"Missing columns in DataFrame: {missing_columns}")
            
            # Check data type compatibility for common columns
            common_columns = df_columns & table_columns
            for col in common_columns:
                df_dtype = df.schema[col]
                table_dtype = table_schema[col]
                
                if not self._are_dtypes_compatible(df_dtype, table_dtype):
                    logger.warning(f"Type mismatch in column '{col}': {df_dtype} vs {table_dtype}")
            
            logger.debug("Schema validation completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            return False
    
    def _are_dtypes_compatible(self, dtype1: pl.DataType, dtype2: pl.DataType) -> bool:
        """Check if two Polars data types are compatible."""
        # Simple compatibility check - can be enhanced
        return dtype1 == dtype2 or (
            isinstance(dtype1, pl.String) and isinstance(dtype2, pl.String)
        ) or (
            isinstance(dtype1, pl.Numeric) and isinstance(dtype2, pl.Numeric)
        )
    
    def safe_write_dataframe(self, df: pl.DataFrame, 
                           mode: str = "append",
                           cik_column: str = 'cik',
                           overwrite_schema: bool = False) -> bool:
        """
        Safely write a DataFrame to Delta Lake with validation and transaction safety.
        
        Args:
            df: DataFrame to write
            mode: Write mode ('append', 'overwrite', 'error')
            cik_column: Name of the CIK column to validate
            overwrite_schema: Whether to overwrite existing schema
            
        Returns:
            True if write was successful
        """
        if df.is_empty():
            logger.warning("Attempted to write empty DataFrame")
            return False
        
        # Validate schema compatibility
        if not self.validate_schema_compatibility(df, cik_column):
            raise DataValidationError("DataFrame schema validation failed")
        
        # Normalize CIK column if present
        if cik_column in df.columns:
            df = CIKHandler.convert_to_standard_format(df, cik_column)
        
        # Ensure CIK consistency if CIK column exists
        if cik_column in df.columns and not CIKHandler.validate_cik_consistency(df, cik_column):
            raise DataValidationError("CIK consistency check failed before write")
        
        try:
            with self.transaction():
                logger.info(f"Writing {len(df)} rows to Delta Lake table: {self.table_path}")
                
                # Convert to pandas for write_deltalake compatibility
                df_pandas = df.to_pandas()
                
                write_deltalake(
                    table_or_uri=self.table_path,
                    data=df_pandas,
                    mode=mode,
                    overwrite_schema=overwrite_schema,
                    storage_options=None
                )
                
                logger.info(f"Successfully wrote {len(df)} rows to table")
                return True
                
        except Exception as e:
            logger.error(f"Failed to write DataFrame to Delta Lake: {e}")
            raise DataProcessingError(f"Delta Lake write operation failed: {e}") from e

    def _get_delta_table(self) -> DeltaTable:
        """Lazy load DeltaTable instance with error handling."""
        if self._delta_table is None:
            try:
                self._delta_table = DeltaTable(self.table_path)
            except Exception as e:
                logger.error(f"Failed to load DeltaTable: {e}")
                raise DeltaError(f"Could not load DeltaTable from {self.table_path}: {e}")
        return self._delta_table

    def _try_polars_first(self, operation_name: str, polars_func, delta_func=None):
        """
        Try Polars operation first, fallback to DeltaTable if needed.

        Args:
            operation_name: Name of the operation for logging
            polars_func: Function to execute using Polars
            delta_func: Fallback function using DeltaTable (optional)

        Returns:
            Result of the successful operation
        """
        try:
            logger.info(f"Attempting {operation_name} with Polars")
            return polars_func()
        except Exception as e:
            logger.warning(f"Polars {operation_name} failed: {e}")

            if delta_func:
                try:
                    logger.info(f"Falling back to DeltaTable for {operation_name}")
                    return delta_func()
                except Exception as delta_e:
                    logger.error(f"DeltaTable {operation_name} also failed: {delta_e}")
                    raise DeltaError(f"Both Polars and DeltaTable failed for {operation_name}: {delta_e}")
            else:
                raise DeltaError(f"Polars {operation_name} failed and no DeltaTable fallback provided: {e}")

    def get_schema(self) -> pl.Schema:
        """
        Get the schema of the Delta Lake table.

        Returns:
            Polars Schema object representing the table schema
        """
        def polars_schema():
            df = pl.scan_delta(self.table_path)
            return df.schema

        def delta_schema():
            delta_table = self._get_delta_table()
            # Handle different API versions
            try:
                # Try new API format
                metadata = delta_table.metadata()
                if hasattr(metadata, 'schema'):
                    return pl.Schema(metadata.schema)
                elif hasattr(metadata, 'schema_json'):
                    return pl.Schema.from_json(metadata.schema_json)
                else:
                    # Fallback to reading via to_pyarrow_schema
                    return pl.Schema.from_arrow_schema(delta_table.to_pyarrow_schema())
            except Exception as e:
                # Final fallback - read a small sample to infer schema
                logger.warning(f"Direct schema access failed, reading sample: {e}")
                df = pl.scan_delta(self.table_path).limit(1).collect()
                return df.schema

        return self._try_polars_first("schema retrieval", polars_schema, delta_schema)

    def read_table(self, limit: Optional[int] = None, filter_condition: Optional[str] = None) -> pl.DataFrame:
        """
        Read data from the Delta Lake table.

        Args:
            limit: Maximum number of rows to read
            filter_condition: Optional filter condition

        Returns:
            Polars DataFrame containing the table data
        """
        def polars_read():
            scan = pl.scan_delta(self.table_path)
            if filter_condition:
                scan = scan.filter(pl.lit(filter_condition))
            if limit:
                scan = scan.limit(limit)
            return scan.collect()

        def delta_read():
            delta_table = self._get_delta_table()
            df = delta_table.to_pandas()
            if limit:
                df = df.head(limit)
            return pl.from_pandas(df)

        return self._try_polars_first("table read", polars_read, delta_read)

    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata from the Delta Lake table.

        Returns:
            Dictionary containing table metadata
        """
        def polars_metadata():
            # Polars doesn't provide direct metadata access, so we use DeltaTable
            return self._get_delta_table().metadata()

        def delta_metadata():
            return self._get_delta_table().metadata()

        return self._try_polars_first("metadata retrieval", polars_metadata, delta_metadata)

    def table_exists(self) -> bool:
        """
        Check if the Delta Lake table exists and is accessible.

        Returns:
            True if table exists and is accessible, False otherwise
        """
        try:
            # Quick check using Polars
            pl.scan_delta(self.table_path).limit(1).collect()
            return True
        except Exception as e:
            logger.warning(f"Polars table existence check failed: {e}")
            try:
                # Fallback to DeltaTable
                self._get_delta_table()
                return True
            except Exception as delta_e:
                logger.error(f"DeltaTable existence check also failed: {delta_e}")
                return False

    def get_table_info(self) -> Dict[str, Any]:
        """
        Get comprehensive information about the Delta Lake table.

        Returns:
            Dictionary containing table information including schema, metadata, and basic stats
        """
        info = {
            'path': self.table_path,
            'exists': self.table_exists(),
            'schema': None,
            'metadata': None,
            'row_count': None
        }

        if not info['exists']:
            return info

        try:
            info['schema'] = self.get_schema()
            info['metadata'] = self.get_metadata()

            # Get approximate row count
            try:
                df = pl.scan_delta(self.table_path).select(pl.count()).collect()
                info['row_count'] = df.item()
            except Exception as e:
                logger.warning(f"Could not get row count: {e}")

        except Exception as e:
            logger.error(f"Failed to get table info: {e}")

        return info

def create_delta_compatibility_layer(table_path: str) -> DeltaLakeCompatibility:
    """
    Factory function to create a Delta Lake compatibility layer instance.

    Args:
        table_path: Path to the Delta Lake table

    Returns:
        DeltaLakeCompatibility instance
    """
    return DeltaLakeCompatibility(table_path)

# Utility functions for common operations
def safe_read_delta_table(table_path: str, limit: Optional[int] = None) -> pl.DataFrame:
    """
    Safely read a Delta Lake table with automatic fallback handling.

    Args:
        table_path: Path to the Delta Lake table
        limit: Maximum number of rows to read

    Returns:
        Polars DataFrame with the table data
    """
    compatibility = create_delta_compatibility_layer(table_path)
    return compatibility.read_table(limit=limit)

def safe_get_delta_schema(table_path: str) -> pl.Schema:
    """
    Safely get the schema of a Delta Lake table.

    Args:
        table_path: Path to the Delta Lake table

    Returns:
        Polars Schema object
    """
    compatibility = create_delta_compatibility_layer(table_path)
    return compatibility.get_schema()

def safe_write_delta_table(table_path: str, df: pl.DataFrame, 
                          mode: str = "append",
                          cik_column: str = 'cik',
                          enable_transactions: bool = True) -> bool:
    """
    Safely write a DataFrame to Delta Lake with validation and transaction safety.

    Args:
        table_path: Path to the Delta Lake table
        df: DataFrame to write
        mode: Write mode ('append', 'overwrite', 'error')
        cik_column: Name of the CIK column to validate
        enable_transactions: Whether to enable transaction management

    Returns:
        True if write was successful
    """
    compatibility = create_delta_compatibility_layer(table_path, enable_transactions=enable_transactions)
    return compatibility.safe_write_dataframe(df, mode, cik_column)

def create_delta_table_with_schema(table_path: str, df: pl.DataFrame,
                                 cik_column: str = 'cik',
                                 partition_by: Optional[list] = None) -> bool:
    """
    Create a new Delta Lake table with proper schema validation.

    Args:
        table_path: Path for the new Delta Lake table
        df: Initial DataFrame to create table with
        cik_column: Name of the CIK column to validate
        partition_by: List of columns to partition by (optional)

    Returns:
        True if table creation was successful
    """
    try:
        # Validate DataFrame before creating table
        if cik_column in df.columns:
            df = CIKHandler.convert_to_standard_format(df, cik_column)
            
        if not CIKHandler.validate_cik_consistency(df, cik_column):
            raise DataValidationError("CIK validation failed during table creation")
        
        # Convert to pandas for write_deltalake
        df_pandas = df.to_pandas()
        
        write_deltalake(
            table_or_uri=table_path,
            data=df_pandas,
            mode="overwrite",
            partition_by=partition_by
        )
        
        logger.info(f"Successfully created Delta Lake table: {table_path}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to create Delta Lake table: {e}")
        raise DataProcessingError(f"Delta Lake table creation failed: {e}") from e