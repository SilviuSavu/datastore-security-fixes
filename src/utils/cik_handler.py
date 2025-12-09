"""
Centralized CIK (Central Index Key) handling utilities.

Provides consistent validation, normalization, and type handling for CIK values
throughout the DataStore project. Ensures data integrity by enforcing a
standardized CIK format.
"""

import re
from typing import Union, Optional
import pandas as pd
import polars as pl

from .logging import DataStoreLogger, DataValidationError

logger = DataStoreLogger()


class CIKHandler:
    """
    Handles CIK validation, normalization, and type consistency.
    
    Standardizes CIK values to 10-digit zero-padded strings throughout
    the application to prevent data type inconsistencies.
    """
    
    # CIK validation pattern - numeric string or integer
    CIK_PATTERN = re.compile(r'^\d{1,10}$')
    STANDARD_LENGTH = 10
    
    @classmethod
    def normalize_cik(cls, cik: Union[str, int, float, pd.Series, pl.Series]) -> Union[str, pd.Series, pl.Series]:
        """
        Normalize CIK to standardized 10-digit zero-padded string format.
        
        Args:
            cik: CIK value in various formats (string, int, float, pandas/Polars Series)
            
        Returns:
            Normalized CIK as 10-digit string or Series of strings
            
        Raises:
            DataValidationError: If CIK format is invalid
        """
        if isinstance(cik, (pd.Series, pl.Series)):
            return cls._normalize_series(cik)
        
        try:
            # Convert to string first
            cik_str = str(cik).strip()
            
            # Handle potential float representations (remove decimal part)
            if '.' in cik_str:
                logger.warning(f"CIK appears to be float: {cik_str}")
                cik_str = cik_str.split('.')[0]
            
            # Validate format
            if not cls.CIK_PATTERN.match(cik_str):
                raise DataValidationError(f"Invalid CIK format: {cik_str}")
            
            # Zero-pad to standard length (only if less than 10 digits)
            if len(cik_str) < cls.STANDARD_LENGTH:
                normalized = cik_str.zfill(cls.STANDARD_LENGTH)
            else:
                normalized = cik_str  # Already 10 digits, don't truncate
            logger.debug(f"Normalized CIK: {cik} -> {normalized}")
            
            return normalized
            
        except (ValueError, TypeError) as e:
            raise DataValidationError(f"Failed to normalize CIK '{cik}': {str(e)}") from e
    
    @classmethod
    def _normalize_series(cls, series: Union[pd.Series, pl.Series]) -> Union[pd.Series, pl.Series]:
        """Normalize a pandas or Polars Series of CIK values."""
        if isinstance(series, pl.Series):
            # Polars implementation - convert to string first for mixed types
            str_series = series.cast(pl.Utf8, strict=False)
            return str_series.map_elements(
                lambda x: cls._normalize_single_value(x) if x is not None else None,
                return_dtype=pl.Utf8
            )
        else:
            # Pandas implementation
            return series.astype(str).apply(cls._normalize_single_value)
    
    @classmethod
    def _normalize_single_value(cls, value: str) -> Optional[str]:
        """Normalize a single CIK string value."""
        if pd.isna(value) or value is None or value == 'None':
            return None
        
        try:
            value = str(value).strip()
            if '.' in value:
                value = value.split('.')[0]
            
            if not cls.CIK_PATTERN.match(value):
                return None
            
            # Zero-pad to standard length (only if less than 10 digits)
            if len(value) < cls.STANDARD_LENGTH:
                return value.zfill(cls.STANDARD_LENGTH)
            else:
                return value  # Already 10 digits, don't truncate
            
        except Exception as e:
            return None
    
    @classmethod
    def validate_cik_format(cls, cik: Union[str, int, pd.Series, pl.Series]) -> bool:
        """
        Validate if CIK follows the expected format.
        
        Args:
            cik: CIK value to validate
            
        Returns:
            True if valid, False otherwise
        """
        try:
            cls.normalize_cik(cik)
            return True
        except DataValidationError:
            return False
    
    @classmethod
    def convert_to_standard_format(cls, df: Union[pd.DataFrame, pl.DataFrame], 
                                  cik_column: str = 'cik') -> Union[pd.DataFrame, pl.DataFrame]:
        """
        Convert DataFrame CIK column to standard format in place.
        
        Args:
            df: DataFrame containing CIK column
            cik_column: Name of the CIK column
            
        Returns:
            DataFrame with normalized CIK column
        """
        if cik_column not in df.columns:
            logger.warning(f"CIK column '{cik_column}' not found in DataFrame")
            return df
        
        logger.debug(f"Normalizing CIK column '{cik_column}' in DataFrame")
        
        if isinstance(df, pl.DataFrame):
            # Polars implementation
            return df.with_columns([
                pl.col(cik_column).map_elements(
                    lambda x: cls._normalize_single_value(str(x)) if x is not None else None
                ).alias(cik_column)
            ])
        else:
            # Pandas implementation
            df_copy = df.copy()
            normalized_series = cls.normalize_cik(df_copy[cik_column])
            df_copy[cik_column] = normalized_series
            # Preserve column name
            df_copy[cik_column].name = cik_column
            return df_copy
    
    @classmethod
    def ensure_string_type(cls, cik: Union[str, int, float]) -> str:
        """
        Ensure CIK is returned as a properly formatted string.
        
        Args:
            cik: CIK value in any format
            
        Returns:
            Standardized 10-digit CIK string
        """
        normalized = cls.normalize_cik(cik)
        if normalized is None:
            raise DataValidationError(f"Cannot convert CIK '{cik}' to valid string")
        return normalized
    
    @classmethod
    def validate_cik_consistency(cls, df: Union[pd.DataFrame, pl.DataFrame], 
                               cik_column: str = 'cik') -> bool:
        """
        Validate CIK consistency across a DataFrame.
        
        Args:
            df: DataFrame to check
            cik_column: Name of the CIK column
            
        Returns:
            True if all CIKs are valid and consistent
        """
        if cik_column not in df.columns:
            logger.error(f"CIK column '{cik_column}' not found in DataFrame")
            return False
        
        # Check for null values
        if isinstance(df, pl.DataFrame):
            null_count = df.select([pl.col(cik_column).is_null().sum()])[cik_column][0]
        else:
            null_count = df[cik_column].isnull().sum()
        
        if null_count > 0:
            logger.warning(f"Found {null_count} null CIK values")
            return False
        
        # Validate format consistency
        if isinstance(df, pl.DataFrame):
            unique_ciks = df.select([pl.col(cik_column).unique()])[cik_column].to_list()
        else:
            unique_ciks = df[cik_column].unique().tolist()
        
        invalid_ciks = []
        for cik in unique_ciks:
            if not cls.validate_cik_format(cik):
                invalid_ciks.append(cik)
        
        if invalid_ciks:
            logger.error(f"Found invalid CIK formats: {invalid_ciks[:5]}...")  # Show first 5
            return False
        
        logger.debug(f"All CIK values in column '{cik_column}' are valid")
        return True


# Convenience functions for backward compatibility
def normalize_cik(cik: Union[str, int, float, pd.Series, pl.Series]) -> Union[str, pd.Series, pl.Series]:
    """Convenience function to normalize CIK values."""
    return CIKHandler.normalize_cik(cik)


def validate_cik_format(cik: Union[str, int, pd.Series, pl.Series]) -> bool:
    """Convenience function to validate CIK format."""
    return CIKHandler.validate_cik_format(cik)


def convert_to_standard_format(df: Union[pd.DataFrame, pl.DataFrame], 
                              cik_column: str = 'cik') -> Union[pd.DataFrame, pl.DataFrame]:
    """Convenience function to convert DataFrame CIK column to standard format."""
    return CIKHandler.convert_to_standard_format(df, cik_column)
