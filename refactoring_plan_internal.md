# Internal Refactoring Plan (No File Splitting)

## Revised Approach: Internal Refactoring Within Existing Files

Based on the requirement to refactor without splitting .py files into multiple files, here's the updated plan focusing on internal improvements:

## 1. Configuration System (Internal to Existing Files)

**Goal:** Replace hardcoded paths and magic strings with internal configuration sections

### Implementation Strategy:

```python
# Add at the top of each file (after imports)
class _Config:
    """Internal configuration for this module"""

    # Path Configuration
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    BRONZE_DELTA_SEC = os.path.join(PROJECT_ROOT, "bronze", "delta", "sec_bulk")
    SILVER_FUNDAMENTALS = os.path.join(PROJECT_ROOT, "silver", "fundamentals")

    # CIK Configuration
    CIK_FORMAT = "string"  # or "numeric"
    DEFAULT_CIK_LENGTH = 10
    CIK_PADDING_CHAR = "0"

    # Processing Configuration
    DEFAULT_CHUNK_SIZE = 1000
    MAX_MEMORY_USAGE_MB = 2000

# Usage throughout file:
# config = _Config()
# df['cik'] = df['cik'].astype(str).str.zfill(config.DEFAULT_CIK_LENGTH)
```

## 2. CIK Data Type Standardization (Internal Functions)

**Goal:** Create helper functions within existing files to standardize CIK handling

### Implementation Strategy:

```python
def _normalize_cik(cik: Union[str, int, float]) -> str:
    """
    Standardize CIK format to 10-digit zero-padded string

    Args:
        cik: CIK value (can be string, int, or float)

    Returns:
        str: Normalized 10-digit CIK string

    Raises:
        ValueError: If CIK cannot be normalized
    """
    try:
        # Convert to string, remove any decimal points, pad to 10 digits
        cik_str = str(int(float(str(cik))))
        return cik_str.zfill(10)
    except (ValueError, TypeError) as e:
        logger.log_error(f"CIK normalization failed for {cik}: {str(e)}")
        raise ValueError(f"Invalid CIK format: {cik}")

def _validate_cik_dataframe(df: pd.DataFrame, cik_column: str = 'cik') -> bool:
    """
    Validate that all CIKs in a DataFrame follow the standard format

    Args:
        df: DataFrame to validate
        cik_column: Name of CIK column

    Returns:
        bool: True if all CIKs are valid, False otherwise
    """
    if cik_column not in df.columns:
        logger.log_warning("validate_cik_dataframe", f"CIK column {cik_column} not found")
        return False

    invalid_ciks = []
    for cik in df[cik_column].unique():
        try:
            normalized = _normalize_cik(cik)
            if len(normalized) != 10 or not normalized.isdigit():
                invalid_ciks.append(cik)
        except ValueError:
            invalid_ciks.append(cik)

    if invalid_ciks:
        logger.log_warning("validate_cik_dataframe",
                          f"Found {len(invalid_ciks)} invalid CIKs: {invalid_ciks[:5]}...")
        return False

    return True
```

## 3. Complex Logic Refactoring (Helper Functions)

**Goal:** Break down complex nested logic into smaller, focused helper functions

### Implementation Strategy for `standardize.py`:

```python
def _create_financial_mapping_helper(mapping_name: str, tag_list: List[str], pivot_df: pd.DataFrame) -> pd.Series:
    """
    Helper function to extract financial data using tag mapping

    Args:
        mapping_name: Name of the mapping (for logging)
        tag_list: List of possible tags to try
        pivot_df: Pivoted DataFrame containing the data

    Returns:
        pd.Series: Extracted data series
    """
    if pivot_df is None or pivot_df.empty:
        logger.log_debug(f"create_financial_mapping_helper", f"No data available for {mapping_name}")
        return pd.Series(np.nan, index=pivot_df.index if pivot_df is not None else [])

    available_tags = [t for t in tag_list if t in pivot_df.columns]
    if not available_tags:
        logger.log_debug(f"create_financial_mapping_helper", f"No available tags for {mapping_name}: {tag_list}")
        return pd.Series(np.nan, index=pivot_df.index)

    # Use the first available tag (prioritized by order in tag_list)
    result = pivot_df[available_tags].bfill(axis=1).iloc[:, 0]
    logger.log_debug(f"create_financial_mapping_helper", f"Used {available_tags[0]} for {mapping_name}")
    return result

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
```

## 4. Dead Code Removal Strategy

**Goal:** Systematically identify and remove dead code while preserving functionality

### Implementation Approach:

1. **Identify dead code patterns:**
   - Empty functions with only `pass`
   - Commented-out code blocks
   - Unused imports
   - Unreachable code after `return` statements

2. **Create removal checklist:**
   ```python
   # TODO: Remove dead code
   # - Lines 455-456: validate_coverage() empty function
   # - Lines 534-535: Commented print statements
   # - Lines 186-189: Unused SIC parsing fallback
   # - Lines 362-364: Unused ticker extraction logic
   ```

3. **Add deprecation warnings for transitional code:**
   ```python
   def old_function():
       """DEPRECATED: Use new_function() instead. Will be removed in v2.0"""
       warnings.warn("old_function is deprecated, use new_function instead",
                    DeprecationWarning, stacklevel=2)
       return new_function()
   ```

## 5. Documentation Enhancement Strategy

**Goal:** Add comprehensive inline documentation without changing file structure

### Implementation Approach:

1. **Add module-level docstrings:**
   ```python
   """
   Standardization Module - Financial Data Processing

   This module handles the transformation of raw financial data into standardized formats.
   It processes SEC XBRL data, market prices, and macroeconomic indicators.

   Key Features:
   - CIK-based chunking for memory efficiency
   - Financial statement type detection
   - Comprehensive data validation
   - Forward-fill for time series continuity

   Usage:
       from src.etl.standardization.standardize import standardize_fundamentals
       standardize_fundamentals()
   """
   ```

2. **Enhance function docstrings with examples:**
   ```python
   def standardize(df_raw: pd.DataFrame, chunk_size: int = 1000) -> pd.DataFrame:
       """
       Standardize raw financial data into common metrics format.

       Args:
           df_raw: Raw DataFrame containing financial data with columns:
                  ['adsh', 'cik', 'tag', 'value', 'period', 'filed', 'form', 'fy', 'fp', 'sic']
           chunk_size: Number of CIKs to process in each batch (default: 1000)

       Returns:
           pd.DataFrame: Standardized DataFrame with financial metrics and CIK/date indexing

       Example:
           >>> raw_data = pd.read_parquet("bronze/sec_bulk/2023.parquet")
           >>> standardized = SECMapper.standardize(raw_data)
           >>> print(standardized[['cik', 'date', 'totalRevenue', 'netIncome']].head())

       Notes:
           - Uses CIK-based chunking to prevent memory issues
           - Automatically detects statement types (BS, IS, CF)
           - Applies forward-fill for time series continuity
       """
   ```

## 6. Type Hints Implementation Strategy

**Goal:** Add comprehensive type hints to improve code safety and IDE support

### Implementation Approach:

```python
from typing import Union, Optional, List, Dict, Tuple, Any, Set

# Before:
def normalize_cik(cik):
    return str(int(cik)).zfill(10)

# After:
def normalize_cik(cik: Union[str, int, float]) -> str:
    """
    Normalize CIK to 10-digit zero-padded string format.

    Args:
        cik: CIK value (string, int, or float)

    Returns:
        str: Normalized 10-digit CIK string

    Raises:
        ValueError: If CIK cannot be converted to valid format
    """
    try:
        return str(int(float(str(cik)))).zfill(10)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid CIK format: {cik}") from e

# Complex function example:
def process_fundamentals_batch(
    df_std: pd.DataFrame,
    df_map: Optional[pd.DataFrame],
    gics_mapping: Optional[pd.DataFrame] = None,
    min_year: int = 2009
) -> Optional[pd.DataFrame]:
    """
    Process a batch of standardized fundamental data with mapping and filtering.

    Args:
        df_std: Standardized DataFrame from SECMapper
        df_map: Optional CIK-to-ticker mapping DataFrame
        gics_mapping: Optional GICS sector mapping DataFrame
        min_year: Minimum fiscal year to include (default: 2009)

    Returns:
        Optional[pd.DataFrame]: Processed DataFrame with mappings and filtering, or None if error

    Raises:
        ProcessingError: If critical processing steps fail
    """
```

## 7. Unit Testing Strategy (Internal to Files)

**Goal:** Add comprehensive unit tests within the same files using conditional execution

### Implementation Approach:

```python
def _run_internal_tests() -> bool:
    """
    Run internal unit tests for this module.

    Returns:
        bool: True if all tests pass, False otherwise
    """
    import unittest
    from io import StringIO
    import sys

    class TestCIKHandling(unittest.TestCase):
        def test_normalize_cik(self):
            test_cases = [
                ("320193", "0000320193"),
                (320193, "0000320193"),
                (320193.0, "0000320193"),
                ("0000320193", "0000320193"),
                ("00320193", "0000320193"),
            ]

            for input_val, expected in test_cases:
                with self.subTest(input=input_val):
                    result = _normalize_cik(input_val)
                    self.assertEqual(result, expected)

        def test_invalid_cik(self):
            with self.assertRaises(ValueError):
                _normalize_cik("invalid")
            with self.assertRaises(ValueError):
                _normalize_cik(None)

    class TestFinancialProcessing(unittest.TestCase):
        def test_statement_processing(self):
            # Create test DataFrame
            test_data = {
                'qtrs': [0.0, 1.0, 4.0, 0.5],
                'fp': ['Q1', 'Q2', 'FY', 'Q3']
            }
            df = pd.DataFrame(test_data)

            bs, is, cf = _process_statement_type(df)

            # Balance Sheet should have qtrs < 0.1
            self.assertEqual(len(bs), 1)
            self.assertEqual(bs['qtrs'].iloc[0], 0.0)

            # Income Statement should have specific qtrs ranges
            self.assertEqual(len(is), 2)  # 1.0 and 4.0

    # Run tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestCIKHandling)
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestFinancialProcessing))

    # Capture output
    stream = StringIO()
    runner = unittest.TextTestRunner(stream=stream)
    result = runner.run(suite)

    # Log results
    logger.log_info(f"Internal Tests: {result.testsRun} tests, {len(result.failures)} failures, {len(result.errors)} errors")

    if result.wasSuccessful():
        logger.log_info("✅ All internal tests passed")
        return True
    else:
        logger.log_error("❌ Some internal tests failed")
        for test, traceback in result.failures + result.errors:
            logger.log_error(f"Test failed: {test}\n{traceback}")
        return False

if __name__ == "__main__":
    # Run tests if executed directly
    success = _run_internal_tests()
    sys.exit(0 if success else 1)
```

## Implementation Roadmap (Updated)

### Phase 1: Configuration and CIK Standardization (Days 1-2)
1. **Add internal configuration classes** to `standardize.py` and `bronze_data_pipeline.py`
2. **Create CIK helper functions** within existing files
3. **Replace hardcoded paths** with config references
4. **Standardize CIK handling** throughout both files
5. **Add basic type hints** to critical functions

### Phase 2: Logic Refactoring (Days 3-4)
1. **Extract helper functions** for complex logic in `standardize.py`
2. **Refactor statement processing** into focused functions
3. **Improve memory management** in chunk processing
4. **Add comprehensive logging** to helper functions
5. **Update main functions** to use new helpers

### Phase 3: Dead Code Cleanup and Documentation (Days 5-6)
1. **Identify and remove dead code** systematically
2. **Add deprecation warnings** for transitional code
3. **Enhance module documentation** with clear examples
4. **Add comprehensive docstrings** to all public functions
5. **Update inline comments** to reflect current implementation

### Phase 4: Testing and Validation (Days 7-8)
1. **Add internal unit tests** to both files
2. **Create test data fixtures** for financial processing
3. **Implement validation tests** for CIK handling
4. **Add performance benchmarks** for critical functions
5. **Run comprehensive test suite** and fix any issues

## Expected Benefits (Updated)

1. **Improved Maintainability** - Clear internal structure without file splitting
2. **Enhanced Code Safety** - Comprehensive type hints and validation
3. **Better Performance** - Optimized helper functions and memory management
4. **Easier Debugging** - Focused functions with clear responsibilities
5. **Self-Documenting Code** - Comprehensive inline documentation
6. **Built-in Testing** - Internal test suites for immediate validation
7. **Preserved File Structure** - All improvements within existing files

## Risk Assessment (Updated)

| Risk | Mitigation Strategy |
|------|---------------------|
| Breaking changes in existing functionality | Comprehensive internal testing before changes |
| Increased file size with added code | Focus on code density and remove dead code |
| Complexity in large functions | Break down into focused helper functions |
| Performance regression | Add performance benchmarks to tests |
| Documentation maintenance | Use docstring templates and automation |

This approach ensures we address all technical debt issues while maintaining the existing file structure and improving code quality through internal refactoring.