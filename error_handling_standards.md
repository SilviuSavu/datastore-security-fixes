# DataStore Error Handling Standards

## 1. Exception Handling Principles

### 1.1 General Rules

1. **Never swallow exceptions silently** - Always log or handle appropriately
2. **Be specific** - Catch the most specific exception possible
3. **Provide context** - Include relevant information about what failed
4. **Fail gracefully** - When possible, continue operation or provide fallback
5. **Clean up resources** - Ensure proper cleanup in finally blocks

### 1.2 Exception Handling Pattern

```python
# RECOMMENDED PATTERN
try:
    # Primary operation
    result = risky_operation(data)

except SpecificException as e:
    # Handle specific error case
    logger.warning(f"Specific error in {context}: {str(e)}")
    result = fallback_operation()

except AnotherSpecificException as e:
    # Handle another specific case
    logger.error(f"Critical error in {context}: {str(e)}")
    raise ProcessingError(f"Cannot complete {operation}") from e

except Exception as e:
    # Catch-all for unexpected errors
    logger.critical(f"Unexpected error in {context}: {str(e)}")
    raise  # Re-raise to maintain stack trace
```

## 2. Logging Standards

### 2.1 Log Levels and Usage

| Level | Usage | Example |
|-------|-------|---------|
| **CRITICAL** | System-wide failures, unrecoverable errors | `logger.critical("Database connection failed")` |
| **ERROR** | Function-level failures, operation cannot complete | `logger.error("File processing failed: invalid format")` |
| **WARNING** | Recoverable issues, degraded functionality | `logger.warning("Fallback to secondary data source")` |
| **INFO** | Normal operation milestones | `logger.info("Processed 1000 records successfully")` |
| **DEBUG** | Detailed debugging information | `logger.debug("Data transformation: before={x}, after={y}")` |

### 2.2 Log Message Format

**Required components**:
- Timestamp (auto-added by logger)
- Module/function context
- Relevant identifiers (file paths, CIKs, etc.)
- Clear error description

**Examples**:
```python
# GOOD
logger.error(f"Data validation failed for CIK {cik}: {str(e)}")

# BAD - Missing context
logger.error("Validation failed")
```

## 3. Memory Management Guidelines

### 3.1 Data Processing Patterns

```python
# RECOMMENDED: Batch processing with explicit cleanup
def process_large_file(file_path: str):
    logger.info(f"Starting processing: {file_path}")

    try:
        with pd.read_csv(file_path, chunksize=1000) as reader:
            for i, chunk in enumerate(reader):
                try:
                    process_chunk(chunk)

                    # Explicit cleanup every 10 batches
                    if i % 10 == 0:
                        gc.collect()
                        logger.debug(f"Memory cleanup after batch {i}")

                except Exception as e:
                    logger.error(f"Batch {i} failed: {str(e)}")
                    continue

    except Exception as e:
        logger.critical(f"File processing failed: {str(e)}")
        raise
    finally:
        # Ensure resources are released
        if 'reader' in locals():
            del reader
        gc.collect()
```

### 3.2 Context Managers

```python
# RECOMMENDED: Use context managers for resource management
def load_data_safely(file_path: str):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        return data

    except FileNotFoundError:
        logger.warning(f"File not found: {file_path}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {file_path}: {str(e)}")
        raise DataFormatError(f"Invalid data format: {file_path}")
```

## 4. Error Recovery Strategies

### 4.1 Retry Logic

```python
def with_retry(operation, max_attempts=3, delay=1):
    """Retry decorator for transient failures"""
    for attempt in range(max_attempts):
        try:
            return operation()
        except TransientError as e:
            if attempt < max_attempts - 1:
                logger.warning(f"Attempt {attempt + 1} failed, retrying: {str(e)}")
                time.sleep(delay)
            else:
                logger.error(f"Operation failed after {max_attempts} attempts")
                raise
```

### 4.2 Fallback Mechanisms

```python
def get_data_with_fallback(primary_source, secondary_source):
    try:
        return primary_source.fetch()
    except PrimarySourceError as e:
        logger.warning(f"Primary source failed, using fallback: {str(e)}")
        try:
            return secondary_source.fetch()
        except Exception as e:
            logger.error(f"Both sources failed: {str(e)}")
            raise DataUnavailableError("No data sources available")
```

## 5. Custom Exception Hierarchy

```python
class DataStoreError(Exception):
    """Base exception for all DataStore errors"""
    pass

class DataProcessingError(DataStoreError):
    """Errors related to data processing"""
    pass

class DataValidationError(DataStoreError):
    """Errors related to data validation"""
    pass

class ResourceError(DataStoreError):
    """Errors related to resource access"""
    pass

class ConfigurationError(DataStoreError):
    """Errors related to configuration"""
    pass
```

## 6. Implementation Checklist

### 6.1 For New Code

- [ ] Use specific exception types where possible
- [ ] Include proper logging with context
- [ ] Implement resource cleanup in finally blocks
- [ ] Add appropriate error recovery mechanisms
- [ ] Follow the established logging format

### 6.2 For Existing Code Refactoring

- [ ] Replace generic `except Exception:` with specific handling
- [ ] Replace `print()` statements with proper logging
- [ ] Add context to all error messages
- [ ] Implement memory management in data processing loops
- [ ] Add proper resource cleanup

## 7. Testing Requirements

### 7.1 Unit Tests

```python
def test_error_handling():
    # Test that exceptions are properly caught and logged
    with patch('module.logger.error') as mock_log:
        with pytest.raises(ProcessingError):
            function_that_fails()
        mock_log.assert_called_once_with("Expected error message")
```

### 7.2 Integration Tests

```python
def test_memory_management():
    # Test that memory is properly managed
    initial_memory = get_memory_usage()
    process_large_dataset()
    final_memory = get_memory_usage()
    assert final_memory <= initial_memory * 1.1  # No more than 10% increase
```

## 8. Monitoring and Maintenance

### 8.1 Error Monitoring

- Implement centralized error tracking
- Set up alerts for critical errors
- Regularly review error logs for patterns

### 8.2 Performance Monitoring

- Track memory usage in data processing
- Monitor error rates and types
- Set up automated testing for error handling paths

## 9. Migration Plan

### 9.1 Priority Order

1. **Critical**: Files with data processing loops (`src/etl/*`, `src/models/*`)
2. **High**: Files with external dependencies (`src/feature_store/*`, `src/data/*`)
3. **Medium**: Utility and debugging files (`src/debug/*`, `src/cli/*`)
4. **Low**: Configuration and setup files

### 9.2 Implementation Strategy

1. **Phase 1**: Create logging infrastructure
2. **Phase 2**: Update critical data processing files
3. **Phase 3**: Add memory management improvements
4. **Phase 4**: Comprehensive testing and validation
5. **Phase 5**: Documentation and training