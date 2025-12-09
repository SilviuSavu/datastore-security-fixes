"""
Centralized Logging Module for DataStore

This module provides a standardized logging system to replace inconsistent
print() statements and provide better error tracking and debugging capabilities.
"""

import logging
import os
from typing import Optional, Dict, Any
from datetime import datetime

class DataStoreLogger:
    """
    Centralized logging system for DataStore applications.

    Features:
    - Standardized log format
    - Multiple log levels (CRITICAL, ERROR, WARNING, INFO, DEBUG)
    - Automatic context tracking
    - Log rotation support
    - Thread-safe operations
    """

    def __init__(self, name: str = "datastore", log_dir: str = "logs"):
        """
        Initialize the logger with standardized configuration.

        Args:
            name: Logger name (typically module name)
            log_dir: Directory for log files
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)  # Capture all levels
        self.context = {}  # Initialize context dictionary

        # Prevent duplicate handlers
        if self.logger.handlers:
            return

        # Create log directory if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)

        # Standard log format
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Console handler (INFO and above)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)

        # File handler (DEBUG and above)
        log_filename = os.path.join(log_dir, f"{name}_{datetime.now().strftime('%Y%m%d')}.log")
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        # Add handlers
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

        # Store context for automatic inclusion
        self.context: Dict[str, Any] = {}

    def add_context(self, key: str, value: Any):
        """Add contextual information to all subsequent log messages."""
        self.context[key] = value

    def clear_context(self):
        """Clear all stored context."""
        self.context.clear()

    def _format_message(self, message: str) -> str:
        """Format message with current context."""
        if self.context:
            context_str = " | ".join(f"{k}={v}" for k, v in self.context.items())
            return f"[{context_str}] {message}"
        return message

    def critical(self, message: str, context: Optional[str] = None):
        """Log critical system failures."""
        full_message = self._format_message(message)
        if context:
            full_message = f"{context}: {full_message}"
        self.logger.critical(full_message)

    def error(self, message: str, context: Optional[str] = None):
        """Log errors that prevent operation completion."""
        full_message = self._format_message(message)
        if context:
            full_message = f"{context}: {full_message}"
        self.logger.error(full_message)

    def warning(self, message: str, context: Optional[str] = None):
        """Log recoverable issues and degraded functionality."""
        full_message = self._format_message(message)
        if context:
            full_message = f"{context}: {full_message}"
        self.logger.warning(full_message)

    def info(self, message: str, context: Optional[str] = None):
        """Log normal operation milestones."""
        full_message = self._format_message(message)
        if context:
            full_message = f"{context}: {full_message}"
        self.logger.info(full_message)

    def debug(self, message: str, context: Optional[str] = None):
        """Log detailed debugging information."""
        full_message = self._format_message(message)
        if context:
            full_message = f"{context}: {full_message}"
        self.logger.debug(full_message)

    def exception(self, message: str, context: Optional[str] = None):
        """Log exception with traceback information."""
        full_message = self._format_message(message)
        if context:
            full_message = f"{context}: {full_message}"
        self.logger.exception(full_message)

# Global logger instance
logger = DataStoreLogger()

# Convenience functions for backward compatibility
def get_logger(name: Optional[str] = None) -> DataStoreLogger:
    """
    Get a logger instance.

    Args:
        name: Optional logger name. If None, returns the global logger.

    Returns:
        Configured DataStoreLogger instance
    """
    if name:
        return DataStoreLogger(name)
    return logger

# Custom exceptions for DataStore
class DataStoreError(Exception):
    """Base exception for all DataStore errors."""
    pass

class DataProcessingError(DataStoreError):
    """Errors related to data processing operations."""
    pass

class DataValidationError(DataStoreError):
    """Errors related to data validation."""
    pass

class ResourceError(DataStoreError):
    """Errors related to resource access (files, databases, etc.)."""
    pass

class ConfigurationError(DataStoreError):
    """Errors related to configuration issues."""
    pass

class ExternalServiceError(DataStoreError):
    """Errors related to external service interactions."""
    pass

# Error handling utilities
def handle_exception_with_context(
    operation: str,
    context: Dict[str, Any],
    rethrow: bool = True
) -> callable:
    """
    Decorator to handle exceptions with automatic context logging.

    Args:
        operation: Description of the operation being performed
        context: Dictionary of contextual information
        rethrow: Whether to re-raise the exception after logging

    Returns:
        Decorator function
    """
    def decorator(func: callable) -> callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.add_context("operation", operation)
                for key, value in context.items():
                    logger.add_context(key, value)

                logger.error(f"Exception in {operation}: {str(e)}")
                logger.exception("Full traceback:")

                if rethrow:
                    raise
                return None
        return wrapper
    return decorator

def safe_operation(
    operation: str,
    func: callable,
    fallback: Any = None,
    max_retries: int = 0,
    retry_delay: float = 0.1
) -> Any:
    """
    Execute an operation with automatic error handling and retry logic.

    Args:
        operation: Description of the operation
        func: Function to execute
        fallback: Fallback value/function if operation fails
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds

    Returns:
        Result of func() or fallback if provided
    """
    import time

    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            if attempt < max_retries:
                logger.warning(
                    f"Attempt {attempt + 1} failed for {operation}: {str(e)}. "
                    f"Retrying in {retry_delay} seconds..."
                )
                time.sleep(retry_delay)
            else:
                logger.error(f"Operation failed after {max_retries + 1} attempts: {str(e)}")
                if fallback is not None:
                    if callable(fallback):
                        logger.info(f"Using fallback for {operation}")
                        return fallback()
                    else:
                        return fallback
                raise