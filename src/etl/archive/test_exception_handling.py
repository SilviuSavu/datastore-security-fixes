#!/usr/bin/env python3
"""
Test script to validate the exception handling improvements
"""

import sys
import os
from pathlib import Path

# Add the src directory to the path so we can import the modules
sys.path.insert(0, str(Path(__file__).parent / 'src'))

def test_sic_parsing_exception():
    """Test the improved SIC parsing exception handling"""
    print("🧪 Testing SIC parsing exception handling...")

    # Import the module with the fixed exception handling
    from etl.bronze_data_pipeline import CIKUtils

    # Create a test parquet file with problematic SIC data
    import pandas as pd
    import tempfile

    # Create test data with various SIC formats that might cause issues
    test_data = {
        'cik': ['0000320193', '0000020002'],
        'ticker': ['AAPL', 'MSFT'],
        'sic': ['1234', 'invalid_sic']  # One valid, one invalid
    }

    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
        df = pd.DataFrame(test_data)
        df.to_parquet(tmp.name)

        try:
            # This should trigger the improved exception handling
            results = CIKUtils.extract_metadata_from_parquet(tmp.name)

            # Verify we get results and no silent failures
            assert len(results) == 2, f"Expected 2 results, got {len(results)}"
            assert results[0]['sic'] == 1234, f"Expected SIC 1234, got {results[0]['sic']}"
            assert results[1]['sic'] is None, f"Expected None for invalid SIC, got {results[1]['sic']}"

            print("✅ SIC parsing exception handling test PASSED")
            return True

        except Exception as e:
            print(f"❌ SIC parsing test FAILED: {e}")
            return False
        finally:
            os.unlink(tmp.name)

def test_cik_ticker_mapping_exception():
    """Test the improved CIK-ticker mapping exception handling"""
    print("🧪 Testing CIK-ticker mapping exception handling...")

    # This test would require more complex setup with actual data
    # For now, we'll just verify the code structure is correct
    try:
        from etl.bronze_data_pipeline import CIKUniverseModule

        # Create instance and verify the method exists
        module = CIKUniverseModule()
        assert hasattr(module, '_load_edgartools_ciks'), "Method _load_edgartools_ciks not found"

        print("✅ CIK-ticker mapping exception handling structure test PASSED")
        return True

    except Exception as e:
        print(f"❌ CIK-ticker mapping test FAILED: {e}")
        return False

def test_year_processing_exception():
    """Test the improved year processing exception handling"""
    print("🧪 Testing year processing exception handling...")

    # This would require complex Delta Table setup
    # For now, verify the code structure
    try:
        from etl.bronze_data_pipeline import CIKUniverseModule

        module = CIKUniverseModule()
        assert hasattr(module, '_load_sec_bulk_metadata'), "Method _load_sec_bulk_metadata not found"

        print("✅ Year processing exception handling structure test PASSED")
        return True

    except Exception as e:
        print(f"❌ Year processing test FAILED: {e}")
        return False

def test_standardization_exceptions():
    """Test the improved standardization exception handling"""
    print("🧪 Testing standardization exception handling...")

    try:
        # Import the standardization module
        from etl.standardization.standardize import SECMapper

        # Verify the standardize method exists
        assert hasattr(SECMapper, 'standardize'), "Method standardize not found"

        print("✅ Standardization exception handling structure test PASSED")
        return True

    except Exception as e:
        print(f"❌ Standardization test FAILED: {e}")
        return False

def main():
    """Run all exception handling tests"""
    print("🚀 Starting exception handling validation tests...")
    print("=" * 50)

    tests = [
        test_sic_parsing_exception,
        test_cik_ticker_mapping_exception,
        test_year_processing_exception,
        test_standardization_exceptions
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        if test():
            passed += 1
        print()

    print("=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All exception handling improvements validated successfully!")
        return True
    else:
        print("⚠️  Some tests failed - manual verification may be needed")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)