#!/usr/bin/env python3
"""
Refactoring Validation Script
=============================

Validates the technical debt refactoring implementation in both files.
"""

import os
import sys
import ast
import re
from pathlib import Path

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

def validate_file_structure():
    """Validate that both files have the expected refactored structure"""
    print("🔍 Validating file structure...")

    files_to_check = [
        'src/etl/standardization/standardize.py',
        'src/etl/bronze_data_pipeline.py'
    ]

    required_components = {
        'standardize.py': [
            '_Config class',
            '_normalize_cik function',
            '_batch_cik_normalization function',
            '_process_financial_statement function',
            '_map_financial_concepts function',
            '_calculate_derived_metrics function',
            'CIK standardization',
            'Financial processing helpers',
            'Comprehensive documentation'
        ],
        'bronze_data_pipeline.py': [
            '_PipelineConfig class',
            '_normalize_cik function',
            '_batch_cik_normalization function',
            '_process_financial_data_chunk function',
            '_standardize_financial_value function',
            '_validate_financial_dataframe function',
            'CIK standardization',
            'Financial processing helpers',
            'Comprehensive documentation'
        ]
    }

    results = {}

    for file_path in files_to_check:
        full_path = os.path.join(project_root, file_path)
        if not os.path.exists(full_path):
            print(f"❌ File not found: {file_path}")
            continue

        with open(full_path, 'r', encoding='utf-8') as f:
            content = f.read()

        file_results = {
            'file': file_path,
            'exists': True,
            'components_found': [],
            'components_missing': [],
            'line_count': len(content.split('\n')),
            'has_documentation': False,
            'has_config_system': False,
            'has_cik_standardization': False,
            'has_helper_functions': False
        }

        # Check for required components
        for component in required_components.get(os.path.basename(file_path), []):
            if component.lower() in content.lower():
                file_results['components_found'].append(component)
            else:
                file_results['components_missing'].append(component)

        # Check for documentation
        file_results['has_documentation'] = '"""' in content and 'Technical Debt Addressed:' in content

        # Check for config system
        file_results['has_config_system'] = '_config' in content.lower() or 'pipelineconfig' in content.lower()

        # Check for CIK standardization
        file_results['has_cik_standardization'] = '_normalize_cik' in content

        # Check for helper functions
        file_results['has_helper_functions'] = '_process_' in content and '_validate_' in content

        results[file_path] = file_results

    return results

def validate_cik_standardization():
    """Validate CIK standardization implementation"""
    print("\n🔍 Validating CIK standardization...")

    # Test standardize.py
    try:
        from src.etl.standardization.standardize import _normalize_cik, _batch_cik_normalization

        test_cases = [
            ('320193', '0000320193'),
            ('0000320193', '0000320193'),
            (320193, '0000320193'),
            (320193.0, '0000320193'),
            ('  320193  ', '0000320193')
        ]

        all_passed = True
        for input_val, expected in test_cases:
            try:
                result = _normalize_cik(input_val)
                if result == expected:
                    print(f"✅ CIK normalization: {input_val} -> {result}")
                else:
                    print(f"❌ CIK normalization failed: {input_val} -> {result} (expected {expected})")
                    all_passed = False
            except Exception as e:
                print(f"❌ CIK normalization error for {input_val}: {str(e)}")
                all_passed = False

        return all_passed

    except ImportError as e:
        print(f"❌ Failed to import standardize.py functions: {str(e)}")
        return False

def validate_helper_functions():
    """Validate helper function implementation"""
    print("\n🔍 Validating helper functions...")

    try:
        # Test standardize.py helpers
        from src.etl.standardization.standardize import (
            _process_financial_statement,
            _map_financial_concepts,
            _calculate_derived_metrics
        )

        # Test financial statement processing
        statement_data = {
            'Assets': 1000000,
            'Liabilities': 500000,
            'Revenue': 2000000,
            'NetIncome': 300000
        }

        processed = _process_financial_statement(statement_data)
        has_metrics = 'working_capital' in processed and 'profit_margin' in processed

        # Test concept mapping
        concepts = ['Revenue', 'NetIncome', 'Assets', 'Liabilities', 'UnknownConcept']
        mapped = _map_financial_concepts(concepts)
        mapping_ok = mapped['Revenue'] == 'revenue' and mapped['UnknownConcept'] == 'other'

        print(f"✅ Financial statement processing: {'✅' if has_metrics else '❌'}")
        print(f"✅ Concept mapping: {'✅' if mapping_ok else '❌'}")

        return has_metrics and mapping_ok

    except ImportError as e:
        print(f"❌ Failed to import helper functions: {str(e)}")
        return False
    except Exception as e:
        print(f"❌ Helper function validation error: {str(e)}")
        return False

def validate_bronze_pipeline():
    """Validate bronze pipeline refactoring"""
    print("\n🔍 Validating bronze pipeline refactoring...")

    try:
        from src.etl.bronze_data_pipeline import (
            _PipelineConfig,
            _normalize_cik,
            _process_financial_data_chunk
        )

        # Test config initialization
        config = _PipelineConfig()
        config_ok = hasattr(config, 'PROJECT_ROOT') and hasattr(config, 'CIK_FORMAT')

        # Test CIK normalization
        cik_ok = _normalize_cik('320193') == '0000320193'

        print(f"✅ Pipeline config: {'✅' if config_ok else '❌'}")
        print(f"✅ CIK normalization: {'✅' if cik_ok else '❌'}")

        return config_ok and cik_ok

    except ImportError as e:
        print(f"❌ Failed to import bronze pipeline: {str(e)}")
        return False
    except Exception as e:
        print(f"❌ Bronze pipeline validation error: {str(e)}")
        return False

def main():
    """Run all validation checks"""
    print("🚀 Starting refactoring validation...")
    print("=" * 50)

    # Validate file structure
    structure_results = validate_file_structure()

    for file_path, results in structure_results.items():
        print(f"\n📄 {file_path}:")
        print(f"   Lines: {results['line_count']}")
        print(f"   Documentation: {'✅' if results['has_documentation'] else '❌'}")
        print(f"   Config System: {'✅' if results['has_config_system'] else '❌'}")
        print(f"   CIK Standardization: {'✅' if results['has_cik_standardization'] else '❌'}")
        print(f"   Helper Functions: {'✅' if results['has_helper_functions'] else '❌'}")

        if results['components_missing']:
            print(f"   ⚠️  Missing components: {', '.join(results['components_missing'])}")

    # Validate specific functionality
    cik_ok = validate_cik_standardization()
    helpers_ok = validate_helper_functions()
    bronze_ok = validate_bronze_pipeline()

    # Summary
    print("\n" + "=" * 50)
    print("📊 VALIDATION SUMMARY:")
    print(f"📄 File Structure: {'✅' if all(r['exists'] for r in structure_results.values()) else '❌'}")
    print(f"🔢 CIK Standardization: {'✅' if cik_ok else '❌'}")
    print(f"🛠️ Helper Functions: {'✅' if helpers_ok else '❌'}")
    print(f"📦 Bronze Pipeline: {'✅' if bronze_ok else '❌'}")

    all_passed = all([
        all(r['exists'] for r in structure_results.values()),
        cik_ok,
        helpers_ok,
        bronze_ok
    ])

    print(f"\n🎯 Overall Result: {'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}")
    print("=" * 50)

    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)