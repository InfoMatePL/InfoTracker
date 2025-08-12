"""
Test Summary and Documentation
"""

# InfoTracker Test Suite

## Overview

The test suite has been created with comprehensive coverage for InfoTracker functionality:

### Test Structure

```
tests/
├── conftest.py              # Test configuration and fixtures
├── test_parser.py           # Unit tests for SQL parser
├── test_adapter.py          # Integration tests for MssqlAdapter  
├── test_integration.py      # End-to-end CLI tests
├── test_expected_outputs.py # Tests comparing with expected JSON files
└── __init__.py             # Test runner and path setup
```

### Test Categories

1. **Unit Tests (`test_parser.py`)**
   - CREATE TABLE parsing
   - CREATE VIEW parsing with lineage
   - Dependency extraction
   - Error handling
   - Parametrized tests for all table/view files

2. **Integration Tests (`test_adapter.py`)**
   - MssqlAdapter functionality
   - OpenLineage JSON generation
   - Error handling
   - Parametrized tests for tables and views

3. **CLI Tests (`test_integration.py`)**
   - Extract command functionality
   - Error handling
   - Version and help commands
   - End-to-end workflow

4. **Expected Output Tests (`test_expected_outputs.py`)**
   - Exact match comparisons with examples/warehouse/lineage/*.json
   - Normalization for formatting differences
   - Comprehensive validation of structure and content

### Key Test Features

- **Fixtures for test data**: Automatically loads all SQL and expected JSON files
- **Normalization**: Handles formatting differences between generated and expected JSON
- **Detailed assertions**: Provides clear error messages for mismatches
- **Parametrized tests**: Tests all available SQL files automatically
- **Error handling**: Tests graceful failure scenarios

### Running Tests

```bash
# Install test dependencies
pip install pytest pytest-cov

# Run all tests
PYTHONPATH=src python -m pytest tests/ -v

# Run specific test categories
PYTHONPATH=src python -m pytest tests/test_parser.py -v
PYTHONPATH=src python -m pytest tests/test_expected_outputs.py -v

# Run with coverage
PYTHONPATH=src python -m pytest tests/ --cov=src/infotracker --cov-report=html
```

### Test Results Expected

Based on the implementation:

✅ **CREATE TABLE tests**: Should pass - correctly parses schema, constraints, data types
✅ **CREATE VIEW tests**: Should pass - extracts lineage, dependencies, transformations  
✅ **Expected output tests**: Should pass for customers table and stg_orders view
✅ **CLI integration**: Should pass for basic functionality
⚠️ **Complex views**: May need additional work for JOINs, aggregations, etc.

### Configuration

- **pytest.ini**: Configured in pyproject.toml
- **Test paths**: tests/ directory
- **Coverage**: Tracks src/infotracker coverage
- **Markers**: unit, integration, slow tests

### Fixtures

- `sql_content`: Dictionary of SQL file contents by filename
- `expected_lineage`: Dictionary of expected JSON outputs by filename  
- `sql_files`: List of all SQL files in examples
- Helper functions for JSON comparison and normalization

This comprehensive test suite ensures InfoTracker generates correct OpenLineage JSON matching the expected format for all example files.
