"""
Test configuration and fixtures for InfoTracker tests.
"""
import pytest
import json
from pathlib import Path
from typing import Dict, Any

# Test data directories
EXAMPLES_DIR = Path(__file__).parent.parent / "examples" / "warehouse"
SQL_DIR = EXAMPLES_DIR / "sql"
LINEAGE_DIR = EXAMPLES_DIR / "lineage"


@pytest.fixture
def sql_files():
    """Fixture providing all SQL test files."""
    return list(SQL_DIR.glob("*.sql"))


@pytest.fixture
def expected_lineage():
    """Fixture providing all expected lineage JSON files."""
    lineage_files = {}
    for json_file in LINEAGE_DIR.glob("*.json"):
        with open(json_file, 'r') as f:
            lineage_files[json_file.stem] = json.load(f)
    return lineage_files


@pytest.fixture
def sql_content():
    """Fixture providing SQL content by filename."""
    sql_content = {}
    for sql_file in SQL_DIR.glob("*.sql"):
        with open(sql_file, 'r') as f:
            sql_content[sql_file.stem] = f.read()
    return sql_content


def normalize_json(data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize JSON for comparison by removing formatting differences."""
    if isinstance(data, dict):
        return {k: normalize_json(v) for k, v in sorted(data.items())}
    elif isinstance(data, list):
        return [normalize_json(item) for item in data]
    else:
        return data


def assert_json_equal(actual: Dict[str, Any], expected: Dict[str, Any], path: str = ""):
    """Compare two JSON objects with detailed error reporting."""
    actual_norm = normalize_json(actual)
    expected_norm = normalize_json(expected)
    
    if actual_norm != expected_norm:
        print(f"\nMismatch at path: {path}")
        print(f"Expected: {json.dumps(expected_norm, indent=2)}")
        print(f"Actual:   {json.dumps(actual_norm, indent=2)}")
        assert actual_norm == expected_norm, f"JSON mismatch at {path}"
