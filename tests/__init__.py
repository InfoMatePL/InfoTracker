"""
Test runner script and pytest configuration.
"""
import sys
from pathlib import Path

# Add the src directory to the path for imports
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

# This file can be run directly to execute tests
if __name__ == "__main__":
    import pytest
    
    # Run tests with verbose output
    exit_code = pytest.main([
        "-v", 
        "--tb=short",
        str(Path(__file__).parent)
    ])
    sys.exit(exit_code)
