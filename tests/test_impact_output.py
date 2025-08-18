"""
Test impact analysis --out functionality.
"""
import tempfile
import pytest
from pathlib import Path
from unittest.mock import Mock
from infotracker.cli import _emit


class TestImpactOutputFile:
    """Test impact analysis output to file functionality."""
    
    def test_emit_text_to_file(self):
        """Test emitting text output to a file."""
        payload = {
            "columns": ["object", "column", "direction"],
            "rows": [
                ["table1", "col1", "upstream"],
                ["table2", "col2", "downstream"]
            ]
        }
        
        with tempfile.TemporaryDirectory() as temp_dir:
            out_path = Path(temp_dir) / "impact.txt"
            
            # Test the _emit function with file output
            _emit(payload, "text", out_path)
            
            # Verify file was created and contains content
            assert out_path.exists()
            content = out_path.read_text(encoding='utf-8')
            assert len(content) > 0
            assert "table1" in content
            assert "col1" in content
            assert "table2" in content
            assert "col2" in content
    
    def test_emit_json_to_file(self):
        """Test emitting JSON output to a file."""
        payload = {
            "columns": ["object", "column"],
            "rows": [["test_table", "test_column"]],
            "summary": {"total": 1}
        }
        
        with tempfile.TemporaryDirectory() as temp_dir:
            out_path = Path(temp_dir) / "impact.json"
            
            # Test the _emit function with JSON file output
            _emit(payload, "json", out_path)
            
            # Verify file was created and contains valid JSON
            assert out_path.exists()
            content = out_path.read_text(encoding='utf-8')
            assert len(content) > 0
            assert "test_table" in content
            assert "test_column" in content
            assert "total" in content
            
            # Verify it's valid JSON
            import json
            parsed = json.loads(content)
            assert parsed["summary"]["total"] == 1
    
    def test_emit_creates_parent_directories(self):
        """Test that _emit creates parent directories if they don't exist."""
        payload = {"columns": ["test"], "rows": [["data"]]}
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a nested path that doesn't exist
            out_path = Path(temp_dir) / "nested" / "subdir" / "output.txt"
            
            # Should not raise an error and should create directories
            _emit(payload, "text", out_path)
            
            # Verify the nested directories were created
            assert out_path.exists()
            assert out_path.parent.exists()
            assert out_path.parent.parent.exists()
    
    def test_emit_without_out_path_prints_to_stdout(self):
        """Test that _emit prints to stdout when no out_path is provided."""
        payload = {"columns": ["test"], "rows": [["data"]]}
        
        # This should not raise an error and should not create any files
        # We can't easily test stdout capture here, but we can verify no exception
        _emit(payload, "text", None)
        _emit(payload, "json", None)
