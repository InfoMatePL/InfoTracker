"""
End-to-end integration tests for the InfoTracker CLI.
"""
import pytest
import json
import tempfile
import shutil
from pathlib import Path
from typer.testing import CliRunner

from infotracker.cli import app
from .conftest import assert_json_equal


class TestCLIIntegration:
    """Test cases for CLI integration."""

    def setup_method(self):
        """Set up test instance."""
        self.runner = CliRunner()

    def test_extract_command_basic(self, tmp_path):
        """Test the extract command with basic functionality."""
        # Create temporary directories
        sql_dir = tmp_path / "sql"
        out_dir = tmp_path / "output"
        sql_dir.mkdir()
        out_dir.mkdir()
        
        # Create a simple test SQL file
        test_sql = """CREATE TABLE STG.dbo.TestTable (
            ID INT PRIMARY KEY,
            Name NVARCHAR(100) NOT NULL
        );"""
        
        sql_file = sql_dir / "01_test.sql"
        sql_file.write_text(test_sql)
        
        # Run the extract command
        result = self.runner.invoke(app, [
            "extract",
            "--sql-dir", str(sql_dir),
            "--out-dir", str(out_dir)
        ])
        
        assert result.exit_code == 0
        
        # Check that output file was created
        output_file = out_dir / "01_test.json"
        assert output_file.exists()
        
        # Verify the output content
        with open(output_file) as f:
            output_data = json.load(f)
        
        assert output_data["eventType"] == "COMPLETE"
        assert "01_test" in output_data["job"]["name"]
        assert len(output_data["outputs"]) == 1
        assert "schema" in output_data["outputs"][0]["facets"]

    def test_extract_command_with_examples(self):
        """Test the extract command with the example dataset."""
        # Use a temporary directory for output
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_dir = Path(tmp_dir) / "lineage"
            out_dir.mkdir()
            
            # Get the examples directory
            examples_dir = Path(__file__).parent.parent / "examples" / "warehouse" / "sql"
            
            if not examples_dir.exists():
                pytest.skip("Examples directory not found")
            
            # Run the extract command
            result = self.runner.invoke(app, [
                "extract",
                "--sql-dir", str(examples_dir),
                "--out-dir", str(out_dir)
            ])
            
            assert result.exit_code == 0
            
            # Check that output files were created
            expected_files = [
                "01_customers.json",
                "02_orders.json", 
                "03_products.json",
                "04_order_items.json",
                "10_stg_orders.json"
            ]
            
            for filename in expected_files:
                output_file = out_dir / filename
                if output_file.exists():
                    # Verify the file contains valid JSON
                    with open(output_file) as f:
                        data = json.load(f)
                    assert "eventType" in data
                    assert "outputs" in data

    def test_extract_command_error_handling(self, tmp_path):
        """Test extract command error handling."""
        # Test with non-existent directory
        result = self.runner.invoke(app, [
            "extract",
            "--sql-dir", str(tmp_path / "nonexistent"),
            "--out-dir", str(tmp_path / "output")
        ])
        
        # Should fail gracefully
        assert result.exit_code != 0

    def test_impact_command_placeholder(self):
        """Test impact command (placeholder functionality)."""
        result = self.runner.invoke(app, [
            "impact",
            "--selector", "STG.dbo.Customers.CustomerID"
        ])
        
        # Should run without error (even if functionality is placeholder)
        assert result.exit_code == 0

    def test_diff_command_placeholder(self):
        """Test diff command (placeholder functionality)."""
        result = self.runner.invoke(app, [
            "diff",
            "--base", "main",
            "--head", "feature"
        ])
        
        # Should run without error (even if functionality is placeholder)
        assert result.exit_code == 0

    def test_version_command(self):
        """Test version command."""
        result = self.runner.invoke(app, ["--version"])
        
        assert result.exit_code == 0
        assert "infotracker" in result.stdout.lower()

    def test_help_command(self):
        """Test help command."""
        result = self.runner.invoke(app, ["--help"])
        
        assert result.exit_code == 0
        assert "extract" in result.stdout
        assert "impact" in result.stdout
        assert "diff" in result.stdout
