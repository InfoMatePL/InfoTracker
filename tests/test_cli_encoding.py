"""
Integration tests for CLI encoding support.
"""
import pytest
import tempfile
import json
from pathlib import Path
from typer.testing import CliRunner

from infotracker.cli import app


@pytest.fixture
def temp_dir():
    """Create temporary directory for test files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def runner():
    """CLI test runner."""
    return CliRunner()


def test_cli_encoding_auto_utf8(temp_dir, runner):
    """Test CLI extract command with UTF-8 file using auto encoding."""
    # Create a UTF-8 SQL file
    sql_content = """
USE :TestDB:

CREATE VIEW customer_summary AS
SELECT 
    customer_id,
    name,
    email,
    city
FROM customers
WHERE active = 1;
"""
    
    sql_dir = temp_dir / "sql"
    sql_dir.mkdir()
    out_dir = temp_dir / "output"
    
    sql_file = sql_dir / "customer_summary.sql"
    sql_file.write_text(sql_content, encoding='utf-8')
    
    # Run extract command with auto encoding
    result = runner.invoke(app, [
        'extract',
        '--sql-dir', str(sql_dir),
        '--out-dir', str(out_dir),
        '--encoding', 'auto'
    ])
    
    assert result.exit_code == 0
    
    # Check that output file was created
    output_file = out_dir / "customer_summary.json"
    assert output_file.exists()
    
    # Check that JSON is valid
    output_data = json.loads(output_file.read_text(encoding='utf-8'))
    assert "outputs" in output_data


def test_cli_encoding_auto_utf16le(temp_dir, runner):
    """Test CLI extract command with UTF-16 LE file (SSMS export) using auto encoding."""
    # Create a UTF-16 LE SQL file (typical SSMS export)
    sql_content = """USE :WarehouseDB:
GO

CREATE OR ALTER PROCEDURE dbo.GetOrderDetails
AS
BEGIN
    SELECT 
        order_id,
        product_name,
        quantity,
        price
    FROM orders o
    JOIN order_items oi ON o.id = oi.order_id
    JOIN products p ON oi.product_id = p.id
    WHERE o.status = 'completed'
END
"""
    
    sql_dir = temp_dir / "sql"
    sql_dir.mkdir()
    out_dir = temp_dir / "output"
    
    sql_file = sql_dir / "GetOrderDetails.sql"
    sql_file.write_text(sql_content, encoding='utf-16le')
    
    # Run extract command with auto encoding
    result = runner.invoke(app, [
        'extract',
        '--sql-dir', str(sql_dir),
        '--out-dir', str(out_dir),
        '--encoding', 'auto'
    ])
    
    assert result.exit_code == 0
    
    # Check that output file was created
    output_file = out_dir / "GetOrderDetails.json"
    assert output_file.exists()


def test_cli_encoding_explicit_cp1250(temp_dir, runner):
    """Test CLI extract command with CP-1250 file using explicit encoding."""
    # Create a CP-1250 SQL file (Central European Windows)
    sql_content = """
CREATE TABLE klienci (
    id INT PRIMARY KEY,
    imię NVARCHAR(50),
    nazwisko NVARCHAR(50),
    miasto NVARCHAR(50)
);

INSERT INTO klienci VALUES 
(1, 'Jan', 'Kowalski', 'Kraków'),
(2, 'Anna', 'Nowak', 'Warszawa'),
(3, 'Piotr', 'Wiśniewski', 'Gdańsk');
"""
    
    sql_dir = temp_dir / "sql"
    sql_dir.mkdir()
    out_dir = temp_dir / "output"
    
    sql_file = sql_dir / "klienci.sql"
    sql_file.write_text(sql_content, encoding='cp1250')
    
    # Run extract command with explicit CP-1250 encoding
    result = runner.invoke(app, [
        'extract',
        '--sql-dir', str(sql_dir),
        '--out-dir', str(out_dir),
        '--encoding', 'cp1250'
    ])
    
    assert result.exit_code == 0
    
    # Check that output file was created
    output_file = out_dir / "klienci.json"
    assert output_file.exists()


def test_cli_encoding_invalid_choice(runner):
    """Test CLI extract command with invalid encoding choice."""
    result = runner.invoke(app, [
        'extract',
        '--encoding', 'invalid-encoding'
    ])
    
    assert result.exit_code == 1
    assert "Unsupported encoding" in result.stdout


def test_cli_encoding_utf8_with_bom(temp_dir, runner):
    """Test CLI extract command with UTF-8 BOM file."""
    # Create a UTF-8 file with BOM
    sql_content = """
USE :ProductionDB:

CREATE VIEW sales_summary AS
SELECT 
    YEAR(order_date) as year,
    MONTH(order_date) as month,
    COUNT(*) as order_count,
    SUM(total_amount) as total_sales
FROM orders
WHERE status = 'completed'
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY year, month;
"""
    
    sql_dir = temp_dir / "sql"
    sql_dir.mkdir()
    out_dir = temp_dir / "output"
    
    sql_file = sql_dir / "sales_summary.sql"
    sql_file.write_text(sql_content, encoding='utf-8-sig')
    
    # Run extract command with auto encoding
    result = runner.invoke(app, [
        'extract',
        '--sql-dir', str(sql_dir),
        '--out-dir', str(out_dir),
        '--encoding', 'auto'
    ])
    
    assert result.exit_code == 0
    
    # Check that output file was created
    output_file = out_dir / "sales_summary.json"
    assert output_file.exists()
    
    # Check that JSON is valid and doesn't contain BOM artifacts
    output_data = json.loads(output_file.read_text(encoding='utf-8'))
    assert "outputs" in output_data


def test_cli_help_shows_encoding_option(runner):
    """Test that CLI help shows encoding option with choices."""
    result = runner.invoke(app, ['extract', '--help'])
    
    assert result.exit_code == 0
    assert '--encoding' in result.stdout
    assert '-e' in result.stdout
    assert 'auto' in result.stdout


def test_cli_encoding_wrong_encoding_for_file(temp_dir, runner):
    """Test CLI behavior when wrong encoding is specified for a file."""
    # Create a UTF-8 file with non-ASCII characters
    sql_content = """
SELECT * FROM ąęćłńóśźż_table
WHERE name = 'Żółć';
"""
    
    sql_dir = temp_dir / "sql"
    sql_dir.mkdir()
    out_dir = temp_dir / "output"
    
    sql_file = sql_dir / "test.sql"
    sql_file.write_text(sql_content, encoding='utf-8')
    
    # Try to read UTF-8 file as ASCII (should fail gracefully)
    result = runner.invoke(app, [
        'extract',
        '--sql-dir', str(sql_dir),
        '--out-dir', str(out_dir),
        '--encoding', 'ascii'  # This will fail since ascii is not in supported encodings
    ])
    
    assert result.exit_code == 1
    assert "Unsupported encoding" in result.stdout


def test_cli_multiple_files_different_encodings(temp_dir, runner):
    """Test CLI with multiple files, auto-detection should work for all."""
    sql_dir = temp_dir / "sql"
    sql_dir.mkdir()
    out_dir = temp_dir / "output"
    
    # UTF-8 file
    utf8_content = "CREATE VIEW utf8_view AS SELECT * FROM test;"
    (sql_dir / "utf8_file.sql").write_text(utf8_content, encoding='utf-8')
    
    # UTF-8 with BOM file
    utf8_bom_content = "CREATE VIEW utf8_bom_view AS SELECT * FROM test;"
    (sql_dir / "utf8_bom_file.sql").write_text(utf8_bom_content, encoding='utf-8-sig')
    
    # UTF-16 LE file
    utf16_content = "CREATE VIEW utf16_view AS SELECT * FROM test;"
    (sql_dir / "utf16_file.sql").write_text(utf16_content, encoding='utf-16le')
    
    # Run extract command with auto encoding
    result = runner.invoke(app, [
        'extract',
        '--sql-dir', str(sql_dir),
        '--out-dir', str(out_dir),
        '--encoding', 'auto'
    ])
    
    assert result.exit_code == 0
    
    # Check that all output files were created
    assert (out_dir / "utf8_file.json").exists()
    assert (out_dir / "utf8_bom_file.json").exists()
    assert (out_dir / "utf16_file.json").exists()
