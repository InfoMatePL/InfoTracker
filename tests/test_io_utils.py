"""
Tests for io_utils encoding detection and reading functionality.
"""
import pytest
import tempfile
from pathlib import Path

from infotracker.io_utils import read_text_safely, get_supported_encodings, _detect_bom, _normalize_content



def test_read_utf8_without_bom():
    """Test reading UTF-8 file without BOM."""
    content = "SELECT * FROM ąęćłńóśźż_table;"
    
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.sql') as f:
        f.write(content)
        temp_path = f.name
    
    try:
        # Test auto detection
        result = read_text_safely(temp_path, encoding="auto")
        assert result == content
        
        # Test explicit UTF-8
        result = read_text_safely(temp_path, encoding="utf-8")
        assert result == content
        
    finally:
        Path(temp_path).unlink()


def test_read_utf8_with_bom():
    """Test reading UTF-8 file with BOM."""
    content = "SELECT * FROM ąęćłńóśźż_table;"
    
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8-sig', delete=False, suffix='.sql') as f:
        f.write(content)
        temp_path = f.name
    
    try:
        # Test auto detection
        result = read_text_safely(temp_path, encoding="auto")
        assert result == content
        assert not result.startswith('\ufeff')  # BOM should be removed
        
        # Test explicit UTF-8-sig
        result = read_text_safely(temp_path, encoding="utf-8-sig")
        assert result == content
        
    finally:
        Path(temp_path).unlink()


def test_read_utf16_le():
    """Test reading UTF-16 LE file (typical SSMS output)."""
    content = "USE :TestDB:\r\nCREATE VIEW test AS SELECT * FROM ąęćłńóśźż_table;"
    expected = "USE :TestDB:\nCREATE VIEW test AS SELECT * FROM ąęćłńóśźż_table;"
    
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.sql') as f:
        f.write(content.encode('utf-16le'))
        temp_path = f.name
    
    try:
        # Test auto detection
        result = read_text_safely(temp_path, encoding="auto")
        assert result == expected
        
        # Test explicit UTF-16LE
        result = read_text_safely(temp_path, encoding="utf-16le")
        assert result == expected
        
    finally:
        Path(temp_path).unlink()


def test_read_utf16_be():
    """Test reading UTF-16 BE file."""
    content = "SELECT * FROM ąęćłńóśźż_table;"
    
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.sql') as f:
        f.write(content.encode('utf-16be'))
        temp_path = f.name
    
    try:
        # Test auto detection
        result = read_text_safely(temp_path, encoding="auto")
        assert result == content
        
        # Test explicit UTF-16BE
        result = read_text_safely(temp_path, encoding="utf-16be")
        assert result == content
        
    finally:
        Path(temp_path).unlink()


def test_read_cp1250():
    """Test reading CP-1250 file (Central European Windows encoding)."""
    content = "SELECT * FROM test_table WHERE name = 'Kraków';"
    
    with tempfile.NamedTemporaryFile(mode='w', encoding='cp1250', delete=False, suffix='.sql') as f:
        f.write(content)
        temp_path = f.name
    
    try:
        # Test auto detection (should work as fallback)
        result = read_text_safely(temp_path, encoding="auto")
        assert result == content
        
        # Test explicit CP-1250
        result = read_text_safely(temp_path, encoding="cp1250")
        assert result == content
        
    finally:
        Path(temp_path).unlink()


def test_invalid_encoding_with_strict():
    """Test that UTF-8 file forced as CP-1250 raises validation error."""
    content = "SELECT * FROM ąęćłńóśźż_table;"
    
    # Create UTF-8 file but try to read as CP-1250 (should fail with validation)
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.sql') as f:
        f.write(content.encode('utf-8'))
        temp_path = f.name
    
    try:
        with pytest.raises(UnicodeDecodeError) as exc_info:
            read_text_safely(temp_path, encoding="cp1250")
        
        error_msg = str(exc_info.value)
        assert "appears to be UTF-8" in error_msg
        assert "cp1250" in error_msg
        assert "Try --encoding auto" in error_msg
        
    finally:
        Path(temp_path).unlink()


def test_correct_cp1250_encoding():
    """Test that actual CP-1250 file works correctly with forced CP-1250 encoding."""
    # Content that's naturally CP-1250 (Latin-2)
    content = "SELECT * FROM tabela WHERE miasto = 'Kraków' AND data > '2023-01-01';"
    
    with tempfile.NamedTemporaryFile(mode='w', encoding='cp1250', delete=False, suffix='.sql') as f:
        f.write(content)
        temp_path = f.name
    
    try:
        # Should work fine with forced CP-1250
        result = read_text_safely(temp_path, encoding="cp1250")
        assert result == content
        
        # Should also work with auto
        result = read_text_safely(temp_path, encoding="auto")
        assert result == content
        
    finally:
        Path(temp_path).unlink()


def test_correct_utf8_forced():
    """Test that UTF-8 file works correctly with forced UTF-8 encoding."""
    content = "SELECT * FROM ąęćłńóśźż_table WHERE name = 'test';"
    
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.sql') as f:
        f.write(content)
        temp_path = f.name
    
    try:
        # Should work fine with forced UTF-8
        result = read_text_safely(temp_path, encoding="utf-8")
        assert result == content
        
    finally:
        Path(temp_path).unlink()


def test_malformed_text_validation():
    """Test validation of malformed text with poor quality score."""
    # Create content that would decode but look malformed (lots of control chars)
    malformed_content = "".join(chr(i) for i in range(32))  # Control characters
    
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.sql') as f:
        # Encode as Latin-1 so it will decode with CP-1250 but look bad
        f.write(malformed_content.encode('latin-1'))
        temp_path = f.name
    
    try:
        with pytest.raises(UnicodeDecodeError) as exc_info:
            read_text_safely(temp_path, encoding="cp1250")
        
        error_msg = str(exc_info.value)
        assert "looks malformed" in error_msg
        assert "quality=" in error_msg
        assert "Try --encoding auto" in error_msg
        
    finally:
        Path(temp_path).unlink()


def test_validation_helper_functions():
    """Test the validation helper functions directly."""
    from infotracker.io_utils import _looks_like_utf8, _text_quality_score, _looks_like_sql
    
    # Test _looks_like_utf8
    utf8_bytes = "Hello ąęćłńóśźż".encode('utf-8')
    ascii_bytes = "Hello world".encode('ascii')
    assert _looks_like_utf8(utf8_bytes) == True
    assert _looks_like_utf8(ascii_bytes) == False  # No non-ASCII chars
    
    # Test _text_quality_score
    good_text = "SELECT * FROM table WHERE id = 1;"
    bad_text = "".join(chr(i) for i in range(32))  # Control chars
    assert _text_quality_score(good_text) == 1.0
    assert _text_quality_score(bad_text) < 0.5
    
    # Test _looks_like_sql
    sql_text = "CREATE VIEW test AS SELECT * FROM table;"
    non_sql_text = "This is just regular text without SQL keywords."
    assert _looks_like_sql(sql_text) == True
    assert _looks_like_sql(non_sql_text) == False


def test_line_ending_normalization():
    """Test that line endings are normalized to \\n."""
    # Test with Windows line endings
    content_windows = "Line 1\r\nLine 2\r\nLine 3"
    expected = "Line 1\nLine 2\nLine 3"
    
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.sql') as f:
        f.write(content_windows.encode('utf-8'))
        temp_path = f.name
    
    try:
        result = read_text_safely(temp_path, encoding="utf-8")
        assert result == expected
        
    finally:
        Path(temp_path).unlink()
    
    # Test with Mac line endings
    content_mac = "Line 1\rLine 2\rLine 3"
    expected = "Line 1\nLine 2\nLine 3"
    
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.sql') as f:
        f.write(content_mac.encode('utf-8'))
        temp_path = f.name
    
    try:
        result = read_text_safely(temp_path, encoding="utf-8")
        assert result == expected
        
    finally:
        Path(temp_path).unlink()


def test_empty_file():
    """Test reading empty file."""
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.sql') as f:
        f.write("")
        temp_path = f.name
    
    try:
        result = read_text_safely(temp_path, encoding="auto")
        assert result == ""
        
    finally:
        Path(temp_path).unlink()


def test_nonexistent_file():
    """Test reading non-existent file raises appropriate error."""
    with pytest.raises(IOError) as exc_info:
        read_text_safely("/nonexistent/path.sql", encoding="auto")
    
    assert "Cannot read file" in str(exc_info.value)


def test_detect_bom():
    """Test BOM detection functionality."""
    # UTF-8 BOM
    assert _detect_bom(b'\xef\xbb\xbfHello') == 'utf-8-sig'
    
    # UTF-16 LE BOM
    assert _detect_bom(b'\xff\xfeH\x00e\x00l\x00l\x00o\x00') == 'utf-16le'
    
    # UTF-16 BE BOM  
    assert _detect_bom(b'\xfe\xff\x00H\x00e\x00l\x00l\x00o') == 'utf-16be'
    
    # No BOM
    assert _detect_bom(b'Hello') is None
    
    # Empty
    assert _detect_bom(b'') is None


def test_normalize_content():
    """Test content normalization."""
    # Test BOM removal
    assert _normalize_content('\ufeffHello World') == 'Hello World'
    
    # Test line ending normalization
    assert _normalize_content('Line1\r\nLine2\rLine3\n') == 'Line1\nLine2\nLine3\n'
    
    # Test combination
    assert _normalize_content('\ufeffLine1\r\nLine2') == 'Line1\nLine2'


def test_auto_fallback_ordering():
    """Test that auto detection tries encodings in the right order."""
    # Create a file that's valid UTF-8 but would also be valid CP-1250
    content = "SELECT * FROM test_table;"
    
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.sql') as f:
        f.write(content.encode('utf-8'))
        temp_path = f.name
    
    try:
        result = read_text_safely(temp_path, encoding="auto")
        assert result == content
        # Should detect as UTF-8, not CP-1250
        
    finally:
        Path(temp_path).unlink()


def test_file_with_mixed_content():
    """Test file with SQL content that might appear in SSMS exports."""
    content = """USE :ProductionDB:
DECLARE @param INT = 1
SET @param = 2
GO

CREATE OR ALTER PROCEDURE dbo.GetCustomers
AS
BEGIN
    SELECT 
        customer_id,
        name,
        email
    FROM customers 
    WHERE active = 1
END
"""
    
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.sql') as f:
        f.write(content.encode('utf-16le'))
        temp_path = f.name
    
    try:
        result = read_text_safely(temp_path, encoding="auto")
        # Should preserve content structure but normalize line endings
        assert "USE :ProductionDB:" in result
        assert "CREATE OR ALTER PROCEDURE" in result
        assert "\r\n" not in result  # Line endings should be normalized
        
    finally:
        Path(temp_path).unlink()
