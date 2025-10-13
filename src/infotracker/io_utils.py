"""
I/O utilities for safe text file reading with encoding detection.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional, List

logger = logging.getLogger(__name__)

# Common encodings to try in fallback order
COMMON_ENCODINGS = [
    'utf-8',
    'utf-8-sig',
    'utf-16le', 
    'utf-16be',
    'cp1250'
]

# Regexes to strip ANSI escape sequences and BiDi control characters
import re
_ANSI_ESC_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_BIDI_CTRL_RE = re.compile(r"[\u200E\u200F\u202A-\u202E\u2066-\u2069]")

def _strip_ansi_bidi(s: str) -> str:
    """Remove ANSI escape sequences and Unicode BiDi control chars from text."""
    if not s:
        return s
    s = _ANSI_ESC_RE.sub('', s)
    s = _BIDI_CTRL_RE.sub('', s)
    return s


def read_text_safely(path: str | Path, encoding: str = "auto") -> str:
    """
    Safely read text file with encoding detection.
    
    Args:
        path: Path to the file to read
        encoding: Encoding to use. If "auto", will attempt to detect encoding.
                 Supported: "auto", "utf-8", "utf-8-sig", "utf-16", "utf-16le", "utf-16be", "cp1250"
    
    Returns:
        File content as string with normalized line endings
        
    Raises:
        UnicodeDecodeError: If file cannot be decoded with specified/detected encoding
        FileNotFoundError: If file doesn't exist
        IOError: If file cannot be read
    """
    file_path = Path(path)
    
    try:
        # Read file as binary first
        with open(file_path, 'rb') as f:
            raw_content = f.read()
    except Exception as e:
        raise IOError(f"Cannot read file {file_path}: {e}")
    
    if not raw_content:
        return ""
    
    if encoding != "auto":
        # If user forced non-UTF-8 but bytes look like UTF-8, fail early with a clear message
        if encoding.lower() not in ("utf-8", "utf-8-sig") and _looks_like_utf8(raw_content):
            raise UnicodeDecodeError(
                encoding, raw_content, 0, len(raw_content),
                f"File {file_path} appears to be UTF-8 but '{encoding}' was forced. "
                f"Try --encoding auto or --encoding utf-8."
            )
        # Use specified encoding
        # 1) Dekodowanie – łap wyłącznie błędy DEKODOWANIA
        try:
            content = raw_content.decode(encoding, errors="strict")
        except UnicodeDecodeError as e:
            raise UnicodeDecodeError(
                encoding, raw_content, e.start, e.end,
                f"Cannot decode {file_path} with {encoding}. "
                f"Try --encoding auto or specify different encoding (e.g., --encoding cp1250)"
            )
        # 2) Walidacja – POZA try/except, żeby nie nadpisać komunikatu "looks malformed"
        _validate_forced_encoding(raw_content, encoding, content, file_path)
        logger.debug(f"Successfully read {file_path} with encoding {encoding}")
    else:
        # Auto-detect encoding
        content = _detect_and_decode(raw_content, file_path)
    
    # Normalize line endings and remove BOM artifacts
    content = _normalize_content(content)
    
    return content


def _detect_and_decode(raw_content: bytes, file_path: Path) -> str:
    """Smarter auto-detection: evaluate multiple encodings and pick best scored candidate.

    Score = prior (hint strength) + text_quality + SQL bonus (+ small tie-breakers).
    """
    candidates: list[tuple[str, str, float]] = []  # (encoding, text, prior)
    have_known = False

    # 1) BOM (strong signal)
    bom = _detect_bom(raw_content)
    if bom:
        try:
            txt = raw_content.decode(bom, errors="strict")
            candidates.append((bom, txt, 2.0))
            have_known = True
        except UnicodeDecodeError:
            pass

    # 2) Heuristic UTF-16 by NUL distribution
    utf16_guess = _looks_like_utf16(raw_content)
    if utf16_guess:
        try:
            txt = raw_content.decode(utf16_guess, errors="strict")
            candidates.append((utf16_guess, txt, 0.5))
            have_known = True
        except UnicodeDecodeError:
            pass

    # 3) Try common encodings (unordered, we will rank)
    for enc in COMMON_ENCODINGS:
        try:
            txt = raw_content.decode(enc, errors="strict")
            candidates.append((enc, txt, 0.0))
            have_known = True
        except UnicodeDecodeError:
            continue

    # 4) charset-normalizer (if available)
    # Only try charset-normalizer if no known candidates succeeded
    if not have_known:
        try:
            import charset_normalizer
            result = charset_normalizer.from_bytes(raw_content)
            if result and result.best():
                enc = result.best().encoding or "unknown"
                txt = str(result.best())
                candidates.append((enc, txt, 0.1))
        except Exception:
            pass

    if not candidates:
        raise UnicodeDecodeError(
            "auto-detect", raw_content, 0, len(raw_content),
            f"Cannot decode {file_path} with any known encoding. Try --encoding cp1250 or --encoding utf-16."
        )

    # 5) Score candidates
    best = None
    best_score = -1.0
    for enc, txt, prior in candidates:
        q = _text_quality_score(txt)
        sqlish = _looks_like_sql(txt)
        score = prior + q + (1.0 if sqlish else 0.0)
        # tie-breakers / preferences
        if enc in ("utf-8", "utf-8-sig"):
            score += 0.05
        if enc.lower() in ("cp1250", "windows-1250", "iso-8859-2"):
            score += 0.03
        if q < 0.85 and not sqlish:
            score -= 0.15
        if score > best_score:
            best = (enc, txt, q, sqlish, score)
            best_score = score

    chosen_enc, chosen_txt, q, sqlish, score = best  # type: ignore[misc]
    logger.debug(
        "Auto-picked %s for %s (score=%.2f, quality=%.2f, sqlish=%s)",
        chosen_enc, file_path, score, q, sqlish,
    )
    return chosen_txt


def _looks_like_utf16(raw: bytes) -> Optional[str]:
    # Heurystyka: jeśli >20% bajtów to NUL, to prawie na pewno UTF-16.
    if not raw:
        return None
    null_count = raw.count(0)
    if null_count / len(raw) < 0.20:
        return None

    even_nulls = sum(1 for i in range(0, len(raw), 2) if raw[i] == 0)
    odd_nulls  = sum(1 for i in range(1, len(raw), 2) if raw[i] == 0)

    # Jeśli wyraźna przewaga po jednej stronie – wybierz endian
    if even_nulls > odd_nulls * 1.5:
        return "utf-16be"
    if odd_nulls > even_nulls * 1.5:
        return "utf-16le"
    return None

def _looks_like_utf8(raw: bytes) -> bool:
    """
    Check if bytes look like UTF-8 encoded text with non-ASCII characters.
    
    Args:
        raw: Raw bytes to check
        
    Returns:
        True if bytes strictly decode as UTF-8 and contain non-ASCII chars
    """
    if not raw:
        return False
    
    try:
        decoded = raw.decode('utf-8', errors='strict')
        # Check if it contains non-ASCII characters (indicating it's likely UTF-8)
        return any(ord(c) > 127 for c in decoded)
    except UnicodeDecodeError:
        return False


def _text_quality_score(s: str) -> float:
    """
    Calculate text quality score based on printable/whitespace character ratio.
    
    Args:
        s: Text string to analyze
        
    Returns:
        Score from 0.0 to 1.0, where 1.0 means all characters are printable/whitespace
    """
    if not s:
        return 1.0
    
    printable_count = sum(1 for c in s if c.isprintable() or c.isspace())
    return printable_count / len(s)


def _looks_like_sql(s: str) -> bool:
    """
    Check if text contains common SQL tokens.
    
    Args:
        s: Text string to check
        
    Returns:
        True if text contains SQL-like tokens
    """
    import re
    
    sql_tokens = [
        r'\bSELECT\b', r'\bFROM\b', r'\bCREATE\b', r'\bTABLE\b', 
        r'\bVIEW\b', r'\bWHERE\b', r'\bJOIN\b', r'\bINSERT\b', 
        r'\bINTO\b', r'\bEXEC\b', r'\bPROCEDURE\b', r'\bFUNCTION\b',
        r'\bALTER\b', r'\bUPDATE\b', r'\bDELETE\b'
    ]
    
    # Check if any SQL tokens are present (case-insensitive)
    text_upper = s.upper()
    return any(re.search(token, text_upper) for token in sql_tokens)


def _validate_forced_encoding(raw: bytes, forced: str, decoded: str, file_path: Path):
    """
    Validate that forced encoding makes sense for the given content.
    
    Args:
        raw: Raw file bytes
        forced: Forced encoding name
        decoded: Decoded text content
        file_path: File path for error messages
        
    Raises:
        UnicodeDecodeError: If forced encoding appears to be wrong
    """
    # If forced encoding is not UTF-8 but file looks like UTF-8, warn user
    if forced.lower() not in ['utf-8', 'utf-8-sig'] and _looks_like_utf8(raw):
        raise UnicodeDecodeError(
            forced, raw, 0, len(raw),
            f"File {file_path} appears to be UTF-8 but '{forced}' was forced. "
            f"Try --encoding auto or --encoding utf-8."
        )
    
    # Check text quality and SQL-like content
    quality_score = _text_quality_score(decoded)
    has_sql_tokens = _looks_like_sql(decoded)
    
    # If quality is poor and no SQL tokens found, likely wrong encoding
    if quality_score < 0.90 and not has_sql_tokens:
        raise UnicodeDecodeError(
            forced, raw, 0, len(raw),
            f"Decoded text with '{forced}' looks malformed (quality={quality_score:.2f}). "
            f"Try --encoding auto."
        )


def _detect_bom(raw_content: bytes) -> Optional[str]:
    """
    Detect BOM (Byte Order Mark) and return appropriate encoding.
    
    Args:
        raw_content: Raw file bytes
        
    Returns:
        Encoding name if BOM detected, None otherwise
    """
    if raw_content.startswith(b'\xef\xbb\xbf'):
        return 'utf-8-sig'
    elif raw_content.startswith(b'\xff\xfe'):
        # Could be UTF-16 LE or UTF-32 LE, check for UTF-32
        if len(raw_content) >= 4 and raw_content[2:4] == b'\x00\x00':
            return None  # UTF-32 LE, not supported in common encodings
        return 'utf-16le'
    elif raw_content.startswith(b'\xfe\xff'):
        return 'utf-16be'
    elif raw_content.startswith(b'\x00\x00\xfe\xff'):
        return None  # UTF-32 BE, not supported in common encodings
    elif raw_content.startswith(b'\xff\xfe\x00\x00'):
        return None  # UTF-32 LE, not supported in common encodings
    
    return None


def _normalize_content(content: str) -> str:
    """
    Normalize content by fixing line endings and removing BOM artifacts.
    
    Args:
        content: Decoded content string
        
    Returns:
        Normalized content string
    """
    # Strip ANSI/BiDi controls first
    content = _strip_ansi_bidi(content)

    # Normalize line endings to \n
    content = content.replace('\r\n', '\n').replace('\r', '\n')
    
    # Remove BOM character if present (shouldn't happen with utf-8-sig but just in case)
    if content.startswith('\ufeff'):
        content = content[1:]
    
    return content


def get_supported_encodings() -> List[str]:
    """Get list of supported encodings."""
    return ["auto"] + COMMON_ENCODINGS
