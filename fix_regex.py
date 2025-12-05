#!/usr/bin/env python3
"""Fix regex pattern in procedures.py to capture table name instead of alias"""

with open('src/infotracker/parser_modules/procedures.py', 'r') as f:
    content = f.read()

# The old regex pattern from line 1257 and 1391
old_pattern = r"r'(?:LEFT\s+|RIGHT\s+|INNER\s+|OUTER\s+|CROSS\s+|FULL\s+)?JOIN\s+([\w.#@]+)'"

# New pattern that captures table name BEFORE optional alias
# Uses lookahead to ensure we're followed by ON or WHERE or comma or JOIN
new_pattern = r"r'(?:LEFT\s+|RIGHT\s+|INNER\s+|OUTER\s+|CROSS\s+|FULL\s+)?JOIN\s+([#\w.]+)(?:\s+(?:AS\s+)?[#\w.]+)?(?=\s+ON|\s+WHERE|\s+,|\s+JOIN|\s+;|\s*$)'"

print(f"Searching for pattern: {old_pattern}")
print(f"Will replace with: {new_pattern}")

count = content.count(old_pattern)
print(f"Found {count} occurrences")

if count > 0:
    content = content.replace(old_pattern, new_pattern)
    with open('src/infotracker/parser_modules/procedures.py', 'w') as f:
        f.write(content)
    print(f"✓ Patched {count} occurrences successfully")
else:
    print("✗ Pattern not found! Checking file content...")
    # Print the exact strings around line 1257
    lines = content.split('\n')
    for i in range(1255, 1260):
        if i < len(lines):
            print(f"Line {i+1}: {lines[i][:100]}")
