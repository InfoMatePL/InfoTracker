"""Debug script to analyze CTE parsing issue in tetafk"""
import sys
sys.path.insert(0, 'src')

from pathlib import Path
import sqlglot
from sqlglot import expressions as exp

# Read tetafk SQL
tetafk_sql = Path('build/input/test2/StoredProcedure.dbo.update_TrialBalance_tetafk_BV.sql')
sql_content = tetafk_sql.read_bytes().decode('latin1', errors='ignore')

# Extract the WITH statement that creates #tetafk_temp
# Find "WITH AccountBalance AS" to "INTO #tetafk_temp"
import re

# Find the second WITH statement (around line 250)
lines = sql_content.split('\n')
with_start = None
for i, line in enumerate(lines):
    if 'WITH AccountBalance AS' in line and i > 200:  # Second WITH
        with_start = i
        break

if not with_start:
    print("Could not find WITH AccountBalance statement")
    sys.exit(1)

# Find INTO #tetafk_temp (end of WITH block)
with_end = None
for i in range(with_start, len(lines)):
    if 'INTO #tetafk_temp' in lines[i]:
        with_end = i
        break

if not with_end:
    print("Could not find INTO #tetafk_temp")
    sys.exit(1)

# Extract the WITH...SELECT statement
with_block = '\n'.join(lines[with_start:with_end+1])

print("="*80)
print("EXTRACTED WITH BLOCK")
print("="*80)
print(f"Lines {with_start+1} to {with_end+1}")
print(f"Total lines: {with_end - with_start + 1}")

# Count CTEs in raw SQL
cte_count = with_block.count(' AS (')
print(f"\nCTE definitions found (counting ' AS ('): {cte_count}")

# Find CTE names
cte_names = []
for match in re.finditer(r'(WITH|,)\s+(\w+)\s+AS\s*\(', with_block, re.IGNORECASE):
    cte_names.append(match.group(2))

print(f"CTE names found: {cte_names}")

# Try to parse with sqlglot
print("\n" + "="*80)
print("SQLGLOT PARSING")
print("="*80)

try:
    # Parse the WITH statement
    parsed = sqlglot.parse_one(with_block, dialect='tsql')
    print(f"Parsed type: {type(parsed).__name__}")
    
    # Check if it's a Select with WITH clause
    if isinstance(parsed, exp.Select):
        with_clause = parsed.args.get('with')
        if with_clause:
            print(f"WITH clause found: {type(with_clause).__name__}")
            if hasattr(with_clause, 'expressions'):
                print(f"CTE expressions in WITH clause: {len(with_clause.expressions)}")
                for i, cte in enumerate(with_clause.expressions):
                    cte_name = str(cte.alias) if hasattr(cte, 'alias') else '???'
                    print(f"  CTE {i+1}: {cte_name}")
            else:
                print("WITH clause has no 'expressions' attribute")
        else:
            print("No WITH clause found in parsed SELECT")
    else:
        print(f"Parsed as {type(parsed).__name__}, not Select")
        
except Exception as e:
    print(f"Parse error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*80)
print("CONCLUSION")
print("="*80)

if len(cte_names) == 3:
    print(f"✓ Raw SQL has 3 CTEs: {cte_names}")
else:
    print(f"⚠ Raw SQL has {len(cte_names)} CTEs: {cte_names}")

# Check if sqlglot parsed all CTEs
try:
    parsed = sqlglot.parse_one(with_block, dialect='tsql')
    if isinstance(parsed, exp.Select):
        with_clause = parsed.args.get('with')
        if with_clause and hasattr(with_clause, 'expressions'):
            sqlglot_count = len(with_clause.expressions)
            if sqlglot_count == len(cte_names):
                print(f"✓ sqlglot parsed all {sqlglot_count} CTEs")
            else:
                print(f"✗ sqlglot only parsed {sqlglot_count} CTEs, expected {len(cte_names)}")
                print("  This is the problem! sqlglot is not parsing all CTEs correctly.")
        else:
            print("✗ sqlglot did not parse WITH clause properly")
except:
    print("✗ sqlglot failed to parse")


