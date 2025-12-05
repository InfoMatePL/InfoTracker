import sqlglot
from sqlglot import expressions as exp

# Test 1: ;WITH syntax - SKIP (known to fail)
print("Test 1 - ;WITH syntax: SKIPPED (known to fail)\n")

# Test 2: WITH syntax (no semicolon)
sql2 = "WITH CTE AS (SELECT 1 AS x) SELECT * FROM CTE"
parsed2 = sqlglot.parse_one(sql2, dialect='tsql')
print(f"Test 2 - WITH syntax (no semicolon):")
print(f"  Parsed: {parsed2}")
print(f"  Type: {type(parsed2)}")
if isinstance(parsed2, exp.Select):
    with_clause2 = parsed2.args.get('with')
    print(f"  With clause: {with_clause2}")
    if with_clause2 and hasattr(with_clause2, 'expressions'):
        print(f"  CTE count: {len(with_clause2.expressions)}")
else:
    print(f"  Not a Select expression!")
print()

# Test 3: Multi-statement with ;WITH
sql3 = """
INSERT INTO #temp VALUES (1);
;WITH CTE AS (SELECT * FROM #temp)
SELECT * FROM CTE
"""
statements3 = sqlglot.parse(sql3, dialect='tsql')
print(f"Test 3 - Multi-statement with ;WITH:")
print(f"  Statement count: {len(statements3)}")
for i, stmt in enumerate(statements3):
    print(f"  Statement {i}: type={type(stmt)}, sql={str(stmt)[:50]}")
    if isinstance(stmt, exp.Select):
        with_clause = stmt.args.get('with')
        print(f"    With clause: {with_clause}")

# Test 4: FIXED - Remove semicolon before WITH
sql4_broken = ";WITH CTE AS (SELECT 1 AS x) SELECT * FROM CTE"
sql4_fixed = sql4_broken.replace(';WITH', '\nWITH')
print(f"\nTest 4 - FIX: Remove semicolon:")
print(f"  Original: {sql4_broken[:40]}")
print(f"  Fixed: {sql4_fixed[:40]}")
try:
    parsed4 = sqlglot.parse_one(sql4_fixed, dialect='tsql')
    print(f"  Parsed: SUCCESS")
    print(f"  Type: {type(parsed4)}")
    print(f"  SQL repr: {parsed4.sql()[:80]}")
    print(f"  Args keys: {list(parsed4.args.keys())}")
    if isinstance(parsed4, exp.Select):
        with_clause4 = parsed4.args.get('with')
        print(f"  With clause via args.get('with'): {with_clause4}")
        # Try different ways to access WITH
        if hasattr(parsed4, 'ctes'):
            print(f"  parsed4.ctes: {parsed4.ctes}")
        # Print full AST to see structure
        print(f"  Full AST args:")
        for key, value in parsed4.args.items():
            print(f"    {key}: {type(value)} = {str(value)[:50]}")
except Exception as e:
    print(f"  FAILED: {e}")
