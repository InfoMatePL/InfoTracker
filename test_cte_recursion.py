from infotracker.parser import TSQLParser
import logging
import sys

# Setup logging to see DEBUG messages
logging.basicConfig(
    level=logging.DEBUG,
    format='%(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

sql = '''
WITH AccountBalance AS (
    SELECT col1, col2 FROM dbo.Table1
),
CumulativesCalculated AS (
    SELECT col1, col2 FROM AccountBalance
)
SELECT col1, col2 INTO #temp FROM CumulativesCalculated
'''

print("=" * 80)
print("Testing CTE recursion")
print("=" * 80)

parser = TSQLParser()
obj = parser.parse_sql_string(sql, object_hint='test')

print("\n" + "=" * 80)
print("Results:")
print("=" * 80)
print(f'Dependencies: {obj.dependencies}')
print(f'CTE registry: {list(parser.cte_registry.keys())}')
print(f'Lineage count: {len(obj.lineage) if obj.lineage else 0}')
if obj.lineage:
    for lin in obj.lineage[:3]:
        print(f'  - {lin.output_column}: {len(lin.input_fields)} input fields')
        for inp in lin.input_fields[:2]:
            print(f'    - {inp.table_name}.{inp.column_name}')
