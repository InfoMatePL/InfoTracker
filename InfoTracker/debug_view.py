#!/usr/bin/env python3

import sys
sys.path.insert(0, '/home/pawel/projects/praktyki2/InfoTracker/src')

from infotracker.parser import SqlParser

sql = '''CREATE VIEW dbo.stg_orders AS
SELECT
    o.OrderID,
    o.CustomerID,
    CAST(o.OrderDate AS DATE) AS OrderDate,
    CASE WHEN o.OrderStatus IN ('shipped', 'delivered') THEN 1 ELSE 0 END AS IsFulfilled
FROM dbo.Orders AS o;'''

parser = SqlParser()
try:
    obj_info = parser.parse_sql_file(sql, '10_stg_orders')
    print('Success!')
    print('Object name:', obj_info.name)
    print('Schema name:', obj_info.schema.name)
    print('Dependencies:', obj_info.dependencies)
    print('Lineage count:', len(obj_info.lineage))
    for lineage in obj_info.lineage:
        print(f'  {lineage.output_column}: {lineage.transformation_type.value} from {[str(inp) for inp in lineage.input_fields]}')
except Exception as e:
    print('Error:', e)
    import traceback
    traceback.print_exc()
