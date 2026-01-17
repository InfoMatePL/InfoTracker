import sys
sys.path.insert(0, 'src')

from infotracker.parser import SqlParser
from pathlib import Path

parser = SqlParser(dialect='tsql')
sql = Path('build/PROD/EDW_CORE/StoredProcedures/StoredProcedure.dbo.update_stage_mis_LeadTime.sql').read_text(encoding='windows-1250')
result = parser.parse_sql_file(sql, 'update_stage_mis_LeadTime')

step3_cols = parser.temp_registry.get('#LeadTime_STEP3', [])
step4_cols = parser.temp_registry.get('#LeadTime_STEP4', [])

print(f'STEP3 has {len(step3_cols)} columns')
print(f'STEP4 has {len(step4_cols)} columns (expected: {len(step3_cols)}+4)')
print()
print('New columns in STEP4 (not in STEP3):')
new_cols = [col for col in step4_cols if col not in step3_cols]
for col in new_cols:
    print(f'  - {col}')
