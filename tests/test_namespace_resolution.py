import pathlib
from pathlib import Path

from infotracker.parser import SqlParser
from infotracker.io_utils import read_text_safely
from infotracker.object_db_registry import ObjectDbRegistry

BASE = Path(__file__).resolve().parent.parent
SQL_DIR = BASE / "examples" / "warehouse" / "sql2"
DB_MAP = BASE / "build" / "object_db_map.json"


def _parse(sql_file: Path):
    parser = SqlParser(dialect="tsql")
    # Inject learned object→DB mapping
    parser.registry = ObjectDbRegistry.load(DB_MAP)
    # No explicit default DB to force registry use when DB is missing
    parser.set_default_database(None)
    # Use robust reader (handles BOM and common encodings)
    sql = read_text_safely(sql_file, encoding="auto")
    return parser.parse_sql_file(sql, object_hint=sql_file.stem)


def test_create_table_uses_registry_namespace_leadpartner_ref():
    obj = _parse(SQL_DIR / "Table.dbo.LeadPartner_ref.sql")
    assert obj.schema.name.lower() == "dbo.leadpartner_ref"
    # Expect EDW_CORE from registry mapping
    assert obj.schema.namespace == "mssql://localhost/EDW_CORE"


def test_create_table_uses_registry_namespace_load_camino_partner():
    obj = _parse(SQL_DIR / "Table.dbo.load_camino_partner.sql")
    assert obj.schema.name.lower() == "dbo.load_camino_partner"
    # Expect STG from registry mapping
    assert obj.schema.namespace == "mssql://localhost/STG"


def test_create_table_uses_registry_namespace_leadpartner_sat_ms_mis():
    obj = _parse(SQL_DIR / "Table.dbo.LeadPartner_sat_ms_mis.sql")
    assert obj.schema.name.lower() == "dbo.leadpartner_sat_ms_mis"
    # Expect EDW_CORE from registry mapping
    assert obj.schema.namespace == "mssql://localhost/EDW_CORE"
