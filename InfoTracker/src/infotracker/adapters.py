from __future__ import annotations

import json
import logging
from typing import Protocol, Dict

from .parser import SqlParser
from .lineage import OpenLineageGenerator


logger = logging.getLogger(__name__)


class Adapter(Protocol):
    name: str
    dialect: str

    def extract_lineage(self, sql: str, object_hint: str | None = None) -> str: ...


class MssqlAdapter:
    name = "mssql"
    dialect = "tsql"

    def __init__(self):
        self.parser = SqlParser(dialect=self.dialect)
        self.lineage_generator = OpenLineageGenerator()

    def extract_lineage(self, sql: str, object_hint: str | None = None) -> str:
        """Extract lineage from SQL and return OpenLineage JSON."""
        try:
            # Parse the SQL and extract object information
            obj_info = self.parser.parse_sql_file(sql, object_hint)
            
            # Generate OpenLineage JSON
            job_name = f"warehouse/sql/{object_hint}.sql" if object_hint else None
            return self.lineage_generator.generate(obj_info, job_name=job_name, object_hint=object_hint)
            
        except Exception as exc:
            logger.error(f"Failed to extract lineage from SQL: {exc}")
            
            # Return error payload in OpenLineage format
            error_payload = {
                "eventType": "COMPLETE",
                "eventTime": "2025-01-01T00:00:00Z",
                "run": {"runId": "00000000-0000-0000-0000-000000000000"},
                "job": {
                    "namespace": "infotracker/examples",
                    "name": f"warehouse/sql/{object_hint or 'unknown'}.sql"
                },
                "inputs": [],
                "outputs": [{
                    "namespace": "mssql://localhost/InfoTrackerDW",
                    "name": object_hint or "unknown",
                    "facets": {
                        "schema": {"fields": []},
                        "columnLineage": {"fields": []},
                    }
                }],
                "warnings": [str(exc)]
            }
            return json.dumps(error_payload, indent=2)


_ADAPTERS: Dict[str, Adapter] = {
    "mssql": MssqlAdapter(),
}


def get_adapter(name: str) -> Adapter:
    if name not in _ADAPTERS:
        raise KeyError(f"Unknown adapter '{name}'. Available: {', '.join(_ADAPTERS)}")
    return _ADAPTERS[name]

