### OpenLineage mapping

InfoTracker emits OpenLineage-compliant payloads. This document explains how internal graphs map to OpenLineage entities and facets.

#### Datasets
- Dataset `namespace`: adapter name or logical namespace (e.g., `mssql`)
- Dataset `name`: fully qualified object name (e.g., `dbo.fct_sales`)

#### Facets
- `schema`: list of fields with `name` and `type`
- `columnLineage`: per-field lineage, transformation metadata, and inputs

#### Example payload (excerpt)
```json
{
  "namespace": "mssql",
  "name": "dbo.fct_sales",
  "facets": {
    "schema": {
      "fields": [
        {"name": "OrderDate", "type": "DATE"},
        {"name": "Revenue", "type": "DECIMAL(18,2)"}
      ]
    },
    "columnLineage": {
      "fields": [
        {
          "name": "Revenue",
          "transformationType": "AGGREGATION",
          "transformationDescription": "SUM(Quantity * UnitPrice)",
          "inputFields": [
            {"namespace": "mssql", "name": "dbo.stg_order_items", "field": "Quantity"},
            {"namespace": "mssql", "name": "dbo.stg_order_items", "field": "UnitPrice"}
          ]
        }
      ]
    }
  }
}
```

#### Guidelines
- Keep `transformationDescription` concise (AST summary), avoid full SQL
- Preserve display casing from source; normalize internally as needed
- Ensure deterministic field ordering to make diffs stable 