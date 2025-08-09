### Adapters and extensibility

#### Forge your adapter (smithing for data heroes)
In the forge of Integration Keep, you’ll craft adapters that turn raw SQL into neatly qualified lineage. Sparks may fly; that’s normal.

- Materials: `parse`, `qualify`, `resolve`, `to_openlineage`
- Armor enchantments: case-normalization, bracket taming, and dialect charms
- Future artifacts: Snowflake blade, BigQuery bow, Postgres shield

If an imp named “Case Insensitivity” throws a tantrum, feed it brackets: `[like_this]`.

#### Audience & prerequisites
- Audience: engineers implementing or extending dialect adapters (Level 2: After basics)
- Prerequisites: Python; SQL basics; familiarity with SQLGlot or similar parser

Define an adapter interface:
- parse(sql) → AST
- qualify(ast) → fully qualified refs (db.schema.object)
- resolve(ast, catalog) → output schema + expressions
- to_openlineage(object) → columnLineage facet

MS SQL adapter (first):
- Use `SQLGlot`/`sqllineage` for parsing/lineage hints
- Handle T-SQL specifics: temp tables, SELECT INTO, variables, functions
- Normalize identifiers (brackets vs quotes), case-insensitivity

Future adapters: Snowflake, BigQuery, Postgres, etc. 

### Adapter interface (pseudocode)
```python
class Adapter(Protocol):
    name: str
    dialect: str

    def parse(self, sql: str) -> AST: ...
    def qualify(self, ast: AST, default_db: str | None) -> AST: ...
    def resolve(self, ast: AST, catalog: Catalog) -> tuple[Schema, ColumnLineage]: ...
    def to_openlineage(self, obj_name: str, schema: Schema, lineage: ColumnLineage) -> dict: ...
```

### MS SQL specifics
- Case-insensitive identifiers; bracket quoting `[name]`
- Temp tables (`#t`) live in tempdb; scope to procedure; support SELECT INTO schema inference
- Variables (`@v`) and their use in filters/windows; capture expressions for context
- GETDATE/DATEADD and common built-ins; treat as CONSTANT/ARITHMETIC transformations
- JOINs default to INNER; OUTER joins affect nullability
- Parser: prefer SQLGlot for AST; use sqllineage as an optional hint only

### Mini example (very small, illustrative)
```python
class MssqlAdapter(Adapter):
    name = "mssql"
    dialect = "tsql"

    def parse(self, sql: str) -> AST:
        return sqlglot.parse_one(sql, read=self.dialect)

    def qualify(self, ast: AST, default_db: str | None) -> AST:
        # apply name normalization and database/schema defaults
        return qualify_identifiers(ast, default_db)

    def resolve(self, ast: AST, catalog: Catalog) -> tuple[Schema, ColumnLineage]:
        schema = infer_schema(ast, catalog)
        lineage = extract_column_lineage(ast, catalog)
        return schema, lineage

    def to_openlineage(self, obj_name: str, schema: Schema, lineage: ColumnLineage) -> dict:
        return build_openlineage_payload(obj_name, schema, lineage)
```

### How to Test Your Adapter
Create a test.py:
```python
adapter = MssqlAdapter()
ast = adapter.parse("SELECT * FROM table")
print(ast)
```
// Run: python test.py

### Adding a new adapter
1. Implement the interface; configure SQLGlot dialect
2. Provide normalization rules (case, quoting, name resolution)
3. Add adapter-specific tests using a small example corpus
4. Document limitations and differences

### Adapter testing template
- Create 3 SQL files: simple select, join with alias, aggregation with group by
- Write expected schema (columns, types, nullability)
- Write expected lineage (inputs per output column)
- Run extraction and compare to expected JSON in CI 

### Adapter selection and registry
```python
ADAPTERS: dict[str, Adapter] = {
    "mssql": MssqlAdapter(),
}

def get_adapter(name: str) -> Adapter:
    return ADAPTERS[name]
```

### Catalog handling
- Accept a `catalog.yml` with known schemas for external refs
- Use catalog to resolve `*`, disambiguate references, and provide types when DDL is missing
- Warn on unknown objects; continue best-effort

### Common pitfalls
- Case-insensitive matching; normalize but preserve display casing
- Bracket/quoted identifiers: `[Name]` vs `"Name"`
- Temp table scoping and lifetime
- SELECT INTO column ordinals and inferred types
- Variables used in expressions and filters

### See also
- `docs/algorithm.md`
- `docs/cli_usage.md`
- `docs/overview.md` 