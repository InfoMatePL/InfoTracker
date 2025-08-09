### Adapters and extensibility

#### Forge your adapter (smithing for data heroes)
In the forge of Integration Keep, you’ll craft adapters that turn raw SQL into neatly qualified lineage. Sparks may fly; that’s normal.

- Materials: `parse`, `qualify`, `resolve`, `to_openlineage`
- Armor enchantments: case-normalization, bracket taming, and dialect charms
- Future artifacts: Snowflake blade, BigQuery bow, Postgres shield

If an imp named “Case Insensitivity” throws a tantrum, feed it brackets: `[like_this]`.

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

### Adding a new adapter
1. Implement the interface; configure SQLGlot dialect
2. Provide normalization rules (case, quoting, name resolution)
3. Add adapter-specific tests using a small example corpus
4. Document limitations and differences 