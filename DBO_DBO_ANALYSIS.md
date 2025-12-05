# "dbo.dbo" Pattern Analysis - All Potential Creation Points

## Summary
Searched all Python files in `src/infotracker/` for patterns that could create "dbo.dbo" as a table name. Found multiple locations where this could occur through string concatenation, fallback logic, and _split_fqn behavior.

---

## 1. PRIMARY DETECTION & DIAGNOSTIC CODE

### File: `src/infotracker/parser_modules/select_lineage.py`

**Line 664-666**: DIAGNOSTIC CODE (already detects the issue)
```python
if sch == "dbo" and tbl == "dbo":
    logger.warning(f"DIAGNOSTIC_FINAL: FOUND dbo.dbo! table_fqn={table_fqn}, db={db}, sch={sch}, tbl={tbl}, col_name={col_exp.name}, effective_db={effective_db}")
    logger.debug(f"_append_column_ref: Full context - qual={qual}, is_cte={is_cte}")
```
**Context**: After splitting FQN, code detects when both schema and table are "dbo" and logs warning.

---

## 2. SPLIT_FQN LOGIC - ROOT CAUSE

### File: `src/infotracker/parser_modules/names.py`

**Line 11-17**: `_cached_split_fqn_core` function - DEFAULT "dbo" INJECTION
```python
@lru_cache(maxsize=65536)
def _cached_split_fqn_core(fqn: str):
    parts = (fqn or "").split(".")
    if len(parts) >= 3:
        return parts[0], parts[1], ".".join(parts[2:])
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return None, "dbo", (parts[0] if parts else None)  # <-- DEFAULT "dbo" HERE
```
**Issue**: When FQN has only 1 part (e.g., just a table name), returns `(None, "dbo", table_name)`. If the FQN input is literally "dbo", this returns `(None, "dbo", "dbo")`.

**Line 37-42**: `_split_fqn` wrapper
```python
def _split_fqn(self, fqn: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Split fully qualified name into (db, schema, table) using cached core and context default."""
    db, sch, tbl = _cached_split_fqn_core(fqn)
    if db is None:
        db = self.current_database or self.default_database
    return db, sch, tbl
```
**Usage**: Called from 13+ locations throughout the codebase.

---

## 3. TABLE NAME CONCATENATION - DIRECT USAGES

### File: `src/infotracker/parser_modules/select_lineage.py`

**Line 447**: CTE fallback - creates ColumnReference with `f"{sch}.{tbl}"`
```python
ref = ColumnReference(
    namespace=self._canonical_namespace(effective_db),
    table_name=f"{sch}.{tbl}",  # <-- CONCATENATION
    column_name=col_exp.name,
)
```
**Risk**: If `sch="dbo"` and `tbl="dbo"`, creates `table_name="dbo.dbo"`

**Line 577**: Another CTE fallback with same pattern
```python
ColumnReference(
    namespace=self._canonical_namespace(effective_db),
    table_name=f"{sch}.{tbl}",  # <-- CONCATENATION
    column_name=col_exp.name,
)
```

**Line 660-662**: Main FINAL ColumnReference creation
```python
out_list.append(
    ColumnReference(
        namespace=self._canonical_namespace(effective_db),
        table_name=f"{sch}.{tbl}",  # <-- CONCATENATION (PRIMARY)
        column_name=col_exp.name,
    )
)
```
**Context**: This is where detected "dbo.dbo" is being created.

---

## 4. DEPENDENCIES REGISTRY LEARNING

### File: `src/infotracker/parser_modules/dml.py`

**Line 73**: INSERT INTO parsing - learns dependencies
```python
self.registry.learn_from_targets(f"{sch}.{tbl}", db)
```
**Risk**: If `sch="dbo"` and `tbl="dbo"`, learns `"dbo.dbo"` → db mapping.

**Line 244**: INSERT INTO ... EXEC parsing
```python
self.registry.learn_from_targets(f"{sch}.{tbl}", db)
```

**Line 326**: INSERT INTO ... SELECT parsing
```python
self.registry.learn_from_targets(f"{sch}.{tbl}", db)
```

---

## 5. OBJECT NAME FORMATTING

### File: `src/infotracker/parser_modules/string_fallbacks.py`

**Line 314-319**: String fallback - constructs full_name
```python
if len(parts) == 2:
    db = (self.current_database or self.default_database or "InfoTrackerDW")
    sch, tbl = parts
    full_name = f"{db}.{sch}.{tbl}"
elif:
    db = (self.current_database or self.default_database or "InfoTrackerDW")
    sch = "dbo"
    tbl = parts[0]
    full_name = f"{db}.{sch}.{tbl}"  # <-- CONCATENATION
```
**Risk**: If input is "dbo", creates `full_name="InfoTrackerDW.dbo.dbo"`

---

## 6. FUNCTION LOCATIONS USING _SPLIT_FQN

### File: `src/infotracker/parser_modules/create_handlers.py`

**Line 82-84**: CREATE TABLE handler
```python
db_raw, sch_raw, tbl_raw = self._split_fqn(raw_ident)
if self.registry and db_raw:
    self.registry.learn_from_create("table", f"{sch_raw}.{tbl_raw}", db_raw)  # <-- CONCATENATION
```
**Locations**: 
- Line 82-84 (CREATE TABLE)
- Line 227-229 (CREATE VIEW)
- Line 312-314 (CREATE FUNCTION)

### File: `src/infotracker/parser_modules/functions.py`

**Line 38**: Function definition extraction
```python
db_raw, sch_raw, tbl_raw = self._split_fqn(raw_ident)
```

**Line 68**: Another function context
```python
db_raw, sch_raw, tbl_raw = self._split_fqn(raw_ident)
```

### File: `src/infotracker/parser_modules/procedures.py`

**Line 406**: Procedure handler - learns dependencies
```python
db_raw, sch_raw, tbl_raw = self._split_fqn(raw_ident)
```

**Line 449**: Another procedure context
```python
db_raw, sch_raw, tbl_raw = self._split_fqn(raw_ident)
```

**Line 514**: Procedure body parsing
```python
db_raw, sch_raw, tbl_raw = self._split_fqn(raw_ident)
```

---

## 7. FALLBACK LOGIC WITH "dbo" DEFAULTS

### File: `src/infotracker/parser_modules/names.py`

**Line 110**: _ns_and_name function - dbt mode
```python
nm = f"{self.default_schema or 'dbo'}.{last}"  # <-- "dbo" FALLBACK
```
**Risk**: If `self.default_schema` is "dbo" and `last` is "dbo", creates `"dbo.dbo"`

**Line 131**: Context schema resolution
```python
schema = getattr(self, '_ctx_schema', None) or self.default_schema or "dbo"  # <-- "dbo" DEFAULT
```

**Line 147**: _qualify_table function
```python
sch = getattr(tbl, "db", None) or "dbo"  # <-- "dbo" DEFAULT
```
**Risk**: If `tbl.db` is None/empty and tbl.name is "dbo", creates schema="dbo", name="dbo"

---

## 8. STRING FALLBACK PROCESSING

### File: `src/infotracker/parser_modules/string_fallbacks.py`

**Line 306**: String fallback - splits FQN
```python
db, sch, tbl = self._split_fqn(full_name)
```
**Risk**: If `full_name="dbo"`, results in `(db, "dbo", "dbo")`

**Line 440**: INSERT INTO ... EXEC temp materialization
```python
sch = getattr(self, 'default_schema', None) or "dbo"  # <-- "dbo" DEFAULT
```
**Line 442**: Constructs table_name
```python
table_name = f"{db}.{sch}.{label}.#{temp_name}"  # <-- CONCATENATION
```

---

## 9. REGISTRY RESOLUTION

### File: `src/infotracker/parser_modules/names.py`

**Line 248-261**: _get_full_table_name - Registry fallback
```python
if len(parts) <= 2 and getattr(self, "registry", None):
    weak_defaults = {"infotrackerdb", "infotrackerdw"}
    if db_to_use and str(db_to_use).lower() in weak_defaults:
        if len(parts) == 2:
            schema_table = ".".join(parts)
        elif len(parts) == 1 and parts[0]:
            schema_table = f"dbo.{parts[0]}"  # <-- CONCATENATION
        # ...
        if schema_table:
            resolved_db = self.registry.resolve("table", schema_table, fallback=db_to_use)
```
**Risk**: If parts[0]="dbo", creates `schema_table="dbo.dbo"`

---

## 10. DEPS.PY - DEPENDENCY EXTRACTION

### File: `src/infotracker/parser_modules/deps.py`

**Line 172-188**: Extracts dependencies using _get_full_table_name
```python
full_name = self._get_full_table_name(table_name)
```
**Risk**: Inherited from _get_full_table_name (see #9 above)

---

## 11. OPENLINEAGE NAMESPACE HANDLING

### File: `src/infotracker/lineage.py`

**Line 38-40**: _ns_for_dep function - temp table detection
```python
if "#" in d and "." in d:
    parts = d.split(".")
    if len(parts) >= 3:
        db = parts[0]
        return f"mssql://localhost/{db.upper()}"
```
**Risk**: If d="dbo.dbo#temp", extracts "dbo" as db, but this is actually the schema.

---

## 12. ENGINE TEMP TABLE HANDLING

### File: `src/infotracker/engine.py`

**Line 473**: Phase 3 temp table reconstruction
```python
ns_tmp, table_name = parser._ns_and_name(tmp, obj_type_hint="temp_table")
```
**Risk**: Calls _ns_and_name which could produce "dbo.dbo" format

---

## 13. QUALIFIED TABLE UTILITY

### File: `src/infotracker/parser_modules/names.py`

**Line 296-300**: _qualify_table function
```python
def _qualify_table(self, tbl: exp.Table) -> str:
    name = tbl.name
    sch = getattr(tbl, "db", None) or "dbo"  # <-- "dbo" DEFAULT
    db = getattr(tbl, "catalog", None) or self.current_database or self.default_database
    return ".".join([p for p in [db, sch, name] if p])
```
**Risk**: If `name="dbo"` and `tbl.db=None`, produces `"dbo.dbo"` in result

---

## SUMMARY TABLE - ALL LOCATIONS

| File | Line(s) | Function | Pattern | Risk Level |
|------|---------|----------|---------|-----------|
| select_lineage.py | 664-666 | _append_column_ref | Detection code | N/A (diagnostic) |
| select_lineage.py | 447, 577, 660 | _append_column_ref | `f"{sch}.{tbl}"` ColumnRef | HIGH |
| names.py | 11-17 | _cached_split_fqn_core | `return None, "dbo", parts[0]` | HIGH (root cause) |
| dml.py | 73, 244, 326 | DML handlers | `f"{sch}.{tbl}"` registry | MEDIUM |
| string_fallbacks.py | 306-319 | String fallback | `f"{db}.{sch}.{tbl}"` | MEDIUM |
| string_fallbacks.py | 440, 442 | Temp materialization | `f"{db}.{sch}.{label}"` | MEDIUM |
| create_handlers.py | 82-84, 227-229, 312-314 | CREATE handlers | `f"{sch_raw}.{tbl_raw}"` | MEDIUM |
| names.py | 248-261 | _get_full_table_name | `f"dbo.{parts[0]}"` | MEDIUM |
| names.py | 110 | _ns_and_name | `f"{schema or 'dbo'}.{last}"` | MEDIUM |
| names.py | 131, 147 | Context resolution | `or "dbo"` defaults | MEDIUM |
| names.py | 296-300 | _qualify_table | `or "dbo"` then join | MEDIUM |
| lineage.py | 38-40 | _ns_for_dep | Temp namespace extract | LOW |
| engine.py | 473 | Phase 3 | _ns_and_name call | MEDIUM |
| deps.py | 172-188 | Dependency extraction | _get_full_table_name | MEDIUM |

---

## ROOT CAUSE CHAIN

1. **Input**: FQN string "dbo" (single part)
2. **_cached_split_fqn_core** (Line 11-17): Returns `(None, "dbo", "dbo")`
   - 1-part string gets: schema="dbo" (default), table=parts[0]="dbo"
3. **Concatenation Points**: Multiple locations use `f"{sch}.{tbl}"` to create table_name
4. **Result**: `table_name="dbo.dbo"` appears in ColumnReference objects

---

## RECOMMENDATIONS FOR FIXES

1. **Validate FQN input** before processing (check for single "dbo" part)
2. **Add check in _cached_split_fqn_core** to detect this pattern
3. **Prevent concatenation** of identical schema and table names
4. **Audit all f-string concatenations** using sch/tbl variables
5. **Add unit tests** for single-part identifiers, especially "dbo"
