from __future__ import annotations

from typing import Set, Optional

from sqlglot import expressions as exp
import re
from typing import Set


def _extract_dependencies(self, stmt: exp.Expression) -> Set[str]:
    """Extract dependencies from a Select/Union expression.

    Enhancements:
    - Recurse into Subquery nodes robustly
    - Handle wrapped Select under Subquery directly
    - Include table-valued function calls appearing in FROM/JOIN (heuristic)
    """
    deps: Set[str] = set()
    if isinstance(stmt, exp.Union):
        if isinstance(stmt.left, (exp.Select, exp.Union)):
            deps.update(_extract_dependencies(self, stmt.left))
        if isinstance(stmt.right, (exp.Select, exp.Union)):
            deps.update(_extract_dependencies(self, stmt.right))
        return deps
    if not isinstance(stmt, exp.Select):
        # Allow a Subquery wrapper to be passed accidentally
        if isinstance(stmt, exp.Subquery) and isinstance(stmt.this, exp.Select):
            return _extract_dependencies(self, stmt.this)
        return deps

    select_stmt = stmt
    self._process_ctes(select_stmt)

    for table in select_stmt.find_all(exp.Table):
        table_name = self._get_table_name(table)
        if table_name == "unknown":
            continue
        try:
            if getattr(table, 'catalog', None) and self.registry:
                cat = str(table.catalog).strip('[]')
                if cat and cat.lower() not in {"view", "function", "procedure", "tempdb"}:
                    sch = str(table.db) if getattr(table, 'db', None) else 'dbo'
                    nm = f"{sch}.{table.name}"
                    self.registry.learn_from_references(nm, cat)
        except Exception:
            pass
        simple_name = table_name.split('.')[-1]
        if simple_name in self.cte_registry:
            with_clause = select_stmt.args.get('with')
            if with_clause and hasattr(with_clause, 'expressions'):
                for cte in with_clause.expressions:
                    if hasattr(cte, 'alias') and str(cte.alias) == simple_name:
                        if isinstance(cte.this, exp.Select):
                            cte_deps = _extract_dependencies(self, cte.this)
                            deps.update(cte_deps)
                        break
        else:
            deps.add(table_name)

    # Recurse into scalar / lateral subqueries
    for subquery in select_stmt.find_all(exp.Subquery):
        try:
            inner = subquery.this
            if isinstance(inner, exp.Select):
                deps.update(_extract_dependencies(self, inner))
        except Exception:
            continue

    # Heuristic: table-valued function calls may appear as Anonymous/Func in FROM/JOIN context
    for func in select_stmt.find_all(exp.Anonymous):
        try:
            parent = func.parent
            if isinstance(parent, (exp.From, exp.Join, exp.Lateral)):
                fname = getattr(func, 'name', None)
                if fname:
                    low = fname.lower()
                    if low not in {"getdate","sysdatetime","row_number","count","sum","min","max","avg","cast","convert"}:
                        deps.add(self._get_full_table_name(fname))
        except Exception:
            continue
    return deps


def _expand_dependency_to_base_tables(self, dep_name: str, context_stmt: exp.Expression) -> Set[str]:
    expanded: Set[str] = set()
    simple_name = dep_name.split('.')[-1]
    if simple_name in self.cte_registry:
        if isinstance(context_stmt, exp.Select) and context_stmt.args.get('with'):
            with_clause = context_stmt.args.get('with')
            if hasattr(with_clause, 'expressions'):
                for cte in with_clause.expressions:
                    if hasattr(cte, 'alias') and str(cte.alias) == simple_name:
                        if isinstance(cte.this, exp.Select):
                            cte_deps = _extract_dependencies(self, cte.this)
                            for cte_dep in cte_deps:
                                expanded.update(_expand_dependency_to_base_tables(self, cte_dep, cte.this))
                        break
        return expanded

    if simple_name in self.temp_registry:
        expanded.add(dep_name)
        return expanded

    expanded.add(dep_name)
    return expanded


def _is_cte_reference(self, dep_name: str) -> bool:
    simple_name = dep_name.split('.')[-1]
    return simple_name in self.cte_registry


def _extract_basic_dependencies(self, sql_content: str) -> Set[str]:
    """Basic extraction of table dependencies using string patterns (FROM/JOIN/etc.)."""
    dependencies: Set[str] = set()

    cleaned_sql = re.sub(r'--.*?(?=\n|$)', '', sql_content, flags=re.MULTILINE)
    cleaned_sql = re.sub(r'/\*.*?\*/', '', cleaned_sql, flags=re.DOTALL)

    from_pattern = r'FROM\s+([^\s\(\),]+(?:\.[^\s\(\),]+)*)'
    join_pattern = r'JOIN\s+([^\s\(\),]+(?:\.[^\s\(\),]+)*)'
    update_pattern = r'UPDATE\s+([^\s\(\),]+(?:\.[^\s\(\),]+)*)'
    delete_from_pattern = r'DELETE\s+FROM\s+([^\s\(\),]+(?:\.[^\s\(\),]+)*)'
    merge_into_pattern = r'MERGE\s+INTO\s+([^\s\(\),]+(?:\.[^\s\(\),]+)*)'

    sql_keywords = {
        'select','from','join','on','where','group','having','order','into',
        'update','delete','merge','as','and','or','not','case','when','then','else','set',
        'distinct','top','with','nolock','commit','rollback','transaction','begin','try','catch','exists'
    }
    builtin_functions = {
        'getdate','sysdatetime','xact_state','row_number','count','sum','min','max','avg',
        'cast','convert','try_convert','coalesce','isnull','iif','len','substring','replace',
        'upper','lower','ltrim','rtrim','trim','dateadd','datediff','format','hashbytes','md5'
    }
    sql_types = {
        'varchar','nvarchar','char','nchar','text','ntext',
        'int','bigint','smallint','tinyint','numeric','decimal','money','smallmoney','float','real',
        'bit','binary','varbinary','image',
        'datetime','datetime2','smalldatetime','date','time','datetimeoffset',
        'uniqueidentifier','xml','cursor','table'
    }

    matches = []
    for pat in (from_pattern, join_pattern, update_pattern, delete_from_pattern, merge_into_pattern):
        matches.extend(re.findall(pat, cleaned_sql, re.IGNORECASE))

    insert_pattern = r'INSERT\s+INTO\s+([^\s\(\),]+(?:\.[^\s\(\),]+)*)'
    create_pattern = r'CREATE\s+(?:OR\s+ALTER\s+)?(?:TABLE|VIEW|PROCEDURE|FUNCTION)\s+([^\s\(\),]+(?:\.[^\s\(\),]+)*)'
    select_into_pattern = r'INTO\s+([^\s\(\),]+(?:\.[^\s\(\),]+)*)'

    insert_targets = set()
    for match in re.findall(insert_pattern, cleaned_sql, re.IGNORECASE):
        table_name = self._normalize_table_ident(match.strip())
        if not table_name.startswith('#'):
            full_name = self._get_full_table_name(table_name)
            parts = full_name.split('.')
            if len(parts) >= 2:
                simplified = f"{parts[-2]}.{parts[-1]}"
                insert_targets.add(simplified)
    for match in re.finditer(create_pattern, cleaned_sql, re.IGNORECASE):
        table_name = self._normalize_table_ident(match.group(1).strip())
        if not table_name.startswith('#'):
            full_name = self._get_full_table_name(table_name)
            parts = full_name.split('.')
            if len(parts) >= 2:
                simplified = f"{parts[-2]}.{parts[-1]}"
                insert_targets.add(simplified)
    for match in re.findall(select_into_pattern, cleaned_sql, re.IGNORECASE):
        table_name = self._normalize_table_ident(match.strip())
        if not table_name.startswith('#'):
            full_name = self._get_full_table_name(table_name)
            parts = full_name.split('.')
            if len(parts) >= 2:
                simplified = f"{parts[-2]}.{parts[-1]}"
                insert_targets.add(simplified)

    for match in matches:
        table_name = match.strip()
        if re.search(r'\w+\s*\(', table_name):
            continue
        if not table_name:
            continue
        if table_name.lower() in sql_keywords or table_name.lower() in builtin_functions or table_name.lower() in sql_types:
            continue
        if ' AS ' in table_name.upper():
            table_name = table_name.split(' AS ')[0].strip()
        elif ' ' in table_name and not '.' in table_name.split()[-1]:
            table_name = table_name.split()[0]
        table_name = self._normalize_table_ident(table_name)
        if table_name.startswith('@') or '+' in table_name or (table_name.startswith('[') and table_name.endswith(']') and '.' not in table_name):
            continue
        if not table_name.startswith('#'):
            full_name = self._get_full_table_name(table_name)
            from ..openlineage_utils import sanitize_name
            full_name = sanitize_name(full_name)
            parts = full_name.split('.')
            if len(parts) >= 3:
                qualified_name = full_name
            elif len(parts) == 2:
                db_to_use = self.current_database or self.default_database or "InfoTrackerDW"
                qualified_name = f"{db_to_use}.{full_name}"
            else:
                db_to_use = self.current_database or self.default_database or "InfoTrackerDW"
                qualified_name = f"{db_to_use}.dbo.{table_name}"
            check_parts = qualified_name.split('.')
            if len(check_parts) >= 2:
                simplified_for_check = f"{check_parts[-2]}.{check_parts[-1]}"
                if simplified_for_check not in insert_targets:
                    dependencies.add(qualified_name)
    return dependencies
