from __future__ import annotations

from typing import Optional, List, Set
from sqlglot import expressions as exp

from ..models import ObjectInfo, TableSchema, ColumnSchema, ColumnLineage, ColumnReference, TransformationType


def _is_select_into(self, statement: exp.Select) -> bool:
    return statement.args.get('into') is not None


def _is_insert_exec(self, statement: exp.Insert) -> bool:
    expression = statement.expression
    return (
        hasattr(expression, 'expressions') and 
        expression.expressions and 
        isinstance(expression.expressions[0], exp.Command) and
        str(expression.expressions[0]).upper().startswith('EXEC')
    )


def _parse_select_into(self, statement: exp.Select, object_hint: Optional[str] = None) -> ObjectInfo:
    into_expr = statement.args.get('into')
    if not into_expr:
        raise ValueError("SELECT INTO requires INTO clause")

    raw_target = self._get_table_name(into_expr, object_hint)
    try:
        parts = (raw_target or "").split('.')
        if len(parts) >= 3 and self.registry:
            db, sch, tbl = parts[0], parts[1], ".".join(parts[2:])
            self.registry.learn_from_targets(f"{sch}.{tbl}", db)
    except Exception:
        pass
    ns, nm = self._ns_and_name(raw_target, obj_type_hint="table")
    namespace = ns
    table_name = nm

    dependencies = self._extract_dependencies(statement)
    lineage, output_columns = self._extract_column_lineage(statement, table_name)

    if raw_target and (raw_target.startswith('#') or 'tempdb..#' in str(raw_target)):
        simple_key = self._extract_temp_name(raw_target if '#' in raw_target else '#' + raw_target)
        if not simple_key.startswith('#'):
            simple_key = f"#{simple_key}"
        namespace, table_name = self._ns_and_name(simple_key, obj_type_hint="temp_table")
        temp_cols = [col.name for col in output_columns]
        ver_key = self._temp_next(simple_key)
        self.temp_registry[ver_key] = temp_cols
        self.temp_registry[simple_key] = temp_cols
        base_sources: Set[str] = set()
        for d in dependencies:
            is_dep_temp = ('#' in d or 'tempdb' in d.lower())
            if not is_dep_temp:
                base_sources.add(d)
            else:
                dep_simple = self._extract_temp_name(d) if '#' in d else d.split('.')[-1]
                if not dep_simple.startswith('#'):
                    dep_simple = f"#{dep_simple}"
                dep_bases = self.temp_sources.get(dep_simple, set())
                if dep_bases:
                    base_sources.update(dep_bases)
                else:
                    base_sources.add(d)
        self.temp_sources[simple_key] = base_sources
        try:
            col_map = {lin.output_column: list(lin.input_fields or []) for lin in (lineage or [])}
            self.temp_lineage[ver_key] = col_map
            self.temp_lineage[simple_key] = col_map
        except Exception:
            pass

    final_dependencies: Set[str] = set()
    for d in dependencies:
        is_dep_temp = ('#' in d or 'tempdb' in d.lower())
        if not is_dep_temp:
            final_dependencies.add(d)
        else:
            final_dependencies.add(d)
            dep_simple = self._extract_temp_name(d) if '#' in d else d.split('.')[-1]
            if not dep_simple.startswith('#'):
                dep_simple = f"#{dep_simple}"
            dep_bases = self.temp_sources.get(dep_simple, set())
            if dep_bases:
                final_dependencies.update(dep_bases)

    schema = TableSchema(namespace=namespace, name=table_name, columns=output_columns)
    self.schema_registry.register(schema)

    return ObjectInfo(
        name=table_name,
        object_type="temp_table" if (raw_target and (raw_target.startswith('#') or 'tempdb..#' in raw_target)) else "table",
        schema=schema,
        lineage=lineage,
        dependencies=final_dependencies
    )


def _parse_insert_exec(self, statement: exp.Insert, object_hint: Optional[str] = None) -> ObjectInfo:
    raw_target = self._get_table_name(statement.this, object_hint)
    try:
        parts = (raw_target or "").split('.')
        if len(parts) >= 3 and self.registry:
            db, sch, tbl = parts[0], parts[1], ".".join(parts[2:])
            self.registry.learn_from_targets(f"{sch}.{tbl}", db)
    except Exception:
        pass
    ns, nm = self._ns_and_name(raw_target, obj_type_hint="table")
    namespace = ns
    table_name = nm

    expression = statement.expression
    if hasattr(expression, 'expressions') and expression.expressions:
        exec_command = expression.expressions[0]
        dependencies = set()
        procedure_name = None
        exec_text = str(exec_command)
        if exec_text.upper().startswith('EXEC'):
            parts = exec_text.split()
            if len(parts) > 1:
                raw_proc_name = self._clean_proc_name(parts[1])
                procedure_name = self._get_full_table_name(raw_proc_name)
                dependencies.add(procedure_name)
        target_columns: List[ColumnSchema] = []
        try:
            cols_arg = statement.args.get('columns') if hasattr(statement, 'args') else None
            if cols_arg:
                for i, c in enumerate(cols_arg or []):
                    name = None
                    if hasattr(c, 'name') and getattr(c, 'name'):
                        name = str(getattr(c, 'name'))
                    elif hasattr(c, 'this'):
                        name = str(getattr(c, 'this'))
                    else:
                        name = str(c)
                        if name:
                            target_columns.append(ColumnSchema(name=str(name).strip('[]'), data_type="unknown", ordinal=i, nullable=True))
        except Exception:
            target_columns = []
        output_columns = target_columns or [
            ColumnSchema(name="output_col_1", data_type="unknown", ordinal=0, nullable=True),
            ColumnSchema(name="output_col_2", data_type="unknown", ordinal=1, nullable=True),
        ]
        if raw_target and (str(raw_target).startswith('#') or 'tempdb..#' in str(raw_target)):
                # Canonical simple temp key (e.g., '#temp')
                simple_key = (str(raw_target).split('.')[-1] if '.' in str(raw_target) else str(raw_target))
                if not simple_key.startswith('#'):
                    simple_key = f"#{simple_key}"
                # Output naming for temp materialization: DB.schema.<object_hint>.#temp
                db = self.current_database or self.default_database or "InfoTrackerDW"
                sch = getattr(self, 'default_schema', None) or "dbo"
                label = (object_hint or "object")
                table_name = f"{db}.{sch}.{label}.{simple_key}"
                namespace = self._canonical_namespace(db)
        lineage = []
        if procedure_name:
            ns_proc, nm_proc = self._ns_and_name(procedure_name)
            for i, col in enumerate(output_columns):
                input_col = col.name if target_columns else "*"
                lineage.append(ColumnLineage(
                    output_column=col.name,
                    input_fields=[ColumnReference(namespace=ns_proc, table_name=nm_proc, column_name=input_col)],
                    transformation_type=TransformationType.EXEC,
                    transformation_description=f"INSERT INTO {table_name} EXEC {nm_proc}"
                ))
        schema = TableSchema(namespace=namespace, name=table_name, columns=output_columns)
        self.schema_registry.register(schema)
        return ObjectInfo(
            name=table_name,
            object_type="temp_table" if (raw_target and (str(raw_target).startswith('#') or 'tempdb..#' in str(raw_target))) else "table",
            schema=schema,
            lineage=lineage,
            dependencies=dependencies
        )
    raise ValueError("Could not parse INSERT INTO ... EXEC statement")


def _parse_insert_select(self, statement: exp.Insert, object_hint: Optional[str] = None) -> Optional[ObjectInfo]:
    from ..openlineage_utils import sanitize_name
    raw_target = self._get_table_name(statement.this, object_hint)
    try:
        parts = (raw_target or "").split('.')
        if len(parts) >= 3 and self.registry:
            db, sch, tbl = parts[0], parts[1], ".".join(parts[2:])
            self.registry.learn_from_targets(f"{sch}.{tbl}", db)
    except Exception:
        pass
    ns, nm = self._ns_and_name(raw_target, obj_type_hint="table")
    namespace = ns
    table_name = nm
    select_expr = statement.expression
    if not isinstance(select_expr, exp.Select):
        return None
    dependencies = self._extract_dependencies(select_expr)
    lineage, output_columns = self._extract_column_lineage(select_expr, table_name)
    table_name = sanitize_name(table_name)
    raw_is_temp = bool(raw_target and (str(raw_target).startswith('#') or 'tempdb' in str(raw_target)))
    if raw_is_temp:
        simple_name = (str(raw_target).split('.')[-1] if '.' in str(raw_target) else str(raw_target))
        if not simple_name.startswith('#'):
            simple_name = f"#{simple_name}"
        namespace, table_name = self._ns_and_name(simple_name, obj_type_hint="temp_table")
        temp_cols = [col.name for col in output_columns]
        self.temp_registry[simple_name] = temp_cols
        base_sources: Set[str] = set()
        for d in dependencies:
            is_dep_temp = ('#' in d or 'tempdb' in d.lower())
            if not is_dep_temp:
                base_sources.add(d)
            else:
                dep_simple = (d.split('.')[-1] if '.' in d else d)
                if not dep_simple.startswith('#'):
                    dep_simple = f"#{dep_simple}"
                dep_bases = self.temp_sources.get(dep_simple, set())
                if dep_bases:
                    base_sources.update(dep_bases)
                else:
                    base_sources.add(d)
        self.temp_sources[simple_name] = base_sources
        try:
            col_map = {lin.output_column: list(lin.input_fields or []) for lin in (lineage or [])}
            self.temp_lineage[simple_name] = col_map
        except Exception:
            pass

    schema = TableSchema(namespace=namespace, name=table_name, columns=output_columns)
    self.schema_registry.register(schema)

    final_dependencies: Set[str] = set()
    for d in dependencies:
        is_dep_temp = ('#' in d or 'tempdb' in d.lower())
        if not is_dep_temp:
            final_dependencies.add(d)
        else:
            final_dependencies.add(d)
            dep_simple = (d.split('.')[-1] if '.' in d else d)
            if not dep_simple.startswith('#'):
                dep_simple = f"#{dep_simple}"
            dep_bases = self.temp_sources.get(dep_simple, set())
            if dep_bases:
                final_dependencies.update(dep_bases)

    return ObjectInfo(
        name=table_name,
        object_type="temp_table" if raw_is_temp else "table",
        schema=schema,
        lineage=lineage,
        dependencies=final_dependencies
    )
