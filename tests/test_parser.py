"""
Unit tests for the SQL parser component.
"""
import pytest
import json
from pathlib import Path

from infotracker.parser import SqlParser
from infotracker.models import TransformationType


class TestSqlParser:
    """Test cases for SQL parser functionality."""

    def setup_method(self):
        """Set up test instance."""
        self.parser = SqlParser()

    def test_create_table_parsing(self, sql_content, expected_lineage):
        """Test parsing of CREATE TABLE statements."""
        # Test the customers table
        sql = sql_content["01_customers"]
        obj_info = self.parser.parse_sql_file(sql, "01_customers")
        
        assert obj_info.name == "dbo.Customers"
        assert obj_info.object_type == "table"
        assert len(obj_info.schema.columns) == 4
        assert len(obj_info.lineage) == 0  # Tables don't have lineage
        assert len(obj_info.dependencies) == 0  # Tables don't have dependencies
        
        # Check column details
        columns = {col.name: col for col in obj_info.schema.columns}
        
        # CustomerID - Primary Key (not nullable)
        assert "CustomerID" in columns
        assert columns["CustomerID"].data_type == "int"
        assert columns["CustomerID"].nullable == False
        
        # CustomerName - NOT NULL
        assert "CustomerName" in columns
        assert columns["CustomerName"].data_type == "nvarchar(100)"
        assert columns["CustomerName"].nullable == False
        
        # Email - NULL allowed
        assert "Email" in columns
        assert columns["Email"].data_type == "nvarchar(255)"
        assert columns["Email"].nullable == True
        
        # SignupDate - NULL allowed
        assert "SignupDate" in columns
        assert columns["SignupDate"].data_type == "date"
        assert columns["SignupDate"].nullable == True

    def test_create_view_parsing(self, sql_content):
        """Test parsing of CREATE VIEW statements."""
        # Test the stg_orders view
        sql = sql_content["10_stg_orders"]
        obj_info = self.parser.parse_sql_file(sql, "10_stg_orders")
        
        assert obj_info.name == "dbo.stg_orders"
        assert obj_info.object_type == "view"
        assert len(obj_info.schema.columns) == 4
        assert len(obj_info.lineage) == 4
        assert obj_info.dependencies == {"STG.dbo.Orders"}
        
        # Check lineage details
        lineage_by_column = {lin.output_column: lin for lin in obj_info.lineage}
        
        # OrderID - Identity transformation
        assert "OrderID" in lineage_by_column
        orderid_lineage = lineage_by_column["OrderID"]
        assert orderid_lineage.transformation_type == TransformationType.IDENTITY
        assert len(orderid_lineage.input_fields) == 1
        assert orderid_lineage.input_fields[0].table_name == "dbo.Orders"
        assert orderid_lineage.input_fields[0].column_name == "OrderID"
        
        # CustomerID - Identity transformation
        assert "CustomerID" in lineage_by_column
        customerid_lineage = lineage_by_column["CustomerID"]
        assert customerid_lineage.transformation_type == TransformationType.IDENTITY
        
        # OrderDate - CAST transformation
        assert "OrderDate" in lineage_by_column
        orderdate_lineage = lineage_by_column["OrderDate"]
        assert orderdate_lineage.transformation_type == TransformationType.CAST
        assert "CAST" in orderdate_lineage.transformation_description.upper()
        
        # IsFulfilled - CASE transformation
        assert "IsFulfilled" in lineage_by_column
        isfulfilled_lineage = lineage_by_column["IsFulfilled"]
        assert isfulfilled_lineage.transformation_type == TransformationType.CASE
        assert len(isfulfilled_lineage.input_fields) == 1
        assert isfulfilled_lineage.input_fields[0].column_name == "Status"

    def test_dependency_extraction(self, sql_content):
        """Test extraction of table dependencies."""
        # Test a view with JOIN (if available)
        if "40_fct_sales" in sql_content:
            sql = sql_content["40_fct_sales"]
            obj_info = self.parser.parse_sql_file(sql, "40_fct_sales")
            
            # Should have dependencies on both tables in the JOIN
            assert len(obj_info.dependencies) >= 1
            # The exact dependencies depend on the SQL content

    def test_error_handling(self):
        """Test error handling for invalid SQL."""
        invalid_sql = "INVALID SQL STATEMENT;"
        obj_info = self.parser.parse_sql_file(invalid_sql, "test")
        
        # Should return an object with minimal information
        assert obj_info.name == "test"
        assert obj_info.object_type == "unknown"

    @pytest.mark.parametrize("sql_file", [
        "01_customers", "02_orders", "03_products", "04_order_items"
    ])
    def test_all_table_files(self, sql_content, sql_file):
        """Test parsing of all CREATE TABLE files."""
        if sql_file not in sql_content:
            pytest.skip(f"SQL file {sql_file} not found")
            
        sql = sql_content[sql_file]
        obj_info = self.parser.parse_sql_file(sql, sql_file)
        
        assert obj_info.object_type == "table"
        assert len(obj_info.schema.columns) > 0
        assert len(obj_info.lineage) == 0
        assert len(obj_info.dependencies) == 0

    @pytest.mark.parametrize("sql_file", [
        "10_stg_orders", "11_stg_order_items", "12_stg_customers"
    ])
    def test_staging_view_files(self, sql_content, sql_file):
        """Test parsing of staging view files."""
        if sql_file not in sql_content:
            pytest.skip(f"SQL file {sql_file} not found")
            
        sql = sql_content[sql_file]
        obj_info = self.parser.parse_sql_file(sql, sql_file)
        
        assert obj_info.object_type == "view"
        assert len(obj_info.schema.columns) > 0
        assert len(obj_info.lineage) > 0
        assert len(obj_info.dependencies) > 0

    def test_procedure_temp_cte_union_keeps_temp_and_cte_inputs(self):
        sql = """
CREATE OR ALTER PROCEDURE dbo.update_Con_ParCust_sate_asefl
AS
BEGIN
    DROP TABLE IF EXISTS #tmp_stage_asefl_PartyCustomer_Contract_lnk;
    SELECT * INTO #tmp_stage_asefl_PartyCustomer_Contract_lnk
    FROM dbo.stage_asefl_PartyCustomer_Contract_lnk;

    ;WITH current_effectivity AS (
        SELECT hk_l_Party_Contract, hk_h_Contract, dv_start_date, dv_record_source, DV_Tenant_ID, dv_load_date
        FROM dbo.Contract_PartyCustomer_sate_asefl_current
    ),
    driverkey AS (
        SELECT DISTINCT hk_h_Contract, dv_load_date
        FROM #tmp_stage_asefl_PartyCustomer_Contract_lnk
    )
    INSERT INTO dbo.Contract_PartyCustomer_sate_asefl
    (hk_l_Party_Contract, DV_start_date, DV_end_date, DV_load_date, DV_record_source, DV_Tenant_ID)
    SELECT
        hk_l_Party_Contract,
        dv_load_date,
        CAST('9999-12-31' AS DATETIME),
        dv_load_date,
        dv_record_source,
        DV_Tenant_ID
    FROM #tmp_stage_asefl_PartyCustomer_Contract_lnk stg
    WHERE NOT EXISTS (
        SELECT 1 FROM current_effectivity efs
        WHERE stg.hk_l_Party_Contract = efs.hk_l_Party_Contract
    )
    UNION ALL
    SELECT
        hk_l_Party_Contract,
        dv_start_date,
        drv.dv_load_date,
        COALESCE(NULLIF(drv.dv_load_date, efs.dv_load_date), GETDATE()),
        dv_record_source,
        DV_Tenant_ID
    FROM current_effectivity efs
    JOIN driverkey drv ON efs.hk_h_Contract = drv.hk_h_Contract;
END
"""
        obj_info = self.parser.parse_sql_file(sql, "update_Con_ParCust_sate_asefl")

        assert obj_info.object_type == "table"
        assert obj_info.schema.name == "dbo.Contract_PartyCustomer_sate_asefl"
        assert obj_info.lineage

        # Regression: first UNION branch must preserve direct temp->target column input.
        lineage_by_col = {ln.output_column: ln for ln in obj_info.lineage}
        hk_inputs = {f.table_name for f in lineage_by_col["hk_l_Party_Contract"].input_fields}
        assert any("tmp_stage_asefl_partycustomer_contract_lnk" in t.lower() for t in hk_inputs)
        assert any("current_effectivity" in t.lower() for t in hk_inputs)

        # Regression: no unscoped temp alias in dependencies.
        deps_lower = {d.lower() for d in obj_info.dependencies}
        assert "dbo.tmp_stage_asefl_partycustomer_contract_lnk" not in deps_lower
        assert any("tmp_stage_asefl_partycustomer_contract_lnk" in d for d in deps_lower)

    def test_case_insensitive_temp_aliases_unify_to_single_scoped_name(self):
        sql = """
CREATE OR ALTER PROCEDURE dbo.proc_temp_case
AS
BEGIN
    DROP TABLE IF EXISTS #Tmp_CASE_A;
    SELECT * INTO #Tmp_CASE_A FROM dbo.stage_a;

    ;WITH cte_a AS (
        SELECT DISTINCT id, dt
        FROM dbo.tmp_case_a
    )
    INSERT INTO dbo.target_case(id, dt)
    SELECT id, dt FROM #tmp_case_a t
    UNION ALL
    SELECT id, dt FROM cte_a;
END
"""
        obj_info = self.parser.parse_sql_file(sql, "proc_temp_case")
        assert obj_info.lineage

        all_input_tables = {
            f.table_name.lower()
            for ln in obj_info.lineage
            for f in ln.input_fields
        }
        # Must map all variants (#Tmp_CASE_A / dbo.tmp_case_a / #tmp_case_a) to one temp identity.
        assert any("tmp_case_a" in n for n in all_input_tables)
        assert "dbo.tmp_case_a" not in all_input_tables

    def test_merge_output_into_table_variable_keeps_merge_target(self):
        """MERGE target must stay the durable table; OUTPUT INTO @Z is not the primary output."""
        sql = """
CREATE PROCEDURE dbo.p_merge_out_tv
AS
BEGIN
    DECLARE @Z TABLE (id INT, a INT);
    MERGE dbo.TargetX AS t
    USING (SELECT col1 AS a FROM dbo.SourceY) AS s
    ON t.id = s.a
    WHEN MATCHED THEN UPDATE SET t.a = s.a
    WHEN NOT MATCHED THEN INSERT (id, a) VALUES (1, s.a)
    OUTPUT $action INTO @Z;
END
"""
        obj_info = self.parser.parse_sql_file(sql, "p_merge_out_tv")
        assert obj_info.object_type == "table"
        name = (obj_info.schema.name if obj_info.schema else obj_info.name or "").lower()
        assert "targetx" in name
        assert "@z" not in name
        deps_blob = " ".join(obj_info.dependencies or []).lower()
        assert "sourcey" in deps_blob

    def test_cte_same_alias_isolated_between_two_procedures(self):
        """Regression: shared CTE alias must not collide across procedures (scoped registry keys)."""
        sql_a = """
CREATE OR ALTER PROCEDURE dbo.proc_cte_a
AS
BEGIN
    ;WITH cte AS (SELECT id FROM dbo.Table_A)
    INSERT INTO dbo.TargetA(id) SELECT id FROM cte;
END
"""
        sql_b = """
CREATE OR ALTER PROCEDURE dbo.proc_cte_b
AS
BEGIN
    ;WITH cte AS (SELECT id FROM dbo.Table_B)
    INSERT INTO dbo.TargetB(id) SELECT id FROM cte;
END
"""
        p = SqlParser()
        oa = p.parse_sql_file(sql_a, "proc_cte_a")
        assert any("table_a" in (d or "").lower() for d in (oa.dependencies or []))
        assert not any("table_b" in (d or "").lower() for d in (oa.dependencies or []))

        ob = p.parse_sql_file(sql_b, "proc_cte_b")
        assert any("table_b" in (d or "").lower() for d in (ob.dependencies or []))
        assert not any("table_a" in (d or "").lower() for d in (ob.dependencies or []))

    def test_cte_scoped_key_depends_on_procedure_context(self):
        """Canonical CTE key must change when _ctx_obj changes (no global 'procedure' collision)."""
        p = SqlParser()
        p._ctx_obj = "dbo.proc_one"
        k1 = p._cte_scoped_key("cte")
        p._ctx_obj = "dbo.proc_two"
        k2 = p._cte_scoped_key("cte")
        assert k1 != k2
        assert k1.endswith("$cte") and k2.endswith("$cte")

    def test_insert_select_stops_before_rowcount_select_without_semicolon(self):
        """INSERT...SELECT must not absorb the following SELECT @var = @@ROWCOUNT when ';' is missing."""
        sql = """
CREATE PROCEDURE dbo.p_ins_rowcount
AS
BEGIN
    SELECT 1 AS a INTO #x;
    INSERT INTO dbo.TargetT (a)
    SELECT a FROM #x
    SELECT @rc = @@ROWCOUNT
    COMMIT
END
"""
        obj_info = self.parser.parse_sql_file(sql, "p_ins_rowcount")
        assert obj_info.lineage, "expected column lineage for TargetT"
        by_col = {ln.output_column: ln for ln in obj_info.lineage}
        assert "a" in by_col
        inputs_blob = " ".join(
            f"{f.table_name}.{f.column_name}" for f in by_col["a"].input_fields
        ).lower()
        assert "@@rowcount" not in inputs_blob
        assert "x" in inputs_blob or "#x" in inputs_blob

    def test_basic_dependencies_parenthesized_from_join(self):
        """FROM (t1 alias INNER JOIN t2 alias ON ...) must still yield table dependencies."""
        sql = """
SELECT 1
FROM (
  EDW_CORE.dbo.Table_A a
  INNER JOIN EDW_CORE.dbo.Table_B b ON a.id = b.id
) x
"""
        deps = self.parser._extract_basic_dependencies(sql)
        dep_l = " ".join(deps).lower()
        assert "table_a" in dep_l
        assert "table_b" in dep_l

    def test_max_loaddate_over_values_not_outer_table_column(self):
        """MAX(LoadDate) over VALUES(...) must not resolve LoadDate to outer FROM table."""
        sql = """
SELECT
  CAST((
    SELECT MAX(LoadDate) FROM (
      VALUES (Document_hub.DV_load_date), (Other_tbl.dt)
    ) AS V(LoadDate)
  ) AS datetime2(3)) AS MaxLoadDate,
  Document_hub.hk AS hk
INTO #tmp
FROM EDW_CORE.dbo.Document_hub Document_hub
CROSS JOIN EDW_CORE.dbo.Other_tbl Other_tbl;
"""
        obj_info = self.parser.parse_sql_file(sql, "batch_maxval")
        assert obj_info.lineage
        ml = [ln for ln in obj_info.lineage if str(ln.output_column).lower() == "maxloaddate"]
        assert ml, "expected MaxLoadDate output"
        blob = " ".join(
            f"{r.table_name}.{r.column_name}".lower() for r in ml[0].input_fields
        )
        assert "document_hub.maxloaddate" not in blob
        assert "dv_load_date" in blob or "dt" in blob

    def test_select_into_temp_multijoin_column_lineage(self):
        """Multi-column SELECT INTO #t with JOINs must keep per-column lineage (regression: alias filter)."""
        sql = """
SELECT
    a.id AS id_col,
    b.name AS name_col,
    a.x AS x_col
INTO #t_join
FROM dbo.TableA a
INNER JOIN dbo.TableB b ON a.id = b.id;
"""
        obj_info = self.parser.parse_sql_file(sql, "batch_temp_join")
        assert obj_info.object_type == "temp_table"
        assert len(obj_info.schema.columns) >= 3
        by_col = {ln.output_column: ln for ln in obj_info.lineage}
        assert {"id_col", "name_col", "x_col"}.issubset(set(by_col))
        id_tables = {f.table_name.lower() for f in by_col["id_col"].input_fields}
        name_tables = {f.table_name.lower() for f in by_col["name_col"].input_fields}
        x_tables = {f.table_name.lower() for f in by_col["x_col"].input_fields}
        assert any("tablea" in t for t in id_tables)
        assert any("tableb" in t for t in name_tables)
        assert any("tablea" in t for t in x_tables)
        dep_blob = " ".join(d.lower() for d in (obj_info.dependencies or []))
        assert "tablea" in dep_blob
        assert "tableb" in dep_blob
