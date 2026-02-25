import json
import os
import re
import urllib.request
from contextlib import contextmanager, nullcontext
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Generator, List, Literal, Optional, Union, overload
from uuid import uuid4

from data_seeder import DbtDataSeeder
from dbt_utils import get_database_and_schema_properties
from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.clients.dbt.factory import RunnerMethod, create_dbt_runner
from logger import get_logger
from ruamel.yaml import YAML

PYTEST_XDIST_WORKER = os.environ.get("PYTEST_XDIST_WORKER", None)
SCHEMA_NAME_SUFFIX = f"_{PYTEST_XDIST_WORKER}" if PYTEST_XDIST_WORKER else ""

_DEFAULT_VARS = {
    "disable_dbt_invocation_autoupload": True,
    "disable_dbt_artifacts_autoupload": True,
    "columns_upload_strategy": "none",
    "disable_run_results": True,
    "disable_freshness_results": True,
    "debug_logs": True,
    "schema_name_suffix": SCHEMA_NAME_SUFFIX,
}

DEFAULT_DUMMY_CODE = "SELECT 1 AS col"

logger = get_logger(__name__)


def get_dbt_runner(
    target: str, project_dir: str, runner_method: Optional[RunnerMethod] = None
) -> BaseDbtRunner:
    return create_dbt_runner(
        project_dir,
        target=target,
        vars=_DEFAULT_VARS.copy(),
        raise_on_failure=False,
        runner_method=runner_method,
    )


class DbtProject:
    def __init__(
        self,
        target: str,
        project_dir: str,
        runner_method: Optional[RunnerMethod] = None,
    ):
        self.dbt_runner = get_dbt_runner(target, project_dir, runner_method)
        self.target = target
        self.runner_method = runner_method

        self.project_dir_path = Path(project_dir)
        self.models_dir_path = self.project_dir_path / "models"
        self.tmp_models_dir_path = self.models_dir_path / "tmp"
        self.seeds_dir_path = self.project_dir_path / "data"

    def run_query(self, prerendered_query: str):
        results = json.loads(
            self.dbt_runner.run_operation(
                "elementary.render_run_query",
                macro_args={"prerendered_query": prerendered_query},
            )[0]
        )
        return results

    @staticmethod
    def read_table_query(
        table_name: str,
        where: Optional[str] = None,
        group_by: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        column_names: Optional[List[str]] = None,
    ):
        return f"""
            SELECT {', '.join(column_names) if column_names else '*'}
            FROM {{{{ ref('{table_name}') }}}}
            {f"WHERE {where}" if where else ""}
            {f"GROUP BY {group_by}" if group_by else ""}
            {f"ORDER BY {order_by}" if order_by else ""}
            {f"LIMIT {limit}" if limit else ""}
            """

    def read_table(
        self,
        table_name: str,
        where: Optional[str] = None,
        group_by: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        column_names: Optional[List[str]] = None,
        raise_if_empty: bool = True,
    ) -> List[dict]:
        query = self.read_table_query(
            table_name, where, group_by, order_by, limit, column_names
        )
        results = self.run_query(query)
        if raise_if_empty and len(results) == 0:
            raise ValueError(
                f"Table '{table_name}' with the '{where}' condition is empty."
            )
        return results

    @overload
    def test(
        self,
        test_id: str,
        dbt_test_name: str,
        test_args: Optional[Dict[str, Any]] = None,
        test_column: Optional[str] = None,
        columns: Optional[List[dict]] = None,
        data: Optional[List[dict]] = None,
        as_model: bool = False,
        table_name: Optional[str] = None,
        materialization: str = "table",  # Only relevant if as_model=True
        test_vars: Optional[dict] = None,
        elementary_enabled: bool = True,
        model_config: Optional[Dict[str, Any]] = None,
        test_config: Optional[Dict[str, Any]] = None,
        *,
        multiple_results: Literal[False] = False,
    ) -> Dict[str, Any]:
        ...

    @overload
    def test(
        self,
        test_id: str,
        dbt_test_name: str,
        test_args: Optional[Dict[str, Any]] = None,
        test_column: Optional[str] = None,
        columns: Optional[List[dict]] = None,
        data: Optional[List[dict]] = None,
        as_model: bool = False,
        table_name: Optional[str] = None,
        materialization: str = "table",  # Only relevant if as_model=True
        test_vars: Optional[dict] = None,
        elementary_enabled: bool = True,
        model_config: Optional[Dict[str, Any]] = None,
        test_config: Optional[Dict[str, Any]] = None,
        *,
        multiple_results: Literal[True],
    ) -> List[Dict[str, Any]]:
        ...

    def test(
        self,
        test_id: str,
        dbt_test_name: str,
        test_args: Optional[Dict[str, Any]] = None,
        test_column: Optional[str] = None,
        columns: Optional[List[dict]] = None,
        data: Optional[List[dict]] = None,
        as_model: bool = False,
        table_name: Optional[str] = None,
        materialization: str = "table",  # Only relevant if as_model=True
        test_vars: Optional[dict] = None,
        elementary_enabled: bool = True,
        model_config: Optional[Dict[str, Any]] = None,
        column_config: Optional[Dict[str, Any]] = None,
        test_config: Optional[Dict[str, Any]] = None,
        *,
        multiple_results: bool = False,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        if columns and test_column:
            raise ValueError("You can't specify both 'columns' and 'test_column'.")
        test_vars = test_vars or {}
        test_vars["elementary_enabled"] = elementary_enabled

        test_id = test_id.replace("[", "_").replace("]", "_")
        if not table_name:
            table_name = test_id

        test_args = test_args or {}
        table_yaml: Dict[str, Any] = {"name": test_id}

        if model_config:
            table_yaml.update(model_config)

        if columns:
            table_yaml["columns"] = columns

        test_yaml = {dbt_test_name: {"arguments": test_args}}
        if test_config:
            test_yaml[dbt_test_name]["config"] = test_config

        if test_column is None:
            table_yaml["tests"] = [test_yaml]
        else:
            column_def = {
                "name": test_column,
                "tests": [test_yaml],
            }
            if column_config:
                column_def["config"] = column_config
            table_yaml["columns"] = [column_def]

        temp_table_ctx: Any
        if as_model:
            props_yaml = {
                "version": 2,
                "models": [table_yaml],
            }
            temp_table_ctx = self.create_temp_model_for_existing_table(
                test_id, materialization
            )
        else:
            database_property, schema_property = get_database_and_schema_properties(
                self.target
            )
            props_yaml = {
                "version": 2,
                "sources": [
                    {
                        "name": "test_data",
                        "schema": f"{{{{ target.{schema_property} }}}}{SCHEMA_NAME_SUFFIX}",
                        "database": f"{{{{ target.{database_property} }}}}",
                        "tables": [table_yaml],
                    }
                ],
            }
            temp_table_ctx = nullcontext()

        if data:
            self.seed(data, table_name)

        with temp_table_ctx:
            with NamedTemporaryFile(
                dir=self.tmp_models_dir_path,
                prefix="integration_tests_",
                suffix=".yaml",
            ) as props_file:
                YAML().dump(props_yaml, props_file)
                relative_props_path = Path(props_file.name).relative_to(
                    self.project_dir_path
                )
                test_process_success = self.dbt_runner.test(
                    select=str(relative_props_path), vars=test_vars
                )

        if elementary_enabled:
            if multiple_results:
                return self._read_test_results(test_id)
            else:
                return self._read_single_test_result(test_id)
        else:
            # If we disabled elementary, elementary_test_results will also be empty. So we'll simulate the result
            # based on the process status (which means we can't differentiate between fail and error)
            test_result = {
                "status": "pass" if test_process_success else "fail_or_error"
            }
            return [test_result] if multiple_results else test_result

    def seed(self, data: List[dict], table_name: str):
        with DbtDataSeeder(
            self.dbt_runner, self.project_dir_path, self.seeds_dir_path
        ).seed(data, table_name):
            self._fix_seed_if_needed(table_name, data)

    def _fix_seed_if_needed(self, table_name: str, data: Optional[List[dict]] = None):
        # Hack for BigQuery - seems like we get empty strings instead of nulls in seeds, so we
        # fix them here.
        if self.runner_method == RunnerMethod.FUSION and self.target == "bigquery":
            self.dbt_runner.run_operation(
                "elementary_tests.replace_empty_strings_with_nulls",
                macro_args={"table_name": table_name},
            )
        # On ClickHouse, columns are non-Nullable by default, so NULL values in CSVs become
        # default values (0 for Int, '' for String, etc.). We fix this by altering columns to
        # Nullable and updating default values back to NULLs directly via the ClickHouse HTTP
        # API, since dbt's run_query/statement don't reliably execute DDL on ClickHouse.
        elif self.target == "clickhouse" and data:
            self._fix_clickhouse_seed_nulls(table_name, data)

    def _fix_clickhouse_seed_nulls(self, table_name: str, data: List[dict]):
        """Fix ClickHouse seed tables where NULL values became default values.

        ClickHouse columns are non-Nullable by default, so NULL values in CSV seeds
        become default values (0 for Int, '' for String, etc.). This method:
        1. Determines which columns had NULL values in the original data
        2. ALTERs those columns to Nullable types
        3. Rebuilds the table via INSERT SELECT with nullIf() to restore NULLs

        Uses the ClickHouse HTTP API directly because dbt's run_query/statement
        don't reliably execute DDL on ClickHouse.
        """
        # Find columns that contain at least one NULL in the original data
        nullable_columns: set = set()
        for row in data:
            for col_name, value in row.items():
                if value is None:
                    nullable_columns.add(col_name)
        if not nullable_columns:
            return

        schema = f"default{SCHEMA_NAME_SUFFIX}"
        ch_host = os.environ.get("CLICKHOUSE_HOST", "localhost")
        ch_port = os.environ.get("CLICKHOUSE_PORT", "8123")
        ch_user = os.environ.get("CLICKHOUSE_USER", "default")
        ch_password = os.environ.get("CLICKHOUSE_PASSWORD", "default")
        ch_url = f"http://{ch_host}:{ch_port}"

        def ch_query(query: str) -> str:
            encoded = query.encode("utf-8")
            req = urllib.request.Request(
                f"{ch_url}/?user={ch_user}&password={ch_password}&mutations_sync=1",
                data=encoded,
            )
            with urllib.request.urlopen(req, timeout=60) as resp:  # noqa: S310
                return resp.read().decode("utf-8")

        # Get all columns and their types
        # Validate identifiers to prevent SQL injection
        if not re.fullmatch(r"[A-Za-z0-9_]+", schema):
            raise ValueError(f"Invalid schema name: {schema!r}")
        if not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
            raise ValueError(f"Invalid table name: {table_name!r}")

        cols_result = ch_query(
            f"SELECT name, type FROM system.columns "
            f"WHERE database = '{schema}' AND table = '{table_name}'"
        ).strip()
        if not cols_result:
            logger.warning(
                "ClickHouse fix: no columns found for %s.%s – "
                "schema may be wrong (using '%s'). NULLs will not be repaired.",
                schema,
                table_name,
                schema,
            )
            return

        columns = []
        for line in cols_result.split("\n"):
            parts = line.strip().split("\t")
            if len(parts) == 2:
                columns.append((parts[0], parts[1]))

        # Build SELECT expressions: use nullIf() for nullable columns
        select_exprs = []
        for col_name, col_type in columns:
            if col_name in nullable_columns:
                # Strip Nullable(...) wrapper from a prior run to avoid
                # Nullable(Nullable(...)) nesting
                base_type = col_type
                if base_type.startswith("Nullable(") and base_type.endswith(")"):
                    base_type = base_type[len("Nullable(") : -1]
                # Get the default value for this type to use with nullIf
                if (
                    base_type == "String"
                    or base_type.startswith("FixedString")
                    or base_type.startswith("LowCardinality")
                ):
                    default_val = "''"
                elif base_type.startswith("Int") or base_type.startswith("UInt"):
                    default_val = "0"
                elif base_type.startswith("Float"):
                    default_val = "0"
                else:
                    default_val = "defaultValueOfTypeName('" + base_type + "')"
                select_exprs.append(
                    f"nullIf(`{col_name}`, {default_val})::Nullable({base_type}) as `{col_name}`"
                )
            else:
                select_exprs.append(f"`{col_name}`")

        # Rebuild the table: CREATE temp AS SELECT with nullIf, EXCHANGE, DROP
        tmp_name = f"{table_name}_tmp_nullable"
        select_sql = ", ".join(select_exprs)

        logger.info(
            "ClickHouse fix: rebuilding %s.%s with Nullable columns: %s",
            schema,
            table_name,
            nullable_columns,
        )

        ch_query(f"DROP TABLE IF EXISTS {schema}.{tmp_name}")
        try:
            ch_query(
                f"CREATE TABLE {schema}.{tmp_name} "
                f"ENGINE = MergeTree() ORDER BY tuple() "
                f"AS SELECT {select_sql} FROM {schema}.{table_name}"
            )
            ch_query(f"EXCHANGE TABLES {schema}.{table_name} AND {schema}.{tmp_name}")
        finally:
            ch_query(f"DROP TABLE IF EXISTS {schema}.{tmp_name}")

    @contextmanager
    def seed_context(
        self, data: List[dict], table_name: str
    ) -> Generator[None, None, None]:
        with DbtDataSeeder(
            self.dbt_runner, self.project_dir_path, self.seeds_dir_path
        ).seed(data, table_name):
            self._fix_seed_if_needed(table_name, data)
            yield

    @contextmanager
    def create_temp_model_for_existing_table(
        self,
        table_name: str,
        materialization: Optional[str] = None,
        raw_code: Optional[str] = None,
    ):
        model_path = self.tmp_models_dir_path.joinpath(f"{table_name}.sql")
        code = raw_code or DEFAULT_DUMMY_CODE
        model_text = ""
        if materialization:
            model_text += f"{{{{ config(materialized='{materialization}') }}}}\n"
        model_text += code
        model_path.write_text(model_text)
        relative_model_path = model_path.relative_to(self.project_dir_path)
        try:
            yield relative_model_path
        finally:
            model_path.unlink()

    def _read_test_results(self, table_name: str) -> List[Dict[str, Any]]:
        test_execution_id_subquery = self.read_table_query(
            "elementary_test_results",
            where=f"lower(table_name) = lower('{table_name}')",
            order_by="created_at DESC",
            column_names=["test_execution_id"],
            limit=1,
        )
        return self.read_table(
            "elementary_test_results",
            where=f"test_execution_id IN ({test_execution_id_subquery})",
        )

    def _read_single_test_result(self, table_name: str) -> Dict[str, Any]:
        results = self._read_test_results(table_name)
        if len(results) == 0:
            raise Exception(f"No test result found for table {table_name}")
        if len(results) > 1:
            raise Exception(f"Multiple test results found for table {table_name}")
        return results[0]

    @contextmanager
    def write_yaml(self, content: dict, name: Optional[str] = None):
        name = name or f"{uuid4()}.yaml"
        path = self.models_dir_path / name
        with open(path, "w") as f:
            YAML().dump(content, f)

        try:
            yield path
        finally:
            path.unlink()
