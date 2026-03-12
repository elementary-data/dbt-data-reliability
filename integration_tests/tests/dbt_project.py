import json
import os
from contextlib import contextmanager, nullcontext
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Generator, List, Literal, Optional, Union, overload
from uuid import uuid4

from adapter_query_runner import AdapterQueryRunner, UnsupportedJinjaError
from data_seeder import (
    ClickHouseDirectSeeder,
    DbtDataSeeder,
    SparkS3CsvSeeder,
    VerticaDirectSeeder,
)
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


class SelectLimit:
    """Cross-adapter TOP / LIMIT helper.

    On T-SQL (Fabric / SQL Server) ``SELECT TOP n ...`` is used instead of
    ``... LIMIT n``.  Instances expose two string properties so that callers
    can build dialect-agnostic queries::

        sl = SelectLimit(1, is_tsql=True)
        query = f"SELECT {sl.top}col FROM t ORDER BY x {sl.limit}"
        # → "SELECT TOP 1 col FROM t ORDER BY x "
    """

    def __init__(self, n: int, is_tsql: bool) -> None:
        self.top = f"TOP {n} " if is_tsql else ""
        self.limit = "" if is_tsql else f"LIMIT {n}"


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

        self._query_runner: Optional[AdapterQueryRunner] = None

    def _get_query_runner(self) -> AdapterQueryRunner:
        """Lazily initialize the direct adapter query runner."""
        if self._query_runner is None:
            self._query_runner = AdapterQueryRunner(
                str(self.project_dir_path), self.target
            )
        return self._query_runner

    def run_query(self, prerendered_query: str):
        # Fast path: queries that only contain {{ ref() }} / {{ source() }}
        # can be executed directly through the adapter, bypassing
        # run_operation log parsing entirely.
        try:
            return self._get_query_runner().run_query(prerendered_query)
        except UnsupportedJinjaError:
            logger.debug("Query contains complex Jinja; falling back to run_operation")

        # Slow path: full Jinja rendering via run_operation.
        return self._run_query_with_run_operation(prerendered_query)

    def _run_query_with_run_operation(self, prerendered_query: str):
        """Execute a query via run_operation."""
        run_operation_results = self.dbt_runner.run_operation(
            "elementary.render_run_query",
            macro_args={"prerendered_query": prerendered_query},
        )
        if not run_operation_results:
            raise RuntimeError(
                f"run_operation('elementary.render_run_query') returned no output. "
                f"Query: {prerendered_query!r}"
            )
        return json.loads(run_operation_results[0])

    @property
    def is_tsql(self) -> bool:
        """Return True when the target uses T-SQL dialect (Fabric / SQL Server)."""
        return self.target in ("fabric", "sqlserver")

    def select_limit(self, n: int = 1) -> "SelectLimit":
        """Return cross-adapter TOP/LIMIT helpers for use in raw SQL strings.

        Usage::

            sl = dbt_project.select_limit(1)
            query = f"SELECT {sl.top}col FROM t ORDER BY x {sl.limit}"
        """
        return SelectLimit(n, self.is_tsql)

    def samples_query(self, test_id: str, order_by: str = "created_at desc") -> str:
        """Build a cross-adapter query to fetch test result sample rows.

        This is the shared implementation of the ``SAMPLES_QUERY`` template
        that was previously duplicated across multiple test files.
        """
        sl = self.select_limit(1)
        return f"""
            with latest_elementary_test_result as (
                select {sl.top}id
                from {{{{ ref("elementary_test_results") }}}}
                where lower(table_name) = lower('{test_id}')
                order by {order_by}
                {sl.limit}
            )

            select result_row
            from {{{{ ref("test_result_rows") }}}}
            where elementary_test_results_id in (select * from latest_elementary_test_result)
        """

    def read_table_query(
        self,
        table_name: str,
        where: Optional[str] = None,
        group_by: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        column_names: Optional[List[str]] = None,
    ):
        columns = ", ".join(column_names) if column_names else "*"
        top_clause = f"TOP {limit} " if limit and self.is_tsql else ""
        limit_clause = f"LIMIT {limit}" if limit and not self.is_tsql else ""
        return f"""
            SELECT {top_clause}{columns}
            FROM {{{{ ref('{table_name}') }}}}
            {f"WHERE {where}" if where else ""}
            {f"GROUP BY {group_by}" if group_by else ""}
            {f"ORDER BY {order_by}" if order_by else ""}
            {limit_clause}
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
            source_def: Dict[str, Any] = {
                "name": "test_data",
                "schema": f"{{{{ target.{schema_property} }}}}{SCHEMA_NAME_SUFFIX}",
                "tables": [table_yaml],
            }
            if database_property is not None:
                source_def["database"] = f"{{{{ target.{database_property} }}}}"
            props_yaml = {
                "version": 2,
                "sources": [source_def],
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

    def _read_profile_schema(self) -> str:
        """Read the base schema name from the rendered dbt profiles.yml."""
        profiles_dir = os.environ.get("DBT_PROFILES_DIR", os.path.expanduser("~/.dbt"))
        profiles_path = Path(profiles_dir) / "profiles.yml"
        if not profiles_path.exists():
            raise RuntimeError(f"dbt profiles not found at: {profiles_path}")
        yaml = YAML()
        with profiles_path.open() as fh:
            profiles = yaml.load(fh) or {}
        try:
            return profiles["elementary_tests"]["outputs"][self.target]["schema"]
        except KeyError as exc:
            raise RuntimeError(
                f"Missing schema for target '{self.target}' in {profiles_path}"
            ) from exc

    def _create_seeder(
        self,
    ) -> Union[
        DbtDataSeeder, ClickHouseDirectSeeder, SparkS3CsvSeeder, VerticaDirectSeeder
    ]:
        """Return the fastest available seeder for the current target."""
        if self.target == "clickhouse":
            runner = self._get_query_runner()
            schema = runner.schema_name + SCHEMA_NAME_SUFFIX
            return ClickHouseDirectSeeder(runner, schema, self.seeds_dir_path)
        if self.target == "spark":
            # Read schema from dbt profiles directly — avoids creating an
            # AdapterQueryRunner (which corrupts dbt global state via
            # set_from_args / reset_adapters).
            schema = self._read_profile_schema() + SCHEMA_NAME_SUFFIX
            return SparkS3CsvSeeder(schema, self.seeds_dir_path)
        if self.target == "vertica":
            # Vertica's COPY command (used by dbt seed) rejects empty CSV
            # fields for non-string columns.  Use direct INSERT instead.
            # Read schema from profiles directly (like Spark) to avoid
            # initialising an AdapterQueryRunner we don't need — Vertica
            # uses a direct vertica_python connection, not the dbt adapter.
            schema = self._read_profile_schema() + SCHEMA_NAME_SUFFIX
            return VerticaDirectSeeder(None, schema, self.seeds_dir_path)
        return DbtDataSeeder(
            self.dbt_runner, self.project_dir_path, self.seeds_dir_path
        )

    def seed(self, data: List[dict], table_name: str):
        with self._create_seeder().seed(data, table_name):
            self._fix_seed_if_needed(table_name)

    def _fix_seed_if_needed(self, table_name: str) -> None:
        # Hack for BigQuery - seems like we get empty strings instead of nulls in seeds, so we
        # fix them here.
        if self.runner_method == RunnerMethod.FUSION and self.target == "bigquery":
            self.dbt_runner.run_operation(
                "elementary_tests.replace_empty_strings_with_nulls",
                macro_args={"table_name": table_name},
            )

    @contextmanager
    def seed_context(
        self, data: List[dict], table_name: str
    ) -> Generator[None, None, None]:
        with self._create_seeder().seed(data, table_name):
            self._fix_seed_if_needed(table_name)
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
