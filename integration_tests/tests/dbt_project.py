import json
import os
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
            self._fix_seed_if_needed(table_name)

    def _fix_seed_if_needed(self, table_name: str):
        # Hack for BigQuery - seems like we get empty strings instead of nulls in seeds, so we
        # fix them here
        if self.runner_method == RunnerMethod.FUSION and self.target == "bigquery":
            self.dbt_runner.run_operation(
                "elementary_tests.replace_empty_strings_with_nulls",
                macro_args={"table_name": table_name},
            )

    @contextmanager
    def seed_context(
        self, data: List[dict], table_name: str
    ) -> Generator[None, None, None]:
        with DbtDataSeeder(
            self.dbt_runner, self.project_dir_path, self.seeds_dir_path
        ).seed(data, table_name):
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
