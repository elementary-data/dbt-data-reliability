import json
import os
from contextlib import contextmanager, nullcontext
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Literal, Optional, Union, overload
from uuid import uuid4

from data_seeder import DbtDataSeeder
from elementary.clients.dbt.dbt_runner import DbtRunner
from logger import get_logger
from ruamel.yaml import YAML

PYTEST_XDIST_WORKER = os.environ.get("PYTEST_XDIST_WORKER", None)
SCHEMA_NAME_SUFFIX = f"_{PYTEST_XDIST_WORKER}" if PYTEST_XDIST_WORKER else ""

_DEFAULT_VARS = {
    "disable_dbt_invocation_autoupload": True,
    "disable_dbt_artifacts_autoupload": True,
    "disable_dbt_columns_autoupload": True,
    "disable_run_results": True,
    "debug_logs": True,
    "collect_metrics": False,
    "schema_name_suffix": SCHEMA_NAME_SUFFIX,
}

DEFAULT_DUMMY_CODE = "SELECT 1 AS col"

logger = get_logger(__name__)


def get_dbt_runner(target: str, project_dir: str) -> DbtRunner:
    return DbtRunner(
        project_dir,
        target=target,
        vars=_DEFAULT_VARS.copy(),
        raise_on_failure=False,
    )


class DbtProject:
    def __init__(self, target: str, project_dir: str):
        self.dbt_runner = get_dbt_runner(target, project_dir)

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
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        column_names: Optional[List[str]] = None,
    ):
        return f"""
            SELECT {', '.join(column_names) if column_names else '*'}
            FROM {{{{ ref('{table_name}') }}}}
            {f"WHERE {where}" if where else ""}
            {f"ORDER BY {order_by}" if order_by else ""}
            {f"LIMIT {limit}" if limit else ""}
            """

    def read_table(
        self,
        table_name: str,
        where: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        column_names: Optional[List[str]] = None,
        raise_if_empty: bool = True,
    ) -> List[dict]:
        query = self.read_table_query(table_name, where, order_by, limit, column_names)
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

        if columns:
            table_yaml["columns"] = columns

        if test_column is None:
            table_yaml["tests"] = [{dbt_test_name: test_args}]
        else:
            table_yaml["columns"] = [
                {"name": test_column, "tests": [{dbt_test_name: test_args}]}
            ]

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
            props_yaml = {
                "version": 2,
                "sources": [
                    {
                        "name": "test_data",
                        "schema": f"{{{{ target.schema }}}}{SCHEMA_NAME_SUFFIX}",
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
        return DbtDataSeeder(
            self.dbt_runner, self.project_dir_path, self.seeds_dir_path
        ).seed(data, table_name)

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
        yield path
        path.unlink()
