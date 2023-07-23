import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional

from data_seeder import DbtDataSeeder
from elementary.clients.dbt.dbt_runner import DbtRunner
from logger import get_logger
from ruamel.yaml import YAML

PATH = Path(__file__).parent.parent / "dbt_project"
MODELS_DIR_PATH = PATH / "models"
TMP_MODELS_DIR_PATH = MODELS_DIR_PATH / "tmp"
SEEDS_DIR_PATH = PATH / "data"

_DEFAULT_VARS = {
    "disable_dbt_invocation_autoupload": True,
    "disable_dbt_artifacts_autoupload": True,
    "disable_run_results": True,
}

logger = get_logger(__name__)


def get_dbt_runner(target: str) -> DbtRunner:
    return DbtRunner(
        str(PATH),
        target=target,
        vars=_DEFAULT_VARS.copy(),
        raise_on_failure=False,
    )


class DbtProject:
    def __init__(self, target: str):
        self.dbt_runner = get_dbt_runner(target)

    def run_query(self, prerendered_query: str):
        results = json.loads(
            self.dbt_runner.run_operation(
                "elementary_tests.render_run_query",
                macro_args={"prerendered_query": prerendered_query},
            )[0]
        )
        return results

    def read_table(
        self,
        table_name: str,
        where: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        column_names: Optional[List[str]] = None,
        raise_if_empty: bool = True,
    ) -> List[dict]:
        query = f"""
        SELECT {', '.join(column_names) if column_names else '*'}
        FROM {{{{ ref('{table_name}') }}}}
        {f"WHERE {where}" if where else ""}
        {f"ORDER BY {order_by}" if order_by else ""}
        {f"LIMIT {limit}" if limit else ""}
        """
        results = self.run_query(query)
        if raise_if_empty and len(results) == 0:
            raise ValueError(
                f"Table '{table_name}' with the '{where}' condition is empty."
            )
        return results

    def test(
        self,
        data: List[dict],
        test_id: str,
        dbt_test_name: str,
        test_args: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        test_args = test_args or {}
        props_yaml = {
            "version": 2,
            "sources": [
                {
                    "name": "test_data",
                    "schema": "test_seeds",
                    "tables": [
                        {
                            "name": test_id,
                            "tests": [{dbt_test_name: test_args}],
                        }
                    ],
                }
            ],
        }

        with self.seed(data, test_id):
            with NamedTemporaryFile(
                dir=TMP_MODELS_DIR_PATH, suffix=".yaml"
            ) as props_file:
                YAML().dump(props_yaml, props_file)
                relative_props_path = Path(props_file.name).relative_to(PATH)
                self.dbt_runner.test(select=str(relative_props_path))
        return self._read_test_result(test_id)

    def seed(self, data: List[dict], table_name: str):
        return DbtDataSeeder(self.dbt_runner).seed(data, table_name)

    def _read_test_result(self, table_name: str) -> Dict[str, Any]:
        return self.read_table(
            "elementary_test_results",
            where=f"lower(table_name) = lower('{table_name}')",
            order_by="created_at DESC",
            limit=1,
        )[0]
