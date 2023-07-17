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
        vars=_DEFAULT_VARS,
        raise_on_failure=False,
    )


class DbtProject:
    def __init__(self, target: str):
        self.dbt_runner = get_dbt_runner(target)

    def run_query(self, query: str):
        results = json.loads(
            self.dbt_runner.run_operation(
                "elementary_tests.run_query",
                macro_args={"query": query},
            )[0]
        )
        return results

    def read_table(
        self,
        table_name: str,
        where: Optional[str] = None,
        column_names: Optional[List[str]] = None,
        raise_if_empty: bool = True,
    ) -> List[dict]:
        query = f"""
        SELECT {', '.join(column_names) if column_names else '*'}
        FROM {{{{ ref('{table_name}') }}}}
        {f"WHERE {where}" if where else ""}
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
            with NamedTemporaryFile(dir=MODELS_DIR_PATH, suffix=".yaml") as props_file:
                YAML().dump(props_yaml, props_file)
                relative_props_path = Path(props_file.name).relative_to(PATH)
                logger.info(f"Testing {test_id}.")
                self.dbt_runner.test(select=str(relative_props_path))
                logger.info(f"Tested {test_id}.")
        return self._read_test_result(test_id)

    def seed(self, data: List[dict], table_name: str):
        return DbtDataSeeder(self.dbt_runner).seed(data, table_name)

    def _read_test_result(self, test_id: str) -> Dict[str, Any]:
        return self.read_table(
            "elementary_test_results",
            where=f"table_name = '{test_id}'",
        )[0]
