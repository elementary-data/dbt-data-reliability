import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional

from data_seeder import DbtDataSeeder
from elementary.clients.dbt.dbt_runner import DbtRunner
from ruamel.yaml import YAML

PATH = Path(__file__).parent.parent / "dbt_project"
MODELS_DIR_PATH = PATH / "models"

_DEFAULT_VARS = {
    "disable_dbt_invocation_autoupload": True,
    "disable_dbt_artifacts_autoupload": True,
    "disable_run_results": True,
}


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

    def read_table(
        self,
        table_name: str,
        where: Optional[str] = None,
        column_names: Optional[List[str]] = None,
        raise_if_empty: bool = True,
    ) -> List[dict]:
        results = json.loads(
            self.dbt_runner.run_operation(
                "read_table",
                macro_args={
                    "table": table_name,
                    "where": where,
                    "column_names": column_names,
                },
            )[0]
        )
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
    ):
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

        with self.seed_data(data, test_id):
            with NamedTemporaryFile(dir=MODELS_DIR_PATH, suffix=".yaml") as props_file:
                YAML().dump(props_yaml, props_file)
                relative_props_path = Path(props_file.name).relative_to(PATH)
                self.dbt_runner.test(select=str(relative_props_path))

    def seed_data(self, data: List[dict], table_name: str):
        return DbtDataSeeder(self.dbt_runner).seed_data(data, table_name)
