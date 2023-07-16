from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional

import dbt_project
from data_seeder import DbtDataSeeder
from ruamel.yaml import YAML

_MODELS_DIR_PATH = dbt_project.PATH / "models"


def run_dbt_test(
    data: List[dict],
    table_name: str,
    test_name: str,
    test_args: Optional[Dict[str, Any]] = None,
):
    test_args = test_args or {}
    dbt_runner = dbt_project.get_dbt_runner()
    props_yaml = {
        "version": 2,
        "sources": [
            {
                "name": "test_data",
                "schema": "test_seeds",
                "tables": [{"name": table_name, "tests": [{test_name: test_args}]}],
            }
        ],
    }

    with DbtDataSeeder().seed_data(data, table_name):
        with NamedTemporaryFile(dir=_MODELS_DIR_PATH, suffix=".yaml") as props_file:
            YAML().dump(props_yaml, props_file)
            relative_props_path = Path(props_file.name).relative_to(dbt_project.PATH)
            dbt_runner.test(select=str(relative_props_path))
