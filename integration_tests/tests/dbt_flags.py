from contextlib import contextmanager
from typing import Any, Dict, Iterator

from dbt_project import DbtProject
from ruamel.yaml import YAML


@contextmanager
def set_flags(dbt_project: DbtProject, flags: Dict[str, Any]) -> Iterator[None]:
    dbt_project_yaml_path = dbt_project.project_dir_path / "dbt_project.yml"
    original_dbt_project_yaml = YAML().load(dbt_project_yaml_path)
    with dbt_project_yaml_path.open("w") as f:
        YAML().dump({**original_dbt_project_yaml, "flags": flags}, f)
    yield
    with dbt_project_yaml_path.open("w") as f:
        YAML().dump(original_dbt_project_yaml, f)
