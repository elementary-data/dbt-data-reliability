from pathlib import Path

from elementary.clients.dbt.dbt_runner import DbtRunner

PATH = Path(__file__).parent.parent / "dbt_project"


def get_dbt_runner(raise_on_failure: bool = True):
    return DbtRunner(str(PATH), raise_on_failure=raise_on_failure)
