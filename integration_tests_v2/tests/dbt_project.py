from pathlib import Path

from elementary.clients.dbt.dbt_runner import DbtRunner

PATH = Path(__file__).parent.parent / "dbt_project"

_DEFAULT_VARS = {
    "disable_dbt_invocation_autoupload": True,
    "disable_dbt_artifacts_autoupload": True,
    "disable_run_results": True,
}


def get_dbt_runner(raise_on_failure: bool = False):
    return DbtRunner(str(PATH), vars=_DEFAULT_VARS, raise_on_failure=raise_on_failure)
