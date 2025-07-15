from typing import Optional

import dbt_project
from elementary.clients.dbt.factory import RunnerMethod


class Environment:
    def __init__(
        self,
        target: str,
        project_dir: str,
        runner_method: Optional[RunnerMethod] = None,
    ):
        self.target = target
        self.dbt_runner = dbt_project.get_dbt_runner(
            target, project_dir, runner_method=runner_method
        )

    def clear(self):
        # drop schema in dremio doesnt work, but we run the dremio tests with docker so its not really important to drop the schema
        if self.target != "dremio":
            self.dbt_runner.run_operation("elementary_tests.clear_env")

    def init(self):
        self.dbt_runner.run(selector="init")
        self.dbt_runner.run(select="elementary")
