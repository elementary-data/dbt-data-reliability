from typing import Optional

import dbt_project
from elementary.clients.dbt.factory import RunnerMethod
from logger import get_logger

logger = get_logger(__name__)


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
        init_success = self.dbt_runner.run(selector="init")
        if not init_success:
            logger.error(
                "Environment init failed: 'dbt run --selector init' returned "
                "failure. The target schema may not have been created. "
                "Subsequent seeds and queries will likely fail with "
                "SCHEMA_NOT_FOUND or TABLE_OR_VIEW_NOT_FOUND."
            )
            raise RuntimeError(
                "Test environment initialization failed during "
                "'dbt run --selector init'. Check the dbt output above "
                "for the root cause."
            )
        elementary_success = self.dbt_runner.run(select="elementary")
        if not elementary_success:
            logger.error(
                "Environment init failed: 'dbt run --select elementary' "
                "returned failure. Elementary models may not be available."
            )
            raise RuntimeError(
                "Test environment initialization failed during "
                "'dbt run --select elementary'. Check the dbt output "
                "above for the root cause."
            )
