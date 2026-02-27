import time
from typing import Optional

import dbt_project
from elementary.clients.dbt.factory import RunnerMethod
from logger import get_logger

logger = get_logger(__name__)

# Retry settings for transient connection errors (e.g. Dremio Docker
# dropping connections under concurrent load from pytest-xdist workers).
_INIT_MAX_RETRIES = 3
_INIT_RETRY_DELAY_SECONDS = 15


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
        self._run_with_retry(
            label="dbt run --selector init",
            run_fn=lambda: self.dbt_runner.run(selector="init"),
            error_detail=(
                "The target schema may not have been created. "
                "Subsequent seeds and queries will likely fail with "
                "SCHEMA_NOT_FOUND or TABLE_OR_VIEW_NOT_FOUND."
            ),
        )
        self._run_with_retry(
            label="dbt run --select elementary",
            run_fn=lambda: self.dbt_runner.run(select="elementary"),
            error_detail="Elementary models may not be available.",
        )

    @staticmethod
    def _run_with_retry(label: str, run_fn, error_detail: str) -> None:
        """Execute *run_fn* with retries for transient failures.

        Dremio OSS (Docker) intermittently drops TCP connections under
        concurrent load from pytest-xdist workers, producing
        ``RemoteDisconnected`` / ``ConnectionError``.  Retrying after a
        short delay is sufficient to recover.
        """
        for attempt in range(1, _INIT_MAX_RETRIES + 1):
            if run_fn():
                return
            if attempt < _INIT_MAX_RETRIES:
                logger.warning(
                    "'%s' failed (attempt %d/%d). Retrying in %ds...",
                    label,
                    attempt,
                    _INIT_MAX_RETRIES,
                    _INIT_RETRY_DELAY_SECONDS,
                )
                time.sleep(_INIT_RETRY_DELAY_SECONDS)

        logger.error(
            "Environment init failed: '%s' returned failure after " "%d attempts. %s",
            label,
            _INIT_MAX_RETRIES,
            error_detail,
        )
        raise RuntimeError(
            f"Test environment initialization failed during "
            f"'{label}' after {_INIT_MAX_RETRIES} attempts. "
            f"Check the dbt output above for the root cause."
        )
