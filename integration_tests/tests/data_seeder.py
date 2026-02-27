import csv
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, List

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from logger import get_logger

# TODO: Write more performant data seeders per adapter.

# Retry settings for transient connection errors (e.g. Dremio Docker
# dropping connections under concurrent load from pytest-xdist workers).
_SEED_MAX_RETRIES = 3
_SEED_RETRY_DELAY_SECONDS = 10

logger = get_logger(__name__)


class DbtDataSeeder:
    def __init__(
        self, dbt_runner: BaseDbtRunner, dbt_project_path: Path, seeds_dir_path: Path
    ):
        self.dbt_runner = dbt_runner
        self.dbt_project_path = dbt_project_path
        self.seeds_dir_path = seeds_dir_path

    @contextmanager
    def seed(self, data: List[dict], table_name: str) -> Generator[None, None, None]:
        seed_path = self.seeds_dir_path.joinpath(f"{table_name}.csv")
        try:
            with seed_path.open("w") as seed_file:
                relative_seed_path = seed_path.relative_to(self.dbt_project_path)
                writer = csv.DictWriter(seed_file, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
                seed_file.flush()
                success = self._seed_with_retry(str(relative_seed_path), table_name)
                if not success:
                    logger.error(
                        "dbt seed failed for '%s' after %d attempts. "
                        "This usually means the target schema does not "
                        "exist or could not be created. Downstream "
                        "queries will fail with TABLE_OR_VIEW_NOT_FOUND.",
                        table_name,
                        _SEED_MAX_RETRIES,
                    )
                    raise RuntimeError(
                        f"dbt seed failed for '{table_name}' after "
                        f"{_SEED_MAX_RETRIES} attempts. Check the dbt "
                        f"output above for the root cause "
                        f"(e.g. SCHEMA_NOT_FOUND)."
                    )

                yield
        finally:
            seed_path.unlink()

    def _seed_with_retry(self, relative_seed_path: str, table_name: str) -> bool:
        """Run dbt seed with retries for transient connection errors.

        Dremio OSS (Docker) intermittently drops TCP connections under
        concurrent load from pytest-xdist workers, producing
        ``RemoteDisconnected`` / ``ConnectionError`` during seed.
        A simple retry with a back-off delay is sufficient to recover.
        """
        for attempt in range(1, _SEED_MAX_RETRIES + 1):
            try:
                success = self.dbt_runner.seed(
                    select=relative_seed_path, full_refresh=True
                )
            except Exception:
                logger.exception(
                    "dbt seed raised an exception for '%s' (attempt %d/%d).",
                    table_name,
                    attempt,
                    _SEED_MAX_RETRIES,
                )
                success = False
            if success:
                return True
            if attempt < _SEED_MAX_RETRIES:
                logger.warning(
                    "dbt seed failed for '%s' (attempt %d/%d). " "Retrying in %ds...",
                    table_name,
                    attempt,
                    _SEED_MAX_RETRIES,
                    _SEED_RETRY_DELAY_SECONDS,
                )
                time.sleep(_SEED_RETRY_DELAY_SECONDS)
        return False
