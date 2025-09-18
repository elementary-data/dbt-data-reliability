import csv
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, List

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from logger import get_logger

# TODO: Write more performant data seeders per adapter.

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
                self.dbt_runner.seed(select=str(relative_seed_path), full_refresh=True)

                yield
        finally:
            seed_path.unlink()
