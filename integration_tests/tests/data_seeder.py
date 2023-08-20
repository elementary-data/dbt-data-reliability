import csv
from typing import List

import dbt_project
from elementary.clients.dbt.dbt_runner import DbtRunner
from logger import get_logger

# TODO: Write more performant data seeders per adapter.

logger = get_logger(__name__)


class DbtDataSeeder:
    def __init__(self, dbt_runner: DbtRunner):
        self.dbt_runner = dbt_runner

    def seed(self, data: List[dict], table_name: str):
        seed_path = dbt_project.SEEDS_DIR_PATH.joinpath(f"{table_name}.csv")
        try:
            with seed_path.open("w") as seed_file:
                relative_seed_path = seed_path.relative_to(dbt_project.PATH)
                writer = csv.DictWriter(seed_file, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
                seed_file.flush()
                self.dbt_runner.seed(select=str(relative_seed_path), full_refresh=True)
        finally:
            seed_path.unlink()
