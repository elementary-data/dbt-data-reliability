import csv
from contextlib import contextmanager
from typing import List

import dbt_project
from elementary.clients.dbt.dbt_runner import DbtRunner
from logger import get_logger

# TODO: Write more performant data seeders per adapter.

logger = get_logger(__name__)


class DbtDataSeeder:
    def __init__(self, dbt_runner: DbtRunner):
        self.dbt_runner = dbt_runner

    @contextmanager
    def seed(self, data: List[dict], table_name: str):
        seed_path = dbt_project.PATH / "data" / f"{table_name}.csv"
        try:
            with open(seed_path, "w") as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            logger.info(f"Seeding {table_name} with {len(data)} rows.")
            self.dbt_runner.seed(select=table_name, full_refresh=True)
            logger.info(f"Seeded {table_name} with {len(data)} rows.")
            yield
        finally:
            seed_path.unlink()
