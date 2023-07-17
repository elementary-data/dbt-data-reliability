import csv
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile
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
        with NamedTemporaryFile(
            mode="w", dir=dbt_project.SEEDS_DIR_PATH, suffix=".csv"
        ) as seed_file:
            relative_seed_path = Path(seed_file.name).relative_to(dbt_project.PATH)
            writer = csv.DictWriter(seed_file, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
            logger.info(f"Seeding {table_name} with {len(data)} rows.")
            self.dbt_runner.seed(select=str(relative_seed_path), full_refresh=True)
            logger.info(f"Seeded {table_name} with {len(data)} rows.")
            yield
