import csv
from contextlib import contextmanager
from typing import List

import dbt_project

# TODO: Write more performant data seeders per adapter.


class DbtDataSeeder:
    def __init__(self):
        self.dbt_runner = dbt_project.get_dbt_runner()

    @contextmanager
    def seed_data(self, data: List[dict], table_name: str):
        seed_path = dbt_project.PATH / "data" / f"{table_name}.csv"
        try:
            with open(seed_path, "w") as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            self.dbt_runner.seed(select=table_name, full_refresh=True)
            yield
        finally:
            seed_path.unlink()
