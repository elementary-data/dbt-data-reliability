import csv
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Generator, List

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from logger import get_logger

if TYPE_CHECKING:
    from adapter_query_runner import AdapterQueryRunner

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
                success = self.dbt_runner.seed(
                    select=str(relative_seed_path), full_refresh=True
                )
                if not success:
                    logger.error(
                        "dbt seed failed for '%s'. This usually means the "
                        "target schema does not exist or could not be created. "
                        "Downstream queries will fail with "
                        "TABLE_OR_VIEW_NOT_FOUND.",
                        table_name,
                    )
                    raise RuntimeError(
                        f"dbt seed failed for '{table_name}'. Check the dbt "
                        f"output above for the root cause (e.g. SCHEMA_NOT_FOUND)."
                    )

                yield
        finally:
            seed_path.unlink()


# Maximum number of rows per INSERT VALUES statement.
_INSERT_BATCH_SIZE = 500


class ClickHouseDirectSeeder:
    """Fast seeder for ClickHouse: executes CREATE TABLE + INSERT directly.

    Bypasses ``dbt seed`` entirely, avoiding the subprocess overhead and
    the need for post-hoc NULL repair.  All columns are created as
    ``Nullable(String)`` so that NULL values are preserved correctly
    (ClickHouse columns are non-Nullable by default).
    """

    def __init__(self, query_runner: "AdapterQueryRunner", schema: str) -> None:
        self._query_runner = query_runner
        self._schema = schema

    @staticmethod
    def _escape(value: object) -> str:
        """Escape a value for a ClickHouse SQL string literal."""
        if value is None or (isinstance(value, str) and value == ""):
            return "NULL"
        text = str(value)
        text = text.replace("\\", "\\\\")
        text = text.replace("'", "\\'")
        return f"'{text}'"

    @contextmanager
    def seed(self, data: List[dict], table_name: str) -> Generator[None, None, None]:
        """Create a table with Nullable(String) columns and insert data."""
        columns = list(data[0].keys())
        col_defs = ", ".join(f"`{col}` Nullable(String)" for col in columns)
        fq_table = f"`{self._schema}`.`{table_name}`"

        self._query_runner.execute_sql(f"DROP TABLE IF EXISTS {fq_table}")
        self._query_runner.execute_sql(
            f"CREATE TABLE {fq_table} ({col_defs}) "
            f"ENGINE = MergeTree() ORDER BY tuple()"
        )

        for batch_start in range(0, len(data), _INSERT_BATCH_SIZE):
            batch = data[batch_start : batch_start + _INSERT_BATCH_SIZE]
            rows_sql = ", ".join(
                "(" + ", ".join(self._escape(row.get(c)) for c in columns) + ")"
                for row in batch
            )
            self._query_runner.execute_sql(f"INSERT INTO {fq_table} VALUES {rows_sql}")

        logger.info(
            "ClickHouseDirectSeeder: loaded %d rows into %s", len(data), fq_table
        )

        yield
