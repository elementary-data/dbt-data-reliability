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
    the need for post-hoc NULL repair.  Column types are inferred from the
    Python values in the seed data and wrapped in ``Nullable()`` so that
    NULL values are preserved correctly (ClickHouse columns are
    non-Nullable by default).
    """

    def __init__(self, query_runner: "AdapterQueryRunner", schema: str) -> None:
        self._query_runner = query_runner
        self._schema = schema

    @staticmethod
    def _infer_column_type(values: List[object]) -> str:
        """Infer a ClickHouse column type from a list of Python values.

        Examines non-None, non-empty-string values and returns a
        ``Nullable(...)`` type string.  Falls back to ``Nullable(String)``
        when all values are None/empty or when types are mixed.
        """
        non_null = [v for v in values if v is not None and v != ""]
        if not non_null:
            return "Nullable(String)"

        # bool is a subclass of int in Python, so check it first.
        # dbt seed infers "True"/"False" CSV values as boolean; dbt-clickhouse
        # maps this to Bool (alias for UInt8).
        if all(isinstance(v, bool) for v in non_null):
            return "Nullable(Bool)"
        if all(isinstance(v, int) and not isinstance(v, bool) for v in non_null):
            return "Nullable(Int64)"
        if all(
            isinstance(v, (int, float)) and not isinstance(v, bool) for v in non_null
        ):
            return "Nullable(Float64)"
        return "Nullable(String)"

    @staticmethod
    def _escape(value: object) -> str:
        """Escape a value for a ClickHouse SQL literal.

        Returns ``NULL`` for None / empty-string, unquoted literals for
        numeric / boolean types, and a quoted+escaped string otherwise.
        """
        if value is None or (isinstance(value, str) and value == ""):
            return "NULL"
        # Booleans → ClickHouse Bool literals (true/false).
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        text = str(value)
        text = text.replace("\\", "\\\\")
        text = text.replace("'", "\\'")
        return f"'{text}'"

    @contextmanager
    def seed(self, data: List[dict], table_name: str) -> Generator[None, None, None]:
        """Create a table with correctly-typed Nullable columns and insert data."""
        columns = list(data[0].keys())
        col_types = {
            col: self._infer_column_type([row.get(col) for row in data])
            for col in columns
        }
        col_defs = ", ".join(f"`{col}` {col_types[col]}" for col in columns)
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
            "ClickHouseDirectSeeder: loaded %d rows into %s (%s)",
            len(data),
            fq_table,
            ", ".join(f"{c}: {t}" for c, t in col_types.items()),
        )

        yield
