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
    """Default seeder: writes a CSV and calls ``dbt seed``."""

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


# Maximum number of rows per INSERT VALUES statement.  Spark's Thrift
# protocol can choke on very large statements, so we batch inserts.
_INSERT_BATCH_SIZE = 500


class SparkDirectSeeder:
    """Fast seeder for Spark: executes CREATE TABLE + INSERT directly.

    Bypasses the ``dbt seed`` subprocess entirely, avoiding the ~4 s
    Python/manifest-parsing overhead per invocation.  Column types are
    inferred from the data to match ``dbt seed`` behaviour (which uses
    agate for type inference).
    """

    def __init__(self, query_runner: "AdapterQueryRunner", schema: str) -> None:
        self._query_runner = query_runner
        self._schema = schema

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _infer_spark_type(values: List[object]) -> str:
        """Infer the best Spark SQL type from a column's values."""
        non_null = [
            v for v in values if v is not None and not (isinstance(v, str) and v == "")
        ]
        if not non_null:
            return "STRING"

        # Python booleans must be checked before int (bool is subclass of int)
        if all(isinstance(v, bool) for v in non_null):
            return "BOOLEAN"

        # Check native Python types first (before stringifying)
        if all(isinstance(v, int) and not isinstance(v, bool) for v in non_null):
            return "BIGINT"
        if all(
            isinstance(v, (int, float)) and not isinstance(v, bool) for v in non_null
        ):
            return "DOUBLE"

        # Fallback: try parsing string representations
        all_int = True
        all_float = True
        for v in non_null:
            text = str(v)
            try:
                int(text)
            except (ValueError, TypeError):
                all_int = False
            try:
                float(text)
            except (ValueError, TypeError):
                all_float = False
            if not all_int and not all_float:
                break

        if all_int:
            return "BIGINT"
        if all_float:
            return "DOUBLE"
        return "STRING"

    @staticmethod
    def _format_value(value: object, col_type: str) -> str:
        """Format a single value for a Spark SQL INSERT VALUES clause."""
        if value is None or (isinstance(value, str) and value == ""):
            return "NULL"

        if col_type == "BOOLEAN":
            # Python bool or string "True"/"False"
            return "TRUE" if str(value).lower() in ("true", "1") else "FALSE"

        if col_type in ("BIGINT", "DOUBLE"):
            return str(value)

        # STRING — escape for Spark SQL literal
        text = str(value)
        text = text.replace("\\", "\\\\")
        text = text.replace("'", "\\'")
        text = text.replace("\n", " ").replace("\r", " ")
        return f"'{text}'"

    # ------------------------------------------------------------------
    # public API (same shape as DbtDataSeeder)
    # ------------------------------------------------------------------

    @contextmanager
    def seed(self, data: List[dict], table_name: str) -> Generator[None, None, None]:
        columns = list(data[0].keys())

        # Infer types from the actual data values.
        col_types = {}
        for col in columns:
            col_values = [row.get(col) for row in data]
            col_types[col] = self._infer_spark_type(col_values)

        col_defs = ", ".join(f"`{col}` {col_types[col]}" for col in columns)
        fq_table = f"`{self._schema}`.`{table_name}`"

        # DROP + CREATE is the fastest way to get a clean table.
        self._query_runner.execute_sql(f"DROP TABLE IF EXISTS {fq_table}")
        self._query_runner.execute_sql(
            f"CREATE TABLE {fq_table} ({col_defs}) USING DELTA"
        )

        # Insert in batches.
        for batch_start in range(0, len(data), _INSERT_BATCH_SIZE):
            batch = data[batch_start : batch_start + _INSERT_BATCH_SIZE]
            rows_sql = ", ".join(
                "("
                + ", ".join(
                    self._format_value(row.get(c), col_types[c]) for c in columns
                )
                + ")"
                for row in batch
            )
            self._query_runner.execute_sql(f"INSERT INTO {fq_table} VALUES {rows_sql}")

        logger.info("SparkDirectSeeder: loaded %d rows into %s", len(data), fq_table)

        yield
