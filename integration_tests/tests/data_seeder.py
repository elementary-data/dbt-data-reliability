import csv
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Generator, List

from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from logger import get_logger

if TYPE_CHECKING:
    from adapter_query_runner import AdapterQueryRunner
    from pyhive.hive import Connection as HiveConnection

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


# Maximum number of rows per INSERT VALUES statement.
_INSERT_BATCH_SIZE = 500


class BaseDirectSeeder(ABC):
    """Base class for direct SQL seeders that bypass ``dbt seed``.

    Subclasses only need to define adapter-specific type names, value
    formatting, and CREATE TABLE syntax.  The shared logic -- CSV writing
    (so ``{{ ref() }}`` works), type inference, batched inserts, and
    cleanup -- lives here.
    """

    def __init__(
        self,
        query_runner: "AdapterQueryRunner",
        schema: str,
        seeds_dir_path: Path,
    ) -> None:
        self._query_runner = query_runner
        self._schema = schema
        self._seeds_dir_path = seeds_dir_path

    # ------------------------------------------------------------------
    # Abstract methods -- subclasses must implement these
    # ------------------------------------------------------------------

    @abstractmethod
    def _type_string(self) -> str:
        """Return the SQL type name for string columns."""

    @abstractmethod
    def _type_boolean(self) -> str:
        """Return the SQL type name for boolean columns."""

    @abstractmethod
    def _type_integer(self) -> str:
        """Return the SQL type name for integer columns."""

    @abstractmethod
    def _type_float(self) -> str:
        """Return the SQL type name for float columns."""

    @abstractmethod
    def _format_value(self, value: object, col_type: str) -> str:
        """Format a single Python value as a SQL literal for INSERT VALUES."""

    @abstractmethod
    def _create_table_sql(self, fq_table: str, col_defs: str) -> str:
        """Return the full CREATE TABLE statement for this adapter."""

    # ------------------------------------------------------------------
    # Shared logic
    # ------------------------------------------------------------------

    def _infer_column_type(self, values: List[object]) -> str:
        """Infer the best SQL type from a column's Python values.

        Checks native Python types first (bool before int, since bool is
        a subclass of int).  Falls back to parsing string representations
        for numeric detection.
        """
        non_null = [
            v for v in values if v is not None and not (isinstance(v, str) and v == "")
        ]
        if not non_null:
            return self._type_string()

        # Python booleans must be checked before int (bool is subclass of int).
        if all(isinstance(v, bool) for v in non_null):
            return self._type_boolean()

        if all(isinstance(v, int) and not isinstance(v, bool) for v in non_null):
            return self._type_integer()
        if all(
            isinstance(v, (int, float)) and not isinstance(v, bool) for v in non_null
        ):
            return self._type_float()

        # Fallback: try parsing string representations.
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
            return self._type_integer()
        if all_float:
            return self._type_float()
        return self._type_string()

    def _write_csv(self, data: List[dict], table_name: str) -> Path:
        """Write a CSV so dbt discovers the seed node (needed for ``{{ ref() }}``)."""
        columns = list(data[0].keys())
        seed_path = self._seeds_dir_path / f"{table_name}.csv"
        with seed_path.open("w") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(data)
        return seed_path

    @contextmanager
    def seed(self, data: List[dict], table_name: str) -> Generator[None, None, None]:
        """Create a table with correctly-typed columns and insert data.

        A CSV file is written to the seeds directory so that dbt can
        discover the seed node for ``{{ ref() }}`` resolution.  The file
        is removed when the context manager exits.
        """
        columns = list(data[0].keys())
        col_types: Dict[str, str] = {
            col: self._infer_column_type([row.get(col) for row in data])
            for col in columns
        }
        col_defs = ", ".join(f"`{col}` {col_types[col]}" for col in columns)
        fq_table = f"`{self._schema}`.`{table_name}`"

        # Write a CSV so dbt discovers the seed node.
        seed_path = self._write_csv(data, table_name)

        try:
            self._query_runner.execute_sql(f"DROP TABLE IF EXISTS {fq_table}")
            self._query_runner.execute_sql(self._create_table_sql(fq_table, col_defs))

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
                self._query_runner.execute_sql(
                    f"INSERT INTO {fq_table} VALUES {rows_sql}"
                )

            logger.info(
                "%s: loaded %d rows into %s (%s)",
                type(self).__name__,
                len(data),
                fq_table,
                ", ".join(f"{c}: {t}" for c, t in col_types.items()),
            )

            yield
        finally:
            seed_path.unlink(missing_ok=True)


class SparkDirectSeeder:
    """Fast seeder for Spark using a direct PyHive/Thrift connection.

    Uses PyHive to execute ``CREATE TABLE`` + ``INSERT INTO`` directly,
    bypassing both ``dbt seed`` and the ``AdapterQueryRunner``.  This is
    important because ``AdapterQueryRunner._create_adapter()`` calls
    ``set_from_args()`` / ``reset_adapters()`` which corrupt global dbt
    state.  Since the test runner uses the in-process ``APIDbtRunner``
    (``dbtRunner().invoke()``), corrupted global state causes subsequent
    ``dbt test`` calls to run with wrong flags, leading to 3-10x
    regressions on multi-call tests (e.g. volume_anomaly).
    """

    _INSERT_BATCH_SIZE = 500

    def __init__(
        self,
        host: str,
        port: int,
        schema: str,
        seeds_dir_path: Path,
    ) -> None:
        self._host = host
        self._port = port
        self._schema = schema
        self._seeds_dir_path = seeds_dir_path

    # ------------------------------------------------------------------
    # Connection helper
    # ------------------------------------------------------------------

    def _connect(self) -> "HiveConnection":
        from pyhive import hive

        return hive.connect(host=self._host, port=self._port)

    def _execute(self, conn: "HiveConnection", sql: str) -> None:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    # Type inference (same logic as BaseDirectSeeder)
    # ------------------------------------------------------------------

    @staticmethod
    def _infer_column_type(values: List[object]) -> str:
        non_null = [
            v for v in values if v is not None and not (isinstance(v, str) and v == "")
        ]
        if not non_null:
            return "STRING"

        if all(isinstance(v, bool) for v in non_null):
            return "BOOLEAN"
        if all(isinstance(v, int) and not isinstance(v, bool) for v in non_null):
            return "BIGINT"
        if all(
            isinstance(v, (int, float)) and not isinstance(v, bool) for v in non_null
        ):
            return "DOUBLE"

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

    # ------------------------------------------------------------------
    # Value formatting
    # ------------------------------------------------------------------

    @staticmethod
    def _format_value(value: object, col_type: str) -> str:
        if value is None or (isinstance(value, str) and value == ""):
            return "NULL"
        if col_type == "BOOLEAN":
            return "TRUE" if str(value).lower() in ("true", "1") else "FALSE"
        if col_type in ("BIGINT", "DOUBLE"):
            return str(value)
        text = str(value)
        text = text.replace("\\", "\\\\")
        text = text.replace("'", "\\'")
        text = text.replace("\n", " ").replace("\r", " ")
        return f"'{text}'"

    # ------------------------------------------------------------------
    # CSV helper
    # ------------------------------------------------------------------

    def _write_csv(self, data: List[dict], table_name: str) -> Path:
        columns = list(data[0].keys())
        seed_path = self._seeds_dir_path / f"{table_name}.csv"
        with seed_path.open("w") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(data)
        return seed_path

    # ------------------------------------------------------------------
    # Seed
    # ------------------------------------------------------------------

    @contextmanager
    def seed(self, data: List[dict], table_name: str) -> Generator[None, None, None]:
        """Create a Delta table via PyHive and insert data."""
        columns = list(data[0].keys())
        col_types: Dict[str, str] = {
            col: self._infer_column_type([row.get(col) for row in data])
            for col in columns
        }
        col_defs = ", ".join(f"`{col}` {col_types[col]}" for col in columns)
        fq_table = f"`{self._schema}`.`{table_name}`"

        seed_path = self._write_csv(data, table_name)

        conn = self._connect()
        try:
            self._execute(conn, f"DROP TABLE IF EXISTS {fq_table}")
            self._execute(conn, f"CREATE TABLE {fq_table} ({col_defs}) USING DELTA")

            for batch_start in range(0, len(data), self._INSERT_BATCH_SIZE):
                batch = data[batch_start : batch_start + self._INSERT_BATCH_SIZE]
                rows_sql = ", ".join(
                    "("
                    + ", ".join(
                        self._format_value(row.get(c), col_types[c]) for c in columns
                    )
                    + ")"
                    for row in batch
                )
                self._execute(conn, f"INSERT INTO {fq_table} VALUES {rows_sql}")

            logger.info(
                "SparkDirectSeeder: loaded %d rows into %s (%s)",
                len(data),
                fq_table,
                ", ".join(f"{c}: {t}" for c, t in col_types.items()),
            )

            yield
        finally:
            seed_path.unlink(missing_ok=True)
            conn.close()


class ClickHouseDirectSeeder(BaseDirectSeeder):
    """Fast seeder for ClickHouse: executes CREATE TABLE + INSERT directly.

    Column types are wrapped in ``Nullable()`` so that NULL values are
    preserved correctly (ClickHouse columns are non-Nullable by default).
    """

    def _type_string(self) -> str:
        return "Nullable(String)"

    def _type_boolean(self) -> str:
        return "Nullable(Bool)"

    def _type_integer(self) -> str:
        return "Nullable(Int64)"

    def _type_float(self) -> str:
        return "Nullable(Float64)"

    def _format_value(self, value: object, col_type: str) -> str:
        if value is None or (isinstance(value, str) and value == ""):
            return "NULL"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        text = str(value)
        text = text.replace("\\", "\\\\")
        text = text.replace("'", "\\'")
        return f"'{text}'"

    def _create_table_sql(self, fq_table: str, col_defs: str) -> str:
        return (
            f"CREATE TABLE {fq_table} ({col_defs}) "
            f"ENGINE = MergeTree() ORDER BY tuple()"
        )
