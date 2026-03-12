import csv
import os
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, ClassVar, Dict, Generator, List, Mapping, Optional

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
        """Initialise with a dbt runner, project path and seeds directory."""
        self.dbt_runner = dbt_runner
        self.dbt_project_path = dbt_project_path
        self.seeds_dir_path = seeds_dir_path

    @contextmanager
    def seed(self, data: List[dict], table_name: str) -> Generator[None, None, None]:
        """Write *data* as a CSV, run ``dbt seed``, and clean up on exit."""
        if not data:
            raise ValueError(f"Seed data for '{table_name}' must not be empty")
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


def infer_column_type_tag(values: List[object]) -> str:
    """Infer an abstract type tag for a column from its Python values.

    Returns one of ``'string'``, ``'boolean'``, ``'integer'``, or
    ``'float'``.  Checks native Python types first (bool before int,
    since ``bool`` is a subclass of ``int``).  Falls back to parsing
    string representations for numeric detection.
    """
    non_null = [
        v for v in values if v is not None and not (isinstance(v, str) and v == "")
    ]
    if not non_null:
        return "string"

    # Python booleans must be checked before int (bool is subclass of int).
    if all(isinstance(v, bool) for v in non_null):
        return "boolean"

    if all(isinstance(v, int) and not isinstance(v, bool) for v in non_null):
        return "integer"
    if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in non_null):
        return "float"

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
        return "integer"
    if all_float:
        return "float"
    return "string"


class BaseSqlInsertSeeder(ABC):
    """Base class for seeders that load data via SQL INSERT statements.

    Subclasses only need to define adapter-specific type names, value
    formatting, and CREATE TABLE syntax.  The shared logic -- CSV writing
    (so ``{{ ref() }}`` works), type inference, batched inserts, and
    cleanup -- lives here.
    """

    def __init__(
        self,
        query_runner: Optional["AdapterQueryRunner"],
        schema: str,
        seeds_dir_path: Path,
    ) -> None:
        """Initialise with a query runner, target schema and seeds directory."""
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

    # Mapping from abstract type tags to adapter-specific type getters.
    _TYPE_TAG_MAP: ClassVar[Mapping[str, str]] = MappingProxyType(
        {
            "string": "_type_string",
            "boolean": "_type_boolean",
            "integer": "_type_integer",
            "float": "_type_float",
        }
    )

    def _infer_column_type(self, values: List[object]) -> str:
        """Infer the best SQL type from a column's Python values.

        Delegates to the shared :func:`infer_column_type_tag` helper and
        maps the abstract tag to the adapter-specific type name via the
        subclass's ``_type_*`` methods.
        """
        tag = infer_column_type_tag(values)
        getter = getattr(self, self._TYPE_TAG_MAP[tag])
        return getter()

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


class SparkS3CsvSeeder:
    """Seeder for Spark that uploads CSVs to MinIO (S3) and creates external tables.

    Bypasses ``dbt seed`` entirely — Spark reads the CSV natively via
    ``CREATE TABLE ... USING CSV``.  This avoids the ``_fix_binding`` bug
    in dbt-spark's session adapter that converts Python ``None`` to the
    string literal ``'None'`` instead of SQL NULL.

    SQL commands are executed via PyHive directly (not through dbt's
    adapter) to avoid corrupting dbt global state (``set_from_args`` /
    ``reset_adapters``).

    NULL handling: Python ``None`` values are written as empty cells in the
    CSV.  Spark's CSV reader treats empty strings as NULL for non-string
    columns by default.  For string columns we set ``nullValue=''`` so
    that empty cells are also read as NULL.

    The S3 CSV files are **not** deleted after the test — they live in
    ephemeral MinIO storage that is destroyed with ``docker compose down``.
    The external table continues to reference the S3 path throughout the
    test lifecycle.
    """

    # MinIO connection defaults (matching docker-compose-spark.yml).
    _MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
    _MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")  # noqa: S105
    _MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")  # noqa: S105
    _S3_BUCKET = os.environ.get("MINIO_BUCKET", "spark-seeds")

    # Spark Thrift Server connection defaults.
    _THRIFT_HOST = os.environ.get("SPARK_THRIFT_HOST", "127.0.0.1")
    _THRIFT_PORT = int(os.environ.get("SPARK_THRIFT_PORT", "10000"))

    def __init__(
        self,
        schema: str,
        seeds_dir_path: Path,
    ) -> None:
        """Initialise with the target Spark schema and seeds directory."""
        self._schema = schema
        self._seeds_dir_path = seeds_dir_path

    def _get_s3_client(self):  # type: ignore[no-untyped-def]
        """Return a boto3 S3 client configured for the local MinIO endpoint."""
        import boto3

        return boto3.client(
            "s3",
            endpoint_url=self._MINIO_ENDPOINT,
            aws_access_key_id=self._MINIO_ACCESS_KEY,
            aws_secret_access_key=self._MINIO_SECRET_KEY,
        )

    @contextmanager
    def _spark_connection(self):  # type: ignore[no-untyped-def]
        """Open a single PyHive connection for the duration of a seed operation."""
        from pyhive import hive

        conn = hive.connect(
            host=self._THRIFT_HOST,
            port=self._THRIFT_PORT,
            username="dbt",
        )
        try:
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _execute(conn, sql: str) -> None:  # type: ignore[no-untyped-def]
        """Execute a single SQL statement on an existing connection."""
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
        finally:
            cursor.close()

    def _write_seed_csv(self, data: List[dict], table_name: str) -> Path:
        """Write a CSV with proper NULL handling.

        ``None`` values are written as empty strings so that Spark's CSV
        reader interprets them as SQL NULL (via ``nullValue ''``).

        ``QUOTE_ALL`` is used so that empty-string cells are emitted as
        ``""`` rather than blank lines — Spark's CSV reader silently
        skips blank lines, which would lose rows containing only NULL
        columns.
        """
        columns = list(data[0].keys())
        seed_path = self._seeds_dir_path / f"{table_name}.csv"
        with seed_path.open("w", newline="") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(columns)
            for row in data:
                writer.writerow(
                    "" if row.get(c) is None else row.get(c) for c in columns
                )
        return seed_path

    # Mapping from abstract type tags to Spark SQL type names.
    _SPARK_TYPE_MAP: ClassVar[Mapping[str, str]] = MappingProxyType(
        {
            "string": "STRING",
            "boolean": "BOOLEAN",
            "integer": "BIGINT",
            "float": "DOUBLE",
        }
    )

    def _infer_spark_schema(self, data: List[dict]) -> str:
        """Build a Spark SQL schema string from the data."""
        columns = list(data[0].keys())
        parts = []
        for col in columns:
            values = [row.get(col) for row in data]
            tag = infer_column_type_tag(values)
            col_type = self._SPARK_TYPE_MAP[tag]
            parts.append(f"`{col}` {col_type}")
        return ", ".join(parts)

    @contextmanager
    def seed(self, data: List[dict], table_name: str) -> Generator[None, None, None]:
        """Upload CSV to MinIO and create a Spark external table.

        The CSV is also written locally so dbt discovers the seed node
        for ``{{ ref() }}`` resolution.  The local CSV is cleaned up
        when the context manager exits to prevent dbt compilation
        errors (duplicate resource names).  The S3 object is **not**
        deleted — the external table references it throughout the test,
        and MinIO storage is ephemeral (destroyed with
        ``docker compose down``).
        """
        if not data:
            raise ValueError(f"Seed data for '{table_name}' must not be empty")

        seed_path = self._write_seed_csv(data, table_name)
        s3_key = f"{self._schema}/{table_name}.csv"
        fq_table = f"`{self._schema}`.`{table_name}`"

        try:
            # Upload CSV to MinIO.
            s3 = self._get_s3_client()
            s3.upload_file(str(seed_path), self._S3_BUCKET, s3_key)

            # Create external table in Spark reading from S3.
            schema_ddl = self._infer_spark_schema(data)
            s3_path = f"s3a://{self._S3_BUCKET}/{s3_key}"

            with self._spark_connection() as conn:
                self._execute(conn, f"CREATE DATABASE IF NOT EXISTS `{self._schema}`")
                self._execute(conn, f"DROP TABLE IF EXISTS {fq_table}")
                self._execute(
                    conn,
                    f"CREATE TABLE {fq_table} ({schema_ddl}) "
                    f"USING CSV "
                    f"OPTIONS ("
                    f"  path '{s3_path}',"
                    f"  header 'true',"
                    f"  nullValue ''"
                    f")",
                )

            logger.info(
                "SparkS3CsvSeeder: loaded %d rows into %s via %s",
                len(data),
                fq_table,
                s3_path,
            )

            yield
        finally:
            seed_path.unlink(missing_ok=True)


class ClickHouseDirectSeeder(BaseSqlInsertSeeder):
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


class VerticaDirectSeeder(BaseSqlInsertSeeder):
    """Fast seeder for Vertica: executes CREATE TABLE + INSERT directly.

    Bypasses ``dbt seed`` (which uses Vertica's COPY command) because COPY
    rejects empty CSV fields for non-string columns instead of treating them
    as NULL.  Direct INSERT statements handle NULL correctly.

    Uses a *direct* ``vertica_python`` connection (rather than dbt's adapter
    connection pool) so that all DDL + DML runs in a single session and can
    be committed atomically.  dbt's ``connection_named`` context manager
    releases (and effectively rolls back) the connection after each
    ``execute_sql`` call, which caused INSERT data to be invisible to
    subsequent ``dbt test`` sessions.

    Vertica uses double-quote identifiers (not backticks), so this class
    overrides the ``seed`` method to use ``"col"`` quoting.
    """

    def _type_string(self) -> str:
        # Must match edr_type_string (varchar(16000)) so that schema-change
        # detection sees a consistent type between seeded tables and
        # elementary metadata columns.
        return "VARCHAR(16000)"

    def _type_boolean(self) -> str:
        return "BOOLEAN"

    def _type_integer(self) -> str:
        return "INTEGER"

    def _type_float(self) -> str:
        return "FLOAT"

    def _format_value(self, value: object, col_type: str) -> str:
        if value is None or (isinstance(value, str) and value == ""):
            return "NULL"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        text = str(value)
        text = text.replace("'", "''")
        return f"'{text}'"

    def _create_table_sql(self, fq_table: str, col_defs: str) -> str:
        return f"CREATE TABLE {fq_table} ({col_defs})"

    @staticmethod
    def _vertica_connection():
        """Open a direct vertica_python connection from env / defaults."""
        import vertica_python  # available in the test venv

        conn_info = {
            "host": os.environ.get("VERTICA_HOST", "localhost"),
            "port": int(os.environ.get("VERTICA_PORT", "5433")),
            "user": os.environ.get("VERTICA_USER", "dbadmin"),
            "password": os.environ.get("VERTICA_PASSWORD", "vertica"),
            "database": os.environ.get("VERTICA_DATABASE", "elementary_tests"),
        }
        return vertica_python.connect(**conn_info)

    @contextmanager
    def seed(self, data: List[dict], table_name: str) -> Generator[None, None, None]:
        """Override base seed to use double-quote identifiers for Vertica."""
        if not data:
            raise ValueError(f"Seed data for '{table_name}' must not be empty")
        columns = list(data[0].keys())
        col_types: Dict[str, str] = {
            col: self._infer_column_type([row.get(col) for row in data])
            for col in columns
        }
        # Vertica uses double-quote identifiers, not backticks.
        col_defs = ", ".join(f'"{col}" {col_types[col]}' for col in columns)
        fq_table = f'"{self._schema}"."{table_name}"'

        seed_path = self._write_csv(data, table_name)

        try:
            # Use a direct connection so DDL + DML share the same session
            # and the COMMIT is guaranteed to persist the data.
            conn = self._vertica_connection()
            try:
                cur = conn.cursor()
                cur.execute(f"DROP TABLE IF EXISTS {fq_table}")
                cur.execute(self._create_table_sql(fq_table, col_defs))

                for batch_start in range(0, len(data), _INSERT_BATCH_SIZE):
                    batch = data[batch_start : batch_start + _INSERT_BATCH_SIZE]
                    rows_sql = ", ".join(
                        "("
                        + ", ".join(
                            self._format_value(row.get(c), col_types[c])
                            for c in columns
                        )
                        + ")"
                        for row in batch
                    )
                    cur.execute(f"INSERT INTO {fq_table} VALUES {rows_sql}")

                conn.commit()
            finally:
                conn.close()

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
