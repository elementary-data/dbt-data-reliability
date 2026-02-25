"""Utilities for fixing ClickHouse seed tables where NULL values became default values.

ClickHouse columns are non-Nullable by default, so NULL values in CSV seeds become
default values (0 for Int, '' for String, etc.). This module provides functions to
repair those tables by:
1. Determining which columns had NULL values in the original data
2. Querying column types from system.columns
3. Rebuilding the table via INSERT SELECT with nullIf() to restore NULLs

Uses the ClickHouse HTTP API directly because dbt's run_query/statement
don't reliably execute DDL on ClickHouse.
"""

import os
import re
import urllib.parse
import urllib.request
from pathlib import Path
from typing import List, Set, Tuple

from logger import get_logger
from ruamel.yaml import YAML

logger = get_logger(__name__)

SCHEMA_NAME_SUFFIX_ENV = os.environ.get("PYTEST_XDIST_WORKER", None)
_SCHEMA_NAME_SUFFIX = f"_{SCHEMA_NAME_SUFFIX_ENV}" if SCHEMA_NAME_SUFFIX_ENV else ""


def get_clickhouse_schema(schema_name_suffix: str = _SCHEMA_NAME_SUFFIX) -> str:
    """Get the ClickHouse database (schema) name from dbt profiles.yml.

    In ClickHouse, database and schema are the same concept. The schema
    name comes from the dbt profile's 'schema' property, with the
    schema_name_suffix appended for parallel test workers.
    """
    profiles_path = Path.home() / ".dbt" / "profiles.yml"
    yaml = YAML()
    with open(profiles_path) as f:
        profiles = yaml.load(f)
    # Navigate to the clickhouse target schema
    base_schema = (
        profiles.get("elementary_tests", {})
        .get("outputs", {})
        .get("clickhouse", {})
        .get("schema", "default")
    )
    return f"{base_schema}{schema_name_suffix}"


def _get_clickhouse_connection_params() -> Tuple[str, str, str]:
    """Return (base_url, user, password) for ClickHouse HTTP API."""
    ch_host = os.environ.get("CLICKHOUSE_HOST", "localhost")
    ch_port = os.environ.get("CLICKHOUSE_PORT", "8123")
    ch_user = os.environ.get("CLICKHOUSE_USER", "default")
    ch_password = os.environ.get("CLICKHOUSE_PASSWORD", "default")
    ch_url = f"http://{ch_host}:{ch_port}"
    return ch_url, ch_user, ch_password


def clickhouse_query_with_api(query: str) -> str:
    """Execute a SQL query against ClickHouse via the HTTP API.

    Uses URL-encoded credentials to handle special characters safely.
    """
    ch_url, ch_user, ch_password = _get_clickhouse_connection_params()
    encoded = query.encode("utf-8")
    query_string = urllib.parse.urlencode(
        {"user": ch_user, "password": ch_password, "mutations_sync": 1}
    )
    req = urllib.request.Request(
        f"{ch_url}/?{query_string}",
        data=encoded,
    )
    with urllib.request.urlopen(req, timeout=60) as resp:  # noqa: S310
        return resp.read().decode("utf-8")


def _find_nullable_columns(data: List[dict]) -> Set[str]:
    """Find columns that contain at least one NULL value in the original data."""
    nullable_columns: Set[str] = set()
    for row in data:
        for col_name, value in row.items():
            if value is None:
                nullable_columns.add(col_name)
    return nullable_columns


def _get_column_types(schema: str, table_name: str) -> List[Tuple[str, str]]:
    """Query system.columns for (name, type) pairs of the given table."""
    # Validate identifiers to prevent SQL injection
    if not re.fullmatch(r"[A-Za-z0-9_]+", schema):
        raise ValueError(f"Invalid schema name: {schema!r}")
    if not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
        raise ValueError(f"Invalid table name: {table_name!r}")

    cols_result = clickhouse_query_with_api(
        f"SELECT name, type FROM system.columns "
        f"WHERE database = '{schema}' AND table = '{table_name}'"
    ).strip()
    if not cols_result:
        return []

    columns: List[Tuple[str, str]] = []
    for line in cols_result.split("\n"):
        parts = line.strip().split("\t")
        if len(parts) == 2:
            columns.append((parts[0], parts[1]))
    return columns


def _build_select_with_null_repair(
    columns: List[Tuple[str, str]], nullable_columns: Set[str]
) -> str:
    """Build a SELECT expression list that uses nullIf() for nullable columns."""
    select_exprs: List[str] = []
    for col_name, col_type in columns:
        if col_name in nullable_columns:
            # Strip Nullable(...) wrapper from a prior run to avoid
            # Nullable(Nullable(...)) nesting
            base_type = col_type
            if base_type.startswith("Nullable(") and base_type.endswith(")"):
                base_type = base_type[len("Nullable(") : -1]
            # Get the default value for this type to use with nullIf
            if (
                base_type == "String"
                or base_type.startswith("FixedString")
                or base_type.startswith("LowCardinality")
            ):
                default_val = "''"
            elif base_type.startswith("Int") or base_type.startswith("UInt"):
                default_val = "0"
            elif base_type.startswith("Float"):
                default_val = "0"
            else:
                default_val = "defaultValueOfTypeName('" + base_type + "')"
            select_exprs.append(
                f"nullIf(`{col_name}`, {default_val})::Nullable({base_type}) as `{col_name}`"
            )
        else:
            select_exprs.append(f"`{col_name}`")
    return ", ".join(select_exprs)


def _rebuild_table_with_nulls(
    schema: str,
    table_name: str,
    select_sql: str,
    nullable_columns: Set[str],
) -> None:
    """Rebuild a ClickHouse table using CREATE temp / EXCHANGE / DROP."""
    tmp_name = f"{table_name}_tmp_nullable"

    logger.info(
        "ClickHouse fix: rebuilding %s.%s with Nullable columns: %s",
        schema,
        table_name,
        nullable_columns,
    )

    clickhouse_query_with_api(f"DROP TABLE IF EXISTS {schema}.{tmp_name}")
    try:
        clickhouse_query_with_api(
            f"CREATE TABLE {schema}.{tmp_name} "
            f"ENGINE = MergeTree() ORDER BY tuple() "
            f"AS SELECT {select_sql} FROM {schema}.{table_name}"
        )
        clickhouse_query_with_api(
            f"EXCHANGE TABLES {schema}.{table_name} AND {schema}.{tmp_name}"
        )
    finally:
        clickhouse_query_with_api(f"DROP TABLE IF EXISTS {schema}.{tmp_name}")


def fix_clickhouse_seed_nulls(
    table_name: str, data: List[dict], schema_name_suffix: str
) -> None:
    """Fix ClickHouse seed tables where NULL values became default values.

    This is the main entry point. It:
    1. Finds which columns had NULL values in the original data
    2. Queries column types from system.columns
    3. Rebuilds the table via INSERT SELECT with nullIf() to restore NULLs
    """
    nullable_columns = _find_nullable_columns(data)
    if not nullable_columns:
        return

    schema = get_clickhouse_schema(schema_name_suffix)
    columns = _get_column_types(schema, table_name)
    if not columns:
        logger.warning(
            "ClickHouse fix: no columns found for %s.%s - "
            "schema may be wrong (using '%s'). NULLs will not be repaired.",
            schema,
            table_name,
            schema,
        )
        return

    select_sql = _build_select_with_null_repair(columns, nullable_columns)
    _rebuild_table_with_nulls(schema, table_name, select_sql, nullable_columns)
