#!/usr/bin/env python3
"""Drop Athena/Glue schemas efficiently using batch API calls.

Unlike dbt's Jinja macros, which drop tables one-by-one, this script uses
Glue's ``BatchDeleteTable`` API to remove up to 100 tables per call.

Usage examples:

    # Drop explicit schema names
    python athena_drop_schemas.py my_schema_1 my_schema_2

    # Drop CI test schemas (base + xdist workers + _elementary variants)
    python athena_drop_schemas.py --ci-test-schemas my_base_schema --num-workers 8

    # Drop stale CI schemas older than 24 hours
    python athena_drop_schemas.py --stale --prefixes dbt_ --max-age-hours 24

AWS credentials are read from the environment (AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION) or from explicit options.
"""

from __future__ import annotations

import base64
import binascii
import json
import os
import re
import sys
from datetime import datetime, timedelta
from typing import Optional

import boto3
import click

BATCH_DELETE_LIMIT = 100


def _get_glue_client(
    region: str,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
):
    kwargs: dict = {"region_name": region}
    if aws_access_key_id and aws_secret_access_key:
        kwargs["aws_access_key_id"] = aws_access_key_id
        kwargs["aws_secret_access_key"] = aws_secret_access_key
    return boto3.client("glue", **kwargs)


def _list_tables(glue_client, database_name: str) -> list[str]:
    """Return all table names in a Glue database (schema)."""
    paginator = glue_client.get_paginator("get_tables")
    tables: list[str] = []
    try:
        for page in paginator.paginate(DatabaseName=database_name):
            tables.extend(t["Name"] for t in page.get("TableList", []))
    except glue_client.exceptions.EntityNotFoundException:
        pass
    return tables


def _batch_delete_tables(
    glue_client, database_name: str, table_names: list[str]
) -> tuple[int, int]:
    """Delete tables in batches of up to 100.

    Returns a tuple: (deleted, failed).

    - "deleted" counts tables successfully deleted in Glue.
    - "failed" counts tables that were attempted but failed to delete (either
      returned in Glue errors or part of a failed batch request).
    """
    deleted = 0
    failed = 0
    for i in range(0, len(table_names), BATCH_DELETE_LIMIT):
        chunk = table_names[i : i + BATCH_DELETE_LIMIT]
        try:
            resp = glue_client.batch_delete_table(
                DatabaseName=database_name, TablesToDelete=chunk
            )
            errors = resp.get("Errors", [])
            deleted += len(chunk) - len(errors)
            failed += len(errors)
            for err in errors:
                click.echo(
                    f"  warning: failed to delete {database_name}.{err['TableName']}: "
                    f"{err['ErrorDetail']['ErrorMessage']}",
                    err=True,
                )
        except Exception as exc:
            click.echo(
                f"  error: batch_delete_table failed for {database_name}: {exc}",
                err=True,
            )
            failed += len(chunk)
    return deleted, failed


def _delete_database(glue_client, database_name: str) -> bool:
    """Delete a Glue database.  Returns True on success."""
    try:
        glue_client.delete_database(Name=database_name)
        return True
    except glue_client.exceptions.EntityNotFoundException:
        return True
    except Exception as exc:
        click.echo(f"  error: delete_database({database_name}) failed: {exc}", err=True)
        return False


def _list_all_databases(glue_client) -> list[str]:
    """Return all database (schema) names in the Glue catalog."""
    paginator = glue_client.get_paginator("get_databases")
    databases: list[str] = []
    for page in paginator.paginate():
        databases.extend(db["Name"] for db in page.get("DatabaseList", []))
    return databases


# ── CI schema timestamp parsing ──────────────────────────────────────────
# Schema naming convention: <prefix><YYMMDD_HHMMSS>_<branch>_<hash>
# Optional suffixes: _elementary, _gw0 … _gw7
_CI_SCHEMA_RE = re.compile(r"^(?P<prefix>[a-z]+_)" r"(?P<ts>\d{6}_\d{6})" r"_")


def _parse_ci_schema_timestamp(
    schema_name: str, prefixes: list[str]
) -> Optional[datetime]:
    """Extract the timestamp from a CI schema name, or None if it doesn't match."""
    m = _CI_SCHEMA_RE.match(schema_name)
    if not m:
        return None
    if m.group("prefix") not in prefixes:
        return None
    try:
        return datetime.strptime(m.group("ts"), "%y%m%d_%H%M%S")
    except ValueError:
        return None


def drop_schemas(glue_client, schema_names: list[str]) -> bool:
    """Drop a list of schemas efficiently using batch table deletion.

    1. List all tables in all schemas up-front.
    2. Batch-delete tables per schema (up to 100 per API call).
    3. Delete the now-empty schemas.

    Returns True if all deletions succeeded, otherwise False.
    """
    if not schema_names:
        click.echo("No schemas to drop.")
        return True

    # Phase 1: collect all tables across all schemas
    schema_tables: dict[str, list[str]] = {}
    total_tables = 0
    for schema in schema_names:
        tables = _list_tables(glue_client, schema)
        if tables:
            schema_tables[schema] = tables
            total_tables += len(tables)

    click.echo(
        f"Found {total_tables} table(s) across {len(schema_tables)} non-empty schema(s) "
        f"(of {len(schema_names)} targeted)."
    )

    # Phase 2: batch-delete all tables
    total_deleted = 0
    total_failed = 0
    for schema, tables in schema_tables.items():
        click.echo(f"  Deleting {len(tables)} table(s) from {schema} ...")
        deleted, failed = _batch_delete_tables(glue_client, schema, tables)
        total_deleted += deleted
        total_failed += failed

    click.echo(f"Deleted {total_deleted} table(s).")

    # Phase 3: drop the now-empty schemas
    dropped = 0
    db_delete_failures = 0
    for schema in schema_names:
        if _delete_database(glue_client, schema):
            dropped += 1
        else:
            db_delete_failures += 1

    click.echo(f"Dropped {dropped}/{len(schema_names)} schema(s).")

    if total_failed or db_delete_failures:
        click.echo(
            "Cleanup completed with failures: "
            f"table_failures={total_failed}, database_failures={db_delete_failures}",
            err=True,
        )
        return False

    return True


def expand_ci_test_schemas(base_schema: str, num_workers: int) -> list[str]:
    """Expand a base schema name into the full list of CI schemas.

    For each suffix ("", "_gw0", …, "_gw<N-1>"), generates both the
    test schema and its _elementary counterpart.
    """
    schemas: list[str] = []
    suffixes = [""] + [f"_gw{i}" for i in range(num_workers)]
    for suffix in suffixes:
        schemas.append(f"{base_schema}{suffix}")
        schemas.append(f"{base_schema}_elementary{suffix}")
    return schemas


def _resolve_credentials(
    region: Optional[str],
    aws_access_key_id: Optional[str],
    aws_secret_access_key: Optional[str],
    secrets_json_env: str = "CI_WAREHOUSE_SECRETS",
) -> tuple[str, Optional[str], Optional[str]]:
    """Resolve AWS credentials, falling back to the CI secrets blob.

    Fills any missing values (region, access key, secret) from the base64-encoded
    JSON blob stored in *secrets_json_env*.

    If the blob is present but malformed, exits with a clear error.
    """
    blob = os.environ.get(secrets_json_env, "").strip()
    secrets: dict = {}

    if blob:
        try:
            secrets = json.loads(base64.b64decode(blob))
        except (binascii.Error, json.JSONDecodeError, TypeError) as exc:
            click.echo(f"error: failed to decode ${secrets_json_env}: {exc}", err=True)
            sys.exit(1)

    resolved_region = (
        region or secrets.get("athena_region") or secrets.get("ATHENA_REGION", "")
    )
    resolved_key = (
        aws_access_key_id
        or secrets.get("athena_aws_access_key_id")
        or secrets.get("ATHENA_AWS_ACCESS_KEY_ID")
    )
    resolved_secret = (
        aws_secret_access_key
        or secrets.get("athena_aws_secret_access_key")
        or secrets.get("ATHENA_AWS_SECRET_ACCESS_KEY")
    )

    if not resolved_region:
        click.echo(
            f"error: --region is required (or provide athena_region in ${secrets_json_env})",
            err=True,
        )
        sys.exit(1)

    if secrets:
        click.echo(f"Resolved Athena credentials from ${secrets_json_env}.")

    return resolved_region, resolved_key, resolved_secret


@click.command()
@click.argument("schemas", nargs=-1)
@click.option(
    "--region",
    envvar="AWS_DEFAULT_REGION",
    default=None,
    help="AWS region (auto-detected from CI_WAREHOUSE_SECRETS if omitted).",
)
@click.option("--aws-access-key-id", envvar="AWS_ACCESS_KEY_ID", default=None)
@click.option("--aws-secret-access-key", envvar="AWS_SECRET_ACCESS_KEY", default=None)
@click.option(
    "--ci-test-schemas",
    default=None,
    help="Base schema name; expands to base + xdist workers + _elementary variants.",
)
@click.option(
    "--num-workers", default=8, show_default=True, help="Number of xdist workers."
)
@click.option(
    "--stale",
    is_flag=True,
    default=False,
    help="Scan all schemas and drop those older than --max-age-hours.",
)
@click.option(
    "--prefixes",
    default=None,
    help="Comma-separated prefixes for --stale mode (e.g. 'dbt_,py_').",
)
@click.option(
    "--max-age-hours",
    default=24,
    show_default=True,
    help="Maximum age in hours for --stale mode.",
)
def main(
    schemas: tuple[str, ...],
    region: Optional[str],
    aws_access_key_id: Optional[str],
    aws_secret_access_key: Optional[str],
    ci_test_schemas: Optional[str],
    num_workers: int,
    stale: bool,
    prefixes: Optional[str],
    max_age_hours: int,
) -> None:
    """Drop Athena/Glue schemas efficiently using batch API calls."""
    region, aws_access_key_id, aws_secret_access_key = _resolve_credentials(
        region, aws_access_key_id, aws_secret_access_key
    )
    glue_client = _get_glue_client(region, aws_access_key_id, aws_secret_access_key)

    target_schemas: list[str] = [s.strip() for s in schemas if s and s.strip()]

    # Expand CI test schemas (base + workers + elementary)
    if ci_test_schemas:
        expanded = expand_ci_test_schemas(ci_test_schemas, num_workers)
        click.echo(
            f"CI test schemas expanded to {len(expanded)} schema(s) from base '{ci_test_schemas}'."
        )
        target_schemas.extend(expanded)

    # Stale schema discovery
    if stale:
        if not prefixes:
            click.echo("error: --prefixes is required with --stale", err=True)
            sys.exit(1)
        prefix_list = [p.strip() for p in prefixes.split(",") if p.strip()]
        if not prefix_list:
            click.echo("error: --prefixes must include at least one prefix", err=True)
            sys.exit(1)
        if max_age_hours < 0:
            click.echo("error: --max-age-hours must be >= 0", err=True)
            sys.exit(1)
        now = datetime.utcnow()
        cutoff = timedelta(hours=max_age_hours)
        all_databases = _list_all_databases(glue_client)
        click.echo(
            f"Scanning {len(all_databases)} database(s) for stale schemas "
            f"(prefixes={prefix_list}, max_age={max_age_hours}h) ..."
        )
        for db_name in sorted(all_databases):
            ts = _parse_ci_schema_timestamp(db_name, prefix_list)
            if ts is not None:
                age = now - ts
                if age > cutoff:
                    click.echo(
                        f"  stale: {db_name} (age: {age.total_seconds() / 3600:.1f}h)"
                    )
                    target_schemas.append(db_name)
                else:
                    click.echo(
                        f"  keep:  {db_name} (age: {age.total_seconds() / 3600:.1f}h)"
                    )

    if not target_schemas:
        click.echo("No schemas to drop.")
        sys.exit(0)

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique_schemas: list[str] = []
    for s in target_schemas:
        if s and s.strip() and s not in seen:
            seen.add(s)
            unique_schemas.append(s)

    click.echo(f"\nTargeting {len(unique_schemas)} schema(s) for deletion.")
    success = drop_schemas(glue_client, unique_schemas)
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
