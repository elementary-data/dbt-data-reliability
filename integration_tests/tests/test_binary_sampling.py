"""T-SQL binary columns must be sampled as hex strings.

T-SQL drivers return binary columns as Python bytes, which used to end up in
test samples as their repr (e.g. "b'\\x1a_,:'"), a string that cannot be turned
back into the original value.
"""

import json
import re
from string import Template
from typing import Dict, List

import pytest
from dbt_project import DbtProject

TESTED_COLUMN = "some_column"
ROW_COUNT = 3

HASH_KEY = "0x1A5F2C3AED089D414CDA644D83608EAD"
# The bytes 'ABC', whose Python repr (b'ABC') has no \x escapes at all.
PRINTABLE_BYTES = "0x414243"
# rowversion values are assigned by the database, so only their shape is known.
ROWVERSION_PATTERN = re.compile(r"^0x[0-9A-F]{16}$")

# Samples are stored either in test_result_rows or inline in elementary_test_results.
STORAGE_MODES = pytest.mark.parametrize(
    "own_table", [True, False], ids=["own_table", "inline"]
)

# A view over a table built by the pre-hooks. A real table is needed because a
# rowversion value can only come from a rowversion column, not from a select.
MODEL_TEMPLATE = Template(
    """
{{ config(materialized="view", pre_hook=[
    "drop table if exists $source_table",
    "create table $source_table ($column_definitions)",
    "insert into $source_table ($inserted_columns) $rows"
]) }}
select * from $source_table
"""
)
SOURCE_TABLE = "{{ this.schema }}.{{ this.identifier }}_source"


def _build_model(
    dbt_project: DbtProject,
    model_name: str,
    column_types: Dict[str, str],
    row: Dict[str, str],
):
    """Build `model_name` with ROW_COUNT copies of `row` and a NULL TESTED_COLUMN.

    `row` maps column names to SQL literals. Columns left out of it, such as a
    rowversion, are filled in by the database.
    """
    column_types = {**column_types, TESTED_COLUMN: "int"}
    row = {**row, TESTED_COLUMN: "null"}
    select_row = "select " + ", ".join(row.values())
    model_sql = MODEL_TEMPLATE.substitute(
        source_table=SOURCE_TABLE,
        column_definitions=", ".join(
            f"{name} {sql_type}" for name, sql_type in column_types.items()
        ),
        inserted_columns=", ".join(row),
        rows=" union all ".join([select_row] * ROW_COUNT),
    )
    with dbt_project.create_temp_model_for_existing_table(
        model_name, raw_code=model_sql
    ) as model_path:
        assert dbt_project.dbt_runner.run(
            select=str(model_path)
        ), "Failed to build the binary model"


def _sample_failing_rows(
    dbt_project: DbtProject,
    model_name: str,
    sampled_columns: List[str],
    own_table: bool,
) -> List[dict]:
    """Fail a test on every row of `model_name` and return the stored samples.

    not_null_with_context is used because plain not_null samples only the
    tested column, while context_columns adds `sampled_columns` to the sample.
    """
    test_result = dbt_project.test(
        model_name,
        "elementary.not_null_with_context",
        dict(column_name=TESTED_COLUMN, context_columns=sampled_columns),
        as_model=True,
        test_vars={
            "enable_elementary_test_materialization": True,
            "store_result_rows_in_own_table": own_table,
        },
    )
    assert test_result["status"] == "fail"

    if not own_table:
        return json.loads(test_result["result_rows"])
    return [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(dbt_project.samples_query(model_name))
    ]


def _model_name(test_id: str) -> str:
    # dbt_project.test() replaces the "[...]" of parametrized ids the same way.
    return test_id.replace("[", "_").replace("]", "_")


@pytest.mark.only_on_targets(["sqlserver", "fabric"])
@STORAGE_MODES
def test_varbinary_sampled_as_hex(
    test_id: str, dbt_project: DbtProject, own_table: bool
):
    model_name = _model_name(test_id)
    _build_model(
        dbt_project,
        model_name,
        column_types={
            "hash_key": "varbinary(16)",
            "printable_bytes": "varbinary(20)",
            "null_bytes": "varbinary(16)",
        },
        row={
            "hash_key": HASH_KEY,
            "printable_bytes": PRINTABLE_BYTES,
            "null_bytes": "null",
        },
    )

    samples = _sample_failing_rows(
        dbt_project,
        model_name,
        ["hash_key", "printable_bytes", "null_bytes"],
        own_table,
    )

    expected_row = {
        "hash_key": HASH_KEY,
        "printable_bytes": PRINTABLE_BYTES,
        "null_bytes": None,
        TESTED_COLUMN: None,
    }
    assert samples == [expected_row] * ROW_COUNT


# Fabric Warehouse tables support neither binary(n) nor rowversion.
@pytest.mark.only_on_targets(["sqlserver"])
@STORAGE_MODES
def test_binary_and_rowversion_sampled_as_hex(
    test_id: str, dbt_project: DbtProject, own_table: bool
):
    model_name = _model_name(test_id)
    _build_model(
        dbt_project,
        model_name,
        column_types={"hash_key": "binary(16)", "row_version": "rowversion"},
        row={"hash_key": HASH_KEY},
    )

    samples = _sample_failing_rows(
        dbt_project, model_name, ["hash_key", "row_version"], own_table
    )

    row_versions = [sample.pop("row_version") for sample in samples]
    assert samples == [{"hash_key": HASH_KEY, TESTED_COLUMN: None}] * ROW_COUNT
    assert all(ROWVERSION_PATTERN.match(value) for value in row_versions), row_versions
    # Every inserted row gets its own rowversion.
    assert len(set(row_versions)) == ROW_COUNT
