import json
import re

import pytest
from dbt_project import DbtProject

COLUMN_NAME = "some_column"
HASH_KEY = "0x1A5F2C3AED089D414CDA644D83608EAD"
# Printable ASCII bytes ('ABC'), whose Python bytes repr has no escapes at all.
PRINTABLE_BYTES = "0x414243"
ROW_COUNT = 3
# rowversion values are assigned by the database, so only their shape is known.
ROWVERSION_PATTERN = re.compile(r"^0x[0-9A-F]{16}$")

# T-SQL drivers return binary columns as Python bytes.
SUPPORTED_TARGETS = ["sqlserver", "fabric"]


@pytest.mark.only_on_targets(SUPPORTED_TARGETS)
@pytest.mark.parametrize("own_table", [True, False], ids=["own_table", "inline"])
def test_binary_columns_sampled_as_hex(
    test_id: str, dbt_project: DbtProject, own_table: bool
):
    # dbt_project.test() sanitizes "[...]" the same way; keep the model name aligned.
    test_id = test_id.replace("[", "_").replace("]", "_")
    # A rowversion column can only come from CREATE TABLE, not from a select
    # expression, so the model is a view over a table built by its pre-hooks.
    source_table = "{{ this.schema }}.{{ this.identifier }}_source"
    row_values = f"({HASH_KEY}, {PRINTABLE_BYTES}, null, null)"
    pre_hooks = [
        f"drop table if exists {source_table}",
        f"create table {source_table} ("
        "hash_key binary(16), printable_bytes varbinary(20),"
        f" null_bytes varbinary(16), {COLUMN_NAME} int, row_version rowversion)",
        f"insert into {source_table}"
        f" (hash_key, printable_bytes, null_bytes, {COLUMN_NAME})"
        f" values {', '.join([row_values] * ROW_COUNT)}",
    ]
    query = (
        f"{{{{ config(pre_hook={json.dumps(pre_hooks)}) }}}}\n"
        f"select * from {source_table}"
    )
    with dbt_project.create_temp_model_for_existing_table(
        test_id, materialization="view", raw_code=query
    ) as model_path:
        assert dbt_project.dbt_runner.run(
            select=str(model_path)
        ), "Failed to build the binary model"

    test_result = dbt_project.test(
        test_id,
        "elementary.not_null_with_context",
        dict(
            column_name=COLUMN_NAME,
            context_columns=[
                "hash_key",
                "printable_bytes",
                "null_bytes",
                "row_version",
            ],
        ),
        as_model=True,
        test_vars={
            "enable_elementary_test_materialization": True,
            "store_result_rows_in_own_table": own_table,
        },
    )
    assert test_result["status"] == "fail"

    if own_table:
        samples = [
            json.loads(row["result_row"])
            for row in dbt_project.run_query(dbt_project.samples_query(test_id))
        ]
    else:
        samples = json.loads(test_result["result_rows"])
    assert len(samples) == ROW_COUNT
    row_versions = set()
    for row in samples:
        row_version = row.pop("row_version")
        assert ROWVERSION_PATTERN.match(row_version), row_version
        row_versions.add(row_version)
        assert row == {
            "hash_key": HASH_KEY,
            "printable_bytes": PRINTABLE_BYTES,
            "null_bytes": None,
            COLUMN_NAME: None,
        }
    # Every inserted row gets its own rowversion.
    assert len(row_versions) == ROW_COUNT
