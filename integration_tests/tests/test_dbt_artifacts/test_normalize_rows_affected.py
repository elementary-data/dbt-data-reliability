import json

import pytest
from dbt_project import DbtProject


@pytest.mark.parametrize(
    "input_value,expected_output",
    [
        (None, None),
        (123, 123),
        (0, 0),
        (-1, -1),
        ("123", 123),
        ("0", 0),
        ("-1", None),
        ("456", 456),
    ],
)
def test_normalize_rows_affected(dbt_project: DbtProject, input_value, expected_output):
    result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_normalize_rows_affected",
        macro_args={"rows_affected": input_value},
    )
    # When the macro returns None, log_macro_results doesn't log anything,
    # so run_operation returns an empty list
    if not result:
        actual_output = None
    else:
        actual_output = json.loads(result[0])
    assert actual_output == expected_output, (
        f"normalize_rows_affected({input_value!r}) returned {actual_output!r}, "
        f"expected {expected_output!r}"
    )
