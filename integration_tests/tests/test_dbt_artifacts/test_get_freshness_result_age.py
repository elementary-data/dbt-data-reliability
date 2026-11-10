import json

import pytest
from dbt_project import DbtProject


@pytest.mark.parametrize(
    "result_dict,expected_output",
    [
        # dbt-core
        ({"age": 120.5}, 120.5),
        ({"age": 0}, 0),
        # dbt-fusion: no `age`, the value is in max_loaded_at_time_ago_in_s
        ({"max_loaded_at_time_ago_in_s": 88.0}, 88.0),
        ({"age": None, "max_loaded_at_time_ago_in_s": 88.0}, 88.0),
        ({}, None),
    ],
)
def test_get_freshness_result_age(
    dbt_project: DbtProject, result_dict, expected_output
):
    """get_freshness_result_age reads `age` (dbt-core) and falls back to
    `max_loaded_at_time_ago_in_s` (dbt-fusion)."""
    result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_get_freshness_result_age",
        macro_args={"result_dict": result_dict},
    )
    # When the macro returns None, log_macro_results doesn't log anything,
    # so run_operation returns an empty list
    if not result:
        actual_output = None
    else:
        actual_output = json.loads(result[0])
    assert actual_output == expected_output, (
        f"get_freshness_result_age({result_dict!r}) returned {actual_output!r}, "
        f"expected {expected_output!r}"
    )
