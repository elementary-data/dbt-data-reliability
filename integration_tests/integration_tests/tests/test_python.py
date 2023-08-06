import json
from typing import Union
import pytest
from parametrization import Parametrization

from dbt_project import DbtProject

COLUMN_NAME = "column_name"
TEST_NAME = "elementary.python"


@pytest.mark.only_on_targets(["snowflake", "bigquery"])
@Parametrization.autodetect_parameters()
@Parametrization.case("pass", python_result=0, expected_status="pass")
@Parametrization.case("fail", python_result=1, expected_status="fail")
def test_python_int(
    test_id: str,
    dbt_project: DbtProject,
    python_result: int,
    expected_status: str,
):
    result = dbt_project.test(
        test_id,
        TEST_NAME,
        dict(
            code_macro="python_mock_test",
            macro_args=dict(result=python_result),
        ),
    )
    assert result["status"] == expected_status


@pytest.mark.only_on_targets(["snowflake", "bigquery"])
def test_python_full_df(test_id: str, dbt_project: DbtProject):
    data = [{COLUMN_NAME: str()}]
    result = dbt_project.test(
        test_id,
        TEST_NAME,
        dict(
            code_macro="python_return_df",
            macro_args=dict(result=json.dumps(data)),
        ),
        data=data,
    )
    assert result["status"] == "fail"


@pytest.mark.only_on_targets(["snowflake", "bigquery"])
def test_python_empty_df(test_id: str, dbt_project: DbtProject):
    data = [{COLUMN_NAME: str()}]
    result = dbt_project.test(
        test_id, TEST_NAME, dict(code_macro="python_return_empty_df"), data=data
    )
    assert result["status"] == "pass"


@pytest.mark.skip_targets(["snowflake", "bigquery"])
def test_invalid_target_python(test_id: str, dbt_project: DbtProject):
    data = [{COLUMN_NAME: str()}]
    result = dbt_project.test(
        test_id,
        TEST_NAME,
        dict(
            code_macro="python_mock_test",
            macro_args=dict(result=True),
        ),
        data=data,
    )
    assert result["status"] == "error"
