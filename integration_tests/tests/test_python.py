import pytest
from dbt_project import DbtProject
from parametrization import Parametrization

COLUMN_NAME = "column_name"
TEST_NAME = "elementary.python"

# BigQuery also supports Python tests, but the tests are currently flaky.
SUPPORTED_TARGETS = ["snowflake"]


@pytest.mark.requires_dbt_version("1.3.0")
@pytest.mark.skip_for_dbt_fusion
class TestPython:
    @pytest.mark.only_on_targets(SUPPORTED_TARGETS)
    @Parametrization.autodetect_parameters()
    @Parametrization.case("pass", python_result=0, expected_status="pass")
    @Parametrization.case("fail", python_result=1, expected_status="fail")
    def test_int(
        self,
        test_id: str,
        dbt_project: DbtProject,
        python_result: int,
        expected_status: str,
    ):
        data = [{COLUMN_NAME: str()}]
        result = dbt_project.test(
            test_id,
            TEST_NAME,
            dict(
                code_macro="python_mock_test",
                macro_args=dict(result=python_result),
            ),
            data=data,
        )
        assert result["status"] == expected_status

    @pytest.mark.only_on_targets(SUPPORTED_TARGETS)
    def test_full_df(self, test_id: str, dbt_project: DbtProject):
        data = [{COLUMN_NAME: str()}]
        result = dbt_project.test(
            test_id,
            TEST_NAME,
            dict(code_macro="python_return_df"),
            data=data,
        )
        assert result["status"] == "fail"

    @pytest.mark.only_on_targets(SUPPORTED_TARGETS)
    def test_empty_df(self, test_id: str, dbt_project: DbtProject):
        data = [{COLUMN_NAME: str()}]
        result = dbt_project.test(
            test_id, TEST_NAME, dict(code_macro="python_return_empty_df"), data=data
        )
        assert result["status"] == "pass"

    @pytest.mark.skip_targets([*SUPPORTED_TARGETS, "bigquery"])
    def test_invalid_target(self, test_id: str, dbt_project: DbtProject):
        data = [{COLUMN_NAME: str()}]
        result = dbt_project.test(
            test_id,
            TEST_NAME,
            dict(
                code_macro="python_mock_test",
                macro_args=dict(result=1),
            ),
            data=data,
        )
        assert result["status"] == "error"
