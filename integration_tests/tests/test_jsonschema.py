import json

import pytest
from dbt_project import DbtProject

MIN_LENGTH = 3
MAX_LENGTH = 5
SCHEMA = {"type": "string", "minLength": MIN_LENGTH, "maxLength": MAX_LENGTH}
COLUMN_NAME = "jsonschema_column"
TEST_NAME = "elementary.json_schema"

# BigQuery also supports JSON schema tests, but the Python tests are currently flaky.
SUPPORTED_TARGETS = ["snowflake"]


@pytest.mark.requires_dbt_version("1.3.0")
@pytest.mark.skip_for_dbt_fusion
class TestJsonschema:
    @pytest.mark.only_on_targets(SUPPORTED_TARGETS)
    def test_valid(self, test_id: str, dbt_project: DbtProject):
        valid_value = json.dumps("".join("*" for _ in range(MIN_LENGTH)))
        data = [{COLUMN_NAME: valid_value}]
        result = dbt_project.test(
            test_id, TEST_NAME, dict(column_name=COLUMN_NAME, **SCHEMA), data=data
        )
        assert result["status"] == "pass"

    @pytest.mark.only_on_targets(SUPPORTED_TARGETS)
    def test_invalid(self, test_id: str, dbt_project: DbtProject):
        invalid_value = json.dumps("".join("*" for _ in range(MIN_LENGTH - 1)))
        data = [{COLUMN_NAME: invalid_value}]
        result = dbt_project.test(
            test_id, TEST_NAME, dict(column_name=COLUMN_NAME, **SCHEMA), data=data
        )
        assert result["status"] == "fail"

    @pytest.mark.skip_targets([*SUPPORTED_TARGETS, "bigquery"])
    def test_invalid_target(self, test_id: str, dbt_project: DbtProject):
        data = [{COLUMN_NAME: str()}]
        result = dbt_project.test(
            test_id, TEST_NAME, dict(column_name=COLUMN_NAME, **SCHEMA), data=data
        )
        assert result["status"] == "error"
