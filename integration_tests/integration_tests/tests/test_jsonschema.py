import json
import pytest

from dbt_project import DbtProject

MIN_LENGTH = 3
MAX_LENGTH = 5
SCHEMA = {
    "type": "string",
    "minLength": MIN_LENGTH,
    "maxLength": MAX_LENGTH
}
COLUMN_NAME = "jsonschema_column"
TEST_NAME = "elementary.json_schema"


@pytest.mark.only_on_targets("snowflake", "bigquery")
def test_valid_jsonschema(test_id: str, dbt_project: DbtProject):
    valid_value = json.dumps(''.join('*' for _ in range(MIN_LENGTH)))
    data = [{COLUMN_NAME: valid_value}]
    result = dbt_project.test(data, test_id, TEST_NAME, dict(column_name=COLUMN_NAME, **SCHEMA))
    assert result["status"] == "pass"


@pytest.mark.only_on_targets("snowflake", "bigquery")
def test_invalid_jsonschema(test_id: str, dbt_project: DbtProject):
    invalid_value = json.dumps(''.join('*' for _ in range(MIN_LENGTH - 1)))
    data = [{COLUMN_NAME: invalid_value}]
    result = dbt_project.test(data, test_id, TEST_NAME, dict(column_name=COLUMN_NAME, **SCHEMA))
    assert result["status"] == "fail"