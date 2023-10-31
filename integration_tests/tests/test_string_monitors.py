from dbt_project import DbtProject

COLUMN_NAME = "string_column"


def test_missing_count(dbt_project: DbtProject, test_id: str):
    missing_values = [None, " ", "null", "NULL"]
    data = [{COLUMN_NAME: value} for value in ["a", "b", "c", " a "] + missing_values]
    dbt_project.seed(data, test_id)
    result = dbt_project.run_query(
        f"select {{{{ elementary.missing_count('{COLUMN_NAME}') }}}} "
        f"as missing_count from {{{{ generate_schema_name() }}}}.{test_id}"
    )[0]
    assert result["missing_count"] == len(missing_values)
