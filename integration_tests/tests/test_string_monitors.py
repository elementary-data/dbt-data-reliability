from dbt_project import DbtProject

COLUMN_NAME = "string_column"


def test_missing_count(dbt_project: DbtProject, test_id: str):
    missing_values = [None, " ", "null", "NULL"]
    data = [{COLUMN_NAME: value} for value in ["a", "b", "c", " a "] + missing_values]
    with dbt_project.seed_context(data, test_id):
        result = dbt_project.run_query(
            f"select {{{{ elementary.missing_count('{COLUMN_NAME}') }}}} "
            f'as missing_count from {{{{ ref("{test_id}") }}}}'
        )[0]
    assert result["missing_count"] == len(missing_values)
