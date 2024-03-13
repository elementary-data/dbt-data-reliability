import pytest
from dbt_project import DbtProject

REGULAR_SOURCE = {
    "name": "regular_source",
    "schema": "{{ target.schema }}",
    "tables": [
        {
            "name": "test_table",
            "columns": [
                {"name": "id", "tests": [{"unique": {}}]},
                {"name": "name", "tests": [{"not_null": {}}]},
            ],
        }
    ],
}

NON_EXISTING_SCHEMA_SOURCE = {
    "name": "non_existing_schema_source",
    "schema": "this_schema_does_not_exist",
    "tables": [
        {
            "name": "test_table",
            "columns": [
                {"name": "id", "tests": [{"unique": {}}]},
                {"name": "name", "tests": [{"not_null": {}}]},
            ],
        }
    ],
}

NON_EXISTING_DATABASE_SOURCE = {
    "name": "regular_source",
    "schema": '"{{ target.schema }}"',
    "database": "this_database_does_not_exist",
    "tables": [
        {
            "name": "test_table",
            "columns": [
                {"name": "id", "tests": [{"unique": {}}]},
                {"name": "name", "tests": [{"not_null": {}}]},
            ],
        }
    ],
}


@pytest.mark.skip_targets(["athena", "databricks", "trino"])
def test_information_schema_columns(dbt_project: DbtProject):
    sources = {"version": 2, "sources": [REGULAR_SOURCE]}
    with dbt_project.write_yaml(sources):
        success = dbt_project.dbt_runner.run(select="information_schema_columns")
        assert success


@pytest.mark.skip_targets(["athena", "databricks", "trino"])
def test_information_schema_non_existing_schema(dbt_project: DbtProject):
    sources = {"version": 2, "sources": [NON_EXISTING_SCHEMA_SOURCE]}
    with dbt_project.write_yaml(sources):
        success = dbt_project.dbt_runner.run(select="information_schema_columns")
        assert success


@pytest.mark.skip_targets(["athena", "databricks", "trino"])
def test_information_schema_non_existing_database(dbt_project: DbtProject):
    sources = {"version": 2, "sources": [NON_EXISTING_DATABASE_SOURCE]}
    with dbt_project.write_yaml(sources):
        success = dbt_project.dbt_runner.run(select="information_schema_columns")
        assert success
