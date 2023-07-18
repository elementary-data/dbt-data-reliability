from dbt_project import DbtProject
from elementary.clients.dbt.dbt_runner import DbtRunner

TEST_MODEL = "one"


def read_model_artifact_row(dbt_project: DbtProject):
    return dbt_project.read_table("dbt_models", where=f"alias = '{TEST_MODEL}'")


def enable_artifacts_autoupload(dbt_runner: DbtRunner):
    new_vars = {
        **dbt_runner.vars,
        # Disabled by default in the tests for performance reasons.
        "disable_dbt_artifacts_autoupload": False,
    }
    dbt_runner.vars = new_vars


def test_artifacts_caching(dbt_project: DbtProject):
    enable_artifacts_autoupload(dbt_project.dbt_runner)
    dbt_project.dbt_runner.run(select=TEST_MODEL, vars={"one_tags": ["hello", "world"]})
    first_row = read_model_artifact_row(dbt_project)
    dbt_project.dbt_runner.run(select=TEST_MODEL, vars={"one_tags": ["world", "hello"]})
    second_row = read_model_artifact_row(dbt_project)
    assert first_row == second_row, "Artifacts are not cached at the on-run-end."

    dbt_project.dbt_runner.run(select=TEST_MODEL)
    first_row = read_model_artifact_row(dbt_project)
    dbt_project.dbt_runner.run(select=TEST_MODEL, vars={"one_owner": "ele"})
    second_row = read_model_artifact_row(dbt_project)
    assert first_row != second_row, "Artifacts are not updated at the on-run-end."
