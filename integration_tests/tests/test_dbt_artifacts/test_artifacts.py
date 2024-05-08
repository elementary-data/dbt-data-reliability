import pytest
from dbt_project import DbtProject

TEST_MODEL = "one"


def read_model_artifact_row(dbt_project: DbtProject):
    return dbt_project.read_table("dbt_models", where=f"alias = '{TEST_MODEL}'")[0]


@pytest.mark.requires_dbt_version("1.4.0")
def test_artifacts_caching(dbt_project: DbtProject):
    # Disabled by default in the tests for performance reasons.
    dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False

    dbt_project.dbt_runner.run(select=TEST_MODEL, vars={"one_tags": ["hello", "world"]})
    first_row = read_model_artifact_row(dbt_project)
    dbt_project.dbt_runner.run(select=TEST_MODEL, vars={"one_tags": ["world", "hello"]})
    second_row = read_model_artifact_row(dbt_project)
    assert first_row == second_row, "Artifacts are not cached at the on-run-end."


def test_artifacts_updating(dbt_project: DbtProject):
    # Disabled by default in the tests for performance reasons.
    dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False

    dbt_project.dbt_runner.run(select=TEST_MODEL)
    first_row = read_model_artifact_row(dbt_project)
    dbt_project.dbt_runner.run(select=TEST_MODEL, vars={"one_owner": "ele"})
    second_row = read_model_artifact_row(dbt_project)
    assert first_row != second_row, "Artifacts are not updated at the on-run-end."


def test_artifacts_collection_in_multiple_row_batches(dbt_project: DbtProject):
    existing_artifacts = dbt_project.read_table("dbt_models")

    dbt_project.dbt_runner.vars[
        "query_max_size"
    ] = 5000  # small value to force multiple batches
    dbt_project.dbt_runner.vars[
        "cache_artifacts"
    ] = False  # force artifacts to be recollected

    dbt_project.dbt_runner.run(select="dbt_models")

    new_artifacts = dbt_project.read_table("dbt_models")

    assert len(existing_artifacts) == len(new_artifacts)


def test_dbt_invocations(dbt_project: DbtProject):
    dbt_project.dbt_runner.vars["disable_dbt_invocation_autoupload"] = False
    dbt_project.dbt_runner.run(selector="one")
    dbt_project.read_table(
        "dbt_invocations", where="yaml_selector = 'one'", raise_if_empty=True
    )
