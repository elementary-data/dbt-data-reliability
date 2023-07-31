import pytest
from dbt.version import __version__
from dbt_project import DbtProject
from packaging import version

TEST_MODEL = "one"
DBT_VERSION = version.parse(version.parse(__version__).base_version)


def read_model_artifact_row(dbt_project: DbtProject):
    return dbt_project.read_table("dbt_models", where=f"alias = '{TEST_MODEL}'")[0]


@pytest.mark.skipif(
    DBT_VERSION < version.parse("1.4.0"), reason="requires dbt 1.4.0 or above"
)
def test_artifacts_caching(dbt_project: DbtProject):
    # Disabled by default in the tests for performance reasons.
    dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False

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


def test_dbt_invocations(dbt_project: DbtProject):
    dbt_project.dbt_runner.vars["disable_dbt_invocation_autoupload"] = False
    dbt_project.dbt_runner.run(selector="one")
    dbt_project.read_table(
        "dbt_invocations", where="yaml_selector = 'one'", raise_if_empty=True
    )
