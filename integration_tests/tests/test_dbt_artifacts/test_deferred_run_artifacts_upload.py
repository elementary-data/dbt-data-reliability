import shutil
from pathlib import Path
from tempfile import mkdtemp

import pytest
from dbt_project import SCHEMA_NAME_SUFFIX, DbtProject

TEST_MODEL = "one"
MISSING_SCHEMA_SUFFIX = f"{SCHEMA_NAME_SUFFIX}_deferred_missing"


@pytest.mark.requires_dbt_version("1.5.0")
@pytest.mark.skip_for_dbt_fusion
@pytest.mark.skip_targets(["dremio"])
def test_artifacts_upload_skipped_when_deferred_elementary_schema_is_missing(
    dbt_project: DbtProject,
):
    # A `dbt run --defer --favor-state` against a target whose Elementary schema
    # was never created (e.g. an ephemeral CI schema) must not fail at the
    # on-run-end artifacts upload - the upload should be skipped, as it is
    # when the relation is missing without deferral.
    dbt_runner = dbt_project.dbt_runner

    # Produce the manifest the deferred run uses as its state.
    assert dbt_runner.run(select=TEST_MODEL)
    state_dir = Path(mkdtemp(prefix="integration_tests_state_"))
    shutil.copy(dbt_project.project_dir_path / "target" / "manifest.json", state_dir)

    # The uploads are disabled by default in the tests for performance reasons.
    deferred_run_vars = {
        "disable_dbt_artifacts_autoupload": False,
        "disable_run_results": False,
        "disable_dbt_invocation_autoupload": False,
        "schema_name_suffix": MISSING_SCHEMA_SUFFIX,
    }
    original_env_vars = dbt_runner.env_vars
    dbt_runner.env_vars = {
        **(original_env_vars or {}),
        "DBT_DEFER": "1",
        "DBT_FAVOR_STATE": "1",
        "DBT_STATE": str(state_dir),
    }
    try:
        deferred_run_success = dbt_runner.run(select=TEST_MODEL, vars=deferred_run_vars)
    finally:
        dbt_runner.env_vars = original_env_vars
        shutil.rmtree(state_dir, ignore_errors=True)
        dbt_runner.run_operation(
            "elementary_tests.clear_env",
            vars={"schema_name_suffix": MISSING_SCHEMA_SUFFIX},
        )

    assert (
        deferred_run_success
    ), "Deferred run failed although the Elementary schema does not exist in the target."
