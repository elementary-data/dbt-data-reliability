"""
Integration tests for dbt group and owner artifact handling.
Covers models, tests, seeds, and snapshots group assignment and artifact table correctness.
"""
import contextlib
import uuid

import pytest
from dbt_project import DbtProject

GROUP_NAME = "test_group"
OWNER_NAME = "Data Spock"
OWNER_EMAIL = "test@example.com"

# Global group config for common cases
GROUP_CONFIG = {
    "groups": [
        {
            "name": GROUP_NAME,
            "owner": {
                "name": OWNER_NAME,
                "email": OWNER_EMAIL,
            },
        }
    ]
}


@contextlib.contextmanager
def _write_group_config(dbt_project: DbtProject, group_config: dict, name: str):
    """Context manager to write a group config YAML file in the dbt project and clean up after."""
    with dbt_project.write_yaml(group_config, name=name) as file_path:
        yield file_path


def _get_group_from_table(
    dbt_project: DbtProject, group_name: str, table_name: str = "dbt_groups"
):
    """Helper to read a group from the dbt_groups artifact table."""
    groups = dbt_project.read_table(table_name, raise_if_empty=True)
    return next((g for g in groups if g["name"] == group_name), None)


def _normalize_empty(val):
    return val if val not in (None, "") else None


def assert_group_name_in_run_results_view(
    dbt_project, view_name, object_name, group_name
):
    """Assert that the view_run_results table has exactly one row for model_name and its group_name matches."""
    view_run_results = dbt_project.read_table(
        view_name, where=f"name = '{object_name}'", raise_if_empty=True
    )

    assert (
        len(view_run_results) == 1
    ), f"Expected 1 view run result, got {len(view_run_results)}"
    view_run_result_row = view_run_results[0]
    assert (
        view_run_result_row["group_name"] == group_name
    ), f"Expected group_name '{group_name}', got '{view_run_result_row['group_name']}'"


def assert_group_name_in_run_results(dbt_project, model_name, group_name):
    assert_group_name_in_run_results_view(
        dbt_project, "dbt_run_results", model_name, group_name
    )


def assert_group_name_in_model_run_results(dbt_project, model_name, group_name):
    assert_group_name_in_run_results_view(
        dbt_project, "model_run_results", model_name, group_name
    )


def assert_group_row_in_db_groups(dbt_project, group_name, owner_name, owner_email):
    """Assert that the group exists in dbt_groups and owner info matches."""
    group_row = _get_group_from_table(dbt_project, group_name)
    assert (
        group_row is not None
    ), f"Group {group_name} not found in dbt_groups artifact table."
    assert _normalize_empty(group_row.get("owner_name")) == _normalize_empty(
        owner_name
    ), f"Expected owner name '{owner_name}', got '{group_row.get('owner_name')}'"
    assert _normalize_empty(group_row.get("owner_email")) == _normalize_empty(
        owner_email
    ), f"Expected owner email '{owner_email}', got '{group_row.get('owner_email')}'"


@pytest.mark.skip_for_dbt_fusion
def test_model_and_groups(dbt_project: DbtProject, tmp_path):
    """
    Test that a model assigned to a group inherits the group attribute in the dbt_models artifact table.
    Asserts that the model row has the correct group_name.
    Asserts that the group exists in the dbt_groups artifact table.
    This test tests both things although it is not a best practice. We decided to do it
    to save running time since these tests are very slow.
    """

    unique_id = str(uuid.uuid4()).replace("-", "_")
    model_name = f"model_with_group_{unique_id}"
    group_name = f"test_group_{unique_id}"
    model_sql = """
    select 1 as col
    """
    schema_yaml = {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "config": {
                    "group": group_name,
                },
                "description": "A model assigned to a group for testing",
            }
        ],
    }
    group_config = {
        "groups": [
            {
                "name": group_name,
                "owner": {
                    "name": OWNER_NAME,
                    "email": OWNER_EMAIL,
                },
            }
        ]
    }
    with _write_group_config(
        dbt_project, group_config, name=f"groups_test_model_inherits_{unique_id}.yml"
    ), dbt_project.write_yaml(
        schema_yaml, name=f"schema_model_with_group_{unique_id}.yml"
    ):
        model_path = tmp_path / f"{model_name}.sql"
        model_path.write_text(model_sql)
        dbt_model_path = dbt_project.models_dir_path / "tmp" / f"{model_name}.sql"
        dbt_model_path.parent.mkdir(parents=True, exist_ok=True)
        dbt_model_path.write_text(model_sql)
        try:
            dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
            dbt_project.dbt_runner.vars["disable_run_results"] = False
            dbt_project.dbt_runner.run(select=model_name)

            models = dbt_project.read_table(
                "dbt_models", where=f"name = '{model_name}'", raise_if_empty=True
            )
            assert len(models) == 1, f"Expected 1 model, got {len(models)}"
            model_row = models[0]
            assert (
                model_row["group_name"] == group_name
            ), f"Expected group_name {group_name}, got {model_row['group_name']}"

            assert_group_row_in_db_groups(
                dbt_project, group_name, OWNER_NAME, OWNER_EMAIL
            )
            assert_group_name_in_run_results(dbt_project, model_name, group_name)
            assert_group_name_in_model_run_results(dbt_project, model_name, group_name)

        finally:
            if dbt_model_path.exists():
                dbt_model_path.unlink()


@pytest.mark.skip_targets(["dremio"])
@pytest.mark.skip_for_dbt_fusion
def test_two_groups(dbt_project: DbtProject, tmp_path):
    """
    Test that two models assigned to two different groups inherit the correct group attribute in the dbt_models artifact table.
    Asserts that both model rows have the correct group_name, and both groups exist in dbt_groups.
    This test also tests that dbt_groups is filled with the correct owner info when name or email are not provided.
    """
    unique_id = str(uuid.uuid4()).replace("-", "_")
    model_name_1 = f"model_1_with_group_{unique_id}"
    model_name_2 = f"model_2_with_group_{unique_id}"
    group_name_1 = f"test_group_1_{unique_id}"
    group_name_2 = f"test_group_2_{unique_id}"
    owner_email_1 = OWNER_EMAIL
    owner_name_2 = "Other Owner"
    model_sql = """
    select 1 as col
    """
    schema_yaml = {
        "version": 2,
        "models": [
            {
                "name": model_name_1,
                "config": {
                    "group": group_name_1,
                },
                "description": "A model assigned to group 1 for testing",
            },
            {
                "name": model_name_2,
                "config": {
                    "group": group_name_2,
                },
                "description": "A model assigned to group 2 for testing",
            },
        ],
    }
    group_config = {
        "groups": [
            {
                "name": group_name_1,
                "owner": {
                    "email": owner_email_1,
                    "slack": "slack_channel_1",
                },
            },
            {
                "name": group_name_2,
                "owner": {
                    "name": owner_name_2,
                },
            },
        ]
    }
    with _write_group_config(
        dbt_project, group_config, name=f"groups_test_two_groups_{unique_id}.yml"
    ), dbt_project.write_yaml(
        schema_yaml, name=f"schema_model_with_two_groups_{unique_id}.yml"
    ):
        # Write both model files
        model_path_1 = tmp_path / f"{model_name_1}.sql"
        model_path_2 = tmp_path / f"{model_name_2}.sql"
        model_path_1.write_text(model_sql)
        model_path_2.write_text(model_sql)
        dbt_model_path_1 = dbt_project.models_dir_path / "tmp" / f"{model_name_1}.sql"
        dbt_model_path_2 = dbt_project.models_dir_path / "tmp" / f"{model_name_2}.sql"
        dbt_model_path_1.parent.mkdir(parents=True, exist_ok=True)
        dbt_model_path_1.write_text(model_sql)
        dbt_model_path_2.parent.mkdir(parents=True, exist_ok=True)
        dbt_model_path_2.write_text(model_sql)
        try:
            dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
            dbt_project.dbt_runner.vars["disable_run_results"] = False
            dbt_project.dbt_runner.run(select=f"{model_name_1} {model_name_2}")

            # Check both models and their groups/owners
            assert_group_row_in_db_groups(
                dbt_project, group_name_1, None, owner_email_1
            )
            assert_group_row_in_db_groups(dbt_project, group_name_2, owner_name_2, None)
            assert_group_name_in_run_results(dbt_project, model_name_1, group_name_1)
            assert_group_name_in_run_results(dbt_project, model_name_2, group_name_2)
            assert_group_name_in_model_run_results(
                dbt_project, model_name_1, group_name_1
            )
            assert_group_name_in_model_run_results(
                dbt_project, model_name_2, group_name_2
            )

        finally:
            if dbt_model_path_1.exists():
                dbt_model_path_1.unlink()
            if dbt_model_path_2.exists():
                dbt_model_path_2.unlink()


def test_test_group_attribute(dbt_project: DbtProject, tmp_path):
    """
    Test that a test on a model assigned to a group inherits the group attribute in the dbt_tests artifact table.
    Asserts that the test row has the correct group_name.
    """
    unique_id = str(uuid.uuid4()).replace("-", "_")
    model_name = f"model_with_group_{unique_id}"
    group_name = f"test_group_{unique_id}"
    schema_yaml = {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "config": {
                    "group": group_name,
                },
                "description": "A model assigned to a group for testing",
                "columns": [{"name": "col", "tests": ["unique"]}],
            }
        ],
    }
    model_sql = """
    select 1 as col
    """
    group_config = {
        "groups": [
            {
                "name": group_name,
                "owner": {
                    "name": OWNER_NAME,
                    "email": OWNER_EMAIL,
                },
            }
        ]
    }
    with _write_group_config(
        dbt_project, group_config, name=f"groups_test_model_inherits_{unique_id}.yml"
    ), dbt_project.write_yaml(
        schema_yaml, name=f"schema_model_with_group_{unique_id}.yml"
    ):
        model_path = tmp_path / f"{model_name}.sql"
        model_path.write_text(model_sql)
        dbt_model_path = dbt_project.models_dir_path / "tmp" / f"{model_name}.sql"
        dbt_model_path.parent.mkdir(parents=True, exist_ok=True)
        dbt_model_path.write_text(model_sql)
        try:
            dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
            dbt_project.dbt_runner.run(select=model_name)
            tests = dbt_project.read_table(
                "dbt_tests", where="name LIKE 'unique%'", raise_if_empty=True
            )
            assert len(tests) == 1, f"Expected 1 test, got {len(tests)}"
            test_row = tests[0]
            assert (
                test_row["group_name"] == group_name
            ), f"Expected group_name {group_name}, got {test_row['group_name']}"
        finally:
            if dbt_model_path.exists():
                dbt_model_path.unlink()


@pytest.mark.requires_dbt_version("1.9.4")
@pytest.mark.skip_targets(["dremio"])
def test_test_override_group(dbt_project: DbtProject, tmp_path):
    """
    Test that a singular test defined in schema.yml, which belongs to a model with a group, but also has a config: section with another group,
    uses the group from the config in the dbt_tests artifact table.
    """
    unique_id = str(uuid.uuid4()).replace("-", "_")
    model_name = f"model_with_group_{unique_id}"
    test_group = f"test_group_{unique_id}"
    override_group = f"override_group_{unique_id}"
    group_config = {
        "groups": [
            {"name": test_group, "owner": {"name": OWNER_NAME, "email": OWNER_EMAIL}},
            {
                "name": override_group,
                "owner": {"name": "Override Owner", "email": "override@example.com"},
            },
        ]
    }
    schema_yaml = {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "config": {
                    "group": test_group,
                },
                "description": "A model assigned to a group for testing",
                "columns": [
                    {
                        "name": "col",
                        "tests": [{"unique": {"config": {"group": override_group}}}],
                    }
                ],
            }
        ],
    }
    model_sql = """
    select 1 as col
    """
    with _write_group_config(
        dbt_project, group_config, name=f"groups_test_override_group_{unique_id}.yml"
    ), dbt_project.write_yaml(
        schema_yaml, name=f"schema_model_with_override_group_{unique_id}.yml"
    ):
        model_path = tmp_path / f"{model_name}.sql"
        model_path.write_text(model_sql)
        dbt_model_path = dbt_project.models_dir_path / "tmp" / f"{model_name}.sql"
        dbt_model_path.parent.mkdir(parents=True, exist_ok=True)
        dbt_model_path.write_text(model_sql)
        try:
            dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
            dbt_project.dbt_runner.run(select=model_name)
            tests = dbt_project.read_table(
                "dbt_tests", where="name LIKE 'unique%'", raise_if_empty=True
            )
            assert len(tests) == 1, f"Expected 1 test, got {len(tests)}"
            test_row = tests[0]
            assert (
                test_row["group_name"] == override_group
            ), f"Expected group_name {override_group}, got {test_row['group_name']}"
        finally:
            if dbt_model_path.exists():
                dbt_model_path.unlink()


@contextlib.contextmanager
def cleanup_file(path):
    try:
        yield
    finally:
        if path.exists():
            path.unlink()


@pytest.mark.skip_targets(["dremio"])
def test_seed_group_attribute(dbt_project: DbtProject, tmp_path):
    """
    Test that a seed assigned to a group inherits the group attribute in the dbt_seeds artifact table.
    Asserts that the seed row has the correct group_name.
    """
    unique_id = str(uuid.uuid4()).replace("-", "_")
    seed_name = f"seed_with_group_{unique_id}"
    group_name = f"test_group_{unique_id}"
    seed_csv = """id,value\n1,foo\n2,bar\n"""
    schema_yaml = {
        "version": 2,
        "seeds": [
            {
                "name": seed_name,
                "config": {
                    "group": group_name,
                },
                "description": "A seed assigned to a group for testing",
            }
        ],
    }
    group_config = {
        "groups": [
            {
                "name": group_name,
                "owner": {
                    "name": OWNER_NAME,
                    "email": OWNER_EMAIL,
                },
            }
        ]
    }
    seed_path = tmp_path / f"{seed_name}.csv"
    dbt_seed_path = dbt_project.seeds_dir_path / f"{seed_name}.csv"
    with cleanup_file(dbt_seed_path):
        with _write_group_config(
            dbt_project, group_config, name=f"groups_test_seed_inherits_{unique_id}.yml"
        ), dbt_project.write_yaml(
            schema_yaml, name=f"schema_seed_with_group_{unique_id}.yml"
        ):
            seed_path.write_text(seed_csv)
            dbt_seed_path.parent.mkdir(parents=True, exist_ok=True)
            dbt_seed_path.write_text(seed_csv)
            # Run dbt seed
            dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
            dbt_project.dbt_runner.vars["disable_run_results"] = False
            dbt_project.dbt_runner.seed(select=seed_name)
            seeds = dbt_project.read_table(
                "dbt_seeds", where=f"name = '{seed_name}'", raise_if_empty=True
            )
            assert len(seeds) == 1, f"Expected 1 seed, got {len(seeds)}"
            seed_row = seeds[0]
            assert (
                seed_row["group_name"] == group_name
            ), f"Expected group_name {group_name}, got {seed_row['group_name']}"

            assert_group_name_in_run_results(dbt_project, seed_name, group_name)
            assert_group_name_in_run_results_view(
                dbt_project, "seed_run_results", seed_name, group_name
            )


@pytest.mark.skip_targets(["dremio"])
@pytest.mark.skip_for_dbt_fusion
def test_snapshot_group_attribute(dbt_project: DbtProject, tmp_path):
    """
    Test that a snapshot assigned to a group inherits the group attribute in the dbt_snapshots artifact table.
    Asserts that the snapshot row has the correct group_name.
    """
    unique_id = str(uuid.uuid4()).replace("-", "_")
    snapshot_name = f"snapshot_with_group_{unique_id}"
    group_name = f"test_group_{unique_id}"
    snapshot_sql = (
        "{% snapshot " + snapshot_name + " %}\n"
        "{{ config(\n"
        "    unique_key='id',\n"
        "    strategy='check',\n"
        "    check_cols='all',\n"
        "    target_schema=target.schema\n"
        ") }}\n"
        "select 1 as id, 'foo' as value\n"
        "{% endsnapshot %}\n"
    )
    schema_yaml = {
        "version": 2,
        "snapshots": [
            {
                "name": snapshot_name,
                "config": {
                    "group": group_name,
                },
                "description": "A snapshot assigned to a group for testing",
            }
        ],
    }
    group_config = {
        "groups": [
            {
                "name": group_name,
                "owner": {
                    "name": OWNER_NAME,
                    "email": OWNER_EMAIL,
                },
            }
        ]
    }
    snapshots_dir = tmp_path / "snapshots"
    snapshot_path = snapshots_dir / f"{snapshot_name}.sql"
    dbt_snapshots_dir = dbt_project.project_dir_path / "snapshots"
    dbt_snapshot_path = dbt_snapshots_dir / f"{snapshot_name}.sql"
    with cleanup_file(dbt_snapshot_path):
        with _write_group_config(
            dbt_project,
            group_config,
            name=f"groups_test_snapshot_inherits_{unique_id}.yml",
        ), dbt_project.write_yaml(
            schema_yaml, name=f"schema_snapshot_with_group_{unique_id}.yml"
        ):
            snapshots_dir.mkdir(parents=True, exist_ok=True)
            snapshot_path.write_text(snapshot_sql)
            # Copy to dbt project snapshots dir
            dbt_snapshots_dir.mkdir(parents=True, exist_ok=True)
            dbt_snapshot_path.write_text(snapshot_sql)
            # Run dbt snapshot (runs all snapshots, as selecting is not supported by the runner)
            dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
            dbt_project.dbt_runner.vars["disable_run_results"] = False
            dbt_project.dbt_runner.snapshot()

            snapshots = dbt_project.read_table(
                "dbt_snapshots", where=f"name = '{snapshot_name}'", raise_if_empty=False
            )
            assert (
                snapshots
            ), f"No rows found in dbt_snapshots for name = '{snapshot_name}'"
            assert len(snapshots) == 1, f"Expected 1 snapshot, got {len(snapshots)}"
            snapshot_row = snapshots[0]
            assert (
                snapshot_row["group_name"] == group_name
            ), f"Expected group_name {group_name}, got {snapshot_row['group_name']}"

            assert_group_name_in_run_results(dbt_project, snapshot_name, group_name)
            assert_group_name_in_run_results_view(
                dbt_project, "snapshot_run_results", snapshot_name, group_name
            )
