from dbt_project import DbtProject
import pytest
from ruamel.yaml import YAML

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


def _write_group_config(dbt_project: DbtProject, group_config: dict, name: str):
    """Helper to write a group config YAML file in the dbt project."""
    return dbt_project.write_yaml(group_config, name=name)


def _get_group_from_table(dbt_project: DbtProject, group_name: str, table_name: str = "dbt_groups"):
    """Helper to read a group from the dbt_groups artifact table."""
    groups = dbt_project.read_table(table_name, raise_if_empty=True)
    return next((g for g in groups if g["name"] == group_name), None)


@pytest.mark.parametrize(
    "group_config, expected_groups, test_name",
    [
        # Single group with owner name and email
        (
            GROUP_CONFIG,
            [(GROUP_NAME, OWNER_NAME, OWNER_EMAIL)],
            "single_group",
        ),
        # Single group with name only, no email
        (
            {"groups": [
                {"name": GROUP_NAME, "owner": {"name": OWNER_NAME}}
            ]},
            [(GROUP_NAME, OWNER_NAME, None)],
            "single_group_without_email",
        ),
        # Single group with email only, no name
        (
            {"groups": [
                {"name": GROUP_NAME, "owner": {"email": OWNER_EMAIL}}
            ]},
            [(GROUP_NAME, None, OWNER_EMAIL)],
            "single_group_without_name",
        ),
        # Single group with owner additional fields
        (
            {"groups": [
                {"name": GROUP_NAME, "owner": {"email": OWNER_EMAIL, "slack": "slack_channel"}}
            ]},
            [(GROUP_NAME, None, OWNER_EMAIL)],
            "single_group_with_additional_fields",
        ),
        # Two groups, each with owner
        (
            {"groups": [
                {"name": "test_group_1", "owner": {"name": "Owner One", "email": "owner1@example.com"}},
                {"name": "test_group_2", "owner": {"name": "Owner Two", "email": "owner2@example.com"}},
            ]},
            [
                ("test_group_1", "Owner One", "owner1@example.com"),
                ("test_group_2", "Owner Two", "owner2@example.com"),
            ],
            "two_groups",
        ),
    ],
    ids=[
        "single_group",
        "single_group_without_email",
        "single_group_without_name",
        "single_group_with_additional_fields",
        "two_groups",
    ]
)
def test_dbt_groups_artifact_parametrized(dbt_project: DbtProject, group_config, expected_groups, test_name):
    """
    Parametrized test for group artifact scenarios:
    - Single group with owner (name and email)
    - Single group with owner (name only, no email)
    - Two groups, each with owner
    Asserts that the group(s) and owner details are present and correct in the dbt_groups artifact table.
    """
    with _write_group_config(dbt_project, group_config, name=f"groups_test_{test_name}.yml"):
        dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
        dbt_project.dbt_runner.run()
        dbt_project.assert_table_exists("dbt_groups")
    for group_name, owner_name, owner_email in expected_groups:
        group = _get_group_from_table(dbt_project, group_name)
        assert group is not None, f"Group {group_name} not found in dbt_groups artifact table."
        assert group.get("owner_name") == owner_name, f"Expected owner name {owner_name}, got {group.get('owner_name')}"
        assert group.get("owner_email") == owner_email, f"Expected owner email {owner_email}, got {group.get('owner_email')}"


def test_model_group_attribute(dbt_project: DbtProject):
    """
    Test that a model assigned to a group inherits the group attribute in the dbt_models artifact table.
    Asserts that the model row has the correct group_name.
    """
    model_name = "model_with_group"
    model_sql = """
    select 1 as col
    """
    schema_yaml = {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "group": GROUP_NAME,
                "description": "A model assigned to a group for testing",
            }
        ]
    }
    with _write_group_config(dbt_project, GROUP_CONFIG, name="groups_test_model_inherits.yml"), \
         dbt_project.write_yaml(schema_yaml, name="schema_model_with_group.yml"):
        model_path = dbt_project.models_dir_path / "tmp" / f"{model_name}.sql"
        model_path.parent.mkdir(parents=True, exist_ok=True)
        model_path.write_text(model_sql)
        try:
            dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
            dbt_project.dbt_runner.run(select=model_name)
            dbt_project.assert_table_exists("dbt_models")
            models = dbt_project.read_table("dbt_models", where=f"name = '{model_name}'", raise_if_empty=True)
            assert len(models) == 1, f"Expected 1 model, got {len(models)}"
            model_row = models[0]
            assert model_row["group_name"] == GROUP_NAME, f"Expected group_name {GROUP_NAME}, got {model_row['group_name']}"
        finally:
            if model_path.exists():
                model_path.unlink()


def test_test_group_attribute(dbt_project: DbtProject):
    """
    Test that a test on a model assigned to a group inherits the group attribute in the dbt_tests artifact table.
    Asserts that the test row has the correct group_name.
    """
    model_name = "model_with_group"
    schema_yaml = {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "group": GROUP_NAME,
                "description": "A model assigned to a group for testing",
                "columns": [
                    {
                        "name": "col",
                        "tests": ["unique"]
                    }
                ]
            }
        ]
    }
    model_sql = """
    select 1 as col
    """
    with _write_group_config(dbt_project, GROUP_CONFIG, name="groups_test_model_inherits.yml"), \
         dbt_project.write_yaml(schema_yaml, name="schema_model_with_group.yml"):
        model_path = dbt_project.models_dir_path / "tmp" / f"{model_name}.sql"
        model_path.parent.mkdir(parents=True, exist_ok=True)
        model_path.write_text(model_sql)
        try:
            dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
            dbt_project.dbt_runner.run(select=model_name)
            dbt_project.assert_table_exists("dbt_tests")
            tests = dbt_project.read_table("dbt_tests", where="name LIKE 'unique%'", raise_if_empty=True)
            assert len(tests) == 1, f"Expected 1 test, got {len(tests)}"
            test_row = tests[0]
            assert test_row["group_name"] == GROUP_NAME, f"Expected group_name {GROUP_NAME}, got {test_row['group_name']}"
        finally:
            if model_path.exists():
                model_path.unlink()


def test_test_override_group(dbt_project: DbtProject):
    """
    Test that a singular test defined in schema.yml, which belongs to a model with a group, but also has a config: section with another group,
    uses the group from the config in the dbt_tests artifact table.
    """
    model_name = "model_with_group"
    test_group = GROUP_NAME
    override_group = "override_group"
    # Write group config with both groups
    group_config = {
        "groups": [
            {"name": test_group, "owner": {"name": OWNER_NAME, "email": OWNER_EMAIL}},
            {"name": override_group, "owner": {"name": "Override Owner", "email": "override@example.com"}},
        ]
    }
    schema_yaml = {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "group": test_group,
                "description": "A model assigned to a group for testing",
                "columns": [
                    {
                        "name": "col",
                        "tests": [
                            {
                                "unique": {
                                    "config": {"group": override_group}
                                }
                            }
                        ]
                    }
                ]
            }
        ]
    }
    model_sql = """
    select 1 as col
    """
    with _write_group_config(dbt_project, group_config, name="groups_test_override_group.yml"), \
         dbt_project.write_yaml(schema_yaml, name="schema_model_with_override_group.yml"):
        model_path = dbt_project.models_dir_path / "tmp" / f"{model_name}.sql"
        model_path.parent.mkdir(parents=True, exist_ok=True)
        model_path.write_text(model_sql)
        try:
            dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
            dbt_project.dbt_runner.run(select=model_name)
            dbt_project.assert_table_exists("dbt_tests")
            tests = dbt_project.read_table("dbt_tests", where="name LIKE 'unique%'", raise_if_empty=True)
            assert len(tests) == 1, f"Expected 1 test, got {len(tests)}"
            test_row = tests[0]
            assert test_row["group_name"] == override_group, f"Expected group_name {override_group}, got {test_row['group_name']}"
        finally:
            if model_path.exists():
                model_path.unlink()


def test_seed_group_attribute(dbt_project: DbtProject):
    """
    Test that a seed assigned to a group inherits the group attribute in the dbt_seeds artifact table.
    Asserts that the seed row has the correct group_name.
    """
    seed_name = "seed_with_group"
    seed_csv = """id,value\n1,foo\n2,bar\n"""

    schema_yaml = {
        "version": 2,
        "seeds": [
            {
                "name": seed_name,
                "group": GROUP_NAME,
                "description": "A seed assigned to a group for testing",
            }
        ]
    }

    seed_path = dbt_project.seeds_dir_path / f"{seed_name}.csv"
    try:
        with _write_group_config(dbt_project, GROUP_CONFIG, name="groups_test_seed_inherits.yml"), \
             dbt_project.write_yaml(schema_yaml, name="schema_seed_with_group.yml"):
            seed_path.parent.mkdir(parents=True, exist_ok=True)
            seed_path.write_text(seed_csv)
            # Run dbt seed
            dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
            dbt_project.dbt_runner.seed(select=seed_name)

            dbt_project.assert_table_exists('dbt_seeds')

            seeds = dbt_project.read_table("dbt_seeds", where=f"name = '{seed_name}'", raise_if_empty=True)
            assert len(seeds) == 1, f"Expected 1 seed, got {len(seeds)}"
            seed_row = seeds[0]
            assert seed_row["group_name"] == GROUP_NAME, f"Expected group_name {GROUP_NAME}, got {seed_row['group_name']}"
    finally:
        if seed_path.exists():
            seed_path.unlink()


def test_snapshot_group_attribute(dbt_project: DbtProject):
    """
    Test that a snapshot assigned to a group inherits the group attribute in the dbt_snapshots artifact table.
    Asserts that the snapshot row has the correct group_name.
    """
    snapshot_name = "snapshot_with_group"
    snapshot_sql = (
        "{% snapshot " + snapshot_name + " %}\n"
        "{{ config(\n"
        "    unique_key='id',\n"
        "    strategy='check',\n"
        "    check_cols='all'\n"
        ") }}\n"
        "select 1 as id, 'foo' as value\n"
        "{% endsnapshot %}\n"
    )
    schema_yaml = {
        "version": 2,
        "snapshots": [
            {
                "name": snapshot_name,
                "group": GROUP_NAME,
                "description": "A snapshot assigned to a group for testing",
            }
        ]
    }
    snapshots_dir = dbt_project.project_dir_path / "snapshots"
    snapshot_path = snapshots_dir / f"{snapshot_name}.sql"
    try:
        with _write_group_config(dbt_project, GROUP_CONFIG, name="groups_test_snapshot_inherits.yml"), \
             dbt_project.write_yaml(schema_yaml, name="schema_snapshot_with_group.yml"):
            snapshots_dir.mkdir(parents=True, exist_ok=True)
            snapshot_path.write_text(snapshot_sql)
            # Debug: print file existence and directory contents
            print(f"DEBUG: Snapshot file exists: {snapshot_path.exists()} at {snapshot_path}")
            print(f"DEBUG: Snapshots dir contents: {list(snapshots_dir.iterdir()) if snapshots_dir.exists() else 'DIR DOES NOT EXIST'}")
            # Run dbt snapshot (runs all snapshots, as selecting is not supported by the runner)
            dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
            dbt_project.dbt_runner.run()

            dbt_project.assert_table_exists('dbt_snapshots')

            snapshots = dbt_project.read_table("dbt_snapshots", where=f"name = '{snapshot_name}'", raise_if_empty=False)
            assert snapshots, f"No rows found in dbt_snapshots for name = '{snapshot_name}'"
            assert len(snapshots) == 1, f"Expected 1 snapshot, got {len(snapshots)}"
            snapshot_row = snapshots[0]
            assert snapshot_row["group_name"] == GROUP_NAME, f"Expected group_name {GROUP_NAME}, got {snapshot_row['group_name']}"
    finally:
        if snapshot_path.exists():
            snapshot_path.unlink()
        # Optionally clean up the snapshots_dir if empty
        if snapshots_dir.exists() and not any(snapshots_dir.iterdir()):
            snapshots_dir.rmdir()
