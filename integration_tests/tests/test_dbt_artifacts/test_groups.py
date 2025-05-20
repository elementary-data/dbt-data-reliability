from dbt_project import DbtProject

GROUP_NAME = "test_group"
OWNER_NAME = "Data Spock"
OWNER_EMAIL = "test@example.com"


def test_dbt_group_artifact(dbt_project: DbtProject):
    # Define a group configuration using constants
    group_config = {
        "groups": [
            {
                "name": GROUP_NAME,
                "owner": {
                    "name": OWNER_NAME,
                    "email": OWNER_EMAIL,
                }
            }
        ]
    }

    # Write the group config to a YAML file in the dbt project
    with dbt_project.write_yaml(group_config, name="groups_test.yml"):
        # Run dbt to generate artifacts
        dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
        dbt_project.dbt_runner.run()

    # Query the dbt_groups artifact table
    groups = dbt_project.read_table("dbt_groups", raise_if_empty=True)
    group = next((g for g in groups if g["name"] == GROUP_NAME), None)

    # Assert the expected group and owner details are present
    assert group is not None, f"Group {GROUP_NAME} not found in dbt_groups artifact table."
    assert group.get("owner_name") == OWNER_NAME, f"Expected owner name {OWNER_NAME}, got {group.get('owner_name')}"
    assert group.get("owner_email") == OWNER_EMAIL, f"Expected owner email {OWNER_EMAIL}, got {group.get('owner_email')}"


def test_dbt_groups_artifact_with_two_groups(dbt_project: DbtProject):
    # Define two group configurations: one with owner, one without
    group1 = {
        "name": "test_group_1",
        "owner": {
            "name": "Owner One",
            "email": "owner1@example.com",
        }
    }
    group2 = {
        "name": "test_group_2",
        "owner": {
            "name": "Owner Two",
            "email": "owner2@example.com",
        }
    }
    group_config = {"groups": [group1, group2]}

    # Write the group config to a YAML file in the dbt project
    with dbt_project.write_yaml(group_config, name="groups_test_two_groups.yml"):
        # Run dbt to generate artifacts
        dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
        dbt_project.dbt_runner.run()

    # Query the dbt_groups artifact table
    groups = dbt_project.read_table("dbt_groups", raise_if_empty=True)
    group1_result = next((g for g in groups if g["name"] == "test_group_1"), None)
    group2_result = next((g for g in groups if g["name"] == "test_group_2"), None)

    # Assert group 1 (with owner)
    assert group1_result is not None, "Group test_group_1 not found in dbt_groups artifact table."
    assert group1_result.get("owner_name") == "Owner One", f"Expected owner name Owner One, got {group1_result.get('owner_name')}"
    assert group1_result.get("owner_email") == "owner1@example.com", f"Expected owner email owner1@example.com, got {group1_result.get('owner_email')}"

    # Assert group 2 (without owner)
    assert group2_result is not None, "Group test_group_2 not found in dbt_groups artifact table."
    assert group2_result.get("owner_name") == "Owner Two", f"Expected owner name Owner Two, got {group2_result.get('owner_name')}"
    assert group2_result.get("owner_email") == "owner2@example.com", f"Expected owner email owner2@example.com, got {group2_result.get('owner_email')}"


def test_model_group_attribute(dbt_project: DbtProject):
    model_name = "model_with_group"
    group_name = GROUP_NAME
    owner_name = OWNER_NAME
    owner_email = OWNER_EMAIL

    group_config = {
        "groups": [
            {
                "name": group_name,
                "owner": {
                    "name": owner_name,
                    "email": owner_email,
                },
            }
        ]
    }

    model_sql = """
    select 1 as col
    """

    schema_yaml = {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "group": group_name,
                "description": "A model assigned to a group for testing",
            }
        ]
    }

    with dbt_project.write_yaml(group_config, name="groups_test_model_inherits.yml"), \
         dbt_project.write_yaml(schema_yaml, name="schema_model_with_group.yml"):
        model_path = dbt_project.models_dir_path / "tmp" / f"{model_name}.sql"
        model_path.parent.mkdir(parents=True, exist_ok=True)
        model_path.write_text(model_sql)
        try:
            # Run dbt
            dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
            dbt_project.dbt_runner.run(select=model_name)

            models = dbt_project.read_table("dbt_models", where=f"name = '{model_name}'", raise_if_empty=True)
            assert len(models) == 1, f"Expected 1 model, got {len(models)}"
            model_row = models[0]
            assert model_row["group_name"] == group_name, f"Expected group_name {group_name}, got {model_row['group_name']}"
        finally:
            if model_path.exists():
                model_path.unlink()


def test_test_group_attribute(dbt_project: DbtProject):
    model_name = "model_with_group"
    group_name = GROUP_NAME
    owner_name = OWNER_NAME
    owner_email = OWNER_EMAIL
    test_args = {"where": "col = 1"}

    schema_yaml = {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "group": group_name,
                "description": "A model assigned to a group for testing",
                "tests": [
                    {"generic": test_args}
                ]
            }
        ]
    }

    group_config = {
        "groups": [
            {
                "name": group_name,
                "owner": {
                    "name": owner_name,
                    "email": owner_email,
                },
            }
        ]
    }

    model_sql = """
    select 1 as col
    """

    schema_yaml = {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "group": group_name,
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

    with dbt_project.write_yaml(group_config, name="groups_test_model_inherits.yml"), \
         dbt_project.write_yaml(schema_yaml, name="schema_model_with_group.yml"):
        model_path = dbt_project.models_dir_path / "tmp" / f"{model_name}.sql"
        model_path.parent.mkdir(parents=True, exist_ok=True)
        model_path.write_text(model_sql)
        try:
            # Run dbt
            dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
            dbt_project.dbt_runner.run(select=model_name)

            tests = dbt_project.read_table("dbt_tests", where=f"name LIKE 'unique%'", raise_if_empty=True)
            assert len(tests) == 1, f"Expected 1 test, got {len(tests)}"
            test_row = tests[0]
            assert test_row["group_name"] == group_name, f"Expected group_name {group_name}, got {test_row['group_name']}"
        finally:
            if model_path.exists():
                model_path.unlink()


