"""
Integration tests for test owner attribution in dbt_tests artifact table.
Tests that test ownership is correctly extracted from the primary model only,
not aggregated from all parent models (CORE-196).
"""
import contextlib
import json
import uuid

import pytest
from dbt_project import DbtProject


@contextlib.contextmanager
def cleanup_file(path):
    """Context manager to clean up a file after the test."""
    try:
        yield
    finally:
        if path.exists():
            path.unlink()


def _parse_model_owners(model_owners_value):
    """
    Parse the model_owners column value which may be a JSON string or list.
    Returns a list of owner strings.
    """
    if model_owners_value is None:
        return []
    if isinstance(model_owners_value, list):
        return model_owners_value
    if isinstance(model_owners_value, str):
        if not model_owners_value or model_owners_value == "[]":
            return []
        try:
            parsed = json.loads(model_owners_value)
            return parsed if isinstance(parsed, list) else [parsed]
        except json.JSONDecodeError:
            return [model_owners_value]
    return []


def test_single_parent_test_owner_attribution(dbt_project: DbtProject):
    """
    Test that a test on a single model correctly inherits the owner from that model.
    This is the baseline case - single parent tests should have the parent's owner.
    """
    unique_id = str(uuid.uuid4()).replace("-", "_")
    model_name = f"model_single_owner_{unique_id}"
    owner_name = "Alice"

    model_sql = (
        """
    {{ config(meta={'owner': '"""
        + owner_name
        + """'}) }}
    select 1 as id
    """
    )

    schema_yaml = {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "description": "A model with a single owner for testing",
                "columns": [{"name": "id", "tests": ["unique"]}],
            }
        ],
    }

    dbt_model_path = dbt_project.models_dir_path / "tmp" / f"{model_name}.sql"
    with cleanup_file(dbt_model_path):
        with dbt_project.write_yaml(
            schema_yaml, name=f"schema_single_owner_{unique_id}.yml"
        ):
            dbt_model_path.parent.mkdir(parents=True, exist_ok=True)
            dbt_model_path.write_text(model_sql)

            dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
            dbt_project.dbt_runner.run(select=model_name)

            tests = dbt_project.read_table(
                "dbt_tests",
                where=f"parent_model_unique_id LIKE '%{model_name}'",
                raise_if_empty=True,
            )

            assert len(tests) == 1, f"Expected 1 test, got {len(tests)}"
            test_row = tests[0]
            model_owners = _parse_model_owners(test_row.get("model_owners"))

            assert model_owners == [
                owner_name
            ], f"Expected model_owners to be ['{owner_name}'], got {model_owners}"


@pytest.mark.skip_targets(["dremio"])
def test_relationship_test_uses_primary_model_owner_only(
    dbt_project: DbtProject,
):
    """
    Test that a relationship test between two models with different owners
    only uses the owner from the PRIMARY model (the one being tested),
    not from the referenced model.

    This is the key test for CORE-196 - previously owners were aggregated
    from all parent models, now only the primary model's owner should be used.
    """
    unique_id = str(uuid.uuid4()).replace("-", "_")
    primary_model_name = f"model_primary_{unique_id}"
    referenced_model_name = f"model_referenced_{unique_id}"
    # Use explicit test name for reliable querying across dbt versions
    test_name = f"rel_primary_owner_{unique_id}"
    primary_owner = "Alice"
    referenced_owner = "Bob"

    primary_model_sql = f"""
    {{{{ config(meta={{'owner': '{primary_owner}'}}) }}}}
    select 1 as id, 1 as ref_id
    """

    referenced_model_sql = f"""
    {{{{ config(meta={{'owner': '{referenced_owner}'}}) }}}}
    select 1 as id
    """

    schema_yaml = {
        "version": 2,
        "models": [
            {
                "name": primary_model_name,
                "description": "Primary model with owner Alice",
                "columns": [
                    {"name": "id"},
                    {
                        "name": "ref_id",
                        "tests": [
                            {
                                "relationships": {
                                    "name": test_name,
                                    "to": f"ref('{referenced_model_name}')",
                                    "field": "id",
                                }
                            }
                        ],
                    },
                ],
            },
            {
                "name": referenced_model_name,
                "description": "Referenced model with owner Bob",
                "columns": [{"name": "id"}],
            },
        ],
    }

    primary_model_path = (
        dbt_project.models_dir_path / "tmp" / f"{primary_model_name}.sql"
    )
    referenced_model_path = (
        dbt_project.models_dir_path / "tmp" / f"{referenced_model_name}.sql"
    )

    with cleanup_file(primary_model_path), cleanup_file(referenced_model_path):
        with dbt_project.write_yaml(
            schema_yaml, name=f"schema_relationship_{unique_id}.yml"
        ):
            primary_model_path.parent.mkdir(parents=True, exist_ok=True)
            primary_model_path.write_text(primary_model_sql)
            referenced_model_path.write_text(referenced_model_sql)

            dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
            dbt_project.dbt_runner.run(
                select=f"{primary_model_name} {referenced_model_name}"
            )

            # Query by explicit test name - more robust across dbt versions
            tests = dbt_project.read_table(
                "dbt_tests",
                where=f"name LIKE '%{test_name}%'",
                raise_if_empty=False,
            )

            assert (
                len(tests) == 1
            ), f"Expected 1 relationship test with name containing '{test_name}', got {len(tests)}. Tests found: {[t.get('name') for t in tests]}"
            test_row = tests[0]
            model_owners = _parse_model_owners(test_row.get("model_owners"))

            assert model_owners == [
                primary_owner
            ], f"Expected model_owners to be ['{primary_owner}'] (primary model only), got {model_owners}. Referenced model owner '{referenced_owner}' should NOT be included."


@pytest.mark.skip_targets(["dremio"])
def test_relationship_test_no_owner_on_primary_model(dbt_project: DbtProject):
    """
    Test that when the primary model has no owner but the referenced model does,
    the test should have empty model_owners (not inherit from referenced model).
    """
    unique_id = str(uuid.uuid4()).replace("-", "_")
    primary_model_name = f"model_no_owner_{unique_id}"
    referenced_model_name = f"model_with_owner_{unique_id}"
    # Use explicit test name for reliable querying across dbt versions
    test_name = f"rel_no_owner_{unique_id}"
    referenced_owner = "Bob"

    primary_model_sql = """
    select 1 as id, 1 as ref_id
    """

    referenced_model_sql = f"""
    {{{{ config(meta={{'owner': '{referenced_owner}'}}) }}}}
    select 1 as id
    """

    schema_yaml = {
        "version": 2,
        "models": [
            {
                "name": primary_model_name,
                "description": "Primary model with NO owner",
                "columns": [
                    {"name": "id"},
                    {
                        "name": "ref_id",
                        "tests": [
                            {
                                "relationships": {
                                    "name": test_name,
                                    "to": f"ref('{referenced_model_name}')",
                                    "field": "id",
                                }
                            }
                        ],
                    },
                ],
            },
            {
                "name": referenced_model_name,
                "description": "Referenced model with owner Bob",
                "columns": [{"name": "id"}],
            },
        ],
    }

    primary_model_path = (
        dbt_project.models_dir_path / "tmp" / f"{primary_model_name}.sql"
    )
    referenced_model_path = (
        dbt_project.models_dir_path / "tmp" / f"{referenced_model_name}.sql"
    )

    with cleanup_file(primary_model_path), cleanup_file(referenced_model_path):
        with dbt_project.write_yaml(
            schema_yaml, name=f"schema_no_owner_{unique_id}.yml"
        ):
            primary_model_path.parent.mkdir(parents=True, exist_ok=True)
            primary_model_path.write_text(primary_model_sql)
            referenced_model_path.write_text(referenced_model_sql)

            dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
            dbt_project.dbt_runner.run(
                select=f"{primary_model_name} {referenced_model_name}"
            )

            # Query by explicit test name - more robust across dbt versions
            tests = dbt_project.read_table(
                "dbt_tests",
                where=f"name LIKE '%{test_name}%'",
                raise_if_empty=False,
            )

            assert (
                len(tests) == 1
            ), f"Expected 1 relationship test with name containing '{test_name}', got {len(tests)}. Tests found: {[t.get('name') for t in tests]}"
            test_row = tests[0]
            model_owners = _parse_model_owners(test_row.get("model_owners"))

            assert (
                model_owners == []
            ), f"Expected model_owners to be empty (primary model has no owner), got {model_owners}. Referenced model owner '{referenced_owner}' should NOT be inherited."


def test_owner_deduplication(dbt_project: DbtProject):
    """
    Test that duplicate owners in a model's owner field are deduplicated.
    For example, if owner is "Alice,Bob,Alice", the result should be ["Alice", "Bob"].
    """
    unique_id = str(uuid.uuid4()).replace("-", "_")
    model_name = f"model_dup_owner_{unique_id}"

    model_sql = """
    {{ config(meta={'owner': 'Alice,Bob,Alice'}) }}
    select 1 as id
    """

    schema_yaml = {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "description": "A model with duplicate owners for testing deduplication",
                "columns": [{"name": "id", "tests": ["unique"]}],
            }
        ],
    }

    dbt_model_path = dbt_project.models_dir_path / "tmp" / f"{model_name}.sql"
    with cleanup_file(dbt_model_path):
        with dbt_project.write_yaml(
            schema_yaml, name=f"schema_dup_owner_{unique_id}.yml"
        ):
            dbt_model_path.parent.mkdir(parents=True, exist_ok=True)
            dbt_model_path.write_text(model_sql)

            dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
            dbt_project.dbt_runner.run(select=model_name)

            tests = dbt_project.read_table(
                "dbt_tests",
                where=f"parent_model_unique_id LIKE '%{model_name}'",
                raise_if_empty=True,
            )

            assert len(tests) == 1, f"Expected 1 test, got {len(tests)}"
            test_row = tests[0]
            model_owners = _parse_model_owners(test_row.get("model_owners"))

            assert (
                len(model_owners) == 2
            ), f"Expected 2 unique owners, got {len(model_owners)}: {model_owners}"
            assert (
                "Alice" in model_owners
            ), f"Expected 'Alice' in model_owners, got {model_owners}"
            assert (
                "Bob" in model_owners
            ), f"Expected 'Bob' in model_owners, got {model_owners}"
