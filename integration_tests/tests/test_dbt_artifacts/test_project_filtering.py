"""
Integration tests for the upload_only_current_project_artifacts config var.

When enabled, artifact uploads should only include resources from the current
project (by package_name), excluding artifacts from dependency packages.
When disabled (default), all artifacts including dependencies should be uploaded.
"""

import uuid

from dbt_project import DbtProject

TEST_MODEL = "one"


def test_default_includes_dependency_artifacts(dbt_project: DbtProject):
    """
    By default (upload_only_current_project_artifacts=false), artifacts from
    dependency packages (like 'elementary') should be present in dbt_models.
    """
    dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
    dbt_project.dbt_runner.vars["cache_artifacts"] = False

    dbt_project.dbt_runner.run(select=TEST_MODEL)

    all_models = dbt_project.read_table("dbt_models", raise_if_empty=True)
    package_names = {row["package_name"] for row in all_models}

    assert "elementary" in package_names, (
        "Expected 'elementary' package artifacts to be present by default, "
        f"but only found packages: {package_names}"
    )
    assert "elementary_tests" in package_names, (
        "Expected 'elementary_tests' package artifacts to be present, "
        f"but only found packages: {package_names}"
    )


def test_filtering_excludes_dependency_artifacts(dbt_project: DbtProject):
    """
    When upload_only_current_project_artifacts=true, only artifacts from the
    current project should be uploaded — dependency packages like 'elementary'
    should be excluded.
    """
    dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
    dbt_project.dbt_runner.vars["cache_artifacts"] = False
    dbt_project.dbt_runner.vars["upload_only_current_project_artifacts"] = True

    dbt_project.dbt_runner.run(select=TEST_MODEL)

    all_models = dbt_project.read_table("dbt_models", raise_if_empty=True)
    package_names = {row["package_name"] for row in all_models}

    assert package_names == {"elementary_tests"}, (
        "Expected only 'elementary_tests' artifacts when filtering is enabled, "
        f"but found packages: {package_names}"
    )


def test_filtering_applies_to_sources(dbt_project: DbtProject):
    """
    When upload_only_current_project_artifacts=true, sources from dependency
    packages should also be excluded.
    """
    dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
    dbt_project.dbt_runner.vars["cache_artifacts"] = False
    dbt_project.dbt_runner.vars["upload_only_current_project_artifacts"] = True

    dbt_project.dbt_runner.run(select=TEST_MODEL)

    sources = dbt_project.read_table("dbt_sources", raise_if_empty=False)
    if sources:
        source_packages = {row["package_name"] for row in sources}
        assert "elementary" not in source_packages, (
            "Expected no 'elementary' sources when filtering is enabled, "
            f"but found packages: {source_packages}"
        )


def test_filtering_applies_to_tests(dbt_project: DbtProject, tmp_path):
    """
    When upload_only_current_project_artifacts=true, tests from the current
    project should still be uploaded.
    """
    unique_id = str(uuid.uuid4()).replace("-", "_")
    model_name = f"filter_test_model_{unique_id}"
    model_sql = "select 1 as col"
    schema_yaml = {
        "version": 2,
        "models": [
            {
                "name": model_name,
                "columns": [{"name": "col", "tests": ["unique"]}],
            }
        ],
    }

    with dbt_project.write_yaml(
        schema_yaml, name=f"schema_filter_test_{unique_id}.yml"
    ):
        dbt_model_path = dbt_project.models_dir_path / "tmp" / f"{model_name}.sql"
        dbt_model_path.parent.mkdir(parents=True, exist_ok=True)
        dbt_model_path.write_text(model_sql)
        try:
            dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
            dbt_project.dbt_runner.vars["cache_artifacts"] = False
            dbt_project.dbt_runner.vars["upload_only_current_project_artifacts"] = True

            dbt_project.dbt_runner.run(select=model_name)

            tests = dbt_project.read_table("dbt_tests", raise_if_empty=True)
            test_packages = {row["package_name"] for row in tests}
            assert test_packages == {"elementary_tests"}, (
                "Expected only 'elementary_tests' tests when filtering is enabled, "
                f"but found packages: {test_packages}"
            )
        finally:
            if dbt_model_path.exists():
                dbt_model_path.unlink()
