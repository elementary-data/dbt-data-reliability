"""Unit tests for dbt_project.py module."""

import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, Mock, patch

import pytest

from dbt_project import (
    DEFAULT_DUMMY_CODE,
    PYTEST_XDIST_WORKER,
    SCHEMA_NAME_SUFFIX,
    DbtProject,
    get_dbt_runner,
)


class TestSchemaNameSuffix:
    """Test the schema name suffix logic based on pytest-xdist worker."""

    def test_schema_name_suffix_with_worker(self):
        """Test that SCHEMA_NAME_SUFFIX is set when PYTEST_XDIST_WORKER exists."""
        # Save original values
        import dbt_project
        from importlib import reload

        original_suffix = dbt_project.SCHEMA_NAME_SUFFIX
        original_worker = dbt_project.PYTEST_XDIST_WORKER

        try:
            with patch.dict(os.environ, {"PYTEST_XDIST_WORKER": "gw3"}):
                # Re-import to get the updated value
                reload(dbt_project)
                assert dbt_project.SCHEMA_NAME_SUFFIX == "_gw3"
        finally:
            # Restore original by reloading without the env var
            if original_worker is None:
                os.environ.pop("PYTEST_XDIST_WORKER", None)
            else:
                os.environ["PYTEST_XDIST_WORKER"] = original_worker
            reload(dbt_project)

    def test_schema_name_suffix_without_worker(self):
        """Test that SCHEMA_NAME_SUFFIX is empty when PYTEST_XDIST_WORKER is not set."""
        with patch.dict(os.environ, {}, clear=True):
            # When no worker is set, suffix should be empty
            # This is the default case
            if PYTEST_XDIST_WORKER is None:
                assert SCHEMA_NAME_SUFFIX == ""


class TestGetDbtRunner:
    """Test the get_dbt_runner function."""

    @patch("dbt_project.create_dbt_runner")
    def test_get_dbt_runner_default_params(self, mock_create_runner):
        """Test that get_dbt_runner calls create_dbt_runner with correct defaults."""
        mock_runner = Mock()
        mock_create_runner.return_value = mock_runner

        result = get_dbt_runner("postgres", "/path/to/project")

        # Verify the call was made with expected parameters
        assert mock_create_runner.call_count == 1
        call_kwargs = mock_create_runner.call_args[1]

        assert call_kwargs["target"] == "postgres"
        assert call_kwargs["raise_on_failure"] is False
        assert call_kwargs["runner_method"] is None

        # Check vars dict
        assert call_kwargs["vars"]["disable_dbt_invocation_autoupload"] is True
        assert call_kwargs["vars"]["disable_dbt_artifacts_autoupload"] is True
        assert call_kwargs["vars"]["columns_upload_strategy"] == "none"
        assert call_kwargs["vars"]["disable_run_results"] is True
        assert call_kwargs["vars"]["disable_freshness_results"] is True
        assert call_kwargs["vars"]["debug_logs"] is True
        # schema_name_suffix comes from SCHEMA_NAME_SUFFIX constant
        assert "schema_name_suffix" in call_kwargs["vars"]

        assert result == mock_runner

    @patch("dbt_project.create_dbt_runner")
    def test_get_dbt_runner_with_runner_method(self, mock_create_runner):
        """Test that get_dbt_runner passes runner_method correctly."""
        from elementary.clients.dbt.factory import RunnerMethod

        mock_runner = Mock()
        mock_create_runner.return_value = mock_runner

        result = get_dbt_runner("postgres", "/path/to/project", RunnerMethod.FUSION)

        assert mock_create_runner.call_args[1]["runner_method"] == RunnerMethod.FUSION


class TestDbtProjectInit:
    """Test DbtProject initialization."""

    @patch("dbt_project.get_dbt_runner")
    def test_init_sets_attributes(self, mock_get_runner):
        """Test that DbtProject.__init__ sets all attributes correctly."""
        mock_runner = Mock()
        mock_get_runner.return_value = mock_runner

        with TemporaryDirectory() as temp_dir:
            project_dir = temp_dir
            project = DbtProject("postgres", project_dir)

            assert project.dbt_runner == mock_runner
            assert project.target == "postgres"
            assert project.runner_method is None
            assert project.project_dir_path == Path(project_dir)
            assert project.models_dir_path == Path(project_dir) / "models"
            assert project.tmp_models_dir_path == Path(project_dir) / "models" / "tmp"
            assert project.seeds_dir_path == Path(project_dir) / "data"
            assert project._query_runner is None

    @patch("dbt_project.get_dbt_runner")
    def test_init_with_runner_method(self, mock_get_runner):
        """Test that DbtProject.__init__ accepts runner_method."""
        from elementary.clients.dbt.factory import RunnerMethod

        mock_runner = Mock()
        mock_get_runner.return_value = mock_runner

        project = DbtProject("postgres", "/tmp", RunnerMethod.FUSION)

        assert project.runner_method == RunnerMethod.FUSION


class TestReadTableQuery:
    """Test the static read_table_query method."""

    def test_basic_query(self):
        """Test basic query generation without filters."""
        query = DbtProject.read_table_query("test_table")

        assert "SELECT *" in query
        assert "FROM {{ ref('test_table') }}" in query
        assert "WHERE" not in query
        assert "GROUP BY" not in query
        assert "ORDER BY" not in query
        assert "LIMIT" not in query

    def test_query_with_where(self):
        """Test query generation with WHERE clause."""
        query = DbtProject.read_table_query("test_table", where="id > 100")

        assert "SELECT *" in query
        assert "FROM {{ ref('test_table') }}" in query
        assert "WHERE id > 100" in query

    def test_query_with_group_by(self):
        """Test query generation with GROUP BY clause."""
        query = DbtProject.read_table_query("test_table", group_by="category")

        assert "SELECT *" in query
        assert "FROM {{ ref('test_table') }}" in query
        assert "GROUP BY category" in query

    def test_query_with_order_by(self):
        """Test query generation with ORDER BY clause."""
        query = DbtProject.read_table_query("test_table", order_by="created_at DESC")

        assert "SELECT *" in query
        assert "FROM {{ ref('test_table') }}" in query
        assert "ORDER BY created_at DESC" in query

    def test_query_with_limit(self):
        """Test query generation with LIMIT clause."""
        query = DbtProject.read_table_query("test_table", limit=10)

        assert "SELECT *" in query
        assert "FROM {{ ref('test_table') }}" in query
        assert "LIMIT 10" in query

    def test_query_with_column_names(self):
        """Test query generation with specific columns."""
        query = DbtProject.read_table_query("test_table", column_names=["id", "name"])

        assert "SELECT id, name" in query
        assert "FROM {{ ref('test_table') }}" in query

    def test_query_with_all_parameters(self):
        """Test query generation with all parameters."""
        query = DbtProject.read_table_query(
            "test_table",
            where="status = 'active'",
            group_by="category",
            order_by="count DESC",
            limit=5,
            column_names=["category", "COUNT(*) as count"],
        )

        assert "SELECT category, COUNT(*) as count" in query
        assert "FROM {{ ref('test_table') }}" in query
        assert "WHERE status = 'active'" in query
        assert "GROUP BY category" in query
        assert "ORDER BY count DESC" in query
        assert "LIMIT 5" in query


class TestDbtProjectReadTable:
    """Test the read_table method."""

    @patch("dbt_project.get_dbt_runner")
    def test_read_table_success(self, mock_get_runner):
        """Test successful table read."""
        mock_runner = Mock()
        mock_get_runner.return_value = mock_runner

        project = DbtProject("postgres", "/tmp")
        project.run_query = Mock(
            return_value=[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        )

        result = project.read_table("test_table")

        assert len(result) == 2
        assert result[0]["id"] == 1
        assert result[1]["name"] == "Bob"

    @patch("dbt_project.get_dbt_runner")
    def test_read_table_empty_raises_error_by_default(self, mock_get_runner):
        """Test that empty results raise ValueError by default."""
        mock_runner = Mock()
        mock_get_runner.return_value = mock_runner

        project = DbtProject("postgres", "/tmp")
        project.run_query = Mock(return_value=[])

        with pytest.raises(ValueError, match="is empty"):
            project.read_table("test_table")

    @patch("dbt_project.get_dbt_runner")
    def test_read_table_empty_no_error_when_disabled(self, mock_get_runner):
        """Test that empty results don't raise error when raise_if_empty=False."""
        mock_runner = Mock()
        mock_get_runner.return_value = mock_runner

        project = DbtProject("postgres", "/tmp")
        project.run_query = Mock(return_value=[])

        result = project.read_table("test_table", raise_if_empty=False)

        assert result == []

    @patch("dbt_project.get_dbt_runner")
    def test_read_table_with_filters(self, mock_get_runner):
        """Test that read_table passes parameters to read_table_query."""
        mock_runner = Mock()
        mock_get_runner.return_value = mock_runner

        project = DbtProject("postgres", "/tmp")
        project.run_query = Mock(return_value=[{"id": 1}])

        result = project.read_table(
            "test_table",
            where="id > 10",
            order_by="id DESC",
            limit=5,
            column_names=["id"],
        )

        # Verify that run_query was called with the right query
        call_args = project.run_query.call_args[0][0]
        assert "WHERE id > 10" in call_args
        assert "ORDER BY id DESC" in call_args
        assert "LIMIT 5" in call_args
        assert "SELECT id" in call_args


class TestDbtProjectTest:
    """Test the test method."""

    @patch("dbt_project.get_dbt_runner")
    def test_test_rejects_both_columns_and_test_column(self, mock_get_runner):
        """Test that ValueError is raised when both columns and test_column are specified."""
        mock_runner = Mock()
        mock_get_runner.return_value = mock_runner

        project = DbtProject("postgres", "/tmp")

        with pytest.raises(ValueError, match="can't specify both"):
            project.test(
                "test_id",
                "test_name",
                columns=[{"name": "col1"}],
                test_column="col2",
            )

    @patch("dbt_project.get_dbt_runner")
    def test_test_id_sanitization(self, mock_get_runner):
        """Test that test_id brackets are replaced with underscores."""
        mock_runner = Mock()
        mock_runner.test.return_value = True
        mock_get_runner.return_value = mock_runner

        with TemporaryDirectory() as temp_dir:
            # Create necessary directories
            models_dir = Path(temp_dir) / "models" / "tmp"
            models_dir.mkdir(parents=True)

            project = DbtProject("postgres", temp_dir)
            project.seed = Mock()
            project._read_single_test_result = Mock(return_value={"status": "pass"})

            # Test ID with brackets
            project.test("test[param1][param2]", "test_name", data=[{"col": "val"}])

            # Verify that _read_single_test_result was called with sanitized name
            call_args = project._read_single_test_result.call_args[0][0]
            assert "[" not in call_args
            assert "]" not in call_args
            assert "_" in call_args


class TestDbtProjectCreateTempModel:
    """Test the create_temp_model_for_existing_table method."""

    @patch("dbt_project.get_dbt_runner")
    def test_create_temp_model_default(self, mock_get_runner):
        """Test creating temporary model with default code."""
        mock_runner = Mock()
        mock_get_runner.return_value = mock_runner

        with TemporaryDirectory() as temp_dir:
            models_tmp_dir = Path(temp_dir) / "models" / "tmp"
            models_tmp_dir.mkdir(parents=True)

            project = DbtProject("postgres", temp_dir)

            with project.create_temp_model_for_existing_table("test_table") as model_path:
                # Model file should exist
                full_path = Path(temp_dir) / model_path
                assert full_path.exists()

                # Check contents
                content = full_path.read_text()
                assert DEFAULT_DUMMY_CODE in content

            # Model should be cleaned up
            assert not full_path.exists()

    @patch("dbt_project.get_dbt_runner")
    def test_create_temp_model_with_materialization(self, mock_get_runner):
        """Test creating temporary model with materialization config."""
        mock_runner = Mock()
        mock_get_runner.return_value = mock_runner

        with TemporaryDirectory() as temp_dir:
            models_tmp_dir = Path(temp_dir) / "models" / "tmp"
            models_tmp_dir.mkdir(parents=True)

            project = DbtProject("postgres", temp_dir)

            with project.create_temp_model_for_existing_table(
                "test_table", materialization="view"
            ) as model_path:
                full_path = Path(temp_dir) / model_path
                content = full_path.read_text()

                assert "{{ config(materialized='view') }}" in content
                assert DEFAULT_DUMMY_CODE in content

    @patch("dbt_project.get_dbt_runner")
    def test_create_temp_model_with_custom_code(self, mock_get_runner):
        """Test creating temporary model with custom SQL code."""
        mock_runner = Mock()
        mock_get_runner.return_value = mock_runner

        with TemporaryDirectory() as temp_dir:
            models_tmp_dir = Path(temp_dir) / "models" / "tmp"
            models_tmp_dir.mkdir(parents=True)

            project = DbtProject("postgres", temp_dir)

            custom_code = "SELECT * FROM custom_table"

            with project.create_temp_model_for_existing_table(
                "test_table", raw_code=custom_code
            ) as model_path:
                full_path = Path(temp_dir) / model_path
                content = full_path.read_text()

                assert custom_code in content
                assert DEFAULT_DUMMY_CODE not in content


class TestDbtProjectReadProfileSchema:
    """Test the _read_profile_schema method."""

    @patch("dbt_project.get_dbt_runner")
    def test_read_profile_schema_success(self, mock_get_runner):
        """Test reading schema from profiles.yml."""
        mock_runner = Mock()
        mock_get_runner.return_value = mock_runner

        with TemporaryDirectory() as temp_dir:
            profiles_dir = Path(temp_dir)
            profiles_file = profiles_dir / "profiles.yml"

            # Create a mock profiles.yml
            profiles_content = """
elementary_tests:
  outputs:
    postgres:
      schema: test_schema
      type: postgres
    spark:
      schema: spark_schema
      type: spark
  target: postgres
"""
            profiles_file.write_text(profiles_content)

            project = DbtProject("postgres", "/tmp")

            with patch.dict(os.environ, {"DBT_PROFILES_DIR": str(profiles_dir)}):
                schema = project._read_profile_schema()

            assert schema == "test_schema"

    @patch("dbt_project.get_dbt_runner")
    def test_read_profile_schema_missing_file(self, mock_get_runner):
        """Test that missing profiles.yml raises RuntimeError."""
        mock_runner = Mock()
        mock_get_runner.return_value = mock_runner

        with TemporaryDirectory() as temp_dir:
            project = DbtProject("postgres", "/tmp")

            with patch.dict(os.environ, {"DBT_PROFILES_DIR": temp_dir}):
                with pytest.raises(RuntimeError, match="profiles not found"):
                    project._read_profile_schema()

    @patch("dbt_project.get_dbt_runner")
    def test_read_profile_schema_missing_target(self, mock_get_runner):
        """Test that missing target in profiles raises RuntimeError."""
        mock_runner = Mock()
        mock_get_runner.return_value = mock_runner

        with TemporaryDirectory() as temp_dir:
            profiles_dir = Path(temp_dir)
            profiles_file = profiles_dir / "profiles.yml"

            profiles_content = """
elementary_tests:
  outputs:
    postgres:
      schema: test_schema
  target: postgres
"""
            profiles_file.write_text(profiles_content)

            # Try to read for a target that doesn't exist
            project = DbtProject("nonexistent_target", "/tmp")

            with patch.dict(os.environ, {"DBT_PROFILES_DIR": str(profiles_dir)}):
                with pytest.raises(RuntimeError, match="Missing schema"):
                    project._read_profile_schema()


class TestDbtProjectCreateSeeder:
    """Test the _create_seeder method."""

    @patch("dbt_project.get_dbt_runner")
    def test_create_seeder_clickhouse(self, mock_get_runner):
        """Test that ClickHouse target uses ClickHouseDirectSeeder."""
        from data_seeder import ClickHouseDirectSeeder

        mock_runner = Mock()
        mock_get_runner.return_value = mock_runner

        with TemporaryDirectory() as temp_dir:
            project = DbtProject("clickhouse", temp_dir)

            # Mock the query runner
            mock_query_runner = Mock()
            mock_query_runner.schema_name = "test_schema"
            project._query_runner = mock_query_runner

            seeder = project._create_seeder()

            assert isinstance(seeder, ClickHouseDirectSeeder)

    @patch("dbt_project.get_dbt_runner")
    def test_create_seeder_spark(self, mock_get_runner):
        """Test that Spark target uses SparkS3CsvSeeder."""
        from data_seeder import SparkS3CsvSeeder

        mock_runner = Mock()
        mock_get_runner.return_value = mock_runner

        with TemporaryDirectory() as temp_dir:
            project = DbtProject("spark", temp_dir)

            # Mock the _read_profile_schema method
            project._read_profile_schema = Mock(return_value="spark_schema")

            seeder = project._create_seeder()

            assert isinstance(seeder, SparkS3CsvSeeder)
            # Schema should include suffix if SCHEMA_NAME_SUFFIX is set
            assert seeder._schema.startswith("spark_schema")
            if SCHEMA_NAME_SUFFIX:
                assert seeder._schema == f"spark_schema{SCHEMA_NAME_SUFFIX}"
            else:
                assert seeder._schema == "spark_schema"

    @patch("dbt_project.get_dbt_runner")
    def test_create_seeder_default(self, mock_get_runner):
        """Test that other targets use DbtDataSeeder."""
        from data_seeder import DbtDataSeeder

        mock_runner = Mock()
        mock_get_runner.return_value = mock_runner

        with TemporaryDirectory() as temp_dir:
            project = DbtProject("postgres", temp_dir)

            seeder = project._create_seeder()

            assert isinstance(seeder, DbtDataSeeder)


class TestDbtProjectFixSeedIfNeeded:
    """Test the _fix_seed_if_needed method."""

    @patch("dbt_project.get_dbt_runner")
    def test_fix_seed_for_bigquery_fusion(self, mock_get_runner):
        """Test that seed fix is applied for BigQuery with Fusion."""
        from elementary.clients.dbt.factory import RunnerMethod

        mock_runner = Mock()
        mock_get_runner.return_value = mock_runner

        project = DbtProject("bigquery", "/tmp", RunnerMethod.FUSION)

        project._fix_seed_if_needed("test_table")

        # Should call run_operation to fix empty strings
        mock_runner.run_operation.assert_called_once_with(
            "elementary_tests.replace_empty_strings_with_nulls",
            macro_args={"table_name": "test_table"},
        )

    @patch("dbt_project.get_dbt_runner")
    def test_fix_seed_not_called_for_other_targets(self, mock_get_runner):
        """Test that seed fix is not called for non-BigQuery targets."""
        from elementary.clients.dbt.factory import RunnerMethod

        mock_runner = Mock()
        mock_get_runner.return_value = mock_runner

        project = DbtProject("postgres", "/tmp", RunnerMethod.FUSION)

        project._fix_seed_if_needed("test_table")

        # Should not call run_operation
        mock_runner.run_operation.assert_not_called()

    @patch("dbt_project.get_dbt_runner")
    def test_fix_seed_not_called_without_fusion(self, mock_get_runner):
        """Test that seed fix is not called without Fusion runner."""
        mock_runner = Mock()
        mock_get_runner.return_value = mock_runner

        project = DbtProject("bigquery", "/tmp")

        project._fix_seed_if_needed("test_table")

        # Should not call run_operation
        mock_runner.run_operation.assert_not_called()


class TestDefaultDummyCode:
    """Test the DEFAULT_DUMMY_CODE constant."""

    def test_default_dummy_code_is_valid_sql(self):
        """Test that DEFAULT_DUMMY_CODE is valid SQL."""
        assert DEFAULT_DUMMY_CODE == "SELECT 1 AS col"