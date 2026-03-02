"""Unit tests for data_seeder.py module."""

import csv
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List
from unittest.mock import MagicMock, Mock, patch, call

import pytest

from data_seeder import (
    BaseDirectSeeder,
    ClickHouseDirectSeeder,
    DbtDataSeeder,
    SparkS3CsvSeeder,
    infer_column_type_tag,
)


class TestInferColumnTypeTag:
    """Test the infer_column_type_tag function for type inference."""

    def test_infer_boolean_type(self):
        """Test that boolean columns are correctly identified."""
        values = [True, False, True]
        assert infer_column_type_tag(values) == "boolean"

    def test_infer_boolean_type_with_none(self):
        """Test that boolean columns with None values are correctly identified."""
        values = [True, None, False, None, True]
        assert infer_column_type_tag(values) == "boolean"

    def test_infer_integer_type(self):
        """Test that integer columns are correctly identified."""
        values = [1, 2, 3, 100, -5]
        assert infer_column_type_tag(values) == "integer"

    def test_infer_integer_type_with_none(self):
        """Test that integer columns with None values are correctly identified."""
        values = [1, None, 2, None, 3]
        assert infer_column_type_tag(values) == "integer"

    def test_infer_float_type(self):
        """Test that float columns are correctly identified."""
        values = [1.5, 2.3, 3.7]
        assert infer_column_type_tag(values) == "float"

    def test_infer_float_type_mixed_int_float(self):
        """Test that mixed int/float columns are identified as float."""
        values = [1, 2.5, 3, 4.7]
        assert infer_column_type_tag(values) == "float"

    def test_infer_string_type(self):
        """Test that string columns are correctly identified."""
        values = ["hello", "world", "test"]
        assert infer_column_type_tag(values) == "string"

    def test_infer_string_type_with_none(self):
        """Test that string columns with None values are correctly identified."""
        values = ["hello", None, "world"]
        assert infer_column_type_tag(values) == "string"

    def test_infer_string_type_with_empty_strings(self):
        """Test that columns with empty strings are identified as string."""
        values = ["hello", "", "world"]
        assert infer_column_type_tag(values) == "string"

    def test_all_none_values(self):
        """Test that all-None columns default to string."""
        values = [None, None, None]
        assert infer_column_type_tag(values) == "string"

    def test_all_empty_strings(self):
        """Test that all-empty-string columns default to string."""
        values = ["", "", ""]
        assert infer_column_type_tag(values) == "string"

    def test_empty_list(self):
        """Test that empty list defaults to string."""
        values: List[object] = []
        assert infer_column_type_tag(values) == "string"

    def test_boolean_not_confused_with_integer(self):
        """Test that booleans are not confused with integers (bool is subclass of int)."""
        # In Python, bool is a subclass of int, so we need to check bool first
        values = [True, False]
        assert infer_column_type_tag(values) == "boolean"

    def test_string_numeric_representations_as_integer(self):
        """Test that string representations of integers are identified as integer."""
        values = ["1", "2", "3", "100"]
        assert infer_column_type_tag(values) == "integer"

    def test_string_numeric_representations_as_float(self):
        """Test that string representations of floats are identified as float."""
        values = ["1.5", "2.3", "3.7"]
        assert infer_column_type_tag(values) == "float"

    def test_string_mixed_numeric_representations(self):
        """Test that mixed string numeric representations are identified as float."""
        values = ["1", "2.5", "3"]
        assert infer_column_type_tag(values) == "float"

    def test_string_non_numeric_representations(self):
        """Test that non-numeric string representations default to string."""
        values = ["1", "2", "not_a_number"]
        assert infer_column_type_tag(values) == "string"

    def test_mixed_types_default_to_string(self):
        """Test that mixed non-compatible types default to string."""
        values = [1, "hello", 2.5]
        assert infer_column_type_tag(values) == "string"


class TestDbtDataSeeder:
    """Test the DbtDataSeeder class."""

    def test_init(self):
        """Test that DbtDataSeeder initializes correctly."""
        mock_runner = Mock()
        project_path = Path("/test/project")
        seeds_path = Path("/test/seeds")

        seeder = DbtDataSeeder(mock_runner, project_path, seeds_path)

        assert seeder.dbt_runner == mock_runner
        assert seeder.dbt_project_path == project_path
        assert seeder.seeds_dir_path == seeds_path

    def test_seed_empty_data_raises_error(self):
        """Test that seeding with empty data raises ValueError."""
        mock_runner = Mock()
        with TemporaryDirectory() as temp_dir:
            project_path = Path(temp_dir)
            seeds_path = Path(temp_dir) / "seeds"
            seeds_path.mkdir()

            seeder = DbtDataSeeder(mock_runner, project_path, seeds_path)

            with pytest.raises(ValueError, match="must not be empty"):
                with seeder.seed([], "test_table"):
                    pass

    def test_seed_creates_csv_and_runs_dbt_seed(self):
        """Test that seed creates CSV file and runs dbt seed command."""
        mock_runner = Mock()
        mock_runner.seed.return_value = True

        with TemporaryDirectory() as temp_dir:
            project_path = Path(temp_dir)
            seeds_path = Path(temp_dir) / "seeds"
            seeds_path.mkdir()

            seeder = DbtDataSeeder(mock_runner, project_path, seeds_path)
            data = [{"col1": "value1", "col2": 123}, {"col1": "value2", "col2": 456}]

            with seeder.seed(data, "test_table"):
                # CSV file should exist
                csv_path = seeds_path / "test_table.csv"
                assert csv_path.exists()

                # Check CSV contents
                with csv_path.open("r") as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                    assert len(rows) == 2
                    assert rows[0]["col1"] == "value1"
                    assert rows[0]["col2"] == "123"
                    assert rows[1]["col1"] == "value2"
                    assert rows[1]["col2"] == "456"

                # dbt seed should have been called
                mock_runner.seed.assert_called_once()

            # CSV file should be cleaned up after context exits
            assert not csv_path.exists()

    def test_seed_failure_raises_runtime_error(self):
        """Test that dbt seed failure raises RuntimeError."""
        mock_runner = Mock()
        mock_runner.seed.return_value = False

        with TemporaryDirectory() as temp_dir:
            project_path = Path(temp_dir)
            seeds_path = Path(temp_dir) / "seeds"
            seeds_path.mkdir()

            seeder = DbtDataSeeder(mock_runner, project_path, seeds_path)
            data = [{"col1": "value1"}]

            with pytest.raises(RuntimeError, match="dbt seed failed"):
                with seeder.seed(data, "test_table"):
                    pass


class TestClickHouseDirectSeeder:
    """Test the ClickHouseDirectSeeder class."""

    def test_type_methods(self):
        """Test that ClickHouse type methods return correct Nullable types."""
        mock_runner = Mock()
        mock_runner.schema_name = "test_schema"
        seeds_path = Path("/test/seeds")

        seeder = ClickHouseDirectSeeder(mock_runner, "test_schema", seeds_path)

        assert seeder._type_string() == "Nullable(String)"
        assert seeder._type_boolean() == "Nullable(Bool)"
        assert seeder._type_integer() == "Nullable(Int64)"
        assert seeder._type_float() == "Nullable(Float64)"

    def test_format_value_none(self):
        """Test that None values are formatted as NULL."""
        mock_runner = Mock()
        seeder = ClickHouseDirectSeeder(mock_runner, "test_schema", Path("/test"))

        assert seeder._format_value(None, "Nullable(String)") == "NULL"

    def test_format_value_empty_string(self):
        """Test that empty strings are formatted as NULL."""
        mock_runner = Mock()
        seeder = ClickHouseDirectSeeder(mock_runner, "test_schema", Path("/test"))

        assert seeder._format_value("", "Nullable(String)") == "NULL"

    def test_format_value_boolean_true(self):
        """Test that boolean True is formatted correctly."""
        mock_runner = Mock()
        seeder = ClickHouseDirectSeeder(mock_runner, "test_schema", Path("/test"))

        assert seeder._format_value(True, "Nullable(Bool)") == "true"

    def test_format_value_boolean_false(self):
        """Test that boolean False is formatted correctly."""
        mock_runner = Mock()
        seeder = ClickHouseDirectSeeder(mock_runner, "test_schema", Path("/test"))

        assert seeder._format_value(False, "Nullable(Bool)") == "false"

    def test_format_value_integer(self):
        """Test that integers are formatted correctly."""
        mock_runner = Mock()
        seeder = ClickHouseDirectSeeder(mock_runner, "test_schema", Path("/test"))

        assert seeder._format_value(42, "Nullable(Int64)") == "42"
        assert seeder._format_value(-10, "Nullable(Int64)") == "-10"

    def test_format_value_float(self):
        """Test that floats are formatted correctly."""
        mock_runner = Mock()
        seeder = ClickHouseDirectSeeder(mock_runner, "test_schema", Path("/test"))

        assert seeder._format_value(3.14, "Nullable(Float64)") == "3.14"
        assert seeder._format_value(-2.5, "Nullable(Float64)") == "-2.5"

    def test_format_value_string_escaping(self):
        """Test that strings are properly escaped."""
        mock_runner = Mock()
        seeder = ClickHouseDirectSeeder(mock_runner, "test_schema", Path("/test"))

        assert seeder._format_value("hello", "Nullable(String)") == "'hello'"
        assert seeder._format_value("it's", "Nullable(String)") == "'it\\'s'"
        assert seeder._format_value("back\\slash", "Nullable(String)") == "'back\\\\slash'"

    def test_create_table_sql(self):
        """Test that CREATE TABLE SQL is generated correctly."""
        mock_runner = Mock()
        seeder = ClickHouseDirectSeeder(mock_runner, "test_schema", Path("/test"))

        col_defs = "`col1` Nullable(String), `col2` Nullable(Int64)"
        sql = seeder._create_table_sql("`schema`.`table`", col_defs)

        assert "CREATE TABLE `schema`.`table`" in sql
        assert col_defs in sql
        assert "ENGINE = MergeTree()" in sql
        assert "ORDER BY tuple()" in sql


class TestSparkS3CsvSeeder:
    """Test the SparkS3CsvSeeder class."""

    def test_init(self):
        """Test that SparkS3CsvSeeder initializes correctly."""
        schema = "test_schema"
        seeds_path = Path("/test/seeds")

        seeder = SparkS3CsvSeeder(schema, seeds_path)

        assert seeder._schema == schema
        assert seeder._seeds_dir_path == seeds_path

    def test_init_with_env_vars(self):
        """Test that environment variables are used for configuration."""
        # Note: These are class attributes that read from env at class definition time,
        # not instance attributes. We test that the defaults work as expected.
        seeder = SparkS3CsvSeeder("test_schema", Path("/test"))

        # Verify default values exist
        assert seeder._MINIO_ENDPOINT is not None
        assert seeder._MINIO_ACCESS_KEY is not None
        assert seeder._MINIO_SECRET_KEY is not None
        assert seeder._S3_BUCKET is not None
        assert seeder._THRIFT_HOST is not None
        assert isinstance(seeder._THRIFT_PORT, int)

    def test_write_seed_csv_with_none_values(self):
        """Test that CSV writing handles None values as empty strings."""
        with TemporaryDirectory() as temp_dir:
            seeds_path = Path(temp_dir)
            seeder = SparkS3CsvSeeder("test_schema", seeds_path)

            data = [
                {"col1": "value1", "col2": 123, "col3": None},
                {"col1": None, "col2": 456, "col3": "value3"},
            ]

            csv_path = seeder._write_seed_csv(data, "test_table")

            assert csv_path.exists()

            # Read and verify CSV contents
            with csv_path.open("r", newline="") as f:
                reader = csv.reader(f)
                rows = list(reader)

                # Check header
                assert rows[0] == ["col1", "col2", "col3"]

                # Check data rows - None should be empty strings, all quoted
                assert rows[1] == ["value1", "123", ""]
                assert rows[2] == ["", "456", "value3"]

            # Clean up
            csv_path.unlink()

    def test_write_seed_csv_uses_quote_all(self):
        """Test that CSV writing uses QUOTE_ALL to prevent blank line skipping."""
        with TemporaryDirectory() as temp_dir:
            seeds_path = Path(temp_dir)
            seeder = SparkS3CsvSeeder("test_schema", seeds_path)

            # Row with all None values - should still produce a non-blank line
            data = [
                {"col1": None, "col2": None, "col3": None},
            ]

            csv_path = seeder._write_seed_csv(data, "test_table")

            # Read raw CSV content
            with csv_path.open("r") as f:
                content = f.read()

            # Should have quotes around empty values
            assert '""' in content
            # Should not have completely blank lines (just newlines)
            lines = content.strip().split("\n")
            assert len(lines) == 2  # header + 1 data row

            # Clean up
            csv_path.unlink()

    def test_infer_spark_schema_basic_types(self):
        """Test that Spark schema inference works for basic types."""
        seeder = SparkS3CsvSeeder("test_schema", Path("/test"))

        data = [
            {"str_col": "hello", "int_col": 123, "float_col": 3.14, "bool_col": True},
            {"str_col": "world", "int_col": 456, "float_col": 2.71, "bool_col": False},
        ]

        schema = seeder._infer_spark_schema(data)

        assert "`str_col` STRING" in schema
        assert "`int_col` BIGINT" in schema
        assert "`float_col` DOUBLE" in schema
        assert "`bool_col` BOOLEAN" in schema

    def test_infer_spark_schema_with_none_values(self):
        """Test that Spark schema inference works with None values."""
        seeder = SparkS3CsvSeeder("test_schema", Path("/test"))

        data = [
            {"col1": None, "col2": 123},
            {"col1": "value", "col2": None},
        ]

        schema = seeder._infer_spark_schema(data)

        # Should infer from non-None values
        assert "`col1` STRING" in schema
        assert "`col2` BIGINT" in schema

    def test_spark_type_map(self):
        """Test that Spark type mapping is correct."""
        assert SparkS3CsvSeeder._SPARK_TYPE_MAP["string"] == "STRING"
        assert SparkS3CsvSeeder._SPARK_TYPE_MAP["boolean"] == "BOOLEAN"
        assert SparkS3CsvSeeder._SPARK_TYPE_MAP["integer"] == "BIGINT"
        assert SparkS3CsvSeeder._SPARK_TYPE_MAP["float"] == "DOUBLE"

    def test_seed_empty_data_raises_error(self):
        """Test that seeding with empty data raises ValueError."""
        seeder = SparkS3CsvSeeder("test_schema", Path("/test"))

        with pytest.raises(ValueError, match="must not be empty"):
            with seeder.seed([], "test_table"):
                pass

    @patch("data_seeder.SparkS3CsvSeeder._get_s3_client")
    @patch("data_seeder.SparkS3CsvSeeder._spark_connection")
    def test_seed_workflow(self, mock_spark_conn, mock_s3_client):
        """Test the complete seed workflow for Spark."""
        # Setup mocks
        mock_s3 = Mock()
        mock_s3_client.return_value = mock_s3

        mock_conn = Mock()
        mock_spark_conn.return_value.__enter__.return_value = mock_conn

        with TemporaryDirectory() as temp_dir:
            seeds_path = Path(temp_dir)
            seeder = SparkS3CsvSeeder("test_schema", seeds_path)

            data = [{"col1": "value1", "col2": 123}]

            with seeder.seed(data, "test_table"):
                # S3 upload should be called
                mock_s3.upload_file.assert_called_once()
                upload_args = mock_s3.upload_file.call_args[0]
                assert upload_args[1] == "spark-seeds"  # bucket
                assert upload_args[2] == "test_schema/test_table.csv"  # key

            # CSV should be cleaned up
            csv_path = seeds_path / "test_table.csv"
            assert not csv_path.exists()

    def test_get_s3_client(self):
        """Test that S3 client is created with correct configuration."""
        # Import boto3 inside the method to avoid import errors if boto3 not installed
        with patch("boto3.client") as mock_boto3_client:
            seeder = SparkS3CsvSeeder("test_schema", Path("/test"))
            seeder._get_s3_client()

            mock_boto3_client.assert_called_once_with(
                "s3",
                endpoint_url=seeder._MINIO_ENDPOINT,
                aws_access_key_id=seeder._MINIO_ACCESS_KEY,
                aws_secret_access_key=seeder._MINIO_SECRET_KEY,
            )


class TestBaseDirectSeeder:
    """Test the BaseDirectSeeder abstract class through a concrete implementation."""

    class ConcreteSeeder(BaseDirectSeeder):
        """Concrete implementation for testing."""

        def _type_string(self) -> str:
            return "VARCHAR"

        def _type_boolean(self) -> str:
            return "BOOLEAN"

        def _type_integer(self) -> str:
            return "INTEGER"

        def _type_float(self) -> str:
            return "FLOAT"

        def _format_value(self, value: object, col_type: str) -> str:
            if value is None:
                return "NULL"
            return str(value)

        def _create_table_sql(self, fq_table: str, col_defs: str) -> str:
            return f"CREATE TABLE {fq_table} ({col_defs})"

    def test_infer_column_type(self):
        """Test that column type inference uses type tag mapping."""
        mock_runner = Mock()
        seeder = self.ConcreteSeeder(mock_runner, "schema", Path("/test"))

        # Test different types
        assert seeder._infer_column_type([1, 2, 3]) == "INTEGER"
        assert seeder._infer_column_type([1.5, 2.5]) == "FLOAT"
        assert seeder._infer_column_type([True, False]) == "BOOLEAN"
        assert seeder._infer_column_type(["a", "b"]) == "VARCHAR"

    def test_write_csv(self):
        """Test that CSV writing works correctly."""
        with TemporaryDirectory() as temp_dir:
            seeds_path = Path(temp_dir)
            mock_runner = Mock()
            seeder = self.ConcreteSeeder(mock_runner, "schema", seeds_path)

            data = [{"col1": "value1", "col2": 123}, {"col1": "value2", "col2": 456}]

            csv_path = seeder._write_csv(data, "test_table")

            assert csv_path.exists()
            assert csv_path == seeds_path / "test_table.csv"

            # Verify CSV contents
            with csv_path.open("r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 2
                assert rows[0]["col1"] == "value1"

            # Clean up
            csv_path.unlink()

    @patch.object(ConcreteSeeder, "_write_csv")
    def test_seed_creates_table_and_inserts_data(self, mock_write_csv):
        """Test that seed creates table and inserts data."""
        with TemporaryDirectory() as temp_dir:
            seeds_path = Path(temp_dir)
            csv_path = seeds_path / "test.csv"
            csv_path.touch()
            mock_write_csv.return_value = csv_path

            mock_runner = Mock()
            seeder = self.ConcreteSeeder(mock_runner, "schema", seeds_path)

            data = [{"col1": "value1", "col2": 123}]

            with seeder.seed(data, "test_table"):
                # Should execute DROP TABLE
                calls = mock_runner.execute_sql.call_args_list
                assert any("DROP TABLE" in str(call) for call in calls)

                # Should execute CREATE TABLE
                assert any("CREATE TABLE" in str(call) for call in calls)

                # Should execute INSERT
                assert any("INSERT INTO" in str(call) for call in calls)

            # CSV should be cleaned up
            assert not csv_path.exists()

    def test_type_tag_map_completeness(self):
        """Test that TYPE_TAG_MAP covers all expected type tags."""
        expected_tags = {"string", "boolean", "integer", "float"}
        assert set(BaseDirectSeeder._TYPE_TAG_MAP.keys()) == expected_tags


class TestIntegrationScenarios:
    """Integration tests for realistic data seeding scenarios."""

    def test_mixed_data_types_inference(self):
        """Test type inference with realistic mixed data."""
        data = [
            {"id": 1, "name": "Alice", "score": 95.5, "active": True, "note": None},
            {"id": 2, "name": "Bob", "score": 87.3, "active": False, "note": "Good"},
            {"id": 3, "name": None, "score": 92.0, "active": True, "note": None},
        ]

        columns = list(data[0].keys())
        expected_types = {
            "id": "integer",
            "name": "string",
            "score": "float",
            "active": "boolean",
            "note": "string",
        }

        for col in columns:
            values = [row.get(col) for row in data]
            inferred = infer_column_type_tag(values)
            assert inferred == expected_types[col], f"Column {col} type mismatch"

    def test_edge_case_all_nulls_in_column(self):
        """Test behavior when a column has only NULL values."""
        data = [
            {"col1": "value", "col2": None},
            {"col1": "another", "col2": None},
        ]

        col2_values = [row.get("col2") for row in data]
        # All None should default to string
        assert infer_column_type_tag(col2_values) == "string"

    def test_numeric_strings_vs_native_numbers(self):
        """Test that native numbers are preferred over string representations."""
        # Native integers
        assert infer_column_type_tag([1, 2, 3]) == "integer"

        # String representations - function will parse these as integers
        assert infer_column_type_tag(["1", "2", "3"]) == "integer"

        # Mixed - the function tries to parse all values as strings, so mixed types
        # with numeric strings will be inferred as integer if all parse as int
        values_mixed = [1, "2", 3]
        # All values can be converted to int, so result is integer
        result = infer_column_type_tag(values_mixed)
        assert result == "integer"

    def test_spark_csv_quote_all_prevents_blank_line_skipping(self):
        """Regression test: QUOTE_ALL should prevent Spark from skipping rows with all NULLs.

        This is a regression test for the fix in commit 4eb9dc6 that uses QUOTE_ALL
        to ensure that rows with all NULL values are written as quoted empty strings
        rather than completely blank lines, which Spark's CSV reader would skip.
        """
        with TemporaryDirectory() as temp_dir:
            seeds_path = Path(temp_dir)
            seeder = SparkS3CsvSeeder("test_schema", seeds_path)

            # Create data with rows that have all NULL values
            data = [
                {"col1": "value1", "col2": "value2"},
                {"col1": None, "col2": None},  # All NULL row
                {"col1": "value3", "col2": "value4"},
            ]

            csv_path = seeder._write_seed_csv(data, "test_table")

            # Read the raw CSV to verify formatting
            with csv_path.open("r") as f:
                lines = f.readlines()

            # Should have header + 3 data rows
            assert len(lines) == 4, "Should have 4 lines total"

            # Second data row (all NULLs) should not be blank - should have quoted empty strings
            all_null_row = lines[2].strip()
            assert all_null_row != "", "Row with all NULLs should not be blank"
            assert '""' in all_null_row, "Row with all NULLs should have quoted empty strings"

            # Verify all rows have content (no blank lines)
            for i, line in enumerate(lines):
                assert line.strip() != "", f"Line {i} should not be blank"

            # Clean up
            csv_path.unlink()