import json
import pytest
from unittest.mock import MagicMock, patch, mock_open
from elementary_neo4j.neo4j_exporter import Neo4jLineageExporter
from elementary_neo4j.neo4j_config import Neo4jConfig


@pytest.fixture
def config():
    return Neo4jConfig(
        uri="bolt://localhost:7687",
        username="neo4j",
        password="test",
        database="neo4j"
    )


@pytest.fixture
def exporter(config):
    with patch("elementary_neo4j.neo4j_exporter.GraphDatabase.driver"):
        return Neo4jLineageExporter(config)


@pytest.fixture
def sample_manifest():
    return {
        "nodes": {
            "model.my_project.dim_customers": {
                "name": "dim_customers",
                "resource_type": "model",
                "schema": "analytics",
                "database": "snowflake_db",
                "package_name": "my_project",
                "description": "Customer dimension table",
                "depends_on": {
                    "nodes": ["source.my_project.raw_customers"]
                }
            },
            "model.my_project.fct_orders": {
                "name": "fct_orders",
                "resource_type": "model",
                "schema": "analytics",
                "database": "snowflake_db",
                "package_name": "my_project",
                "description": "Orders fact table",
                "depends_on": {
                    "nodes": ["model.my_project.dim_customers"]
                }
            }
        },
        "sources": {
            "source.my_project.raw_customers": {
                "name": "raw_customers",
                "resource_type": "source",
                "schema": "raw",
                "database": "snowflake_db",
                "package_name": "my_project",
                "description": "Raw customers source"
            }
        }
    }


def test_extract_nodes_returns_models_and_sources(exporter, sample_manifest):
    nodes = exporter.extract_nodes(sample_manifest)
    assert len(nodes) == 3
    resource_types = [n["resource_type"] for n in nodes]
    assert "model" in resource_types
    assert "source" in resource_types


def test_extract_nodes_contains_correct_fields(exporter, sample_manifest):
    nodes = exporter.extract_nodes(sample_manifest)
    model_node = next(n for n in nodes if n["name"] == "dim_customers")
    assert model_node["schema"] == "analytics"
    assert model_node["database"] == "snowflake_db"
    assert model_node["description"] == "Customer dimension table"


def test_extract_dependencies_correct_count(exporter, sample_manifest):
    dependencies = exporter.extract_dependencies(sample_manifest)
    assert len(dependencies) == 2


def test_extract_dependencies_correct_direction(exporter, sample_manifest):
    dependencies = exporter.extract_dependencies(sample_manifest)
    dep = next(
        d for d in dependencies
        if d["to_id"] == "model.my_project.dim_customers"
    )
    assert dep["from_id"] == "source.my_project.raw_customers"


def test_load_manifest_file_not_found(exporter):
    with pytest.raises(FileNotFoundError):
        exporter.load_manifest("nonexistent/path/manifest.json")


def test_load_manifest_reads_correctly(exporter, sample_manifest):
    mock_data = json.dumps(sample_manifest)
    with patch("builtins.open", mock_open(read_data=mock_data)):
        with patch("pathlib.Path.exists", return_value=True):
            result = exporter.load_manifest("fake/manifest.json")
    assert "nodes" in result
    assert "sources" in result


def test_export_nodes_calls_session(exporter, sample_manifest):
    nodes = exporter.extract_nodes(sample_manifest)
    mock_session = MagicMock()
    exporter.driver.session.return_value.__enter__ = MagicMock(
        return_value=mock_session
    )
    exporter.driver.session.return_value.__exit__ = MagicMock(
        return_value=False
    )
    exporter.export_nodes(nodes)
    assert mock_session.run.call_count == len(nodes)


def test_export_returns_correct_counts(exporter, sample_manifest):
    with patch.object(exporter, "load_manifest", return_value=sample_manifest):
        with patch.object(exporter, "export_nodes"):
            with patch.object(exporter, "export_dependencies"):
                result = exporter.export("fake/manifest.json")
    assert result["nodes_exported"] == 3
    assert result["dependencies_exported"] == 2