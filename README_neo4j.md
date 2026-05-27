# Elementary Neo4j Lineage Exporter

## Overview

This utility exports dbt lineage data from Elementary's dbt manifest into Neo4j
as a property graph. It enables downstream impact analysis, root cause detection,
and lineage visualization directly in Neo4j.

## Motivation

Elementary already captures rich lineage metadata via dbt artifacts. This exporter
makes that lineage available in Neo4j, enabling graph traversal queries for:

- **Impact analysis** — which models are affected if a source schema changes?
- **Root cause detection** — trace data quality issues upstream
- **Lineage visualization** — explore your dbt DAG as a graph

## Graph Model

**Nodes** — each dbt model, source, seed, and snapshot becomes a `DbtNode`:
- `unique_id` — dbt unique identifier (primary key)
- `name` — model/source name
- `resource_type` — model, source, seed, snapshot
- `schema` — target schema in your warehouse
- `database` — target database
- `package_name` — dbt package
- `description` — model documentation

**Relationships** — `(upstream)-[:FEEDS_INTO]->(downstream)`

## Installation

```bash
pip install neo4j
```

## Usage

```python
from elementary_neo4j.neo4j_config import Neo4jConfig
from elementary_neo4j.neo4j_exporter import Neo4jLineageExporter

config = Neo4jConfig(
    uri="bolt://localhost:7687",
    username="neo4j",
    password="your-password"
)

exporter = Neo4jLineageExporter(config)
result = exporter.export("path/to/manifest.json")
print(result)
# {"nodes_exported": 42, "dependencies_exported": 67}
exporter.close()
```

## Environment Variables

```bash
export NEO4J_URI=bolt://localhost:7687
export NEO4J_USERNAME=neo4j
export NEO4J_PASSWORD=your-password
export NEO4J_DATABASE=neo4j
```

Then use:
```python
config = Neo4jConfig.from_env()
```

## Example Neo4j Query

Find all models impacted by a source change:
```cypher
MATCH (source:DbtNode {unique_id: "source.my_project.raw_customers"})-[:FEEDS_INTO*]->(impacted)
RETURN impacted.unique_id, impacted.name, impacted.resource_type
```

## Running Tests

```bash
PYTHONPATH=. python -m pytest tests/test_neo4j_exporter.py -v --ignore=integration_tests
```