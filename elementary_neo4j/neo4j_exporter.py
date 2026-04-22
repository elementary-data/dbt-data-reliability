import json
import logging
from pathlib import Path
from typing import Any, Dict, List

from neo4j import GraphDatabase

from elementary_neo4j.neo4j_config import Neo4jConfig

logger = logging.getLogger(__name__)


class Neo4jLineageExporter:
    """
    Exports dbt lineage from Elementary's dbt manifest into Neo4j.
    Creates nodes for models, sources, seeds, and snapshots, and
    relationships for dependencies between them.
    """

    def __init__(self, config: Neo4jConfig):
        self.config = config
        self.driver = GraphDatabase.driver(
            config.uri,
            auth=(config.username, config.password)
        )

    def close(self):
        self.driver.close()

    def load_manifest(self, manifest_path: str) -> Dict[str, Any]:
        """Load dbt manifest.json from file path."""
        path = Path(manifest_path)
        if not path.exists():
            raise FileNotFoundError(f"Manifest not found: {manifest_path}")
        with open(path, "r") as f:
            return json.load(f)

    def extract_nodes(self, manifest: Dict[str, Any]) -> List[Dict]:
        """Extract model and source nodes from manifest."""
        nodes = []
        for unique_id, node in manifest.get("nodes", {}).items():
            if node.get("resource_type") in ("model", "seed", "snapshot"):
                nodes.append({
                    "unique_id": unique_id,
                    "name": node.get("name"),
                    "resource_type": node.get("resource_type"),
                    "schema": node.get("schema"),
                    "database": node.get("database"),
                    "package_name": node.get("package_name"),
                    "description": node.get("description", ""),
                })
        for unique_id, source in manifest.get("sources", {}).items():
            nodes.append({
                "unique_id": unique_id,
                "name": source.get("name"),
                "resource_type": "source",
                "schema": source.get("schema"),
                "database": source.get("database"),
                "package_name": source.get("package_name"),
                "description": source.get("description", ""),
            })
        return nodes

    def extract_dependencies(self, manifest: Dict[str, Any]) -> List[Dict]:
        """Extract upstream dependencies between exported nodes only."""
        exported_ids = {
            unique_id
            for unique_id, node in manifest.get("nodes", {}).items()
            if node.get("resource_type") in ("model", "seed", "snapshot")
        }
        exported_ids.update(manifest.get("sources", {}).keys())

        dependencies = []
        for unique_id, node in manifest.get("nodes", {}).items():
            if unique_id not in exported_ids:
                continue
            for upstream_id in node.get("depends_on", {}).get("nodes", []):
                if upstream_id not in exported_ids:
                    continue
                dependencies.append({
                    "from_id": upstream_id,
                    "to_id": unique_id,
                })
        return dependencies

    def export_nodes(self, nodes: List[Dict]):
        """Write nodes to Neo4j."""
        with self.driver.session(database=self.config.database) as session:
            for node in nodes:
                session.run(
                    """
                    MERGE (n:DbtNode {unique_id: $unique_id})
                    SET n.name = $name,
                        n.resource_type = $resource_type,
                        n.schema = $schema,
                        n.database = $database,
                        n.package_name = $package_name,
                        n.description = $description
                    """,
                    **node
                )
        logger.info(f"Exported {len(nodes)} nodes to Neo4j")

    def export_dependencies(self, dependencies: List[Dict]):
        """Write dependency relationships to Neo4j."""
        with self.driver.session(database=self.config.database) as session:
            for dep in dependencies:
                session.run(
                    """
                    MATCH (a:DbtNode {unique_id: $from_id})
                    MATCH (b:DbtNode {unique_id: $to_id})
                    MERGE (a)-[:FEEDS_INTO]->(b)
                    """,
                    **dep
                )
        logger.info(f"Exported {len(dependencies)} dependencies to Neo4j")

    def export(self, manifest_path: str):
        """Full export pipeline — nodes + dependencies."""
        logger.info(f"Loading manifest from {manifest_path}")
        manifest = self.load_manifest(manifest_path)
        nodes = self.extract_nodes(manifest)
        dependencies = self.extract_dependencies(manifest)
        self.export_nodes(nodes)
        self.export_dependencies(dependencies)
        logger.info("Neo4j lineage export complete")
        return {
            "nodes_exported": len(nodes),
            "dependencies_exported": len(dependencies)
        }