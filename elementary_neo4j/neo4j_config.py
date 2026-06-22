from dataclasses import dataclass
from typing import Optional


@dataclass
class Neo4jConfig:
    uri: str
    username: str
    password: str
    database: Optional[str] = "neo4j"

    @classmethod
    def from_env(cls) -> "Neo4jConfig":
        import os
        return cls(
            uri=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
            username=os.environ.get("NEO4J_USERNAME", "neo4j"),
            password=os.environ.get("NEO4J_PASSWORD", ""),
            database=os.environ.get("NEO4J_DATABASE", "neo4j"),
        )