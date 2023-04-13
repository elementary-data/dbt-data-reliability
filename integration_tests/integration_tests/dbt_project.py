import dbt.adapters.factory

dbt.adapters.factory.get_adapter = lambda config: config.adapter

import uuid
from typing import Optional
from packaging import version

from dbt.adapters.base import BaseRelation
from dbt.adapters.factory import get_adapter_class_by_name, register_adapter
from dbt.config import RuntimeConfig
from dbt.flags import set_from_args
from dbt.node_types import NodeType
from dbt.parser.manifest import ManifestLoader, process_node
from dbt.parser.sql import SqlBlockParser
from dbt.task.sql import SqlCompileRunner
from dbt.tracking import disable_tracking
from dbt.version import __version__
from pydantic import BaseModel

dbt_version = version.parse(__version__)
COMPILED_CODE = (
    "compiled_code" if dbt_version >= version.parse("1.3.0") else "compiled_sql"
)

# Disable dbt tracking
disable_tracking()


class DbtProject:
    def __init__(self, project_dir, target=None):
        args = Args(project_dir=project_dir, target=target)
        set_from_args(args, args)
        self.config = RuntimeConfig.from_args(args)

        register_adapter(self.config)

        self.adapter_name = self.config.credentials.type
        self.adapter = get_adapter_class_by_name(self.adapter_name)(self.config)
        self.adapter.connections.set_connection_name()
        self.config.adapter = self.adapter

        project_parser = ManifestLoader(
            self.config,
            self.config.load_dependencies(),
            self.adapter.connections.set_query_header,
        )
        self.manifest = project_parser.load()
        self.manifest.build_flat_graph()
        project_parser.save_macros_to_adapter(self.adapter)

        self.sql_parser = SqlBlockParser(self.config, self.manifest, self.config)

        self.relations_to_cleanup = []

    def execute_macro(self, macro_name, **kwargs):
        if "." in macro_name:
            package_name, actual_macro_name = macro_name.split(".", 1)
        else:
            package_name = None
            actual_macro_name = macro_name

        return self.adapter.execute_macro(
            macro_name=actual_macro_name,
            project=package_name,
            kwargs=kwargs,
            manifest=self.manifest,
        )

    def execute_sql(self, sql: str):
        temp_node = self._create_temp_node(sql)

        try:
            sql_compiler = SqlCompileRunner(
                self.config, self.adapter, node=temp_node, node_index=1, num_nodes=1
            )
            compiled_node = sql_compiler.compile(self.manifest)
            compiled_sql = getattr(compiled_node, COMPILED_CODE)
            return self.adapter.execute(compiled_sql, fetch=True)[1]
        finally:
            self.clear_node(temp_node.name)

    def create_relation(
        self,
        database: Optional[str],
        schema: Optional[str],
        name: Optional[str],
        relation_type: str = "table",
    ) -> BaseRelation:
        return self.adapter.Relation.create(database, schema, name, type=relation_type)

    def create_table_as(self, relation, sql, temporary):
        if temporary and self.adapter_name in ["spark", "databricks"]:
            temporary = False
            self.relations_to_cleanup.append(relation)

        create_table_kwargs = {"temporary": temporary, "relation": relation}
        if dbt_version >= version.parse("1.3.0"):
            create_table_kwargs["compiled_code"] = sql
        else:
            create_table_kwargs["sql"] = sql

        create_table_query = self.execute_macro(
            "dbt.create_table_as", **create_table_kwargs
        )
        self.execute_sql(create_table_query)

    def cleanup(self):
        for relation in self.relations_to_cleanup:
            print("Dropping relation: %s" % relation)
            self.execute_macro("dbt.drop_relation_if_exists", relation=relation)

    def clear_test_env(self):
        self.execute_macro("clear_tests")

    def _create_temp_node(self, sql: str):
        """Get a node for SQL execution against adapter"""
        temp_node_name = str(uuid.uuid4())
        self.clear_node(temp_node_name)
        sql_node = self.sql_parser.parse_remote(sql, temp_node_name)
        process_node(self.config, self.manifest, sql_node)
        return sql_node

    def clear_node(self, name: str):
        """Removes the statically named node created by `execute_sql` and `compile_sql` in `dbt.lib`"""
        self.manifest.nodes.pop(
            f"{NodeType.SqlOperation}.{self.config.project_name}.{name}", None
        )


class Args(BaseModel):
    """
    Minimal mock to dbt config arguments
    """

    project_dir: str
    profile: str = None
    target: Optional[str] = None
    threads: Optional[int] = 1
