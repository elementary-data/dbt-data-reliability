{#
  Returns the database name from a node dict.

  ClickHouse does not have a separate database / schema concept — the
  dbt-clickhouse adapter sets node.database to None.  ClickHouse's own
  information_schema populates both the database and schema columns with
  the same value (the database name), so we mirror that behaviour here
  by falling back to the node's schema when the database is None.
#}
{% macro get_node_database(node) %}
    {% do return(adapter.dispatch("get_node_database", "elementary")(node)) %}
{% endmacro %}

{% macro default__get_node_database(node) %}
    {% do return(node.get("database")) %}
{% endmacro %}

{% macro clickhouse__get_node_database(node) %}
    {% do return(node.get("database") or node.get("schema")) %}
{% endmacro %}
