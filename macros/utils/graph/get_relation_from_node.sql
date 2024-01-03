{% macro get_relation_from_node(node) %}
  {% do return(adapter.get_relation(database=node.database,
                                    schema=node.schema,
                                    identifier=elementary.get_table_name_from_node(node))) %}
{% endmacro %}
