{% macro get_existing_indexes(relation) %}
  {% set existing_indexes = [] %}
  {% set existing_indexes_table = elementary.run_query(get_show_indexes_sql(relation)) %}
  {% for index in elementary.agate_to_dicts(existing_indexes_table) %}
    {% do existing_indexes.append({
      "name": index.name,
      "method": index.method,
      "unique": index.unique,
      "columns": index.column_names.split(","),
    }) %}
  {% endfor %}
  {% do return(existing_indexes) %}
{% endmacro %}

{% macro does_index_exist(index, existing_indexes) %}
  {% for existing_index in existing_indexes %}
    {% if index.columns | sort == existing_index.columns | sort %}
      {% do return(true) %}
    {% endif %}
  {% endfor %}
  {% do return(false) %}
{% endmacro %}

{% macro get_index_creation_query(table_nodes, existing_indexes) %}
  {% set all_queries = [] %}
  {% for table_node in table_nodes %}
    {% set relation = api.Relation.create(table_node.database, table_node.schema, table_node.alias) %}
    {% set indexes = table_node.config.get("indexes", []) %}
    {% for _index_dict in indexes %}
      {% if not elementary.does_index_exist(_index_dict, existing_indexes) %}
        {% do all_queries.append(get_create_index_sql(relation, _index_dict)) %}
      {% endif %}
    {% endfor %}
  {% endfor %}
  {% do return(";\n".join(all_queries)) %}
{% endmacro %}

{% macro create_elementary_indexes() %}
  {% if target.type != 'postgres' %}
    {% do exceptions.raise_compiler_error('Indexes only supported for postgres') %}
  {% endif %}
  {% set table_materializations = ["table", "incremental"] %}
  {% set table_nodes = graph.nodes.values() | selectattr('package_name', '==', 'elementary') | selectattr('config.materialized', 'in', table_materializations) %}
  {% for table_node in table_nodes %}
    {% set relation = api.Relation.create(table_node.database, table_node.schema, table_node.alias) %}
    {% set existing_indexes = elementary.get_existing_indexes(relation) %}
    {% set index_creation_query = elementary.get_index_creation_query([table_node], existing_indexes) %}
    {% if index_creation_query %}
      {% do run_query(index_creation_query) %}
    {% endif %}
  {% endfor %}
{% endmacro %}
