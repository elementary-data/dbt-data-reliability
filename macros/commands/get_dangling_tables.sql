{% macro get_dangling_tables(deprecated_models_path=none) %}
  {% set model_schemas, model_tables = elementary.get_model_schemas_and_tables(deprecated_models_path) %}
  {% set db_tables = elementary.get_tables_in_schemas(model_schemas) %}
  {% set rendered_model_tables = [] %}
  {% for model_table in model_tables %}
    {% do rendered_model_tables.append(model_table.render()) %}
  {% endfor %}
  {% for db_table in db_tables %}
    {% if db_table.render() not in rendered_model_tables %}
      {% do print(db_table) %}
    {% endif %}
  {% endfor %}
{% endmacro %}


{% macro get_tables_in_schemas(schemas) %}
  {% set tables = [] %}
  {% for schema_relation in schemas %}
    {% set relations = dbt.list_relations_without_caching(schema_relation) %}
    {# list_relations_without_caching can return either a list of Relation objects or an agate depending on the adapter #}
    {# Jinja has no way for checking if a variable is a list, so we check if it has the append method (method of lists) #}
    {% if relations.append is defined %}
      {# relations is a list of Relation objects #}
      {% do tables.extend(relations) %}
    {% else %}
      {# relations is an agate #}
      {% for relation in elementary.agate_to_dicts(relations) %}
        {% set relation = api.Relation.create(schema_relation.database, schema_relation.schema, relation.name) %}
        {% do tables.append(relation) %}
      {% endfor %}
    {% endif %}
  {% endfor %}
  {% do return(tables) %}
{% endmacro %}


{% macro get_model_schemas_and_tables(deprecated_models_path=none) %}
  {% set model_schemas = [] %}
  {% set model_tables = [] %}

  {% set relevant_nodes = [] %}
  {% for model_node in graph.nodes.values() | selectattr('resource_type', '==', 'model') %}
    {% if not deprecated_models_path %}
      {% do relevant_nodes.append(model_node) %}
    {% elif not model_node.original_file_path.startswith(deprecated_models_path) %}
      {% do relevant_nodes.append(model_node) %}
    {% endif %}
  {% endfor %}
  {% for model_node in relevant_nodes %}
    {% set model_schema = api.Relation.create(model_node.database, model_node.schema).without_identifier() %}
    {% set model_table = api.Relation.create(model_node.database, model_node.schema, model_node.name) %}
    {% if model_schema not in model_schemas %}
      {% do model_schemas.append(model_schema) %}
    {% endif %}
    {% if model_table not in model_tables %}
      {% do model_tables.append(model_table) %}
    {% endif %}
  {% endfor %}
  {% do return([model_schemas, model_tables]) %}
{% endmacro %}
