{% macro get_tables_without_models(deprecated_models_path=none) %}
  {% set model_schemas, model_tables = elementary.get_model_schemas_and_tables(deprecated_models_path) %}
  {% set db_tables = elementary.get_db_tables_in_schemas(model_schemas) %}
  {% for db_table in db_tables %}
    {% if db_table not in model_tables %}
      {% do print(db_table) %}
    {% endif %}
  {% endfor %}
{% endmacro %}


{% macro get_db_tables_in_schemas(schemas) %}
  {% set tables = [] %}
  {% for schema in schemas %}
    {% set db_name, schema_name = schema.split('.') %}
    {% set schema_relation = api.Relation.create(db_name, schema_name).without_identifier() %}
    {% set relations = list_relations_without_caching(schema_relation) %}
    {# list_relations_without_caching can return either a list of Relation objects or an agate depending on the adapter #}
    {% if relations.append is defined %}
      {# relations is a list of Relation objects #}
      {% for relation in relations %}
        {% do tables.append(schema ~ "." ~ relation.identifier) %}  
      {% endfor %}
    {% else %}
      {# relations is an agate #}
      {% for relation in elementary.agate_to_dicts(relations) %}
        {% do tables.append(schema ~ "." ~ relation.name) %}
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
    {% endif %}
    {% if deprecated_models_path and not model_node.original_file_path.startswith(deprecated_models_path) %}
      {% do relevant_nodes.append(model_node) %}
    {% endif %}
  {% endfor %}
  {% for model_node in relevant_nodes %}
    {% set model_schema = model_node.database ~ "." ~ model_node.schema %}
    {% set model_table = model_schema ~ '.' ~ model_node.name %}
    {% if model_schema not in model_schemas %}
      {% do model_schemas.append(model_schema) %}
    {% endif %}
    {% if model_table not in model_tables %}
      {% do model_tables.append(model_table) %}
    {% endif %}
  {% endfor %}
  {% do return([model_schemas, model_tables]) %}
{% endmacro %}
