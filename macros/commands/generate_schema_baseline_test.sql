{% macro generate_schema_baseline_test(model_name) %}
  {% set node = get_node_by_name(model_name) %}
  {% if not node %}
    {% do print("Could not find any model or source by the name '" ~ model_name ~ "'!") %}
    {% do return(none) %}
  {% endif %}

  {% if node.resource_type not in ["source", "model"] %}
    {% do print("Only sources and models are supported for this macro, supplied node type: '" ~ node.resource_type ~ "'") %}
    {% do return(none) %}
  {% endif %}

  {% set node_relation = adapter.get_relation(database=node.database, schema=node.schema, identifier=node.identifier) %}
  {% if not node_relation %}
    {% do print("Table not found in the DB! Cannot create schema test.") %}
    {% do return(none) %}
  {% endif %}

  {% set columns = adapter.get_columns_in_relation(node_relation) %}

  {% set yaml %}
  {%- if node.resource_type == 'source' %}
sources:
  - name: {{ node.source_name }}
    tables:
      - name: {{ node.name }}
        columns:
        {%- for column in columns %}
          - name: {{ column.name }}
            data_type: {{ column.dtype }}
        {% endfor %}
        tests:
          - elementary.schema_changes_from_baseline
  {% else %}
models:
  - name: {{ node.source_name }}
    columns:
    {%- for column in columns %}
      - name: {{ column.name }}
        data_type: {{ column.dtype }}
    {% endfor %}
    tests:
      - elementary.schema_changes_from_baseline
  {% endif -%}
  {% endset %}

  {% do print("Please add the settings below to your " ~ ("source" if node.resource_type == "source" else "model") ~ " definition:") %}
  {% do print(yaml) %}
{% endmacro %}