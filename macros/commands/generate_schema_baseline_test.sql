{% macro generate_schema_baseline_test(name=none, include_sources=True, include_models=True, strict=False) %}
  {% if name %}
    {{ generate_schema_baseline_test_for_node(name, strict=strict) }}
  {% else %}
    {{ generate_schema_baseline_test_for_all_nodes(include_sources=include_sources, include_models=include_models, strict=strict) }}
  {% endif %}
{% endmacro %}

{% macro generate_schema_baseline_test_for_all_nodes(include_sources=True, include_models=True, strict=False) %}
  {% set nodes = get_nodes_from_graph() %}
  {% for node in nodes %}
    {% if node.package_name != 'elementary' and
          ((include_sources and node.resource_type == 'source') or
           (include_models and node.resource_type == 'model')) %}
      {% do print("Generating schema changes from baseline test for " ~ node.resource_type ~ " '" ~ node.name ~ "':") %}
      {{ generate_schema_baseline_test_for_node(node, strict=strict) }}
      {% do print('----------------------------------') %}
    {% endif %}
  {% endfor %}
{% endmacro %}

{% macro generate_schema_baseline_test_for_node(node, strict=False) %}
  {% if node is string %}
    {% set node_name = node %}
    {% set node = get_node_by_name(node_name) %}

    {% if not node %}
      {% do print("Could not find any model or source by the name '" ~ model_name ~ "'!") %}
      {% do return(none) %}
    {% endif %}
  {% endif %}

  {% if node.resource_type not in ["source", "model"] %}
    {% do print("Only sources and models are supported for this macro, supplied node type: '" ~ node.resource_type ~ "'") %}
    {% do return(none) %}
  {% endif %}

  {% set node_relation = get_relation_from_node(node) %}
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
          {%- if strict %}:
              strict: true
          {% endif -%}
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
      {%- if strict %}:
          strict: true
      {% endif -%}
  {% endif -%}
  {% endset %}

  {% do print(yaml) %}
{% endmacro %}
