{% macro get_pii_columns_from_parent_model(flattened_test) %}
  {% set pii_columns = [] %}
  
  {% if not elementary.get_config_var('disable_samples_on_pii_tags') %}
    {% do return(pii_columns) %}
  {% endif %}
  
  {% set parent_model_unique_id = elementary.insensitive_get_dict_value(flattened_test, 'parent_model_unique_id') %}
  {% set parent_model = elementary.get_node(parent_model_unique_id) %}
  
  {% if not parent_model %}
    {% do return(pii_columns) %}
  {% endif %}
  
  {% set column_nodes = parent_model.get("columns") %}
  {% if not column_nodes %}
    {% do return(pii_columns) %}
  {% endif %}
  
  {% set raw_pii_tags = elementary.get_config_var('pii_tags') %}
  {% set pii_tags = (raw_pii_tags if raw_pii_tags is iterable else [raw_pii_tags]) | map('lower') | list %}
  
  {% for column_node in column_nodes.values() %}
    {# column -> tags #}
    {% set column_tags = column_node.get('tags', []) %}

    {# column -> config -> tags #}
    {% set config_dict = column_node.get('config', {}) %}
    {% set config_tags = config_dict.get('tags', []) %}

    {# column -> meta -> tags #}
    {% set meta_dict = column_node.get('meta', {}) %}
    {% set meta_tags = meta_dict.get('tags', []) %}

    {% set all_column_tags = config_tags + column_tags + meta_tags %}
    {% set all_column_tags_lower = all_column_tags | map('lower') | list %}
    
    {% for pii_tag in pii_tags %}
      {% if pii_tag in all_column_tags_lower %}
        {% do pii_columns.append(column_node.get('name')) %}
        {% break %}
      {% endif %}
    {% endfor %}
  {% endfor %}
  
  {% do return(pii_columns) %}
{% endmacro %}
