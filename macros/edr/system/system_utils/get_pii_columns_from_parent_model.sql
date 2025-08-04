{% macro get_column_tags(column_node) %}
  {% set _tags_sources = [
    column_node.get('tags', []),
    column_node.get('config', {}).get('tags', []),
    column_node.get('meta', {}).get('tags', []),
  ] %}

  {% set all_column_tags = [] %}
  {% for src in _tags_sources %}
    {% set tags_list = src if src is iterable and not (src is string) else [src] %}
    {% do all_column_tags.extend(tags_list) %}
  {% endfor %}

  {% do return(all_column_tags | map('lower') | unique | list) %}
{% endmacro %}

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
  
  {% set raw_pii_tags = elementary.get_config_var('pii_tags') %}
  {% if raw_pii_tags is string %}
    {% set pii_tags = [raw_pii_tags|lower] %}
  {% else %}
    {% set pii_tags = (raw_pii_tags or []) | map('lower') | list %}
  {% endif %}
  
  {# Check if the model itself has PII tags - if so, all columns are considered PII #}
  {% set _model_tags_sources = [
    parent_model.get('tags', []),
    parent_model.get('config', {}).get('tags', []),
    parent_model.get('meta', {}).get('tags', [])
  ] %}
  {% set all_model_tags = [] %}
  {% for src in _model_tags_sources %}
    {% set tags_list = src if src is iterable and not (src is string) else [src] %}
    {% do all_model_tags.extend(tags_list) %}
  {% endfor %}
  {% set all_model_tags = all_model_tags | map('lower') | unique | list %}
  
  {% for pii_tag in pii_tags %}
    {% if pii_tag in all_model_tags %}
      {# Model has PII tag, return all column names #}
      {% set column_nodes = parent_model.get("columns") %}
      {% if column_nodes %}
        {% for column_node in column_nodes.values() %}
          {% do pii_columns.append(column_node.get('name')) %}
        {% endfor %}
      {% endif %}
      {% do return(pii_columns) %}
    {% endif %}
  {% endfor %}
  
  {# Model doesn't have PII tags, check individual columns #}
  {% set column_nodes = parent_model.get("columns") %}
  {% if not column_nodes %}
    {% do return(pii_columns) %}
  {% endif %}
  
  {% for column_node in column_nodes.values() %}
    {% set all_column_tags_lower = elementary.get_column_tags(column_node) %}
    
    {% for pii_tag in pii_tags %}
      {% if pii_tag in all_column_tags_lower %}
        {% do pii_columns.append(column_node.get('name')) %}
        {% break %}
      {% endif %}
    {% endfor %}
  {% endfor %}
  
  {% do return(pii_columns) %}
{% endmacro %}
