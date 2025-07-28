{% macro is_pii_table(flattened_test) %}
  {% set disable_samples_on_pii_tables = elementary.get_config_var('disable_samples_on_pii_tables') %}
  {% if not disable_samples_on_pii_tables %}
    {% do return(false) %}
  {% endif %}
  
  {% set pii_table_tags = elementary.get_config_var('pii_table_tags') %}
  {% set model_tags = elementary.insensitive_get_dict_value(flattened_test, 'model_tags', []) %}
  
  {% set intersection = elementary.lists_intersection(model_tags, pii_table_tags) %}
  {% set is_pii = intersection | length > 0 %}
  
  {% do return(is_pii) %}
{% endmacro %}
