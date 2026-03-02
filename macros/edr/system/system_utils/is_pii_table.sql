{% macro is_pii_table(flattened_test) %}
  {% set disable_samples_on_pii_tags = elementary.get_config_var('disable_samples_on_pii_tags') %}
  {% if not disable_samples_on_pii_tags %}
    {% do return(false) %}
  {% endif %}
  
  {% set raw_pii_tags = elementary.get_config_var('pii_tags') %}
  {% set pii_tags = (raw_pii_tags if raw_pii_tags is iterable else [raw_pii_tags]) | map('lower') | list %}
  
  {% set raw_model_tags = elementary.insensitive_get_dict_value(flattened_test, 'model_tags', []) %}
  {% set model_tags = (raw_model_tags if raw_model_tags is iterable else [raw_model_tags]) | map('lower') | list %}
  
  {% set intersection = elementary.lists_intersection(model_tags, pii_tags) %}
  {% set is_pii = intersection | length > 0 %}
  
  {% do return(is_pii) %}
{% endmacro %}
