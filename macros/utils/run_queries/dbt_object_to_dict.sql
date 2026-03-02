{% macro dbt_object_to_dict(agate_table) %}
  {% if elementary.is_dbt_fusion() %}
    {% do return(agate_table) %}
  {% endif %}

  {% do return(agate_table.to_dict()) %}
{% endmacro %}
