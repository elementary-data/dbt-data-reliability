{% macro bigquery_can_access_relation(relation) %}
  {% set query %}
    begin
      select 1
      from {{ relation }}
      limit 1;
    exception when error then
      select 0;
    end
  {% endset %}
  {% set result = elementary.result_value(query) %}
  {% if result == 1 %}
  {% if bigquery__get_columns_from_information_schema %}
  {% endif %}
    {% do return(true) %}
  {% endif %}
  {% do return(false) %}
{% endmacro %}