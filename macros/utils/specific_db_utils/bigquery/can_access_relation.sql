{% macro can_query_relation(relation) %}
  {% do return(adapter.dispatch("can_query_relation", "elementary")(relation)) %}
{% endmacro %}

{% macro bigquery__can_query_relation(relation) %}
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
  {% do return(result == 1) %}
{% endmacro %}

{% macro default__can_query_relation(relation) %}
  {% do exceptions.raise_compiler_error("'can_query_relation' not implemented on '{}'.".format(target.type)) %}
{% endmacro %}
