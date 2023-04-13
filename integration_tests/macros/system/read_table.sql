{% macro read_table(table, where=none) %}
  {% set query %}
    select * from {{ ref(table) }}
    {% if where %}
        where {{ where }}
    {% endif %}
  {% endset %}

  {% set results = elementary.run_query(query) %}
  {% set results_json = elementary.agate_to_json(results) %}
  {% do elementary.edr_log(results_json) %}
{% endmacro %}
