{% materialization view, adapter="duckdb" %}
  {# HACK - Views in DuckDB don't support CTEs, which is a hurdle we can't easily overcome #}
  {# See this ticket - https://github.com/duckdb/duckdb/issues/2479 #}
  {# As a temporary solution, we're overriding the materialization of views to create tables instead. #}
  
  {% do return(dbt.materialization_table_duckdb()) %}
{% endmaterialization %}
