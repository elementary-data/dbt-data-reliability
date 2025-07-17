{% macro edr_create_table_as(temporary, relation, sql_query, drop_first=false, should_commit=false) %}
  {# This macro contains a simplified implementation that replaces our usage of 
     dbt.create_table_as and serves our needs.
     This version also runs the query rather than return the SQL.
  #}

  {% if drop_first %}
    {% do dbt.drop_relation_if_exists(relation) %}
  {% endif %}

  {% set create_query %} 
    create or replace {% if temporary %} temporary {% endif %} table {{ relation }}
    as {{ sql_query }}
  {% endset %}

  {% do elementary.run_query(create_query) %}

  {% if should_commit %}
    {% do adapter.commit() %}    
  {% endif %}
{% endmacro %}
