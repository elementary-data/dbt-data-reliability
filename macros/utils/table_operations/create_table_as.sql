{% macro edr_create_table_as(temporary, relation, sql_query, drop_first=false, should_commit=false) %}
  {# This macro contains a simplified implementation that replaces our usage of 
     dbt.create_table_as and serves our needs.
     This version also runs the query rather than return the SQL.
  #}

  {% if drop_first %}
    {% do dbt.drop_relation_if_exists(relation) %}
  {% endif %}

  {% set create_query = elementary.edr_get_create_table_as_sql(temporary, relation, sql_query) %}
  {% do elementary.run_query(create_query) %}

  {% if should_commit %}
    {% do adapter.commit() %}    
  {% endif %}
{% endmacro %}


{% macro edr_get_create_table_as_sql(temporary, relation, sql_query) %}
  {{ return(adapter.dispatch("edr_get_create_table_as_sql", "elementary")(temporary, relation, sql_query)) }}
{% endmacro %}

{% macro default__edr_get_create_table_as_sql(temporary, relation, sql_query) %}
  {{ dbt.get_create_table_as_sql(temporary, relation, sql_query) }}
{% endmacro %}

{# Simplified versions for dbt-fusion supported adapters as the original dbt macro 
   no longer works outside of the scope of a model's materialization #}

{% macro snowflake__edr_get_create_table_as_sql(temporary, relation, sql_query) %}
  create or replace {% if temporary %} temporary {% endif %} table {{ relation }}
  as {{ sql_query }}
{% endmacro %}

{% macro bigquery__edr_get_create_table_as_sql(temporary, relation, sql_query) %}
  create or replace table {{ relation }}
  {% if temporary %}
  options (expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 hour))
  {% endif %}
  as {{ sql_query }}
{% endmacro %}

{% macro postgres__edr_get_create_table_as_sql(temporary, relation, sql_query) %}
  create {% if temporary %} temporary {% endif %} table {{ relation.include(database=(not temporary), schema=(not temporary)) }}
  as {{ sql_query }}
{% endmacro %}

{% macro databricks__edr_get_create_table_as_sql(temporary, relation, sql_query) %}
  {% if temporary %}
    {% if elementary.is_dbt_fusion() %}
      {# 
         dbt fusion does not run Databricks statements in the same session, so we can't use temp
         views.
         (the view will be dropped later as has_temp_table_support returns False for Databricks)

         More details here - https://github.com/dbt-labs/dbt-fusion/blob/fa78a4099553a805af7629ac80be55e23e24bb4c/crates/dbt-loader/src/dbt_macro_assets/dbt-databricks/macros/relations/table/create.sql#L54
      #}
      {% set relation_type = 'view' %}
    {% else %}
      {% set relation_type = 'temporary view' %}
    {% endif %}
  {% else %}
    {% set relation_type = 'table' %}
  {% endif %}

  create or replace {{ relation_type }} {{ relation }}
  as {{ sql_query }}
{% endmacro %}
