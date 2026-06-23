{% macro edr_create_table_as(
    temporary, relation, sql_query, drop_first=false, should_commit=false
) %}
    {# This macro contains a simplified implementation that replaces our usage of 
     dbt.create_table_as and serves our needs.
     This version also runs the query rather than return the SQL.
  #}
    {% if drop_first %} {% do dbt.drop_relation_if_exists(relation) %} {% endif %}

    {% set create_query = elementary.edr_get_create_table_as_sql(
        temporary, relation, sql_query
    ) %}
    {% do elementary.run_query(create_query) %}

    {% if should_commit %} {% do adapter.commit() %} {% endif %}
{% endmacro %}


{% macro edr_get_create_table_as_sql(temporary, relation, sql_query) %}
    {{
        return(
            adapter.dispatch("edr_get_create_table_as_sql", "elementary")(
                temporary, relation, sql_query
            )
        )
    }}
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

{% macro redshift__edr_get_create_table_as_sql(temporary, relation, sql_query) %}
    {% if temporary and elementary.is_dbt_fusion() %}
        {# dbt-fusion uses connection pooling - temp tables created in one session
       aren't visible in other sessions. Create regular tables instead.
       These are cleaned up by Elementary's normal cleanup logic. #}
    create table {{ relation }}
    as {{ sql_query }}
    {% else %}
    create {% if temporary %} temporary {% endif %} table {{ relation.include(database=(not temporary), schema=(not temporary)) }}
    as {{ sql_query }}
    {% endif %}
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
            {% set relation_type = "view" %}
        {% else %} {% set relation_type = "temporary view" %}
        {% endif %}
    {% else %} {% set relation_type = "table" %}
    {% endif %}

  create or replace {{ relation_type }} {{ relation }}
  as {{ sql_query }}
{% endmacro %}

{% macro clickhouse__edr_get_create_table_as_sql(temporary, relation, sql_query) %}
    {# ClickHouse does not support database-scoped temporary tables, so we force temporary to be false. #}
    {{ dbt.get_create_table_as_sql(false, relation, sql_query) }}
{% endmacro %}

{% macro duckdb__edr_get_create_table_as_sql(temporary, relation, sql_query) %}
  create or replace {% if temporary %} temporary {% endif %} table {{ relation }}
  as {{ sql_query }}
{% endmacro %}

{% macro trino__edr_get_create_table_as_sql(temporary, relation, sql_query) %}
    {# dbt-trino's create_table_as accesses model.config which fails when called
     outside a model context (e.g. from edr_create_table_as). Use simplified SQL. #}
  create table {{ relation }}
  as {{ sql_query }}
{% endmacro %}

{% macro spark__edr_get_create_table_as_sql(temporary, relation, sql_query) %}
    {# Spark: use a temporary view for temp tables, regular table otherwise #}
    {% if temporary %}
    create or replace temporary view {{ relation }}
    as {{ sql_query }}
    {% else %}
    create table {{ relation }}
    as {{ sql_query }}
    {% endif %}
{% endmacro %}

{% macro fabric__edr_get_create_table_as_sql(temporary, relation, sql_query) %}
    {#
        dbt-fabric's fabric__create_table_as implements CTAS via a helper view
        `<table>__dbt_tmp_vw`: it creates the view, then issues
        `CREATE TABLE <table> AS SELECT * FROM <table>__dbt_tmp_vw`. The macro
        does NOT drop the helper view at the end — it relies on the caller
        (e.g. dbt-fabric's own incremental materialization) to drop it via
        `adapter.drop_relation` after the CTAS.

        Elementary's edr_create_table_as does not perform that post-drop, so
        the helper view leaks in the elementary schema every time
        edr_create_table_as is invoked on Fabric. Affects every artifact
        table built this way (dbt_columns, dbt_exposures, dbt_seeds,
        dbt_sources, dbt_tests, plus their `__tmp_<ts>` intermediates).

        Fix: append `EXEC('DROP VIEW IF EXISTS ...')` to the SQL emitted by
        dbt.get_create_table_as_sql so the helper view is dropped in the
        same batch that consumed it. Idempotent — uses IF EXISTS.
    #}
    {{ dbt.get_create_table_as_sql(temporary, relation, sql_query) }}

    {% set tmp_vw_relation = relation.incorporate(
        path={"identifier": relation.identifier ~ '__dbt_tmp_vw'},
        type='view',
    ) %}
    EXEC('DROP VIEW IF EXISTS {{ tmp_vw_relation.include(database=false) }}');
{% endmacro %}

{% macro fabricspark__edr_get_create_table_as_sql(temporary, relation, sql_query) %}
    {{
        return(
            elementary.spark__edr_get_create_table_as_sql(
                temporary, relation, sql_query
            )
        )
    }}
{% endmacro %}
