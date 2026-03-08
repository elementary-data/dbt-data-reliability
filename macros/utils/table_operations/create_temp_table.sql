{% macro create_temp_table(database_name, schema_name, table_name, sql_query) %}
    {% do return(
        adapter.dispatch("create_temp_table", "elementary")(
            database_name, schema_name, table_name, sql_query
        )
    ) %}
{%- endmacro %}

{% macro default__create_temp_table(
    database_name, schema_name, table_name, sql_query
) %}
    {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(
        database=database_name,
        schema=schema_name,
        identifier=table_name,
        type="table",
    ) -%}
    {% set temp_table_relation = elementary.make_temp_table_relation(
        temp_table_relation
    ) %}
    {% do elementary.edr_create_table_as(
        True, temp_table_relation, sql_query, drop_first=temp_table_exists
    ) %}
    {{ return(temp_table_relation) }}
{% endmacro %}

{% macro fabric__create_temp_table(database_name, schema_name, table_name, sql_query) %}
    {#
        T-SQL (Fabric + SQL Server) does not allow CTEs inside subqueries, so the usual
        CREATE TABLE … AS (sql) pattern fails when sql contains a CTE
        (e.g. the accepted_values test).

        Workaround:
          1. CREATE VIEW  … AS <sql>     — CTEs are valid in view definitions
          2. SELECT * INTO <table> FROM <view>  — simple SELECT, no CTE
          3. DROP VIEW …

        We use a regular table (not #temp) because the EXEC scope isolation
        of SQL Server makes #temp tables invisible to the caller.

        sqlserver__ is not needed here because dbt-sqlserver declares
        dependencies=["fabric"], so this macro is found automatically
        via the dispatch chain: sqlserver__ → fabric__ → default__.
    #}
    {% set table_exists, table_relation = dbt.get_or_create_relation(
        database=database_name,
        schema=schema_name,
        identifier=table_name,
        type="table",
    ) -%}

    {% if table_exists %} {% do adapter.drop_relation(table_relation) %} {% endif %}

    {# Helper view — short suffix to stay within identifier-length limits #}
    {% set vw_identifier = (table_name ~ "_vw")[:128] %}
    {% set vw_relation = api.Relation.create(
        database=database_name,
        schema=schema_name,
        identifier=vw_identifier,
        type="view",
    ) %}

    {# SQL Server does not allow database prefix on DROP VIEW / CREATE VIEW #}
    {% set vw_ref = vw_relation.include(database=false) %}
    {% set tbl_ref = table_relation.include(database=false) %}

    {% do elementary.run_query("DROP VIEW IF EXISTS " ~ vw_ref) %}
    {% do elementary.run_query("CREATE VIEW " ~ vw_ref ~ " AS " ~ sql_query) %}
    {% do elementary.run_query("SELECT * INTO " ~ tbl_ref ~ " FROM " ~ vw_ref) %}
    {% do elementary.run_query("DROP VIEW " ~ vw_ref) %}

    {{ return(table_relation) }}
{% endmacro %}

{% macro snowflake__create_temp_table(
    database_name, schema_name, table_name, sql_query
) %}
    {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(
        database=database_name,
        schema=schema_name,
        identifier=table_name,
        type="table",
    ) -%}
    {% set temp_table_relation = elementary.make_temp_table_relation(
        temp_table_relation
    ) %}
    {% set create_query %}
        create or replace temporary table {{ temp_table_relation }} 
        as (
            {{ sql_query }}
        );

    {% endset %}

    {% do elementary.run_query(create_query) %}

    {{ return(temp_table_relation) }}
{% endmacro %}
