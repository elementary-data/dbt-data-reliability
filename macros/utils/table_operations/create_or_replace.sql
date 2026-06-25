{% macro create_or_replace(temporary, relation, sql_query, expiration_hours=none) %}
    {{
        return(
            adapter.dispatch("create_or_replace", "elementary")(
                temporary, relation, sql_query, expiration_hours=expiration_hours
            )
        )
    }}
{% endmacro %}

{# Snowflake and Bigquery #}
{% macro default__create_or_replace(
    temporary, relation, sql_query, expiration_hours=none
) %}
    {% do elementary.edr_create_table_as(
        temporary, relation, sql_query, expiration_hours=expiration_hours
    ) %}
{% endmacro %}

{% macro redshift__create_or_replace(
    temporary, relation, sql_query, expiration_hours=none
) %}
    {% do elementary.edr_create_table_as(
        temporary,
        relation,
        sql_query,
        drop_first=true,
        should_commit=true,
        expiration_hours=expiration_hours,
    ) %}
{% endmacro %}

{% macro postgres__create_or_replace(
    temporary, relation, sql_query, expiration_hours=none
) %}
    {% do elementary.run_query("BEGIN") %}
    {% do elementary.edr_create_table_as(
        temporary,
        relation,
        sql_query,
        drop_first=true,
        expiration_hours=expiration_hours,
    ) %}
    {% do elementary.run_query("COMMIT") %}
{% endmacro %}

{% macro spark__create_or_replace(
    temporary, relation, sql_query, expiration_hours=none
) %}
    {% do elementary.edr_create_table_as(
        temporary,
        relation,
        sql_query,
        drop_first=true,
        should_commit=true,
        expiration_hours=expiration_hours,
    ) %}
{% endmacro %}

{% macro fabricspark__create_or_replace(
    temporary, relation, sql_query, expiration_hours=none
) %}
    {{
        return(
            elementary.spark__create_or_replace(
                temporary, relation, sql_query, expiration_hours=expiration_hours
            )
        )
    }}
{% endmacro %}

{% macro athena__create_or_replace(
    temporary, relation, sql_query, expiration_hours=none
) %}
    {% do elementary.edr_create_table_as(
        temporary,
        relation,
        sql_query,
        drop_first=true,
        expiration_hours=expiration_hours,
    ) %}
{% endmacro %}

{% macro trino__create_or_replace(
    temporary, relation, sql_query, expiration_hours=none
) %}
    {% do elementary.edr_create_table_as(
        temporary,
        relation,
        sql_query,
        drop_first=true,
        expiration_hours=expiration_hours,
    ) %}
{% endmacro %}

{% macro clickhouse__create_or_replace(
    temporary, relation, sql_query, expiration_hours=none
) %}
    {% do elementary.edr_create_table_as(
        temporary,
        relation,
        sql_query,
        drop_first=true,
        expiration_hours=expiration_hours,
    ) %}
{% endmacro %}

{# DuckDB uses CREATE OR REPLACE TABLE, so drop_first is not needed.
   should_commit=true ensures the table survives the ROLLBACK issued by test connections. #}
{% macro duckdb__create_or_replace(
    temporary, relation, sql_query, expiration_hours=none
) %}
    {% do elementary.edr_create_table_as(
        temporary,
        relation,
        sql_query,
        should_commit=true,
        expiration_hours=expiration_hours,
    ) %}
{% endmacro %}
