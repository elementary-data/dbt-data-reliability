{% macro replace_table_data(relation, rows) %}
    {{ return(adapter.dispatch("replace_table_data", "elementary")(relation, rows)) }}
{% endmacro %}

{# Default (Bigquery & Snowflake) - upload data to a temp table, and then atomically replace the table with a new one #}
{% macro default__replace_table_data(relation, rows) %}
    {% set intermediate_relation = elementary.create_intermediate_relation(
        relation, rows, temporary=True
    ) %}
    {% do elementary.edr_create_table_as(
        False, relation, "select * from {}".format(intermediate_relation)
    ) %}
    {% do adapter.drop_relation(intermediate_relation) %}
{% endmacro %}

{# Databricks - truncate and insert (non-atomic) #}
{% macro databricks__replace_table_data(relation, rows) %}
    {% do dbt.truncate_relation(relation) %}
    {% do elementary.insert_rows(
        relation,
        rows,
        should_commit=false,
        chunk_size=elementary.get_config_var("dbt_artifacts_chunk_size"),
    ) %}
{% endmacro %}

{# Spark - truncate and insert (non-atomic) #}
{% macro spark__replace_table_data(relation, rows) %}
    {% call statement("truncate_relation") -%}
        delete from {{ relation }} where 1 = 1
    {%- endcall %}
    {% do elementary.insert_rows(
        relation,
        rows,
        should_commit=false,
        chunk_size=elementary.get_config_var("dbt_artifacts_chunk_size"),
    ) %}
{% endmacro %}

{# FabricSpark - delegate to Spark (non-atomic) #}
{% macro fabricspark__replace_table_data(relation, rows) %}
    {{ return(elementary.spark__replace_table_data(relation, rows)) }}
{% endmacro %}

{# Dremio - truncate and insert (non-atomic) #}
{% macro dremio__replace_table_data(relation, rows) %}
    {% do dbt.truncate_relation(relation) %}
    {% do elementary.insert_rows(
        relation,
        rows,
        should_commit=false,
        chunk_size=elementary.get_config_var("dbt_artifacts_chunk_size"),
    ) %}
{% endmacro %}

{# Postgres - atomically replace data without dropping the table (preserves views).
   Each statement is executed separately for post_hook compatibility. #}
{% macro postgres__replace_table_data(relation, rows) %}
    {% set intermediate_relation = elementary.create_intermediate_relation(
        relation, rows, temporary=True
    ) %}

    {% do elementary.run_query("begin") %}
    {% do elementary.run_query("delete from " ~ relation) %}
    {% do elementary.run_query(
        "insert into " ~ relation ~ " select * from " ~ intermediate_relation
    ) %}
    {% do elementary.run_query("commit") %}

    {% do adapter.drop_relation(intermediate_relation) %}
{% endmacro %}

{# Redshift - replace data without dropping the table (preserves late-binding views).
   Separate statements without explicit transaction for post_hook compatibility
   (Redshift cannot run multiple statements in a single prepared statement).
   NOTE: Non-atomic - if insert fails after delete, data is lost until the next run.
   Acceptable here because these are internal artifact tables that are regenerated. #}
{% macro redshift__replace_table_data(relation, rows) %}
    {% set intermediate_relation = elementary.create_intermediate_relation(
        relation, rows, temporary=True
    ) %}

    {% do elementary.run_query("delete from " ~ relation) %}
    {% do elementary.run_query(
        "insert into " ~ relation ~ " select * from " ~ intermediate_relation
    ) %}

    {% do adapter.drop_relation(intermediate_relation) %}
{% endmacro %}

{% macro athena__replace_table_data(relation, rows) %}
    {% call statement("truncate_relation") -%} delete from {{ relation }} {%- endcall %}
    {% do elementary.insert_rows(
        relation,
        rows,
        should_commit=false,
        chunk_size=elementary.get_config_var("dbt_artifacts_chunk_size"),
    ) %}
{% endmacro %}

{# Trino - drop and recreate (Trino does not support CREATE OR REPLACE TABLE) #}
{% macro trino__replace_table_data(relation, rows) %}
    {% set intermediate_relation = elementary.create_intermediate_relation(
        relation, rows, temporary=True
    ) %}
    {% do elementary.edr_create_table_as(
        False,
        relation,
        "select * from {}".format(intermediate_relation),
        drop_first=true,
    ) %}
    {% do adapter.drop_relation(intermediate_relation) %}
{% endmacro %}

{# DuckDB - truncate and insert with commit to survive ROLLBACK on in-memory databases #}
{% macro duckdb__replace_table_data(relation, rows) %}
    {% do dbt.truncate_relation(relation) %}
    {% do elementary.insert_rows(
        relation,
        rows,
        should_commit=true,
        chunk_size=elementary.get_config_var("dbt_artifacts_chunk_size"),
    ) %}
{% endmacro %}

{# ClickHouse - cluster-aware truncate and insert (non-atomic).
   Uses explicit TRUNCATE with on_cluster_clause for distributed/replicated tables,
   matching the pattern in delete_and_insert.sql and clean_elementary_test_tables.sql. #}
{% macro clickhouse__replace_table_data(relation, rows) %}
    {% do elementary.run_query(
        "truncate table " ~ relation ~ " " ~ on_cluster_clause(relation)
    ) %}
    {% do elementary.insert_rows(
        relation,
        rows,
        should_commit=false,
        chunk_size=elementary.get_config_var("dbt_artifacts_chunk_size"),
    ) %}
{% endmacro %}

{# Vertica - truncate and insert (non-atomic) #}
{% macro vertica__replace_table_data(relation, rows) %}
    {% do dbt.truncate_relation(relation) %}
    {% do elementary.insert_rows(
        relation,
        rows,
        should_commit=false,
        chunk_size=elementary.get_config_var("dbt_artifacts_chunk_size"),
    ) %}
{% endmacro %}

{# Fabric / SQL Server - truncate and insert (non-atomic).
   sqlserver dispatches through fabric via the chain: sqlserver__ -> fabric__ -> default__,
   so this covers both adapters. #}
{% macro fabric__replace_table_data(relation, rows) %}
    {% do dbt.truncate_relation(relation) %}
    {% do elementary.insert_rows(
        relation,
        rows,
        should_commit=false,
        chunk_size=elementary.get_config_var("dbt_artifacts_chunk_size"),
    ) %}
{% endmacro %}
