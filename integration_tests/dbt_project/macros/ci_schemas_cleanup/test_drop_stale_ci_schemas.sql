{#
  Integration-test helper for drop_stale_ci_schemas.

  Creates two CI-style schemas (one with an old timestamp, one recent),
  runs the cleanup macro, checks which schemas survived, cleans up,
  and returns a JSON result dict.
#}
{% macro test_drop_stale_ci_schemas() %}
    {% set database = elementary.target_database() %}
    {% set now = modules.datetime.datetime.utcnow() %}

    {# Old schema: timestamp in the past (2020-01-01 00:00:00) #}
    {% set old_schema = "dbt_200101_000000_citest_00000000" %}
    {# Recent schema: timestamp = now #}
    {% set recent_ts = now.strftime("%y%m%d_%H%M%S") %}
    {% set recent_schema = "dbt_" ~ recent_ts ~ "_citest_11111111" %}

    {{ log("TEST: creating old schema: " ~ old_schema, info=true) }}
    {{ log("TEST: creating recent schema: " ~ recent_schema, info=true) }}

    {# ── Create both schemas ───────────────────────────────────────────── #}
    {% do edr_create_schema(database, old_schema) %}
    {% do edr_create_schema(database, recent_schema) %}

    {# ── Verify both exist before running cleanup ──────────────────────── #}
    {% set old_exists_before = edr_schema_exists(database, old_schema) %}
    {% set recent_exists_before = edr_schema_exists(database, recent_schema) %}
    {{
        log(
            "TEST: old_exists_before="
            ~ old_exists_before
            ~ ", recent_exists_before="
            ~ recent_exists_before,
            info=true,
        )
    }}

    {# ── Run cleanup with a large threshold so only the artificially old
       schema (year 2020) is caught, and real CI schemas from parallel
       workers are safely below the threshold. ──────────────────────────── #}
    {% do drop_stale_ci_schemas(prefixes=["dbt_"], max_age_hours=8760) %}

    {# ── Check which schemas survived ─────────────────────────────────── #}
    {% set old_exists_after = edr_schema_exists(database, old_schema) %}
    {% set recent_exists_after = edr_schema_exists(database, recent_schema) %}
    {{
        log(
            "TEST: old_exists_after="
            ~ old_exists_after
            ~ ", recent_exists_after="
            ~ recent_exists_after,
            info=true,
        )
    }}

    {# ── Cleanup: drop any remaining test schemas ─────────────────────── #}
    {% if old_exists_after is true %}
        {% do edr_drop_schema(database, old_schema) %}
    {% endif %}
    {% if recent_exists_after %}
        {% do edr_drop_schema(database, recent_schema) %}
    {% endif %}

    {# ── Return results ────────────────────────────────────────────────── #}
    {% set results = {
        "old_exists_before": old_exists_before,
        "recent_exists_before": recent_exists_before,
        "old_dropped": not old_exists_after,
        "recent_kept": recent_exists_after,
    } %}
    {% do return(results) %}
{% endmacro %}


{# ── Per-adapter schema creation ─────────────────────────────────────── #}
{% macro edr_create_schema(database, schema_name) %}
    {% do return(
        adapter.dispatch("edr_create_schema", "elementary_tests")(
            database, schema_name
        )
    ) %}
{% endmacro %}

{% macro default__edr_create_schema(database, schema_name) %}
    {% set schema_relation = api.Relation.create(
        database=database, schema=schema_name
    ) %}
    {% do dbt.create_schema(schema_relation) %}
    {% do adapter.commit() %}
{% endmacro %}

{% macro clickhouse__edr_create_schema(database, schema_name) %}
    {% do run_query("CREATE DATABASE IF NOT EXISTS `" ~ schema_name ~ "`") %}
    {% do adapter.commit() %}
{% endmacro %}

{% macro spark__edr_create_schema(database, schema_name) %}
    {% set safe_schema = schema_name | replace("`", "``") %}
    {% do run_query("CREATE DATABASE IF NOT EXISTS `" ~ safe_schema ~ "`") %}
{% endmacro %}

{% macro vertica__edr_create_schema(database, schema_name) %}
    {#- Vertica DDL is auto-committed; an explicit adapter.commit() would
        fail with "no transaction in progress". -#}
    {% do run_query("CREATE SCHEMA IF NOT EXISTS " ~ schema_name) %}
{% endmacro %}
