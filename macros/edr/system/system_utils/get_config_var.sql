{% macro get_elementary_runtime_config(include_defaults=true) %}
    {% set default_config = elementary.get_default_config() %}
    {% set unset = "<ELEMENTARY_UNSET_MAGIC_STRING>" %}
    {% set runtime_config = {} %}
    {% for var_name in default_config.keys() %}
        {% if include_defaults %}
            {% set var_value = var(var_name, default_config.get(var_name)) %}
        {% else %} {% set var_value = var(var_name, unset) %}
        {% endif %}
        {% if var_value != unset %}
            {% do runtime_config.update({var_name: var_value}) %}
        {% endif %}
    {% endfor %}
    {% do return(runtime_config) %}
{% endmacro %}

{% macro get_config_var(var_name) %}
    {% set default_config = elementary.get_default_config() %}
    {% set var_value = var(var_name, default_config.get(var_name)) %}
    {% if var_value is string %}
        {% if var_value.lower() == "true" %} {% do return(true) %}
        {% elif var_value.lower() == "false" %} {% do return(false) %}
        {% endif %}
    {% endif %}
    {% do return(var_value) %}
{% endmacro %}

{# Render a boolean config var as a SQL-compatible boolean expression.
   Standard SQL uses TRUE/FALSE but T-SQL does not support bare boolean literals.
   This macro outputs '1=1' or '1=0' which works across all dialects. #}
{% macro render_bool_config_var(var_name, negate=false) %}
    {% set val = elementary.get_config_var(var_name) %}
    {% if negate %} {% if val %} (1 = 0) {% else %} (1 = 1){% endif %}
    {% else %} {% if val %} (1 = 1) {% else %} (1 = 0){% endif %}
    {% endif %}
{% endmacro %}

{# Render a SQL boolean literal that works across all dialects.
   Standard SQL uses TRUE/FALSE but T-SQL requires cast(1/0 as bit). #}
{% macro edr_boolean_literal(value) %}
    {{ return(adapter.dispatch("edr_boolean_literal", "elementary")(value)) }}
{% endmacro %}

{% macro default__edr_boolean_literal(value) %}
    {% if value %} true {% else %} false{% endif %}
{% endmacro %}

{% macro fabric__edr_boolean_literal(value) %}
    {% if value %} cast(1 as bit) {% else %} cast(0 as bit){% endif %}
{% endmacro %}

{# Render a SQL condition as a boolean column value.
   Produces: case when <condition> then TRUE else FALSE end
   On T-SQL, TRUE/FALSE become cast(1/0 as bit).
   If condition is none/empty, defaults to FALSE. #}
{% macro edr_condition_as_boolean(condition) %}
    case
        when {{ condition or "1=0" }}
        then {{ elementary.edr_boolean_literal(true) }}
        else {{ elementary.edr_boolean_literal(false) }}
    end
{% endmacro %}

{# Compare a SQL expression to TRUE.  Works across all dialects including T-SQL (bit). #}
{% macro edr_is_true(expr) %}
    {{ return(adapter.dispatch("edr_is_true", "elementary")(expr)) }}
{% endmacro %}

{% macro default__edr_is_true(expr) %} {{ expr }} = true {% endmacro %}

{% macro fabric__edr_is_true(expr) %} {{ expr }} = cast(1 as bit) {% endmacro %}

{# Compare a SQL expression to FALSE.  Works across all dialects including T-SQL (bit). #}
{% macro edr_is_false(expr) %}
    {{ return(adapter.dispatch("edr_is_false", "elementary")(expr)) }}
{% endmacro %}

{% macro default__edr_is_false(expr) %} {{ expr }} = false {% endmacro %}

{% macro fabric__edr_is_false(expr) %} {{ expr }} = cast(0 as bit) {% endmacro %}

{# Returns true if the current adapter uses T-SQL dialect (Fabric or SQL Server). #}
{% macro is_tsql() %}
    {{ return(adapter.dispatch("is_tsql", "elementary")()) }}
{% endmacro %}

{% macro default__is_tsql() %} {{ return(false) }} {% endmacro %}

{% macro fabric__is_tsql() %} {{ return(true) }} {% endmacro %}

{% macro get_default_config(var_name) %}
    {{ return(adapter.dispatch("get_default_config", "elementary")()) }}
{%- endmacro -%}

{%- macro default__get_default_config() -%}
    {# We use this macro to define and call vars, as the global vars defined in dbt_project.yml
   of the package are not accessible at on-run-start and on-run-end #}
    {% set default_config = {
        "days_back": 14,
        "anomaly_sensitivity": 3,
        "backfill_days": 2,
        "tests_schema_name": "",
        "debug_logs": false,
        "disable_warn_alerts": false,
        "disable_model_alerts": false,
        "disable_test_alerts": false,
        "disable_source_freshness_alerts": false,
        "disable_run_results": false,
        "disable_freshness_results": false,
        "disable_tests_results": false,
        "disable_dbt_artifacts_autoupload": false,
        "columns_upload_strategy": "enriched_only",
        "disable_dbt_invocation_autoupload": false,
        "disable_skipped_model_alerts": true,
        "disable_skipped_test_alerts": true,
        "dbt_artifacts_chunk_size": 5000,
        "test_sample_row_count": 5,
        "edr_cli_run": false,
        "max_int": 2147483647,
        "custom_run_started_at": none,
        "edr_monitors": elementary.get_default_monitors(),
        "long_string_size": 65535,
        "collect_model_sql": true,
        "query_max_size": 1000000,
        "upload_artifacts_method": "diff",
        "project_name": none,
        "elementary_full_refresh": false,
        "min_training_set_size": 7,
        "cache_artifacts": true,
        "anomaly_direction": "both",
        "store_result_rows_in_own_table": true,
        "mute_dbt_upgrade_recommendation": false,
        "calculate_failed_count": true,
        "tests_use_temp_tables": false,
        "clean_elementary_temp_tables": true,
        "force_metrics_backfill": false,
        "ignore_small_changes": {
            "spike_failure_percent_threshold": none,
            "drop_failure_percent_threshold": none,
        },
        "include_other_warehouse_specific_columns": false,
        "fail_on_zero": false,
        "anomaly_exclude_metrics": none,
        "disable_samples_on_pii_tags": false,
        "pii_tags": ["pii"],
        "enable_samples_on_show_sample_rows_tags": false,
        "show_sample_rows_tags": ["show_sample_rows"],
        "bigquery_disable_partitioning": false,
        "bigquery_disable_clustering": false,
        "upload_only_current_project_artifacts": false,
    } %}
    {{- return(default_config) -}}
{%- endmacro -%}

{%- macro bigquery__get_default_config() -%}
    {% set default_config = elementary.default__get_default_config() %}
    {% do default_config.update({"query_max_size": 250000}) %}
    {{- return(default_config) -}}
{%- endmacro -%}

{%- macro clickhouse__get_default_config() -%}
    {% set default_config = elementary.default__get_default_config() %}
    {% do default_config.update({"query_max_size": 250000}) %}
    {{- return(default_config) -}}
{%- endmacro -%}

{%- macro athena__get_default_config() -%}
    {% set default_config = elementary.default__get_default_config() %}
    {% do default_config.update({"query_max_size": 250000}) %}
    {{- return(default_config) -}}
{%- endmacro -%}

{%- macro trino__get_default_config() -%}
    {% set default_config = elementary.default__get_default_config() %}
    {% do default_config.update({"query_max_size": 250000}) %}
    {{- return(default_config) -}}
{%- endmacro -%}

{%- macro vertica__get_default_config() -%}
    {% set default_config = elementary.default__get_default_config() %}
    {# Reduce batch INSERT query size from default 1,000,000 to avoid
       overwhelming Vertica with very large single statements.  Individual
       column values are bounded by edr_type_long_string (varchar(32000)). #}
    {% do default_config.update({"query_max_size": 250000}) %}
    {{- return(default_config) -}}
{%- endmacro -%}

{%- macro dremio__get_default_config() -%}
    {% set default_config = elementary.default__get_default_config() %}
    {% do default_config.update({"dbt_artifacts_chunk_size": 100}) %}

    {# Caching does work in Dremio, but there is a race between the creation of the temp table
       and its usage, and it's causing failures (querying the same table 2 seconds later works).
       This is likely a bug in Dremio.
       So to be safe we disable caching in Dremio by default. #}
    {% do default_config.update({"cache_artifacts": false}) %}

    {{- return(default_config) -}}
{%- endmacro -%}
