{#
  drop_stale_ci_schemas – clean up timestamped CI schemas.

  Schema naming convention produced by CI:
      <prefix><YYMMDD_HHMMSS>_<branch>_<hash>
  Examples:
      dbt_260228_112345_master_abcd1234
      py_260228_112345_master_abcd1234
      dbt_260228_112345_master_abcd1234_elementary   (suffixed variant)

  Call from a GitHub Actions workflow via:
      dbt run-operation elementary.drop_stale_ci_schemas \
          --args '{prefixes: ["dbt_", "py_"], max_age_hours: 24}'
#}

{% macro drop_stale_ci_schemas(prefixes=None, max_age_hours=24) %}
  {% if prefixes is none %}
    {% set prefixes = ['dbt_', 'py_'] %}
  {% endif %}

  {% set max_age_hours = max_age_hours | int %}
  {% set database = elementary.target_database() %}
  {% set all_schemas = elementary.list_ci_schemas(database) %}
  {# utcnow() is deprecated in Python 3.12+ but modules.datetime.timezone is not
     available in dbt's Jinja context. Both now and strptime produce naive datetimes
     so comparisons are safe. #}
  {% set now = modules.datetime.datetime.utcnow() %}
  {% set max_age_seconds = max_age_hours * 3600 %}
  {% set ns = namespace(dropped=0) %}

  {{ log("CI schema cleanup: scanning " ~ all_schemas | length ~ " schema(s) in database '" ~ database ~ "' for prefixes " ~ prefixes | string, info=true) }}

  {% for schema_name in all_schemas | sort %}
    {% set schema_lower = schema_name.lower() %}
    {% for prefix in prefixes %}
      {% if schema_lower.startswith(prefix.lower()) %}
        {% set remainder = schema_lower[prefix | length :] %}
        {# Timestamp format: YYMMDD_HHMMSS (13 chars) followed by _ #}
        {% if remainder | length >= 14 and remainder[6:7] == '_' and remainder[13:14] == '_' %}
          {% set ts_str = remainder[:13] %}
          {# Validate: positions 0-5 and 7-12 must be digits #}
          {% set digits = ts_str[:6] ~ ts_str[7:] %}
          {% set ns_valid = namespace(ok=true) %}
          {% for c in digits %}
            {% if c not in '0123456789' %}
              {% set ns_valid.ok = false %}
            {% endif %}
          {% endfor %}
          {% if ns_valid.ok %}
            {# Validate date component ranges before strptime to avoid ValueError #}
            {% set mm = ts_str[2:4] | int %}
            {% set dd = ts_str[4:6] | int %}
            {% set hh = ts_str[7:9] | int %}
            {% set mi = ts_str[9:11] | int %}
            {% set ss = ts_str[11:13] | int %}
            {% if 1 <= mm <= 12 and 1 <= dd <= 31 and 0 <= hh <= 23 and 0 <= mi <= 59 and 0 <= ss <= 59 %}
              {% set schema_ts = modules.datetime.datetime.strptime(ts_str, '%y%m%d_%H%M%S') %}
              {% set age_seconds = (now - schema_ts).total_seconds() %}
              {% if age_seconds > max_age_seconds %}
                {{ log("  DROP " ~ schema_name ~ "  (age: " ~ (age_seconds / 3600) | round(1) ~ " h)", info=true) }}
                {% do elementary.drop_ci_schema(database, schema_name) %}
                {% set ns.dropped = ns.dropped + 1 %}
              {% else %}
                {{ log("  keep " ~ schema_name ~ "  (age: " ~ (age_seconds / 3600) | round(1) ~ " h)", info=true) }}
              {% endif %}
            {% else %}
              {{ log("  skip " ~ schema_name ~ "  (invalid timestamp: " ~ ts_str ~ ")", info=true) }}
            {% endif %}
          {% endif %}
        {% endif %}
      {% endif %}
    {% endfor %}
  {% endfor %}

  {{ log("CI schema cleanup complete. Dropped " ~ ns.dropped ~ " stale schema(s).", info=true) }}
{% endmacro %}


{# ── Per-adapter schema drop ─────────────────────────────────────────── #}

{# ── Per-adapter schema listing ─────────────────────────────────────── #}

{% macro list_ci_schemas(database) %}
  {% do return(adapter.dispatch('list_ci_schemas', 'elementary')(database)) %}
{% endmacro %}

{# adapter.list_schemas() is not available in run-operation context
   (RuntimeDatabaseWrapper does not expose it). Use information_schema instead. #}
{% macro default__list_ci_schemas(database) %}
  {% set results = run_query("SELECT schema_name FROM information_schema.schemata") %}
  {% set schemas = [] %}
  {% for row in results %}
    {% do schemas.append(row[0]) %}
  {% endfor %}
  {% do return(schemas) %}
{% endmacro %}

{% macro bigquery__list_ci_schemas(database) %}
  {% set results = run_query("SELECT schema_name FROM `" ~ database ~ "`.INFORMATION_SCHEMA.SCHEMATA") %}
  {% set schemas = [] %}
  {% for row in results %}
    {% do schemas.append(row[0]) %}
  {% endfor %}
  {% do return(schemas) %}
{% endmacro %}

{% macro clickhouse__list_ci_schemas(database) %}
  {% set results = run_query('SHOW DATABASES') %}
  {% set schemas = [] %}
  {% for row in results %}
    {% do schemas.append(row[0]) %}
  {% endfor %}
  {% do return(schemas) %}
{% endmacro %}


{# ── Per-adapter schema drop ─────────────────────────────────────────── #}

{% macro drop_ci_schema(database, schema_name) %}
  {% do return(adapter.dispatch('drop_ci_schema', 'elementary')(database, schema_name)) %}
{% endmacro %}

{% macro default__drop_ci_schema(database, schema_name) %}
  {% set schema_relation = api.Relation.create(database=database, schema=schema_name) %}
  {% do dbt.drop_schema(schema_relation) %}
  {% do adapter.commit() %}
{% endmacro %}

{% macro clickhouse__drop_ci_schema(database, schema_name) %}
  {% do run_query("DROP DATABASE IF EXISTS `" ~ schema_name ~ "`") %}
  {% do adapter.commit() %}
{% endmacro %}


{# ── Per-adapter schema existence check (run-operation safe) ──────── #}
{# adapter.check_schema_exists() is not available in run-operation context.
   This is only used by the integration test; the main macro does not need it. #}

{% macro ci_schema_exists(database, schema_name) %}
  {% do return(adapter.dispatch('ci_schema_exists', 'elementary')(database, schema_name)) %}
{% endmacro %}

{% macro default__ci_schema_exists(database, schema_name) %}
  {% set result = run_query("SELECT schema_name FROM information_schema.schemata WHERE lower(schema_name) = lower('" ~ schema_name ~ "')") %}
  {% do return(result | length > 0) %}
{% endmacro %}

{% macro bigquery__ci_schema_exists(database, schema_name) %}
  {% set result = run_query("SELECT schema_name FROM `" ~ database ~ "`.INFORMATION_SCHEMA.SCHEMATA WHERE lower(schema_name) = lower('" ~ schema_name ~ "')") %}
  {% do return(result | length > 0) %}
{% endmacro %}

{% macro clickhouse__ci_schema_exists(database, schema_name) %}
  {% set result = run_query("SELECT 1 FROM system.databases WHERE name = '" ~ schema_name ~ "' LIMIT 1") %}
  {% do return(result | length > 0) %}
{% endmacro %}
