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

  {% set database = elementary.target_database() %}
  {% set all_schemas = adapter.list_schemas(database) %}
  {% set now = modules.datetime.datetime.utcnow() %}
  {% set max_age_seconds = max_age_hours * 3600 %}
  {% set ns = namespace(dropped=0, errors=0) %}

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
            {% set schema_ts = modules.datetime.datetime.strptime(ts_str, '%y%m%d_%H%M%S') %}
            {% set age_seconds = (now - schema_ts).total_seconds() %}
            {% if age_seconds > max_age_seconds %}
              {{ log("  DROP " ~ schema_name ~ "  (age: " ~ (age_seconds / 3600) | round(1) ~ " h)", info=true) }}
              {% do elementary.drop_ci_schema(database, schema_name) %}
              {% set ns.dropped = ns.dropped + 1 %}
            {% else %}
              {{ log("  keep " ~ schema_name ~ "  (age: " ~ (age_seconds / 3600) | round(1) ~ " h)", info=true) }}
            {% endif %}
          {% endif %}
        {% endif %}
      {% endif %}
    {% endfor %}
  {% endfor %}

  {{ log("CI schema cleanup complete. Dropped " ~ ns.dropped ~ " stale schema(s).", info=true) }}
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
  {% do run_query("DROP DATABASE IF EXISTS " ~ schema_name) %}
  {% do adapter.commit() %}
{% endmacro %}
