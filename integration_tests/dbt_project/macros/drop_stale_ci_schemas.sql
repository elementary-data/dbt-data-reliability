{#
  drop_stale_ci_schemas – clean up timestamped CI schemas.

  Schema naming convention produced by CI:
      <prefix><YYMMDD_HHMMSS>_<branch>_<hash>
  Examples:
      dbt_260228_112345_master_abcd1234
      py_260228_112345_master_abcd1234
      dbt_260228_112345_master_abcd1234_elementary   (suffixed variant)

  Call from a GitHub Actions workflow via:
      dbt run-operation drop_stale_ci_schemas \
          --args '{prefixes: ["dbt_", "py_"], max_age_hours: 24}'
#}

{% macro drop_stale_ci_schemas(prefixes=none, max_age_hours=24) %}
  {% if prefixes is none or prefixes is string or prefixes | length == 0 %}
    {{ exceptions.raise_compiler_error(
         "drop_stale_ci_schemas: 'prefixes' is required and must be a "
         "non-empty list (e.g. ['dbt_', 'py_'])."
       ) }}
  {% endif %}

  {% set max_age_hours = max_age_hours | int %}
  {% set database = elementary.target_database() %}
  {% set all_schemas = list_ci_schemas(database) %}
  {# utcnow() is deprecated in Python 3.12+ but modules.datetime.timezone is not
     available in dbt's Jinja context. Both now and strptime produce naive datetimes
     so comparisons are safe. #}
  {% set now = modules.datetime.datetime.utcnow() %}
  {% set max_age_seconds = max_age_hours * 3600 %}
  {% set ns = namespace(dropped=0) %}

  {{ log("CI schema cleanup: scanning " ~ all_schemas | length ~ " schema(s) in database '" ~ database ~ "' for prefixes " ~ prefixes | string, info=true) }}

  {% for schema_name in all_schemas | sort %}
    {% set schema_ts = parse_ci_schema_name(schema_name, prefixes) %}
    {% if schema_ts is not none %}
      {% set age_seconds = (now - schema_ts).total_seconds() %}
      {% if age_seconds > max_age_seconds %}
        {{ log("  DROP " ~ schema_name ~ "  (age: " ~ (age_seconds / 3600) | round(1) ~ " h)", info=true) }}
        {% do drop_ci_schema(database, schema_name) %}
        {% set ns.dropped = ns.dropped + 1 %}
      {% else %}
        {{ log("  keep " ~ schema_name ~ "  (age: " ~ (age_seconds / 3600) | round(1) ~ " h)", info=true) }}
      {% endif %}
    {% endif %}
  {% endfor %}

  {{ log("CI schema cleanup complete. Dropped " ~ ns.dropped ~ " stale schema(s).", info=true) }}
{% endmacro %}


{# ── CI schema name parser ─────────────────────────────────────────── #}

{% macro parse_ci_schema_name(schema_name, prefixes) %}
  {#
    Parses a CI schema name and returns the embedded timestamp as a datetime,
    or none if the name doesn't match the expected CI naming pattern.

    The pattern after the prefix is: YYMMDD_HHMMSS_<branch>_<hash>[_<suffix>]
  #}
  {% set schema_lower = schema_name.lower() %}
  {% for prefix in prefixes %}
    {% if schema_lower.startswith(prefix.lower()) %}
      {% set remainder = schema_lower[prefix | length :] %}
      {% set match = modules.re.match('^(\\d{6})_(\\d{6})_.+', remainder) %}
      {% if match %}
        {% set ts_str = match.group(1) ~ '_' ~ match.group(2) %}
        {# Validate date component ranges before calling strptime #}
        {% set mm = ts_str[2:4] | int %}
        {% set dd = ts_str[4:6] | int %}
        {% set hh = ts_str[7:9] | int %}
        {% set mi = ts_str[9:11] | int %}
        {% set ss = ts_str[11:13] | int %}
        {% if 1 <= mm <= 12 and 1 <= dd <= 31 and 0 <= hh <= 23 and 0 <= mi <= 59 and 0 <= ss <= 59 %}
          {% do return(modules.datetime.datetime.strptime(ts_str, '%y%m%d_%H%M%S')) %}
        {% endif %}
      {% endif %}
    {% endif %}
  {% endfor %}
  {% do return(none) %}
{% endmacro %}


{# ── Per-adapter schema drop ─────────────────────────────────────────── #}

{% macro drop_ci_schema(database, schema_name) %}
  {% do return(adapter.dispatch('drop_ci_schema', 'elementary_tests')(database, schema_name)) %}
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
