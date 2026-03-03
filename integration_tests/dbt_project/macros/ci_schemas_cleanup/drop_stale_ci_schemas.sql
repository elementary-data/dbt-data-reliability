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

  Generic schema helpers (edr_list_schemas, edr_schema_exists) live in the
  schema_utils/ folder. edr_drop_schema lives in clear_env.sql. CI-specific
  helpers (parse_timestamp_from_ci_schema_name) live alongside this file.
#}
{% macro drop_stale_ci_schemas(prefixes=none, max_age_hours=24) %}
    {% if prefixes is none or prefixes is string or prefixes | length == 0 %}
        {{
            exceptions.raise_compiler_error(
                "drop_stale_ci_schemas: 'prefixes' is required and must be a "
                "non-empty list (e.g. ['dbt_', 'py_'])."
            )
        }}
    {% endif %}

    {% set max_age_hours = max_age_hours | int %}
    {% set database = elementary.target_database() %}
    {% set all_schemas = edr_list_schemas(database) %}
    {# utcnow() is deprecated in Python 3.12+ but modules.datetime.timezone is not
     available in dbt's Jinja context. Both now and the constructed datetime are
     naive, so comparisons are safe. #}
    {% set now = modules.datetime.datetime.utcnow() %}
    {% set max_age_seconds = max_age_hours * 3600 %}
    {% set ns = namespace(dropped=0) %}

    {{
        log(
            "CI schema cleanup: scanning " ~ all_schemas
            | length
            ~ " schema(s) in database '"
            ~ database
            ~ "' for prefixes "
            ~ prefixes
            | string,
            info=true,
        )
    }}

    {% for schema_name in all_schemas | sort %}
        {% set schema_ts = parse_timestamp_from_ci_schema_name(schema_name, prefixes) %}
        {% if schema_ts is not none %}
            {% set age_seconds = (now - schema_ts).total_seconds() %}
            {% if age_seconds > max_age_seconds %}
                {{
                    log(
                        "  DROP " ~ schema_name ~ "  (age: " ~ (age_seconds / 3600)
                        | round(1) ~ " h)",
                        info=true,
                    )
                }}
                {% do edr_drop_schema(database, schema_name) %}
                {% set ns.dropped = ns.dropped + 1 %}
            {% else %}
                {{
                    log(
                        "  keep " ~ schema_name ~ "  (age: " ~ (age_seconds / 3600)
                        | round(1) ~ " h)",
                        info=true,
                    )
                }}
            {% endif %}
        {% endif %}
    {% endfor %}

    {{
        log(
            "CI schema cleanup complete. Dropped " ~ ns.dropped ~ " stale schema(s).",
            info=true,
        )
    }}
{% endmacro %}
