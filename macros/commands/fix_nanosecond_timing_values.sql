{#
  One-time cleanup of nanosecond-precision timing values written by older package versions
  (e.g. '2026-04-03T10:50:50.961498756Z' -> '2026-04-03T10:50:50.961498Z').
  After running it, set bigquery_truncate_nanosecond_timestamps to false so BigQuery
  timestamp casts are plain casts again and can prune partitions.
  Usage: dbt run-operation elementary.fix_nanosecond_timing_values
#}
{% macro fix_nanosecond_timing_values() %}
    {% do return(adapter.dispatch("fix_nanosecond_timing_values", "elementary")()) %}
{% endmacro %}

{% macro bigquery__fix_nanosecond_timing_values() %}
    {% set timing_columns = [
        "execute_started_at",
        "execute_completed_at",
        "compile_started_at",
        "compile_completed_at",
    ] %}
    {% for table_name in ["dbt_run_results", "dbt_source_freshness_results"] %}
        {% set relation = elementary.get_elementary_relation(table_name) %}
        {% if relation is none %}
            {% do print("Relation '{}' does not exist, skipping.".format(table_name)) %}
            {% continue %}
        {% endif %}

        {% set where_clause %}
            {% for column in timing_columns %}
                regexp_contains({{ column }}, r'\.\d{7,}')
                {% if not loop.last %} or {% endif %}
            {% endfor %}
        {% endset %}
        {% set count_query %}
            select count(*) from {{ relation }} where {{ where_clause }}
        {% endset %}
        {% set rows_to_fix = elementary.result_value(count_query) %}
        {% if rows_to_fix == 0 %}
            {% do print("No rows to fix in {}.".format(relation)) %} {% continue %}
        {% endif %}

        {% set update_query %}
            update {{ relation }}
            set
            {% for column in timing_columns %}
                {{ column }} = regexp_replace({{ column }}, r'(\.\d{6})\d+', r'\1')
                {% if not loop.last %},{% endif %}
            {% endfor %}
            where {{ where_clause }}
        {% endset %}
        {% do elementary.run_query(update_query) %}
        {% do print("Fixed {} rows in {}.".format(rows_to_fix, relation)) %}
    {% endfor %}
{% endmacro %}

{% macro default__fix_nanosecond_timing_values() %}
    {% do print(
        "fix_nanosecond_timing_values is only needed on BigQuery, nothing to do on '{}'.".format(
            target.type
        )
    ) %}
{% endmacro %}
