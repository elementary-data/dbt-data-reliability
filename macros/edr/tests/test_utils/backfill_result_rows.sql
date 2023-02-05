{% macro backfill_result_rows() %}
    {% if is_incremental() %}
        {% do return(none) %}
    {% endif %}

    {% set elementary_test_results_relation = ref("elementary", "elementary_test_results") %}
    {% set result_row_exists = elementary.get_column_in_relation(elementary_test_results_relation, "result_rows") is not none %}
    {% if not result_row_exists %}
        {% do return(none) %}
    {% endif %}

    {% set test_result_rows_rows = [] %}
    {% set query %}
    select
      id,
      detected_at,
      result_rows
    from {{ ref("elementary", "elementary_test_results") }}
    where detected_at > {{ elementary.timediff(elementary.cast_as_timestamp("detected_at"), elementary.current_timestamp(), "day") }} < {{ elementary.get_config_var("days_back") }}
    and result_rows is not null
    {% endset %}
    {% set test_results_with_result_rows = elementary.run_query(query) %}
    {% for elementary_test_results_row in test_results_with_result_rows %}
        {% set result_rows = fromjson(elementary_test_results_row.result_rows) %}
        {% for result_row in result_rows %}
            {% do test_result_rows_rows.append({
                "elementary_test_results_id": elementary_test_results_row.id,
                "detected_at": elementary_test_results_row.detected_at.strftime(elementary.get_time_format()),
                "result_row": result_row
            }) %}
        {% endfor %}
    {% endfor %}
    {% do elementary.insert_rows(this, test_result_rows_rows) %}
    {% do return('') %}
{% endmacro %}
