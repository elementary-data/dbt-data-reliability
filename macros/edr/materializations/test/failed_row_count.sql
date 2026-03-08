{% macro get_failed_row_count(flattened_test) %}
    {% set test_result = elementary.get_test_result() %}
    {% if config.get("fail_calc").strip() == elementary.get_failed_row_count_calc(
        flattened_test
    ) %}
        {% do elementary.debug_log("Using test failures as failed_rows value.") %}
        {% do return(test_result.failures | int) %}
    {% endif %}
    {% if elementary.did_test_pass(test_result) %} {% do return(none) %} {% endif %}
    {% set failed_row_count_query = elementary.get_failed_row_count_query(
        flattened_test
    ) %}
    {% if failed_row_count_query %}
        {% set result_count = elementary.result_value(failed_row_count_query) %}
        {% do return(result_count) %}
    {% endif %}
    {% do return(none) %}
{% endmacro %}

{% macro get_failed_row_count_query(flattened_test) %}
    {% set failed_row_count_calc = elementary.get_failed_row_count_calc(
        flattened_test
    ) %}
    {% if failed_row_count_calc %}
        {% set failed_row_count_query = elementary.get_failed_row_count_calc_query(
            failed_row_count_calc
        ) %}
        {% do return(failed_row_count_query) %}
    {% endif %}
    {% do return(none) %}
{% endmacro %}

{% macro get_failed_row_count_calc(flattened_test) %}
    {% if "failed_row_count_calc" in flattened_test["meta"] %}
        {% do return(flattened_test["meta"]["failed_row_count_calc"]) %}
    {% endif %}
    {% set common_test_config = elementary.get_common_test_config(flattened_test) %}
    {% if common_test_config %}
        {% do return(common_test_config.get("failed_row_count_calc")) %}
    {% endif %}
    {% do return(none) %}
{% endmacro %}

{% macro get_failed_row_count_calc_query(failed_row_count_calc) %}
    {{
        return(
            adapter.dispatch("get_failed_row_count_calc_query", "elementary")(
                failed_row_count_calc
            )
        )
    }}
{% endmacro %}

{% macro default__get_failed_row_count_calc_query(failed_row_count_calc) %}
    with results as ({{ sql }})
    select
        {{ failed_row_count_calc }}
        as {{ elementary.escape_reserved_keywords("count") }}
    from results
{% endmacro %}

{% macro fabric__get_failed_row_count_calc_query(failed_row_count_calc) %}
    {# Fabric / T-SQL does not support nested CTEs.
       We create a temp table from the test SQL, then select from it.
       The temp table is session-scoped and cleaned up by on_run_end. #}
    {% set tmp_relation = elementary.edr_make_temp_relation(model) %}
    {% do run_query(
        "select * into " ~ tmp_relation ~ " from (" ~ sql ~ ") as __edr_inner"
    ) %}
    select
        {{ failed_row_count_calc }}
        as {{ elementary.escape_reserved_keywords("count") }}
    from {{ tmp_relation }}
{% endmacro %}
