{% macro test_truncate_timestamp_precision(value) %}
    {{ return(elementary.truncate_timestamp_precision(value)) }}
{% endmacro %}

{% macro test_render_cast_as_timestamp(column_name) %}
    {{ return(elementary.edr_cast_as_timestamp(column_name)) }}
{% endmacro %}

{% macro test_cast_as_timestamp(value) %}
    {% set query %}
        select {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(value)) }} as ts
    {% endset %}
    {% set ts = elementary.run_query(query).columns[0].values()[0] %}
    {{ return(ts.isoformat()) }}
{% endmacro %}

{% macro test_fix_nanosecond_timing_values(model_execution_id, value) %}
    {% set relation = elementary.get_elementary_relation("dbt_run_results") %}
    {% set insert_query %}
        insert into {{ relation }} (model_execution_id, execute_started_at, execute_completed_at)
        values ({{ elementary.edr_quote(model_execution_id) }}, {{ elementary.edr_quote(value) }}, {{ elementary.edr_quote(value) }})
    {% endset %}
    {% do elementary.run_query(insert_query) %}

    {% do elementary.fix_nanosecond_timing_values() %}

    {% set select_query %}
        select execute_started_at, execute_completed_at from {{ relation }}
        where model_execution_id = {{ elementary.edr_quote(model_execution_id) }}
    {% endset %}
    {% set row = elementary.run_query(select_query).rows[0] %}
    {% set delete_query %}
        delete from {{ relation }}
        where model_execution_id = {{ elementary.edr_quote(model_execution_id) }}
    {% endset %}
    {% do elementary.run_query(delete_query) %}
    {{ return([row[0], row[1]]) }}
{% endmacro %}
