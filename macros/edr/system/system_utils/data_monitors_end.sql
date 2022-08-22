{% macro monitors_run_end() %}

    {%- set monitors_run_end_query %}
        update {{ ref('elementary_runs') }}
            set monitors_run_end = {{ elementary.current_timestamp_in_utc() }}
        where run_id = '{{ invocation_id }}'
    {%- endset %}

    {%- do run_query(monitors_run_end_query) -%}
    {%- do edr_log('Finished running data monitors') -%}

    {{ return('') }}

{% endmacro %}