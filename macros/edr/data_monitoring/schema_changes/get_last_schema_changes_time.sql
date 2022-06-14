{% macro get_last_schema_changes_time() %}
    -- depends_on: {{ ref('elementary_test_results') }}
    {%- if execute -%}
        {%- set last_schema_changes_time_query %}
            select max(detected_at) as last_alert_time
            from {{ ref('elementary_test_results') }}
            where test_type = 'schema_change' and test_sub_type != 'table_added'
        {%- endset %}

        {%- set last_schema_changes_query_result = elementary.result_value(last_schema_changes_time_query) %}

        {%- if last_schema_changes_query_result %}
            {{ return(last_schema_changes_query_result) }}
        {%- else %}
            {{ return(none) }}
        {%- endif %}
    {%- endif -%}
    {{- return(none) -}}
{% endmacro %}