{% macro get_last_schema_changes_alert_time() %}
    {%- set last_schema_changes_query %}
        select max(detected_at) as last_alert_time
        from {{ ref('alerts_schema_changes') }}
        where sub_type != 'table_added'
    {%- endset %}

    {%- set last_schema_changes_query_result = elementary.result_value(last_schema_changes_query) %}

    {%- if last_schema_changes_query_result %}
        {{ return(last_schema_changes_query_result) }}
    {%- else %}
        {{ return(null) }}
    {%- endif %}

{% endmacro %}