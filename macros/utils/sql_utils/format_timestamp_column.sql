{% macro format_timestamp_column(column_name) %}
    {{ return(adapter.dispatch('format_timestamp_column', 'elementary')(column_name)) }}
{% endmacro %}

{% macro default__format_timestamp_column(column_name) %}
    {{ return(column_name) }}
{% endmacro %}

{% macro dremio__format_timestamp_column(column_name) %}
    {# 
        Dremio truncates milliseconds when selecting timestamps. 
        Using TO_CHAR with FFF format preserves them. 
    #}
    {{ return('TO_CHAR(' ~ column_name ~ ', \'YYYY-MM-DD"T"HH24:MI:SS.FFF\') as ' ~ column_name) }}
{% endmacro %}
