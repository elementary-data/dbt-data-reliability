{% macro select_timestamp_column(column_name) %}
    {{ return(adapter.dispatch('select_timestamp_column', 'elementary')(column_name)) }}
{% endmacro %}

{% macro default__select_timestamp_column(column_name) %}
    {{ return(elementary.escape_select([column_name])) }}
{% endmacro %}

{% macro dremio__select_timestamp_column(column_name) %}
    {# 
        Dremio truncates milliseconds when selecting timestamps. 
        Using TO_CHAR with FFF format preserves them. 
    #}
    {% set escaped_name = elementary.escape_select([column_name]) %}
    {{ return('TO_CHAR(' ~ escaped_name ~ ', \'YYYY-MM-DD"T"HH24:MI:SS.FFF\') as ' ~ escaped_name) }}
{% endmacro %}
