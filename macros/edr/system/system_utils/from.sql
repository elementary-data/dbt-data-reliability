{% macro from(full_table_name) %}
    {{ adapter.dispatch('from','elementary')(full_table_name) }}
{% endmacro %}

{% macro default__from(full_table_name) %}
    {%- set upper_full_name = full_table_name | upper %}
    {%- set split_full_name = upper_full_name.split('.') %}
    {%- set from_name = '"'~ split_full_name[0] ~'"."'~ split_full_name[1] ~'"."'~ split_full_name[2] ~'"' %}
    {{ return(from_name) }}
{% endmacro %}

{% macro bigquery__from(full_table_name) %}
    {%- set lower_full_name = full_table_name | lower %}
    {%- set split_full_name = lower_full_name.split('.') %}
    {%- set from_name = '`'~ split_full_name[0] ~'`.`'~ split_full_name[1] ~'`.`'~ split_full_name[2] ~'`' %}
    {{ return(from_name) }}
{% endmacro %}
