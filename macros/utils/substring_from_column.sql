{% macro substr_between_two_strings(column, first_string, second_string) %}
    {{ return(adapter.dispatch('substr_between_two_strings','elementary')(column, first_string, second_string)) }}
{% endmacro %}

{# Snowflake #}
{% macro default__substr_between_two_strings(column, first_string, second_string) %}
    {%- if not second_string %}
        {%- set second_string = first_string %}
    {%- endif %}
    {%- set first_string_length = first_string | length %}
    {%- set second_string_length = second_string | length %}
        case
            when {{ column }} like '%{{ first_string }}%'
            then substr({{ column }}, position('{{ first_string }}',query_text)+ {{ first_string_length }},length(substr({{ column }}, position('{{ first_string }}',query_text)+ {{ first_string_length }}))- position(reverse('{{ second_string }}'),reverse(query_text))- {{ second_string_length }}+1)
            else null
        end
{% endmacro %}