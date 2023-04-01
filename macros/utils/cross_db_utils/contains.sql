{% macro contains(string, string_to_search,case_sensitive=False) -%}
    {{ adapter.dispatch('contains', 'elementary') (string, string_to_search, case_sensitive) }}
{%- endmacro %}

{# Snowflake, Databricks #}
{% macro default__contains(string, string_to_search, case_sensitive) %}
    {%- if case_sensitive %}
        contains({{ string }}, {{ string_to_search }})
    {%- else %}
        contains(lower({{ string }}), lower({{ string_to_search }}))
    {%- endif %}
{% endmacro %}

{% macro bigquery__contains(string, string_to_search, case_sensitive) %}
    {%- if case_sensitive %}
        contains_substr({{ string }}, {{ string_to_search }})
    {%- else %}
        contains_substr(lower({{ string }}), lower({{ string_to_search }}))
    {%- endif %}
{% endmacro %}

{% macro postgres__contains(string, string_to_search, case_sensitive) %}
    {%- if case_sensitive %}
        case when
            {{ string }} like '%{{ string_to_search }}%' then true
        else false
    {%- else %}
        case when
            lower({{ string }}) like lower('%{{ string_to_search }}%') then true
        else false
    {%- endif %}
{% endmacro %}