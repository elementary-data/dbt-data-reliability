{% macro not_edr_run() %}
    {%- if var('edr_run') %}
        {{ return(false) }}
    {%- else %}
        {{ return(true) }}
    {%- endif %}
{% endmacro %}