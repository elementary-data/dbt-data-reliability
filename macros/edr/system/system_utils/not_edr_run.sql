{% macro not_edr_run() %}
    {%- if elementary.get_config_var('edr_run') %}
        {{ return(false) }}
    {%- else %}
        {{ return(true) }}
    {%- endif %}
{% endmacro %}