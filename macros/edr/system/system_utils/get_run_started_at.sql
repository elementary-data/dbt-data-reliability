{# This macro is for test purposes only! #}
{# custom_run_started_at should be in ISO format like the original run_started_at.#}

{% macro get_run_started_at() %}
    {% if elementary.get_config_var('custom_run_started_at') %}
        {{ return(elementary.get_config_var('custom_run_started_at')) }}
    {% else %}
        {{ return(run_started_at) }}
    {% endif %}
{% endmacro %}
