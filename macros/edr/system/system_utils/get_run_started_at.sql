{# This macro is for test purposes only! #}
{# custom_run_started_at should be in ISO format. #}

{% macro get_run_started_at() %}
    {% set custom_run_started_at = elementary.get_config_var('custom_run_started_at') %}
    {% if custom_run_started_at %}
        {# dbt run_started_at is fromtype datetime, so we convert the given custom time to be datetime as well. #}
        {{ return(modules.datetime.datetime.fromisoformat(custom_run_started_at)) }}
    {% else %}
        {{ return(run_started_at) }}
    {% endif %}
{% endmacro %}
