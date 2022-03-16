{% macro get_edr_sources_db() %}
    {% set edr_sources_db = elementary.get_config_var('edr_sources_database', database) %}
    {% if not edr_sources_db %}
        {% set edr_sources_db = database %}
    {% endif %}
    {{ return(edr_sources_db) }}
{% endmacro %}
