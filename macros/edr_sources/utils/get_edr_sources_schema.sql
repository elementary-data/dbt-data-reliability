{% macro get_edr_sources_schema() %}
    {% set edr_sources_schema = var('edr_sources_schema', schema) %}
    {% if not edr_sources_schema %}
        {% set edr_sources_schema = schema %}
    {% endif %}
    {{ return(edr_sources_schema) }}
{% endmacro %}
