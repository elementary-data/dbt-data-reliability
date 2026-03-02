{% macro relation_exists(relation) %}
    {%- set loaded_relation = load_relation(relation) -%}
    {% if loaded_relation is not none %}
        {{ return(True) }}
    {% endif %}
    {{ return(False) }}
{% endmacro %}