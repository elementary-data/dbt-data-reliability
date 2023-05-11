{% macro make_temp_relation(base_relation, suffix=none) %}
    {% if not suffix %}
        {% set suffix = modules.datetime.datetime.utcnow().strftime('__tmp_%Y%m%d%H%M%S%f') %}
    {% endif %}

    {% set temp_identifier = base_relation.identifier ~ suffix %}
    {% set temp_relation = base_relation.incorporate(path={"identifier": temp_identifier}) %}
    {{ return(temp_relation) }}
{% endmacro %}
