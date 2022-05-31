{% macro table_name_with_suffix(table_name, suffix) %}
    {% set relation_max_name_length = elementary.get_relation_max_name_length()  %}
    {% if relation_max_name_length %}
        {% set suffix_length = suffix | length %}
        {% set table_name_with_suffix = table_name[:relation_max_name_length - suffix_length] ~ suffix %}
    {% else %}
        {% set table_name_with_suffix = table_name ~ suffix %}
    {% endif %}
    {{ return(table_name_with_suffix) }}
{% endmacro %}
