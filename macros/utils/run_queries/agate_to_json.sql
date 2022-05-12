{% macro agate_to_json(agate_table) %}
    {% set serializable_rows = elementary.agate_to_dicts(agate_table) %}
    {{ return(tojson(serializable_rows)) }}
{% endmacro %}