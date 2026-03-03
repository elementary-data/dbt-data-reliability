{% macro get_elementary_test_table(test_name, table_type) %}
    {% if execute %}
        {% set test_entry = elementary.get_cache("temp_test_table_relations_map").setdefault(test_name, {}) %}
        {% set _relations = test_entry.get(table_type, []) %}
        {% if _relations %}
            {% do return(_relations[-1]) %}
        {% endif %}
    {% endif %}
    {% do return(none) %}
{% endmacro %}
