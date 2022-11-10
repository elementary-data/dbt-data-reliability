{% macro get_elementary_test_table(test_name, table_type) %}
    {% if execute %}
        {% set cache_key = "elementary_test_table|" ~ test_name ~ "|" ~ table_type %}
        {{ return(elementary.get_cache(cache_key)) }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}
