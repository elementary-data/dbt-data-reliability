{% macro create_elementary_test_table(database_name, schema_name, test_name, table_type, sql_query) %}
    {% if execute %}
        {% set temp_table_name = elementary.table_name_with_suffix(test_name, "__" ~ table_type) %}
        {{ elementary.debug_log(table_type ~ ' table: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ temp_table_name) }}

        {% set _, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                   schema=schema_name,
                                                                   identifier=temp_table_name,
                                                                   type='table') -%}

        {# Cache the test table for easy access later #}
        {% set cache_key = "elementary_test_table|" ~ test_name ~ "|" ~ table_type %}
        {% do elementary.set_cache(cache_key, temp_table_relation) %}

        {# Create the table if it doesn't exist #}
        {%- do elementary.create_or_replace(False, temp_table_relation, sql_query) %}

        {{ return(temp_table_relation) }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}
