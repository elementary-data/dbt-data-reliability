{% macro create_elementary_test_table(database_name, schema_name, test_name, table_type, sql_query) %}
    {% if execute %}
        {% set temp_table_name = elementary.table_name_with_suffix(test_name, "__" ~ table_type ~ elementary.get_timestamped_table_suffix()) %}
        {% set temp_table_name = temp_table_name.replace("*", "").replace("-", "_").replace(".", "_") %}

        {{ elementary.debug_log(table_type ~ ' table: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ temp_table_name) }}

        {% set _, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                   schema=schema_name,
                                                                   identifier=temp_table_name,
                                                                   type='table') -%}

        {# Create the table if it doesnt exist #}
        {%- do elementary.create_or_replace(false, temp_table_relation, sql_query) %}

        {# Cache the test table for easy access later #}
        {% set test_entry = elementary.get_cache("temp_test_table_relations_map").setdefault(test_name, {}) %}
        {% do test_entry.update({table_type: temp_table_relation}) %}
        {{ return(temp_table_relation) }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}
