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

        {# Cache the test table for easy access later.
           We use a list per table_type and always append, reading the last element
           on retrieval. This avoids in-place dict key overwrite (which requires
           pop(), unavailable in fusion's minijinja). #}
        {% set test_entry = elementary.get_cache("temp_test_table_relations_map").setdefault(test_name, {}) %}
        {% do test_entry.setdefault(table_type, []) %}
        {% do test_entry[table_type].append(temp_table_relation) %}
        {{ return(temp_table_relation) }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}
