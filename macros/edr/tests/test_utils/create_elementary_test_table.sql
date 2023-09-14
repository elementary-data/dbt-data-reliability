{% macro create_elementary_test_table(database_name, schema_name, test_name, table_type, sql_query) %}
    {% if execute %}
        {% set temp_table_name = elementary.table_name_with_suffix(test_name, "__" ~ table_type ~ elementary.get_timestamped_table_suffix()).replace("*", "") %}
        
        {% set default_identifier_quoting = api.Relation.get_default_quote_policy().get_part("identifier") %}        
        {% if not adapter.config.quoting.get("identifier", default_identifier_quoting) %}
            {% set temp_table_name = adapter.quote(temp_table_name) %}
        {% endif %}

        {{ elementary.debug_log(table_type ~ ' table: ' ~ database_name ~ '.' ~ schema_name ~ '.' ~ temp_table_name) }}

        {% set _, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                   schema=schema_name,
                                                                   identifier=temp_table_name,
                                                                   type='table') -%}

        {# Create the table if it doesn't exist #}
        {%- do elementary.create_or_replace(false, temp_table_relation, sql_query) %}

        {# Cache the test table for easy access later #}
        {% set test_entry = elementary.get_cache("temp_test_table_relations_map").setdefault(test_name, {}) %}
        {% do test_entry.update({table_type: temp_table_relation}) %}
        {{ return(temp_table_relation) }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}
