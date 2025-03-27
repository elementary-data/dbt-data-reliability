{% macro create_elementary_test_table(database_name, schema_name, test_name, table_type, sql_query) %}
    {{ return(adapter.dispatch('create_elementary_test_table', 'elementary')(database_name, schema_name, test_name, table_type, sql_query)) }}
{% endmacro %}

{% macro default__create_elementary_test_table(database_name, schema_name, test_name, table_type, sql_query) %}
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

        {# Create the table if it doesnt exist #}
        {%- do elementary.create_or_replace(false, temp_table_relation, sql_query) %}

        {# Cache the test table for easy access later #}
        {% set test_entry = elementary.get_cache("temp_test_table_relations_map").setdefault(test_name, {}) %}
        {% do test_entry.update({table_type: temp_table_relation}) %}
        {{ return(temp_table_relation) }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}

{% macro fabric__create_elementary_test_table(database_name, schema_name, test_name, table_type, sql_query) %}
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

        {# For Fabric, we need to drop the table first if it exists, then create it #}
        {% set full_table_name = database_name ~ '.' ~ schema_name ~ '.' ~ temp_table_name %}
        
        {# Replace tuple comparison with EXISTS for Fabric compatibility #}
        {% set modified_sql_query = sql_query | replace(
            "and (cast(bucket_start as datetime2(2)), cast(bucket_end as datetime2(2))) in (select edr_bucket_start, edr_bucket_end from buckets)",
            "and exists (select 1 from buckets where buckets.edr_bucket_start = cast(bucket_start as datetime2(2)) and buckets.edr_bucket_end = cast(bucket_end as datetime2(2)))"
        ) %}
        
        {% set fabric_create_query %}
            IF OBJECT_ID('{{ full_table_name }}', 'U') IS NOT NULL 
            DROP TABLE {{ full_table_name }};
            
            SELECT * INTO {{ full_table_name }} FROM (
                {{ modified_sql_query }}
            ) AS source_query;
        {% endset %}
        {% if table_type == "anomaly_scores" %}
        {% endif %}
        {% do run_query(fabric_create_query) %}

        {# Cache the test table for easy access later #}
        {% set test_entry = elementary.get_cache("temp_test_table_relations_map").setdefault(test_name, {}) %}
        {% do test_entry.update({table_type: temp_table_relation}) %}
        {{ return(temp_table_relation) }}
    {% endif %}
    {{ return(none) }}
{% endmacro %}
