{% macro create_source_table(table_name, sql_query, drop_if_exists) %}
    {% set edr_sources_database = var('dbt_artifacts_database', elementary.target_database()) %}
    {% set edr_sources_schema = var('dbt_artifacts_schema', target.schema) %}
    {% set source_table_exists, source_table_relation = dbt.get_or_create_relation(database=edr_sources_database,
                                                                                   schema=edr_sources_schema,
                                                                                   identifier=table_name,
                                                                                   type='table') -%}
    {% if not adapter.check_schema_exists(edr_sources_database, edr_sources_schema) %}
        {{ elementary.debug_log('Creating dbt artifacts schema: '~ edr_sources_database ~'.'~edr_sources_schema) }}
        {% do dbt.create_schema(source_table_relation) %}
        {% do adapter.commit() %}
    {% endif %}
    {% if source_table_exists %}
        {% if drop_if_exists or flags.FULL_REFRESH %}
            {% do adapter.drop_relation(source_table_relation) %}
            {% do run_query(dbt.create_table_as(False, source_table_relation, sql_query)) %}
        {% endif %}
    {% else %}
        {% do run_query(dbt.create_table_as(False, source_table_relation, sql_query)) %}
    {% endif %}
    {{ return(source_table_relation) }}
{% endmacro %}
