{% macro delete_refreshed_incremental_metrics() %}
    {% set database_name, schema_name = elementary.get_package_database_and_schema() %}
    {% set metrics_relation = adapter.get_relation(database=database_name, schema=schema_name, identifier="data_monitoring_metrics") %}
    {% if not metrics_relation %}
      {% do return(none) %}
    {% endif %}

    {%- set get_stored_metric_tables_query -%}
        select full_table_name from {{ metrics_relation }}
        group by full_table_name
    {%- endset -%}
    {% set stored_metric_tables = elementary.result_column_to_list(get_stored_metric_tables_query) %}

    {% set refreshed_tables = [] %}
    {% for result in results %}
      {% do refreshed_tables.append(elementary.model_node_to_full_name(result.node)) %}
    {% endfor %}

    {% set stored_metric_tables_to_delete = elementary.lists_intersection(stored_metric_tables, refreshed_tables) %}
    {%- set delete_stored_metric_tables_query -%}
        delete from {{ metrics_relation }}
        where full_table_name in ({{ "'{}'".format("','".join(stored_metric_tables_to_delete)) }})
    {%- endset -%}
    {% do dbt.run_query(delete_stored_metric_tables_query) %}
{% endmacro %}
