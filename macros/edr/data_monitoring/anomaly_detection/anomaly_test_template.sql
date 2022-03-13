{% macro anomaly_test(metric_name) %}

    {%- set anomaly_test_query = elementary.anomaly_test_query(metric_name) %}
    {{ anomaly_test_query }}

{% endmacro %}


{% macro get_monitors_empty_table_query() %}
        {% set monitors_empty_table_query = elementary.empty_table([('id','str'),
                                                                    ('full_table_name','str'),
                                                                    ('column_name','str'),
                                                                    ('metric_name','str'),
                                                                    ('metric_value','int'),
                                                                    ('timeframe_start','timestamp'),
                                                                    ('timeframe_end','timestamp'),
                                                                    ('timeframe_duration_hours','int'),
                                                                    ('description','str')]) %}
        {{ return(monitors_empty_table_query) }}
{% endmacro %}


{% macro create_temp_table(database_name, schema_name, table_name, sql_query) %}
    {% set temp_table_exists, temp_table_relation = dbt.get_or_create_relation(database=database_name,
                                                                                   schema=schema_name,
                                                                                   identifier=table_name,
                                                                                   type='table') -%}
    {% if temp_table_exists %}
        {% do adapter.drop_relation(temp_table_relation) %}
        {% do run_query(dbt.create_table_as(True, temp_table_relation, sql_query)) %}
    {% else %}
        {% do run_query(dbt.create_table_as(True, temp_table_relation, sql_query)) %}
    {% endif %}
    {{ return(temp_table_relation) }}
{% endmacro %}


{% macro get_anomalies_empty_table_query() %}
        {% set monitors_empty_table_query = elementary.empty_table([('id','str'),
                                                                    ('full_table_name','str'),
                                                                    ('column_name','str'),
                                                                    ('metric_name','str'),
                                                                    ('z_score','int'),
                                                                    ('latest_value','int'),
                                                                    ('value_updated_at','timestamp'),
                                                                    ('metric_avg','int'),
                                                                    ('metric_stddev','int'),
                                                                    ('training_timeframe_start','timestamp'),
                                                                    ('training_timeframe_end','timestamp'),
                                                                    ('values_in_timeframe','int'),
                                                                    ('updated_at','timestamp'),
                                                                    ('description','str')]) %}
        {{ return(monitors_empty_table_query) }}
{% endmacro %}