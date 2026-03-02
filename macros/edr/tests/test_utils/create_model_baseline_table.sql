{% macro create_model_baseline_table(baseline_columns, database_name, schema_name, test_name) %}
    {% set empty_table_query = elementary.empty_table([('column_name','string'),('data_type','string')]) %}
    {% set baseline_table_relation = elementary.create_elementary_test_table(database_name, schema_name,
                                                                             test_name | lower, 'schema_baseline',
                                                                             empty_table_query) %}
    {% do elementary.insert_rows(baseline_table_relation, baseline_columns, should_commit=True, chunk_size=elementary.get_config_var('dbt_artifacts_chunk_size')) %}
    {% do return(baseline_table_relation) %}
{% endmacro %}
