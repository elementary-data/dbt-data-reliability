{% materialization test, default %}
    {% set test_sample_sql %}
        select * from ({{ sql }}) limit {{ elementary.get_config_var('test_sample_row_count') }}
    {% endset %}
    {% set test_sample = dbt.run_query(test_sample_sql) %}
    {% do graph["elementary"]["test_samples"].update({model.unique_id: test_sample}) %}
    {{ return(dbt.materialization_test_default()) }}
{% endmaterialization %}
