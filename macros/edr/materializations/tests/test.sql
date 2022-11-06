{% materialization test, default %}
    {% set test_result = dbt.run_query(get_test_result_sql) %}
    {% do graph["elementary"]["test_results"].update({model.unique_id: test_result}) %}
    {{ return(dbt.materialization_test_default()) }}
{% endmaterialization %}
