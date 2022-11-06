{% macro store_test_sample() %}
    {% set test_sample_sql %}
        select * from ({{ sql }})
        {% if not model.package_name == 'elementary' %}
          limit {{ elementary.get_config_var('test_sample_row_count') }}
        {% endif %}
    {% endset %}
    {% set test_sample = dbt.run_query(test_sample_sql) %}
    {% do graph["elementary"]["test_samples"].update({model.unique_id: test_sample}) %}
{% endmacro %}

{% materialization test, default %}
    {% do elementary.store_test_sample() %}
    {{ return(dbt.materialization_test_default()) }}
{% endmaterialization %}

{% materialization test, adapter='snowflake' %}
    {% do elementary.store_test_sample() %}
    {{ return(dbt.materialization_test_snowflake()) }}
{% endmaterialization %}
