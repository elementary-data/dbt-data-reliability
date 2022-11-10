{% macro store_test_sample() %}
    {% set test_sample_sql %}
        select * from ({{ sql }})
        {% if not model.get('test_metadata', {}).get('namespace') == 'elementary' %}
          limit {{ elementary.get_config_var('test_sample_row_count') }}
        {% endif %}
    {% endset %}
    {% set test_sample = dbt.run_query(test_sample_sql) %}
    {% set test_samples_cache = elementary.get_cache("test_samples") %}
    {% do test_samples_cache.update({model.unique_id: test_sample}) %}
{% endmacro %}

{% materialization test, default %}
    {% do elementary.store_test_sample() %}
    {{ return(dbt.materialization_test_default()) }}
{% endmaterialization %}

{% materialization test, adapter='snowflake' %}
    {% do elementary.store_test_sample() %}
    {{ return(dbt.materialization_test_snowflake()) }}
{% endmaterialization %}
