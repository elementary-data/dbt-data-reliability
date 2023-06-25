{% macro validate_dimension_anomalies() %}
    {% set alerts_relation = ref('alerts_anomaly_detection') %}

    {% set dimension_validation_query %}
        select *,
            {{ elementary.contains('tags', 'should_fail') }} as should_fail
        from {{ alerts_relation }}
        where status in ('fail', 'warn', 'error') and tags like '%dimension_anomalies%'
    {% endset %}
    {% set results = elementary.agate_to_dicts(run_query(dimension_validation_query)) %}
    {% set dimensions_with_problems = [] %}

    {%- set should_fail_descriptions = [] %}
    {%- set should_fail_names = [] %}

    {% for result in results %}
        {%- set should_fail_tag = result.get('should_fail') %}
        {%- set test_name = result.get('test_name') %}
        {%- set alert_description = result.get('alert_description') %}
        {%- if should_fail_tag == True %}
            {%- do should_fail_descriptions.append(alert_description) -%}
            {%- do should_fail_names.append(test_name) -%}
        {%- endif %}
    {% endfor %}

    {{ assert_lists_contain_same_items(should_fail_names, ['elementary_dimension_anomalies_dimension_anomalies_platform', 'elementary_dimension_anomalies_dimension_anomalies_platform__updated_at', 'elementary_dimension_anomalies_dimension_anomalies_platform__version__updated_at']) }}
{% endmacro %}
