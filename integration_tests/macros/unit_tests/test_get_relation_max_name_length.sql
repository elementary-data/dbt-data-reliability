{% macro test_get_relation_max_name_length() %}
    {% if target.type in ['redshift','databricks','spark'] %}
        {{ assert_value(elementary.get_relation_max_name_length(), 127) }}
    {% elif target.type in ['snowflake', 'bigquery'] %}
        {{ assert_value(elementary.get_relation_max_name_length(), none) }}
    {% elif target.type == 'postgres' %}
        {{ assert_value(elementary.get_relation_max_name_length(), 63) }}
    {% endif %}
{% endmacro %}
