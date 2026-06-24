{% macro set_test_table_expiration(relation) %}
    {{ adapter.dispatch("set_test_table_expiration", "elementary")(relation) }}
{% endmacro %}

{% macro default__set_test_table_expiration(relation) %} {% endmacro %}

{% macro bigquery__set_test_table_expiration(relation) %}
    {% set expiration_query %}
        ALTER TABLE {{ relation }}
        SET OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR))
    {% endset %}
    {% do elementary.run_query(expiration_query) %}
{% endmacro %}
