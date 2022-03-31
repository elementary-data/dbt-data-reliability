{% macro clear_tests() %}
    -- depends_on: {{ ref('alerts_data_monitoring') }}
    {% if execute and flags.WHICH == 'test' %}
        -- TODO: change to truncate
        {% set clear_alerts_tables_query %}
            DELETE FROM {{ ref('alerts_data_monitoring') }} where TRUE
        {% endset %}
        {% do run_query(clear_alerts_tables_query) %}
    {% endif %}
    {{ return('') }}
{% endmacro %}
