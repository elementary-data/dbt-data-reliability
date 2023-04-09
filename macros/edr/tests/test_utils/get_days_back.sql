{% macro get_days_back(seasonality=none) %}
    {% set days_back = elementary.get_config_var('days_back') %}
    {% if seasonality and seasonality == 'day_of_week' %}
        {% do return(days_back * 7) %}
    {% endif %}
    {% do return(days_back) %}
{% endmacro %}