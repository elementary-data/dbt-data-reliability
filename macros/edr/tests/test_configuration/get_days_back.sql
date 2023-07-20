{% macro get_days_back(days_back, model_graph_node, seasonality=none) %}
    {% set days_back = elementary.get_test_argument('days_back', days_back, model_graph_node) %}
    {% if seasonality in ["day_of_week", "hour_of_week"] %}
        {% do return(days_back * 7) %}
    {% endif %}
    {% do return(days_back) %}
{% endmacro %}