{% macro get_model_baseline_columns(model) %}
    {# Get baseline columns #}
    {% set model_relation = dbt.load_relation(model) %}
    {% set model_graph_node = elementary.get_model_graph_node(model_relation) %}

    {% set baseline = [] %}
    {% for column in model_graph_node["columns"].values() %}
        {% if column["data_type"] %}
            {% do baseline.append({"column_name": column["name"], "data_type": column["data_type"]}) %}
        {% else %}
            {% do elementary.edr_log("No data type defined for column " ~ column["name"] ~ ", ignoring it") %}
        {% endif %}
    {% endfor %}

    {% do return(baseline) %}
{% endmacro %}
