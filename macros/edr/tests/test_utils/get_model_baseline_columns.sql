{% macro get_model_baseline_columns(model, enforce_types=False) %}
    {# Get baseline columns #}
    {% set model_relation = dbt.load_relation(model) %}
    {% set model_graph_node = elementary.get_model_graph_node(model_relation) %}

    {% set baseline = [] %}
    {% set columns_without_types = [] %}
    {% for column in model_graph_node["columns"].values() %}
        {% set column_info = {"column_name": column["name"], "data_type": column.get("data_type")} %}
        {% if column_info["data_type"] is none %}
            {% do columns_without_types.append(column_info["column_name"]) %}
        {% endif %}
        {% do baseline.append(column_info) %}
    {% endfor %}

    {% if enforce_types %}
        {% do exceptions.raise_compiler_error("Data type not defined for columns `{}`".format(columns_without_types)) %}
    {% else %}
        {% do elementary.edr_log("Warning - missing data types for columns: " ~ columns_without_types) %}
    {% endif %}

    {% do return(baseline) %}
{% endmacro %}
