{% test collect_metrics(
    model,
    metrics=none,
    timestamp_column=none,
    time_bucket=none,
    days_back=64,
    backfill_days=none,
    where_expression=none,
    dimensions=none,
    cloud_monitored=false
) %}

    {{ config(
        tags=['elementary-tests'],
        meta={"elementary": {"include": false}}
    ) }}

    {% if not execute or not elementary.is_test_command() or not elementary.is_elementary_enabled() %}
        {% do return(elementary.no_results_query()) %}
    {% endif %}

    {% do elementary.validate_unique_metric_names(metrics) %}
    {% do elementary.debug_log("Metrics: {}".format(metrics)) %}

    {% if not dimensions %}
        {% set dimensions = [] %}
    {% endif %}

    {% set model_relation = elementary.get_model_relation_for_test(model, elementary.get_test_model()) %}
    {% if not model_relation %}
        {% do exceptions.raise_compiler_error("Unsupported model: " ~ model ~ " (this might happen if you override 'ref' or 'source')") %}
    {% endif %}

    {% set available_table_monitors = elementary.get_available_table_monitors() %}
    {% set available_col_monitors = elementary.get_available_column_monitors() %}

    {% set table_metrics = [] %}
    {% set col_to_metrics = {} %}
    {% for metric in metrics %}
        {% if metric.get("columns") %}
            {% if metric.type not in available_col_monitors %}
                {% if metric.type in available_table_monitors %}
                    {% do exceptions.raise_compiler_error("The metric '{}' is a table metric and shouldn't receive 'columns' argument.".format(metric.type)) %}
                {% endif %}
                {% do exceptions.raise_compiler_error("Unsupported column metric: '{}'.".format(metric.type)) %}
            {% endif %}

            {% if metric.columns is string %}
                {% if metric.columns == "*" %}
                    {% set columns = adapter.get_columns_in_relation(model_relation) %}
                    {% for col in columns %}
                        {% do col_to_metrics.setdefault(col.name.strip('"'), []).append(metric) %}
                    {% endfor %}
                {% else %}
                    {% do col_to_metrics.setdefault(metric.columns, []).append(metric) %}
                {% endif %}
            {% elif metric.columns is sequence %}
                {% for col in metric.columns %}
                    {% do col_to_metrics.setdefault(col, []).append(metric) %}
                {% endfor %}
            {% else %}
                {% do exceptions.raise_compiler_error("Unexpected value provided for 'columns' argument.") %}
            {% endif %}
        {% else %}
            {% if metric.type not in available_table_monitors %}
                {% if metric.type in available_col_monitors %}
                    {% do exceptions.raise_compiler_error("The metric '{}' is a column metric and should receive 'columns' argument.".format(metric.type)) %}
                {% endif %}
                {% do exceptions.raise_compiler_error("Unsupported table metric: '{}'.".format(metric.type)) %}
            {% endif %}

            {% do table_metrics.append(metric) %}
        {% endif %}
    {% endfor %}

    {% if table_metrics %}
        {% do elementary.collect_table_metrics(table_metrics, model, model_relation, timestamp_column, time_bucket, days_back, backfill_days, where_expression, dimensions, collected_by="collect_metrics") %}
    {% endif %}

    {% for col_name, col_metrics in col_to_metrics.items() %}
        {% do elementary.collect_column_metrics(col_metrics, model, model_relation, col_name, timestamp_column, time_bucket, days_back, backfill_days, where_expression, dimensions, collected_by="collect_metrics") %}
    {% endfor %}

    {# This test always passes. #}
    {% do return(elementary.no_results_query()) %}
{% endtest %}
