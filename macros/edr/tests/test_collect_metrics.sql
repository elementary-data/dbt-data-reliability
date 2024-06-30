{% test collect_metrics(
    model,
    metrics=none,
    timestamp_column=none,
    time_bucket=none,
    days_back=64,
    backfill_days=none,
    where_expression=none,
    dimensions=none
) %}

    {{ config(
        tags=['elementary-tests'],
        meta={"elementary": {"include": false}}
    ) }}

    {% if not execute or not elementary.is_test_command() or not elementary.is_elementary_enabled() %}
        {% do return(elementary.no_results_query()) %}
    {% endif %}

    {% do elementary.debug_log("Metrics: {}".format(metrics)) %}

    {% if not dimensions %}
        {% set dimensions = [] %}
    {% endif %}

    {% set model_relation = elementary.get_model_relation_for_test(model, context["model"]) %}
    {% if not model_relation %}
        {% do exceptions.raise_compiler_error("Unsupported model: " ~ model ~ " (this might happen if you override 'ref' or 'source')") %}
    {% endif %}

    {% set table_metrics = [] %}
    {% set col_to_metrics = {} %}
    {% for metric in metrics %}
        {% if metric.get("column") %}
            {% do col_to_metrics.setdefault(metric.column, []).append(metric) %}
        {% else %}
            {% do table_metrics.append(metric) %}
        {% endif %}
    {% endfor %}

    {% if table_metrics %}
        {% do elementary.collect_table_metrics(table_metrics, model, model_relation, timestamp_column, time_bucket, days_back, backfill_days, where_expression, dimensions) %}
    {% endif %}

    {% for col_name, col_metrics in col_to_metrics.items() %}
        {% do elementary.collect_column_metrics(col_metrics, model, model_relation, col_name, timestamp_column, time_bucket, days_back, backfill_days, where_expression, dimensions) %}
    {% endfor %}

    {# This test always passes. #}
    {% do return(elementary.no_results_query()) %}
{% endtest %}
