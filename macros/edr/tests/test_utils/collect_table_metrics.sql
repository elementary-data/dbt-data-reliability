{% macro collect_table_metrics(
    table_metrics,
    model_expr,
    model_relation,
    timestamp_column,
    time_bucket,
    days_back,
    backfill_days,
    where_expression,
    dimensions
) %}
    {% set model_graph_node = elementary.get_model_graph_node(model_relation) %}
    {% set metric_props = elementary.get_metric_properties(model_graph_node, timestamp_column, where_expression, time_bucket, dimensions) %}
    {% set days_back = elementary.get_test_argument('days_back', days_back, model_graph_node) %}
    {% set backfill_days = elementary.get_test_argument('backfill_days', backfill_days, model_graph_node) %}

    {% set metric_names = [] %}
    {% for metric in table_metrics %}
        {% do metric_names.append(metric.name) %}
    {% endfor %}
    {% set table_monitors = elementary.get_final_table_monitors(monitors=metric_names) %}
    {% if not table_monitors %}
        {% do return(none) %}
    {% endif %}

    {% if dimensions and table_monitors != ["row_count"] %}
        {% do exceptions.raise_compiler_error("collect_metrics test does not support non row_count dimensional table metrics.") %}
    {% endif %}

    {% if metric_props.timestamp_column %}
        {% set min_bucket_start, max_bucket_end = elementary.get_metric_buckets_min_and_max(
            model_relation=model_relation,
            backfill_days=backfill_days,
            days_back=days_back,
            monitors=table_monitors,
            metric_properties=metric_props
        ) %}
    {% endif %}


    {% if dimensions %}
        {% set monitoring_query = elementary.dimension_monitoring_query(
            model_expr,
            model_relation,
            dimensions,
            min_bucket_start,
            max_bucket_end,
            metric_properties=metric_props
        ) %}
    {% else %}
        {% set monitoring_query = elementary.table_monitoring_query(
            model_expr,
            model_relation,
            min_bucket_start,
            max_bucket_end,
            table_monitors,
            metric_properties=metric_props
        ) %}
    {% endif %}
    {% do elementary.debug_log('monitoring_query - \n' ~ monitoring_query) %}


    {% set test_table_name = elementary.get_elementary_test_table_name() %}
    {% set database_name, schema_name = elementary.get_package_database_and_schema() %}
    {% set tests_schema_name = elementary.get_elementary_tests_schema(database_name, schema_name) %}

    {% do elementary.create_elementary_test_table(database_name, tests_schema_name, test_table_name, 'metrics', monitoring_query) %}
    {% do elementary.store_metrics_table_in_cache() %}
{% endmacro %}
