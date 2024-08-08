{% macro collect_table_metrics(
    table_metrics,
    model_expr,
    model_relation,
    timestamp_column,
    time_bucket,
    days_back,
    backfill_days,
    where_expression,
    dimensions,
    collected_by=none
) %}
    {% set model_graph_node = elementary.get_model_graph_node(model_relation) %}
    {% set metric_props = elementary.get_metric_properties(model_graph_node, timestamp_column, where_expression, time_bucket, dimensions, collected_by=collected_by) %}
    {% set days_back = elementary.get_test_argument('days_back', days_back, model_graph_node) %}
    {% set backfill_days = elementary.get_test_argument('backfill_days', backfill_days, model_graph_node) %}

    {% set metric_names = [] %}
    {% for metric in table_metrics %}
        {% do metric_names.append(metric.name) %}
    {% endfor %}

    {% if metric_props.timestamp_column %}
        {% set min_bucket_start, max_bucket_end = elementary.get_metric_buckets_min_and_max(
            model_relation=model_relation,
            backfill_days=backfill_days,
            days_back=days_back,
            metric_names=metric_names,
            metric_properties=metric_props
        ) %}
    {% endif %}


    {% if dimensions %}
        {% if table_metrics | length != 1 %}
            {% do exceptions.raise_compiler_error("collect_metrics test with 'dimensions' expects a single 'row_count' metric.") %}
        {% endif %}
        {% set dim_metric = table_metrics[0] %}
        {% if dim_metric.type != "row_count" %}
            {% do exceptions.raise_compiler_error("collect_metrics test does not support non-'row_count' dimensional table metrics.") %}
        {% endif %}

        {% set monitoring_query = elementary.dimension_monitoring_query(
            model_expr,
            model_relation,
            dimensions,
            min_bucket_start,
            max_bucket_end,
            metric_properties=metric_props,
            metric_name=dim_metric.name
        ) %}
    {% else %}
        {% set monitoring_query = elementary.table_monitoring_query(
            model_expr,
            model_relation,
            min_bucket_start,
            max_bucket_end,
            table_metrics,
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
