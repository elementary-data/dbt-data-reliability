{% macro collect_column_metrics(
    model_expr,
    model_relation,
    column_name,
    timestamp_column,
    time_bucket,
    days_back,
    backfill_days,
    where_expression
) %}
    {% set model_graph_node = elementary.get_model_graph_node(model_relation) %}
    {% set metric_props = elementary.get_metric_properties(model_graph_node, timestamp_column, where_expression, time_bucket) %}
    {% set days_back = elementary.get_test_argument('days_back', days_back, model_graph_node) %}
    {% set backfill_days = elementary.get_test_argument('backfill_days', backfill_days, model_graph_node) %}

    {% set column_obj_and_monitors = elementary.get_column_obj_and_monitors(model_relation, column_name) %}
    {% if not column_obj_and_monitors %}
        {% do exceptions.raise_compiler_error("Unable to find column `{}` in `{}`".format(column_name, model_relation)) %}
    {% endif %}
    {% set column_monitors = column_obj_and_monitors.monitors %}
    {% set column_obj = column_obj_and_monitors.column %}

    {% if metric_props.timestamp_column %}
        {% set min_bucket_start, max_bucket_end = elementary.get_metric_buckets_min_and_max(
            model_relation=model_relation,
            backfill_days=backfill_days,
            days_back=days_back,
            monitors=column_monitors,
            column_name=column_name,
            metric_properties=metric_props
        ) %}
    {% endif %}

    {% set column_monitoring_query = elementary.column_monitoring_query(
        model_expr,
        model_relation,
        min_bucket_start,
        max_bucket_end,
        days_back,
        column_obj,
        column_monitors,
        metric_props,
        dimensions
    ) %}
    {% do elementary.debug_log('column_monitoring_query - \n' ~ column_monitoring_query) %}


    {% set test_table_name = elementary.get_elementary_test_table_name() %}
    {% set database_name, schema_name = elementary.get_package_database_and_schema() %}
    {% set tests_schema_name = elementary.get_elementary_tests_schema(database_name, schema_name) %}

    {% do elementary.create_elementary_test_table(database_name, tests_schema_name, test_table_name, 'metrics', column_monitoring_query) %}
    {% do elementary.store_metrics_table_in_cache() %}
{% endmacro %}
