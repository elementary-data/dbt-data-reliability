{% macro get_anomaly_scores_query(test_metrics_table_relation, model_relation, test_configuration, metric_names, column_name = none, columns_only = false, metric_properties = none, data_monitoring_metrics_table=none) %}
    {%- set model_graph_node = elementary.get_model_graph_node(model_relation) %}
    {%- set full_table_name = elementary.model_node_to_full_name(model_graph_node) %}
    {%- set test_execution_id = elementary.get_test_execution_id() %}
    {%- set test_unique_id = elementary.get_test_unique_id() %}
    {%- if not data_monitoring_metrics_table %}
        {#  data_monitoring_metrics_table is none except for integration-tests that test the get_anomaly_scores_query macro,
          and in which case it holds mock history metrics #}
        {%- set data_monitoring_metrics_table = elementary.get_elementary_relation('data_monitoring_metrics') %}
    {%- endif %}

    {%- if elementary.is_incremental_model(model_graph_node) %}
      {%- set latest_full_refresh = elementary.get_latest_full_refresh(model_graph_node) %}
    {%- else %}
      {%- set latest_full_refresh = none %}
    {%- endif %}

    {%- if test_configuration.seasonality == 'day_of_week' %}
        {%- set bucket_seasonality_expr = elementary.edr_day_of_week_expression('bucket_end') %}

    {%- elif test_configuration.seasonality == 'hour_of_day' %}
        {%- set bucket_seasonality_expr = elementary.edr_hour_of_day_expression('bucket_end') %}

    {%- elif test_configuration.seasonality == 'hour_of_week' %}
        {%- set bucket_seasonality_expr = elementary.edr_hour_of_week_expression('bucket_end') %}

    {%- else %}
        {%- set bucket_seasonality_expr = elementary.const_as_text('no_seasonality') %}
    {%- endif %}
    {%- set detection_end = elementary.get_detection_end(test_configuration.detection_delay) %}
    {%- set detection_end_expr = elementary.edr_cast_as_timestamp(elementary.edr_datetime_to_sql(detection_end)) %}
    {%- set min_bucket_start_expr = elementary.get_trunc_min_bucket_start_expr(detection_end, metric_properties, test_configuration.days_back) %}

    {# For timestamped tests, this will be the bucket start, and for non-timestamped tests it will be the
       bucket end (which is the actual time of the test) #}
    {%- set metric_time_bucket_expr = 'case when bucket_start is not null then bucket_start else bucket_end end' %}

    {%- set anomaly_scores_query %}
        {% if test_configuration.timestamp_column %}
            with buckets as (
                select edr_bucket_start, edr_bucket_end
                from ({{ elementary.complete_buckets_cte(metric_properties, min_bucket_start_expr,
                                                         detection_end_expr) }}) results
                where edr_bucket_start >= {{ min_bucket_start_expr }}
                  and edr_bucket_end <= {{ detection_end_expr }}
            ),
        {% else %}
            with
        {% endif %}
        data_monitoring_metrics as (

            select
                id,
                full_table_name,
                column_name,
                metric_name,
                metric_type,
                metric_value,
                source_value,
                bucket_start,
                bucket_end,
                bucket_duration_hours,
                updated_at,
                dimension,
                dimension_value,
                metric_properties
            from {{ data_monitoring_metrics_table }}
            -- We use bucket_end because non-timestamp tests have only bucket_end field.
            where
                bucket_end > {{ min_bucket_start_expr }}
                {% if test_configuration.timestamp_column %}
                    -- For timestamped tests, verify that the bucket we got from the history is actually
                    -- a valid one (this check is important for buckets that are not aligned with a day).
                    and {{ elementary.edr_multi_value_in(
                        [elementary.edr_cast_as_timestamp('bucket_start'), elementary.edr_cast_as_timestamp('bucket_end')],
                        ['edr_bucket_start', 'edr_bucket_end'],
                        'buckets'
                    ) }}
                {% endif %}
                and metric_properties = {{ elementary.dict_to_quoted_json(metric_properties) }}
                {% if latest_full_refresh %}
                    and updated_at > {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(latest_full_refresh)) }}
                {% endif %}
                and upper(full_table_name) = upper('{{ full_table_name }}')
                and metric_name in {{ elementary.strings_list_to_tuple(metric_names) }}
                {%- if column_name %}
                    and upper(column_name) = upper('{{ column_name }}')
                {%- endif %}
                {%- if columns_only %}
                    and column_name is not null
                {%- endif %}
                {% if test_configuration.dimensions %}
                    and dimension = {{ elementary.edr_quote(elementary.join_list(test_configuration.dimensions, '; ')) }}
                {% endif %}
        ),

        union_metrics as (

            select * from data_monitoring_metrics
            union all
            select * from {{ test_metrics_table_relation }}

        ),

        grouped_metrics_duplicates as (

            select
                id,
                full_table_name,
                column_name,
                metric_name,
                metric_value,
                source_value,
                bucket_start,
                bucket_end,
                bucket_duration_hours,
                updated_at,
                dimension,
                dimension_value,

                -- Fields added for the anomaly_exclude_metrics expression used below
                {{ metric_time_bucket_expr }} as metric_time_bucket,
                {{ elementary.edr_cast_as_date(elementary.edr_date_trunc('day', metric_time_bucket_expr))}} as metric_date,

                row_number() over (partition by id order by updated_at desc) as row_num
            from union_metrics

        ),

        grouped_metrics as (

            select
                id as metric_id,
                full_table_name,
                column_name,
                dimension,
                dimension_value,
                metric_name,
                metric_value,
                source_value,
                bucket_start,
                bucket_end,
                {{ bucket_seasonality_expr }} as bucket_seasonality,
                {{ test_configuration.anomaly_exclude_metrics or 'FALSE' }} as is_excluded,
                bucket_duration_hours,
                updated_at
            from grouped_metrics_duplicates
            where row_num = 1
        ),

        time_window_aggregation as (

            select
                metric_id,
                full_table_name,
                column_name,
                dimension,
                dimension_value,
                metric_name,
                metric_value,
                source_value,
                bucket_start,
                bucket_end,
                bucket_seasonality,
                bucket_duration_hours,
                updated_at,
                avg(metric_value) over (partition by metric_name, full_table_name, column_name, dimension, dimension_value, bucket_seasonality order by bucket_end asc rows between unbounded preceding and current row) as training_avg,
                {{ elementary.standard_deviation('metric_value') }} over (partition by metric_name, full_table_name, column_name, dimension, dimension_value, bucket_seasonality order by bucket_end asc rows between unbounded preceding and current row) as training_stddev,
                count(metric_value) over (partition by metric_name, full_table_name, column_name, dimension, dimension_value, bucket_seasonality order by bucket_end asc rows between unbounded preceding and current row) as training_set_size,
                last_value(bucket_end) over (partition by metric_name, full_table_name, column_name, dimension, dimension_value, bucket_seasonality order by bucket_end asc rows between unbounded preceding and current row) training_end,
                first_value(bucket_end) over (partition by metric_name, full_table_name, column_name, dimension, dimension_value, bucket_seasonality order by bucket_end asc rows between unbounded preceding and current row) as training_start
            from grouped_metrics
            where not is_excluded
            {{ dbt_utils.group_by(13) }}
        ),

        anomaly_scores as (

            select
                {{ elementary.generate_surrogate_key([
                 'metric_id',
                 elementary.const_as_string(test_execution_id)
                ]) }} as id,
                metric_id,
                {{ elementary.const_as_string(test_execution_id) }} as test_execution_id,
                {{ elementary.const_as_string(test_unique_id) }} as test_unique_id,
                {{ elementary.current_timestamp_column() }} as detected_at,
                full_table_name,
                column_name,
                metric_name,
                case
                    when training_stddev is null then null
                    when training_set_size = 1 then null  -- Single value case - no historical context for anomaly detection
                    when training_stddev = 0 then 0  -- Stationary data case - valid, all values are identical
                    else (metric_value - training_avg) / (training_stddev)
                end as anomaly_score,
                {{ test_configuration.anomaly_sensitivity }} as anomaly_score_threshold,
                source_value as anomalous_value,
                {{ elementary.edr_cast_as_timestamp('bucket_start') }} as bucket_start,
                {{ elementary.edr_cast_as_timestamp('bucket_end') }} as bucket_end,
                bucket_seasonality,
                metric_value,
                
                {% set limit_values =  elementary.get_limit_metric_values(test_configuration) %}
                case
                    when training_stddev is null or training_set_size = 1 then null
                    when {{ limit_values.min_metric_value }} > 0 or metric_name in {{ elementary.to_sql_list(elementary.get_negative_value_supported_metrics()) }} then {{ limit_values.min_metric_value }}
                    else 0
                end as min_metric_value,
                case 
                    when training_stddev is null or training_set_size = 1 then null
                    else {{ limit_values.max_metric_value }}
                end as max_metric_value,
                training_avg,
                training_stddev,
                training_set_size,
                {{ elementary.edr_cast_as_timestamp('training_start') }} as training_start,
                {{ elementary.edr_cast_as_timestamp('training_end') }} as training_end,
                dimension,
                dimension_value
            from time_window_aggregation
            where
                metric_value is not null
                and training_avg is not null
        )

        select * from anomaly_scores
    {% endset %}
    {{ return(anomaly_scores_query) }}
{% endmacro %}

{% macro get_negative_value_supported_metrics() %}
    {% do return(["min", "max", "average", "standard_deviation", "variance", "sum"]) %}
{% endmacro %}

{% macro get_limit_metric_values(test_configuration) %}
    {%- set min_val -%}
      ((-1) * {{ test_configuration.anomaly_sensitivity }} * training_stddev + training_avg)
    {%- endset -%}

    {% if test_configuration.ignore_small_changes.drop_failure_percent_threshold %}
      {%- set drop_avg_threshold -%}
        ((1 - {{ test_configuration.ignore_small_changes.drop_failure_percent_threshold }}/100.0) * training_avg)
      {%- endset -%}
      {%- set min_val -%}
        {{ elementary.arithmetic_min(drop_avg_threshold, min_val) }}
      {%- endset -%}
    {% endif %}

    {%- set max_val -%}
      ({{ test_configuration.anomaly_sensitivity }} * training_stddev + training_avg)
    {%- endset -%}

    {% if test_configuration.ignore_small_changes.spike_failure_percent_threshold %}
      {%- set spike_avg_threshold -%}
        ((1 + {{ test_configuration.ignore_small_changes.spike_failure_percent_threshold }}/100.0) * training_avg)
      {%- endset -%}
      {%- set max_val -%}
        {{ elementary.arithmetic_max(spike_avg_threshold, max_val) }}
      {%- endset -%}
    {% endif %}

    {{ return({"min_metric_value": min_val, "max_metric_value": max_val}) }}
{% endmacro %}
