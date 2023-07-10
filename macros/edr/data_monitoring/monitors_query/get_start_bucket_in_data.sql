{% macro get_start_bucket_in_data(timestamp_column, min_bucket_start, time_bucket) %}
    {% set bucket_start_datediff_expr %}
      floor({{ elementary.edr_datediff(min_bucket_start, elementary.edr_cast_as_timestamp(timestamp_column), time_bucket.period) }} / {{ time_bucket.count }}) * {{ time_bucket.count }}
    {% endset %}
    {% do return(elementary.edr_cast_as_timestamp(elementary.edr_timeadd(time_bucket.period, elementary.edr_cast_as_int(bucket_start_datediff_expr), min_bucket_start))) %}
{% endmacro %}
