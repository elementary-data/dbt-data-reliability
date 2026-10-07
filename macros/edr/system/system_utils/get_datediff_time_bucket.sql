{#- The time bucket to count with edr_datediff. Weeks are counted as 7 days: where
    a week starts in datediff depends on the adapter (Sunday on Postgres and Spark),
    and may differ from date_trunc and from the first bucket start. -#}
{% macro get_datediff_time_bucket(time_bucket) %}
    {% if time_bucket.period | lower == "week" %}
        {% do return({"period": "day", "count": time_bucket.count * 7}) %}
    {% endif %}
    {% do return(time_bucket) %}
{% endmacro %}
