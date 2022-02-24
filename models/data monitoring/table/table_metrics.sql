-- convert to loop per table

{% set table_to_monitor = var('table_to_monitor') %}
{% set timestamp_field = var('timestamp_field') %}
{% set days_back = var('days_back') %}

{{ metrics_calc(table_to_monitor, timestamp_field, days_back, 24) }}