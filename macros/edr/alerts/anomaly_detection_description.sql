{% macro anomaly_detection_description() %}
    case
        when dimension is not null and column_name is null
        then {{ elementary.dimension_metric_description() }}
        when dimension is not null and column_name is not null
        then {{ elementary.column_dimension_metric_description() }}
        when metric_name = 'freshness'
        then {{ elementary.freshness_description() }}
        when column_name is null
        then {{ elementary.table_metric_description() }}
        when column_name is not null
        then {{ elementary.column_metric_description() }}
        else null
    end as anomaly_description
{% endmacro %}

{% macro freshness_description() %}
    {%- set metric_hours -%}
        {{ elementary.edr_cast_as_string("abs(round(" ~ elementary.edr_cast_as_numeric("metric_value/3600") ~ ", 2))") }}
    {%- endset -%}
    {%- set training_hours -%}
        {{ elementary.edr_cast_as_string("abs(round(" ~ elementary.edr_cast_as_numeric("training_avg/3600") ~ ", 2))") }}
    {%- endset -%}
    {{
        dbt.concat(
            [
                "'Last update was at '",
                "anomalous_value",
                "', which is'",
                metric_hours | trim,
                "' hours without updates (only full buckets are considered). Usually the table is updated within '",
                training_hours | trim,
                "' hours.'",
            ]
        )
    }}
{% endmacro %}

{% macro table_metric_description() %}
    {%- set metric_val -%}
        {{ elementary.edr_cast_as_string("round(" ~ elementary.edr_cast_as_numeric("metric_value") ~ ", 3)") }}
    {%- endset -%}
    {%- set training_val -%}
        {{ elementary.edr_cast_as_string("round(" ~ elementary.edr_cast_as_numeric("training_avg") ~ ", 3)") }}
    {%- endset -%}
    {{
        dbt.concat(
            [
                "'The last '",
                "metric_name",
                "' value is '",
                metric_val | trim,
                "'. The average for this metric is '",
                training_val | trim,
                "'.'",
            ]
        )
    }}
{% endmacro %}

{% macro column_metric_description() %}
    {%- set metric_val -%}
        {{ elementary.edr_cast_as_string("round(" ~ elementary.edr_cast_as_numeric("metric_value") ~ ", 3)") }}
    {%- endset -%}
    {%- set training_val -%}
        {{ elementary.edr_cast_as_string("round(" ~ elementary.edr_cast_as_numeric("training_avg") ~ ", 3)") }}
    {%- endset -%}
    {{
        dbt.concat(
            [
                "'In column '",
                "column_name",
                "', the last '",
                "metric_name",
                "' value is '",
                metric_val | trim,
                "'. The average for this metric is '",
                training_val | trim,
                "'.'",
            ]
        )
    }}
{% endmacro %}

{% macro column_dimension_metric_description() %}
    {%- set metric_val -%}
        {{ elementary.edr_cast_as_string("round(" ~ elementary.edr_cast_as_numeric("metric_value") ~ ", 3)") }}
    {%- endset -%}
    {%- set training_val -%}
        {{ elementary.edr_cast_as_string("round(" ~ elementary.edr_cast_as_numeric("training_avg") ~ ", 3)") }}
    {%- endset -%}
    {{
        dbt.concat(
            [
                "'In column '",
                "column_name",
                "', the last '",
                "metric_name",
                "' value for dimension '",
                "dimension",
                "' is '",
                metric_val | trim,
                "'. The average for this metric is '",
                training_val | trim,
                "'.'",
            ]
        )
    }}
{% endmacro %}

{% macro dimension_metric_description() %}
    {%- set metric_val -%}
        {{ elementary.edr_cast_as_string("round(" ~ elementary.edr_cast_as_numeric("metric_value") ~ ", 3)") }}
    {%- endset -%}
    {%- set training_val -%}
        {{ elementary.edr_cast_as_string("round(" ~ elementary.edr_cast_as_numeric("training_avg") ~ ", 3)") }}
    {%- endset -%}
    {{
        dbt.concat(
            [
                "'The last '",
                "metric_name",
                "' value for dimension '",
                "dimension",
                "' - '",
                "case when dimension_value is null then 'NULL' else dimension_value end",
                "' is '",
                metric_val | trim,
                "'. The average for this metric is '",
                training_val | trim,
                "'.'",
            ]
        )
    }}
{% endmacro %}
