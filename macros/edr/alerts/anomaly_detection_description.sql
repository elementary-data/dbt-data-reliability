{% macro anomaly_detection_description() %}
    {# T-SQL does not support || for string concatenation; use + instead. #}
    {% set c = " + " if target.type in ["fabric", "sqlserver"] else " || " %}
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
    {% set c = " + " if target.type in ["fabric", "sqlserver"] else " || " %}
    'Last update was at '
    {{ c }} anomalous_value
    {{ c }} ', '
    {{ c }} 'which is'
    {{ c }}
    {{
        elementary.edr_cast_as_string(
            "abs(round("
            ~ elementary.edr_cast_as_numeric("metric_value/3600")
            ~ ", 2))"
        )
    }}
    {{ c }} ' hours without updates (only full buckets are considered). Usually the table is updated within '
    {{ c }}
    {{
        elementary.edr_cast_as_string(
            "abs(round("
            ~ elementary.edr_cast_as_numeric("training_avg/3600")
            ~ ", 2))"
        )
    }}
    {{ c }} ' hours.'
{% endmacro %}

{% macro table_metric_description() %}
    {% set c = " + " if target.type in ["fabric", "sqlserver"] else " || " %}
    'The last '
    {{ c }} metric_name
    {{ c }} ' value is '
    {{ c }}
    {{
        elementary.edr_cast_as_string(
            "round(" ~ elementary.edr_cast_as_numeric("metric_value") ~ ", 3)"
        )
    }}
    {{ c }} '. The average for this metric is '
    {{ c }}
    {{
        elementary.edr_cast_as_string(
            "round(" ~ elementary.edr_cast_as_numeric("training_avg") ~ ", 3)"
        )
    }}
    {{ c }} '.'
{% endmacro %}

{% macro column_metric_description() %}
    {% set c = " + " if target.type in ["fabric", "sqlserver"] else " || " %}
    'In column '
    {{ c }} column_name
    {{ c }} ', the last '
    {{ c }} metric_name
    {{ c }} ' value is '
    {{ c }}
    {{
        elementary.edr_cast_as_string(
            "round(" ~ elementary.edr_cast_as_numeric("metric_value") ~ ", 3)"
        )
    }}
    {{ c }} '. The average for this metric is '
    {{ c }}
    {{
        elementary.edr_cast_as_string(
            "round(" ~ elementary.edr_cast_as_numeric("training_avg") ~ ", 3)"
        )
    }}
    {{ c }} '.'
{% endmacro %}

{% macro column_dimension_metric_description() %}
    {% set c = " + " if target.type in ["fabric", "sqlserver"] else " || " %}
    'In column '
    {{ c }} column_name
    {{ c }} ', the last '
    {{ c }} metric_name
    {{ c }} ' value for dimension '
    {{ c }} dimension
    {{ c }} ' is '
    {{ c }}
    {{
        elementary.edr_cast_as_string(
            "round(" ~ elementary.edr_cast_as_numeric("metric_value") ~ ", 3)"
        )
    }}
    {{ c }} '. The average for this metric is '
    {{ c }}
    {{
        elementary.edr_cast_as_string(
            "round(" ~ elementary.edr_cast_as_numeric("training_avg") ~ ", 3)"
        )
    }}
    {{ c }} '.'
{% endmacro %}

{% macro dimension_metric_description() %}
    {% set c = " + " if target.type in ["fabric", "sqlserver"] else " || " %}
    'The last '
    {{ c }} metric_name
    {{ c }} ' value for dimension '
    {{ c }} dimension
    {{ c }} ' - '
    {{ c }} case when dimension_value is null then 'NULL' else dimension_value end
    {{ c }} ' is '
    {{ c }}
    {{
        elementary.edr_cast_as_string(
            "round(" ~ elementary.edr_cast_as_numeric("metric_value") ~ ", 3)"
        )
    }}
    {{ c }} '. The average for this metric is '
    {{ c }}
    {{
        elementary.edr_cast_as_string(
            "round(" ~ elementary.edr_cast_as_numeric("training_avg") ~ ", 3)"
        )
    }}
    {{ c }} '.'
{% endmacro %}
