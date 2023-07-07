select
        {{ elementary.edr_cast_as_timestanp('updated_at') }},
        occurred_at,
        min,
        max,
        zero_count,
        zero_percent,
        average,
        standard_deviation,
        variance,
        sum

from {{ ref("numeric_column_anomalies") }}
