-- depends_on: {{ ref('string_column_anomalies_validation') }}

with training as (
    select * from {{ ref('string_column_anomalies_training') }}
),

{% if elementary.table_exists_in_target('string_column_anomalies_validation') %}
validation as (
    select * from {{ ref('string_column_anomalies_validation') }}
),

source as (
    select * from training
    union all
    select * from validation
),
{% else %}
source as (
    select * from training
),
{% endif %}

final as (
     select
        date as updated_at,
        min_length,
        max_length,
        average_length,
        missing_count,
        missing_percent
     from source
)

select * from final
