-- depends_on: {{ ref('numeric_column_anomalies_validation') }}

with training as (
    select * from {{ ref('numeric_column_anomalies_training') }}
),

{% if elementary.table_exists_in_target('numeric_column_anomalies_validation') %}
 validation as (
     select * from {{ ref('numeric_column_anomalies_validation') }}
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
         min,
         max,
         zero_count,
         zero_percent,
         average,
         standard_deviation,
         variance
     from source
 )

select * from final
