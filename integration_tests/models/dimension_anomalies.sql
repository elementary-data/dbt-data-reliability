-- depends_on: {{ ref('dimension_anomalies_validation') }}

with training as (
    select * from {{ ref('dimension_anomalies_training') }}
),

{% if elementary.table_exists_in_target('dimension_anomalies_validation') %}
 validation as (
     select * from {{ ref('dimension_anomalies_validation') }}
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
         platform,
         version,
         user_id
     from source
 )

select * from final
