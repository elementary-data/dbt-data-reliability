with training as (
    select * from {{ ref('users_per_day_weekly_seasonal_training') }}
),

{% if var("stage") == "validation" %}
 validation as (
     select * from {{ ref('users_per_day_weekly_seasonal_validation') }}
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
         updated_at,
         user_id
     from source
 )

select * from final
