with training as (
    select * from {{ source('training', 'daily_wh_activity_training') }}
),
 source as (
     select * from training
 ),

 final as (
     select
         updated_at
     from source
 )

select * from final
