with training as (
    select * from {{ ref('any_type_column_anomalies_training') }}
),
 validation as (
     select * from {{ ref('any_type_column_anomalies_validation') }}
 ),
 source as (
     select * from training
     union all
     select * from validation
 ),
 final as ( -- all I really want for this table is the updated_at column from any_type_column_anomalies
     select
         updated_at,
         'UNIQUE_STRING_USER_NAME' as user_name
     from source
 )

select * from final
