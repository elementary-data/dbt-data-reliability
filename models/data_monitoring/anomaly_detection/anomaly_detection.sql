with z_score as (

    select * from {{ ref('z_score') }}

),

anomaly_detection as (

    select
        *,
        {{ anomaly_detection_description() }}
    from z_score
    where abs(z_score) > {{ var('z_score_treshold') }}

)

select * from anomaly_detection