with stats as (

    select * from {{ ref('metrics_for_anomalies') }}

),

anomaly_detection as (

     select
         *,
         {{ elementary.anomaly_detection_description() }}
     from stats
     where abs(z_score) > {{ var('anomaly_score_threshold') }}

)

select * from anomaly_detection