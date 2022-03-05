with metrics_for_anomalies as (

    select * from {{ ref('metrics_for_anomalies') }}

),

score_sensitivity as (

    select
        full_table_name,
        column_name,
        metric_name,
        latest_value,
        metric_avg,
        z_score,
        case when abs(z_score) >= 1.5 then true else false end as "is_anomaly_1.5",
        case when abs(z_score) >= 2 then true else false end as "is_anomaly_2",
        case when abs(z_score) >= 2.5 then true else false end as "is_anomaly_2.5",
        case when abs(z_score) >= 3 then true else false end as "is_anomaly_3",
        case when abs(z_score) >= 3.5 then true else false end as "is_anomaly_3.5",
        case when abs(z_score) >= 4 then true else false end as "is_anomaly_4",
        case when abs(z_score) >= 4.5 then true else false end as "is_anomaly_4.5"
    from metrics_for_anomalies
    where abs(z_score) >= 1.5

)

select * from score_sensitivity
