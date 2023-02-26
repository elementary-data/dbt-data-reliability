{{
  config(
    materialized = 'view',
    bind=False
  )
}}

with metrics_anomaly_score as (

    select * from {{ ref('metrics_anomaly_score') }}

),

score_sensitivity as (

    select
        full_table_name,
        column_name,
        metric_name,
        latest_metric_value,
        training_avg as metric_avg,
        training_stddev as metric_stddev,
        anomaly_score,
        case when abs(anomaly_score) >= 1.5 then true else false end as {{ elementary.edr_quote_column('is_anomaly_1_5') }},
        case when abs(anomaly_score) >= 2 then true else false end as {{ elementary.edr_quote_column('is_anomaly_2') }},
        case when abs(anomaly_score) >= 2.5 then true else false end as {{ elementary.edr_quote_column('is_anomaly_2_5') }},
        case when abs(anomaly_score) >= 3 then true else false end as {{ elementary.edr_quote_column('is_anomaly_3') }},
        case when abs(anomaly_score) >= 3.5 then true else false end as {{ elementary.edr_quote_column('is_anomaly_3_5') }},
        case when abs(anomaly_score) >= 4 then true else false end as {{ elementary.edr_quote_column('is_anomaly_4') }},
        case when abs(anomaly_score) >= 4.5 then true else false end as {{ elementary.edr_quote_column('is_anomaly_4_5') }}
    from metrics_anomaly_score
    where abs(anomaly_score) >= 1.5

)

select * from score_sensitivity
