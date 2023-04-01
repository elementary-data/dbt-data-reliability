with dbt as (
    select * from {{ ref('alerts_dbt_tests') }}
),
anomalies as (
    select * from {{ ref('alerts_anomaly_detection') }}
),
schema_changes as (
    select * from {{ ref('alerts_schema_changes') }}
)
select * from dbt
union all
select * from anomalies
union all
select * from schema_changes