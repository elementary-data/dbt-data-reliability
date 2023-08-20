{{ config(materialized="non_dbt") }}
select
    1
    -- depends_on: {{ ref('one') }}
    
