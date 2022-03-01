-- depends_on: {{ ref('alerts_data_monitoring') }}
-- depends_on: {{ ref('alerts_schema_changes') }}
-- depends_on: {{ ref('alerts_dbt') }}


    select 1 as num