{{
  config(
    materialized = 'view',
    enabled = target.type != 'databricks' and target.type != 'spark' | as_bool()
  )
}}


with filtered_information_schema_tables as (

    {{ elementary.get_tables_from_information_schema() }}

)

select *
from filtered_information_schema_tables
where schema_name is not null
