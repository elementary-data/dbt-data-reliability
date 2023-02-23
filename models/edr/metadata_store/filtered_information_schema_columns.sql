{{
  config(
    materialized = 'view',
    enabled = target.type != 'databricks' and target.type != 'spark' | as_bool()
  )
}}

with filtered_information_schema_columns as (

    {{ elementary.get_columns_from_information_schema() }}

)

select *
from filtered_information_schema_columns
where full_table_name is not null