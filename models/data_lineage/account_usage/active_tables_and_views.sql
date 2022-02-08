with active_views as (

select table_catalog as database_name,
       table_schema as schema_name,
       table_name
      from {{ source('snowflake_account_usage','views') }}
      where deleted is null

),

active_tables as (

    select
        table_catalog as database_name,
        table_schema as schema_name,
        table_name
    from {{ source('snowflake_account_usage','tables') }}
    where deleted is null

),

all_active as (

    select * from active_views
        union all
    select * from active_tables

)

select
    database_name,
    schema_name,
    table_name,
    {{ full_table_name() }} as full_table_name
from all_active