-- TODO: add dbs filter

select
    table_catalog_id as database_id,
    table_catalog as database_name,
    table_schema as schema_name,
    view_definition,
    table_name as view_name,
    last_altered,
    table_owner as view_owner,
    deleted as deleted_at
from {{ source('snowflake_account_usage','views') }}

-- Active views only
where deleted_at is null