{{
    config(
        materialized='ephemeral'
    )
}}

with flat_jsons_ps as (
    select full_table_name,
           f.value as columns_jsons
    from   {{ ref('current_and_previous_schemas') }},
           table(flatten( {{ ref('current_and_previous_schemas') }}.previous_schema)) f
    where previous_schema is not null
),

previous_schemas_col as (
    select full_table_name,
        {{trim_quotes('columns_jsons:name')}} as column_name,
        {{trim_quotes('columns_jsons:data_type')}} as data_type,
        {{trim_quotes('columns_jsons:is_nullable')}} as is_nullable
    from flat_jsons_ps
)

select * from previous_schemas_col
