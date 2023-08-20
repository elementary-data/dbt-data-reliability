{{
    config(
        materialized="table",
        tags=var("one_tags", []),
        meta={"owner": var("one_owner", "egk")},
    )
}}

select 1 as one
