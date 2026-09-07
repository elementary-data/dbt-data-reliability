-- Leading comment used by test_compiled_code_preserves_newlines
{{
    config(
        materialized="table",
        tags=var("one_tags", []),
        meta={"owner": var("one_owner", "egk")},
    )
}}

select 1 as {{ elementary.escape_reserved_keywords("one") }}
