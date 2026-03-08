{% macro count_true(column_name) -%}
    coalesce(
        sum(
            case
                when
                    {{
                        elementary.edr_is_true(
                            "cast("
                            ~ column_name
                            ~ " as "
                            ~ elementary.edr_type_bool()
                            ~ ")"
                        )
                    }}
                then 1
                else 0
            end
        ),
        0
    )
{%- endmacro %}

{% macro count_false(column_name) -%}
    coalesce(
        sum(
            case
                when
                    {{
                        elementary.edr_is_true(
                            "cast("
                            ~ column_name
                            ~ " as "
                            ~ elementary.edr_type_bool()
                            ~ ")"
                        )
                    }}
                then 0
                else 1
            end
        ),
        0
    )
{%- endmacro %}
