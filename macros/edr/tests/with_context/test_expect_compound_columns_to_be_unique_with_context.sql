{% test expect_compound_columns_to_be_unique_with_context(
    model, column_list, context_columns=none
) %}
    {%- if not column_list and execute %}
        {{
            exceptions.raise_compiler_error(
                "expect_compound_columns_to_be_unique_with_context: `column_list` must be a non-empty list of columns."
            )
        }}
    {%- endif %}

    {%- set columns = [column_list] if column_list is string else column_list %}

    {#- default_clause=none: `*` would leak elementary_n_records into the sample. -#}
    {%- set select_clause = elementary.get_context_select_clause(
        model=model,
        tested_columns=columns,
        context_columns=context_columns,
        test_name="expect_compound_columns_to_be_unique_with_context",
        default_clause=none,
    ) %}

    select {{ select_clause }}
    from
        (
            select
                *,
                count(*) over (
                    partition by {{ columns | join(", ") }}
                ) as elementary_n_records
            from {{ model }}
            {#- NULLs partition together, so an all-NULL key would report as a
                duplicate. Matches dbt_expectations' `all_values_are_missing`. #}
            where not ({{ columns | join(" is null and ") }} is null)
        ) validation
    where elementary_n_records > 1
{% endtest %}
