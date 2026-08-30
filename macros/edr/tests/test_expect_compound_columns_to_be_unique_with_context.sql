{% test expect_compound_columns_to_be_unique_with_context(
    model, column_list, row_condition=none, context_columns=none
) %}
    {%- set columns = [column_list] if column_list is string else column_list %}
    {%- if not columns %}
        {{
            exceptions.raise_compiler_error(
                "expect_compound_columns_to_be_unique_with_context: `column_list` must be a non-empty list of columns."
            )
        }}
    {%- endif %}

    {#- `n_records` below is a helper column, so the default select list has to
        name every real column instead of using `*`. -#}
    {%- set select_clause = elementary.get_context_select_clause(
        model,
        columns,
        context_columns,
        "expect_compound_columns_to_be_unique_with_context",
        default_clause=none,
    ) %}

    select {{ select_clause }}
    from
        (
            select
                *, count(*) over (partition by {{ columns | join(", ") }}) as n_records
            from {{ model }}
            {%- if row_condition %} where ({{ row_condition }}) {%- endif %}
        ) validation
    where n_records > 1
{% endtest %}
