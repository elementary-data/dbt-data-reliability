{% test expect_column_values_to_not_be_null_with_context(
    model, column_name, context_columns=none
) %}
    {%- set select_clause = elementary.get_context_select_clause(
        model=model,
        tested_columns=[column_name],
        context_columns=context_columns,
        test_name="expect_column_values_to_not_be_null_with_context",
    ) %}

    select {{ select_clause }}
    from {{ model }}
    where {{ column_name }} is null
{% endtest %}
