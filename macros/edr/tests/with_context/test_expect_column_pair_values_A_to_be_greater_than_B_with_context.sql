{% test expect_column_pair_values_A_to_be_greater_than_B_with_context(
    model, column_A, column_B, or_equal=false, context_columns=none
) %}
    {%- set select_clause = elementary.get_context_select_clause(
        model=model,
        tested_columns=[column_A, column_B],
        context_columns=context_columns,
        test_name="expect_column_pair_values_A_to_be_greater_than_B_with_context",
    ) %}

    select {{ select_clause }}
    from {{ model }}
    where not ({{ column_A }} {{ ">=" if or_equal else ">" }} {{ column_B }})
{% endtest %}
