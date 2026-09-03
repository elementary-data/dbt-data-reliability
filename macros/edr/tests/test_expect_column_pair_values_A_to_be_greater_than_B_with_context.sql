{% test expect_column_pair_values_A_to_be_greater_than_B_with_context(
    model,
    column_A,
    column_B,
    or_equal=false,
    row_condition=none,
    context_columns=none
) %}
    {%- set select_clause = elementary.get_context_select_clause(
        model,
        [column_A, column_B],
        context_columns,
        "expect_column_pair_values_A_to_be_greater_than_B_with_context",
    ) %}

    select {{ select_clause }}
    from {{ model }}
    where
        not ({{ column_A }} {{ ">=" if or_equal else ">" }} {{ column_B }})
        {%- if row_condition %} and ({{ row_condition }}) {%- endif %}
{% endtest %}
