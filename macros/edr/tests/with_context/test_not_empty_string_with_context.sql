{% test not_empty_string_with_context(
    model, column_name, trim_whitespace=true, context_columns=none
) %}
    {%- set select_clause = elementary.get_context_select_clause(
        model=model,
        tested_columns=[column_name],
        context_columns=context_columns,
        test_name="not_empty_string_with_context",
    ) %}
    {%- set tested_expression = (
        "trim(" ~ column_name ~ ")" if trim_whitespace else column_name
    ) %}

    select {{ select_clause }}
    from {{ model }}
    where {{ tested_expression }} = ''
{% endtest %}
