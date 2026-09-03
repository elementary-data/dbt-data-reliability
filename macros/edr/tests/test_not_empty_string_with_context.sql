{% test not_empty_string_with_context(
    model, column_name, trim_whitespace=true, context_columns=none
) %}
    {%- set select_clause = elementary.get_context_select_clause(
        model,
        [column_name],
        context_columns,
        "not_empty_string_with_context",
    ) %}
    {%- set tested_expression = (
        "trim(" ~ column_name ~ ")" if trim_whitespace else column_name
    ) %}

    select {{ select_clause }}
    from {{ model }}
    where {{ tested_expression }} = ''
{% endtest %}
