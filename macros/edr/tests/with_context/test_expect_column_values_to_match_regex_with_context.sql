{% test expect_column_values_to_match_regex_with_context(
    model, column_name, regex, is_raw=false, flags="", context_columns=none
) %}
    {%- set select_clause = elementary.get_context_select_clause(
        model=model,
        tested_columns=[column_name],
        context_columns=context_columns,
        test_name="expect_column_values_to_match_regex_with_context",
    ) %}

    select {{ select_clause }}
    from {{ model }}
    where not ({{ elementary.regexp_match(column_name, regex, is_raw, flags) }})
{% endtest %}
