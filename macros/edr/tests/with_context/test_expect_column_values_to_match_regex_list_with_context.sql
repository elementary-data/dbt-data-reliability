{% test expect_column_values_to_match_regex_list_with_context(
    model,
    column_name,
    regex_list,
    match_on="any",
    is_raw=false,
    flags="",
    context_columns=none
) %}
    {%- if not regex_list and execute %}
        {{
            exceptions.raise_compiler_error(
                "expect_column_values_to_match_regex_list_with_context: `regex_list` must be a non-empty list of patterns."
            )
        }}
    {%- endif %}

    {#- A bare string is iterable, so without this each character becomes its
        own pattern and the test silently passes. -#}
    {%- set regex_list = [regex_list] if regex_list is string else regex_list %}

    {%- set select_clause = elementary.get_context_select_clause(
        model=model,
        tested_columns=[column_name],
        context_columns=context_columns,
        test_name="expect_column_values_to_match_regex_list_with_context",
    ) %}

    {%- if match_on | lower not in ["any", "all"] and execute %}
        {{
            exceptions.raise_compiler_error(
                "expect_column_values_to_match_regex_list_with_context: `match_on` must be 'any' or 'all', got '"
                ~ match_on
                ~ "'."
            )
        }}
    {%- endif %}

    {#- match_on="all" requires every pattern to match, "any" requires one. -#}
    {%- set combinator = " and " if match_on | lower == "all" else " or " %}

    {#- Once, not once per pattern, so an unsupported flag warns once. -#}
    {%- set flags = elementary.regexp_sanitize_flags(flags) %}
    {%- set match_conditions = [] %}
    {%- for regex in regex_list %}
        {%- do match_conditions.append(
            "("
            ~ elementary.regexp_match(column_name, regex, is_raw, flags)
            ~ ")"
        ) %}
    {%- endfor %}

    select {{ select_clause }}
    from {{ model }}
    where not ({{ match_conditions | join(combinator) }})
{% endtest %}
