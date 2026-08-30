{% test expect_column_values_to_match_regex_list_with_context(
    model,
    column_name,
    regex_list,
    match_on="any",
    row_condition=none,
    is_raw=false,
    flags="",
    context_columns=none
) %}
    {%- if not regex_list %}
        {{
            exceptions.raise_compiler_error(
                "expect_column_values_to_match_regex_list_with_context: `regex_list` must be a non-empty list of patterns."
            )
        }}
    {%- endif %}

    {%- set select_clause = elementary.get_context_select_clause(
        model,
        [column_name],
        context_columns,
        "expect_column_values_to_match_regex_list_with_context",
    ) %}

    {#- match_on="all" requires every pattern to match, anything else requires one. -#}
    {%- set combinator = " and " if match_on == "all" else " or " %}

    {#- Sanitize once rather than once per pattern, so an unsupported flag warns
        a single time instead of once for every regex in the list. -#}
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
    where
        not ({{ match_conditions | join(combinator) }})
        {%- if row_condition %} and {{ row_condition }} {%- endif %}
{% endtest %}
