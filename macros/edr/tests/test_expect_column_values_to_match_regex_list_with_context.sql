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

    {#- Accept a single pattern given as a bare string. A string is iterable, so
        without this it would be looped over one character at a time and each
        character used as its own pattern, which silently passes the test. The
        emptiness guard above runs first, so "" still raises rather than
        becoming [""], which would match every value. -#}
    {%- set regex_list = [regex_list] if regex_list is string else regex_list %}

    {%- set select_clause = elementary.get_context_select_clause(
        model,
        [column_name],
        context_columns,
        "expect_column_values_to_match_regex_list_with_context",
    ) %}

    {#- Validate rather than fall back: treating an unrecognised value as "any"
        would silently invert what the test asserts. -#}
    {%- if match_on | lower not in ["any", "all"] %}
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
        {%- if row_condition %} and ({{ row_condition }}) {%- endif %}
{% endtest %}
