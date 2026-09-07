{#
    Deprecated, removed in the next release: use `dbt_utils.accepted_range`.

    That test selects `*`, so the stored sample already has every column and
    this variant can only narrow it, never add context. Narrowing has no direct
    replacement.
#}
{% test accepted_range_with_context(
    model,
    column_name,
    min_value=none,
    max_value=none,
    inclusive=true,
    context_columns=none
) %}
    {#- Not exceptions.warn(): that would fail runs using --warn-error. -#}
    {%- do elementary.edr_log_warning(
        "accepted_range_with_context is deprecated and will be removed in the next release. Use dbt_utils.accepted_range instead."
    ) %}

    {%- if min_value is none and max_value is none and execute %}
        {{
            exceptions.raise_compiler_error(
                "accepted_range_with_context: at least one of min_value or max_value must be provided."
            )
        }}
    {%- endif %}

    {%- set select_clause = elementary.get_context_select_clause(
        model=model,
        tested_columns=[column_name],
        context_columns=context_columns,
        test_name="accepted_range_with_context",
    ) %}

    select {{ select_clause }}
    from {{ model }}
    where
        1 = 2
        {%- if min_value is not none %}
            or not {{ column_name }} >{{- "=" if inclusive }} {{ min_value }}
        {%- endif %}
        {%- if max_value is not none %}
            or not {{ column_name }} <{{- "=" if inclusive }} {{ max_value }}
        {%- endif %}
{% endtest %}
