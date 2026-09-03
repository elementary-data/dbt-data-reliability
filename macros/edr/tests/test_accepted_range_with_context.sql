{#
    Deprecated, and scheduled for removal in the next release.

    `dbt_utils.accepted_range` selects `*` unconditionally, so the sample
    Elementary stores for it already contains every column. This variant
    therefore cannot add context to that sample; the only thing it can do is
    narrow it to a chosen subset, which is not what the other `_with_context`
    tests are for.

    Migration: use `dbt_utils.accepted_range`. If you were passing
    `context_columns` to limit which columns reach the stored sample, there is no
    direct replacement.
#}
{% test accepted_range_with_context(
    model,
    column_name,
    min_value=none,
    max_value=none,
    inclusive=true,
    context_columns=none
) %}
    {#- edr_log_warning rather than exceptions.warn(), so that upgrading cannot
        fail a run using --warn-error, and rather than a bare log(), because it
        gates on `execute`. dbt renders generic test bodies while parsing, so an
        ungated log prints once per node on every command that parses, and twice
        per node on dbt test. -#}
    {%- do elementary.edr_log_warning(
        "accepted_range_with_context is deprecated and will be removed in the next release. Use dbt_utils.accepted_range instead."
    ) %}

    {%- if min_value is none and max_value is none %}
        {{
            exceptions.raise_compiler_error(
                "accepted_range_with_context: at least one of min_value or max_value must be provided."
            )
        }}
    {%- endif %}

    {%- set select_clause = elementary.get_context_select_clause(
        model, [column_name], context_columns, "accepted_range_with_context"
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
