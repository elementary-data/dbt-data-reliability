{#
    Returns a boolean expression that is true when `regex` matches anywhere in
    `string`.

    Search semantics, not full-match: this mirrors Python's `re.search`, which
    is what `expect_column_values_to_match_regex` has always meant. Several
    engines get this wrong in opposite directions, so read the per-adapter
    notes before changing one. In particular Snowflake's `regexp_like` and
    Dremio's `regexp_like` both implicitly anchor the pattern at both ends and
    are NOT drop-in replacements for a search.

    Args:
        string: the column or expression to test.
        regex: the pattern, as a plain string.
        is_raw: emit the pattern as a raw string literal. Only Snowflake
            (`$$...$$`) and BigQuery (`r'...'`) have such a syntax; ignored
            elsewhere, where the literal is identical either way.
        flags: regex flags. `i` (case-insensitive) is honored on every adapter
            that supports flags at all. Anything the adapter does not accept is
            dropped with a warning, by `regexp_sanitize_flags` below.
#}
{% macro regexp_match(string, regex, is_raw=false, flags="") %}
    {%- set flags = elementary.regexp_sanitize_flags(flags) %}
    {{ adapter.dispatch("regexp_match", "elementary")(string, regex, is_raw, flags) }}
{% endmacro %}

{#
    Drops flags the current adapter cannot honor and returns the rest, warning
    about what was removed. The flags must actually be stripped, not just warned
    about: engines reject an unknown option outright rather than skipping it.

    Warn rather than raise, so a disabled test carrying a stray flag still
    compiles. Sanitizing is idempotent, so a caller that loops over many
    patterns can sanitize once up front and get a single warning instead of one
    per pattern.
#}
{% macro regexp_sanitize_flags(flags) %}
    {%- if not flags %} {%- do return("") %} {%- endif %}
    {%- set supported = elementary.regexp_supported_flags() %}
    {%- set kept = [] %}
    {%- set dropped = [] %}
    {%- for flag in flags %}
        {%- if flag in supported %} {%- do kept.append(flag) %}
        {%- else %} {%- do dropped.append(flag) %}
        {%- endif %}
    {%- endfor %}
    {%- if dropped %}
        {%- set dropped_list = dropped | join("', '") %}
        {%- do exceptions.warn(
            "regexp_match: flag(s) '"
            ~ dropped_list
            ~ "' are not supported on "
            ~ adapter.type()
            ~ " and have been ignored. Supported flags: '"
            ~ supported
            ~ "'."
        ) %}
    {%- endif %}
    {%- do return(kept | join("")) %}
{% endmacro %}

{#
    Prepends an inline flag group, which every RE2/PCRE engine understands.
    Used by the adapters that take no separate flags argument.
#}
{% macro regexp_inline_flags(regex, flags) %}
    {%- if flags %} {%- do return("(?" ~ flags ~ ")" ~ regex) %}
    {%- else %} {%- do return(regex) %}
    {%- endif %}
{% endmacro %}

{# Each adapter declares the flag alphabet its regex engine accepts, so the
   sanitizing above lives in one place instead of in every implementation. #}
{% macro regexp_supported_flags() %}
    {%- do return(adapter.dispatch("regexp_supported_flags", "elementary")()) %}
{% endmacro %}

{% macro default__regexp_supported_flags() %} {%- do return("") %} {% endmacro %}
{% macro snowflake__regexp_supported_flags() %} {%- do return("cimes") %} {% endmacro %}
{% macro bigquery__regexp_supported_flags() %} {%- do return("imsU") %} {% endmacro %}
{% macro postgres__regexp_supported_flags() %}
    {%- do return("bceimnpqstwx") %}
{% endmacro %}
{% macro redshift__regexp_supported_flags() %} {%- do return("ciep") %} {% endmacro %}
{% macro duckdb__regexp_supported_flags() %} {%- do return("cilmnpsg") %} {% endmacro %}
{% macro spark__regexp_supported_flags() %} {%- do return("idmsuxU") %} {% endmacro %}
{% macro databricks__regexp_supported_flags() %}
    {%- do return("idmsuxU") %}
{% endmacro %}
{% macro fabricspark__regexp_supported_flags() %}
    {%- do return("idmsuxU") %}
{% endmacro %}
{% macro trino__regexp_supported_flags() %} {%- do return("imsU") %} {% endmacro %}
{% macro athena__regexp_supported_flags() %} {%- do return("imsU") %} {% endmacro %}
{% macro clickhouse__regexp_supported_flags() %} {%- do return("imsU") %} {% endmacro %}
{% macro vertica__regexp_supported_flags() %} {%- do return("bcgimnx") %} {% endmacro %}
{% macro dremio__regexp_supported_flags() %} {%- do return("imsx") %} {% endmacro %}

{# Fallback for adapters we have no override for. `regexp_instr` is the most
   widely implemented position function, and > 0 makes it a search. #}
{% macro default__regexp_match(string, regex, is_raw, flags) %}
    regexp_instr({{ string }}, '{{ regex }}') > 0
{% endmacro %}

{# Snowflake: regexp_like is implicitly anchored at both ends, so it cannot be
   used here. regexp_instr(subject, pattern, position, occurrence, option,
   parameters) is a genuine search. Raw strings use $$...$$. #}
{% macro snowflake__regexp_match(string, regex, is_raw, flags) %}
    {%- set pattern = "$$" ~ regex ~ "$$" if is_raw else "'" ~ regex ~ "'" %}
    regexp_instr({{ string }}, {{ pattern }}, 1, 1, 0, '{{ flags }}') > 0
{% endmacro %}

{# BigQuery: regexp_contains is an unanchored search. RE2 takes inline flags. #}
{% macro bigquery__regexp_match(string, regex, is_raw, flags) %}
    {%- set regex = elementary.regexp_inline_flags(regex, flags) %}
    {%- set pattern = "r'" ~ regex ~ "'" if is_raw else "'" ~ regex ~ "'" %}
    regexp_contains({{ string }}, {{ pattern }})
{% endmacro %}

{# Postgres: ~ is a search. Flags go inline rather than via ~*, so the whole
   ARE embedded-option set keeps working and not just case-insensitivity. #}
{% macro postgres__regexp_match(string, regex, is_raw, flags) %}
    {%- set regex = elementary.regexp_inline_flags(regex, flags) %}
    {{ string }} ~ '{{ regex }}'
{% endmacro %}

{# Redshift: regexp_instr takes a parameters argument, unlike its ~ operator. #}
{% macro redshift__regexp_match(string, regex, is_raw, flags) %}
    regexp_instr({{ string }}, '{{ regex }}', 1, 1, 0, '{{ flags }}') > 0
{% endmacro %}

{# DuckDB: regexp_matches is already a boolean search and takes flags directly. #}
{% macro duckdb__regexp_match(string, regex, is_raw, flags) %}
    regexp_matches({{ string }}, '{{ regex }}', '{{ flags }}')
{% endmacro %}

{# Spark/Databricks: rlike is an unanchored search over a Java regex. #}
{% macro spark__regexp_match(string, regex, is_raw, flags) %}
    {%- set regex = elementary.regexp_inline_flags(regex, flags) %}
    {{ string }} rlike '{{ regex }}'
{% endmacro %}

{% macro databricks__regexp_match(string, regex, is_raw, flags) %}
    {{ elementary.spark__regexp_match(string, regex, is_raw, flags) }}
{% endmacro %}

{% macro fabricspark__regexp_match(string, regex, is_raw, flags) %}
    {{ elementary.spark__regexp_match(string, regex, is_raw, flags) }}
{% endmacro %}

{# Trino/Athena: unlike Snowflake, regexp_like here is documented as "contained
   within", so it is already a search. RE2 takes inline flags. #}
{% macro trino__regexp_match(string, regex, is_raw, flags) %}
    {%- set regex = elementary.regexp_inline_flags(regex, flags) %}
    regexp_like({{ string }}, '{{ regex }}')
{% endmacro %}

{% macro athena__regexp_match(string, regex, is_raw, flags) %}
    {{ elementary.trino__regexp_match(string, regex, is_raw, flags) }}
{% endmacro %}

{# ClickHouse: match() is an RE2 search returning UInt8. Compare explicitly so
   the result is a real boolean under `not (...)`. #}
{% macro clickhouse__regexp_match(string, regex, is_raw, flags) %}
    {%- set regex = elementary.regexp_inline_flags(regex, flags) %}
    match({{ string }}, '{{ regex }}') = 1
{% endmacro %}

{# Vertica: regexp_like is a search and takes modifiers as a third argument. #}
{% macro vertica__regexp_match(string, regex, is_raw, flags) %}
    {%- if flags %} regexp_like({{ string }}, '{{ regex }}', '{{ flags }}')
    {%- else %} regexp_like({{ string }}, '{{ regex }}')
    {%- endif %}
{% endmacro %}

{# Dremio: regexp_like is identical to regexp_matches and matches the WHOLE
   input, so a bare pattern would silently only match full-string values. Pad it
   to turn the full match back into a search. (?s) lets . cross newlines.
   Caveat: a user pattern containing ^ or $ still anchors within the padding. #}
{% macro dremio__regexp_match(string, regex, is_raw, flags) %}
    {%- set regex = elementary.regexp_inline_flags(regex, flags) %}
    regexp_like({{ string }}, '(?s).*?(?:{{ regex }}).*?')
{% endmacro %}

{# T-SQL has no regular expression support before SQL Server 2025, so fail with
   an explanation instead of emitting SQL that cannot run. #}
{% macro sqlserver__regexp_match(string, regex, is_raw, flags) %}
    {{
        exceptions.raise_compiler_error(
            "regexp_match: regular expression tests are not supported on SQL Server / Fabric, because T-SQL has no regex functions. Use a LIKE-based test instead."
        )
    }}
{% endmacro %}

{% macro fabric__regexp_match(string, regex, is_raw, flags) %}
    {{ elementary.sqlserver__regexp_match(string, regex, is_raw, flags) }}
{% endmacro %}
