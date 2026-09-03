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
            (`$$...$$`) and BigQuery (`r'...'`) have such a syntax, so on the
            other twelve adapters it is a silent no-op. Escape the pattern
            yourself on any engine that consumes backslashes inside string
            literals: ClickHouse, Redshift, and Spark along with the Databricks
            and Fabric Spark adapters that inherit it. Snowflake is exposed too
            whenever `is_raw` is false, which is the default.
        flags: regex flags. `i` (case-insensitive) is honored on every adapter
            that supports flags at all. Anything the adapter does not accept is
            dropped with a warning, by `regexp_sanitize_flags` below. A leading
            `-` negates, and is only accepted where the engine takes inline
            flags.
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

    A `-` is the one character that is rejected rather than dropped, see below.
#}
{% macro regexp_sanitize_flags(flags) %}
    {%- if not flags %} {%- do return("") %} {%- endif %}

    {#- A `-` is not a flag, it negates the flags after it, so dropping it the way
        an unsupported letter is dropped would ENABLE exactly what the caller asked
        to disable: "-i" would keep "i" and emit a case-insensitive match. There is
        no dialect-independent way to honor a negation either. Postgres ARE has no
        `(?-i)` form at all, and where an alphabet carries a letter and its opposite
        (snowflake/redshift/duckdb `c` vs `i`) "absent" does not mean "off". So fail
        loudly instead of guessing. -#}
    {%- set supported = elementary.regexp_supported_flags() %}
    {#- Adapters whose alphabet carries `-` take flags inline and can express a
        negation, so `-i` passes straight through as `(?-i)`. Where it cannot be
        expressed, refuse rather than drop it: dropping `-` would ENABLE exactly
        what the caller asked to disable. Gated on `execute` for the same reason
        as the SQL Server branch below. -#}
    {%- if "-" in flags and "-" not in supported %}
        {%- if execute %}
            {{
                exceptions.raise_compiler_error(
                    "regexp_match: negated flags are not supported on "
                    ~ adapter.type()
                    ~ ", got '"
                    ~ flags
                    ~ "'. Pass only the flags you want enabled."
                )
            }}
        {%- endif %}
        {%- do return("") %}
    {%- endif %}
    {%- set kept = [] %}
    {%- set dropped = [] %}
    {%- for flag in flags %}
        {%- if flag in supported %} {%- do kept.append(flag) %}
        {%- else %} {%- do dropped.append(flag) %}
        {%- endif %}
    {%- endfor %}
    {%- if dropped %}
        {%- set dropped_list = dropped | join("', '") %}
        {%- do elementary.edr_log_warning(
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
    Prepends an inline flag group. Every regex flavour we target understands
    this syntax: RE2, PCRE, Postgres ARE and Java. Used by the adapters that
    take no separate flags argument.
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
{% macro snowflake__regexp_supported_flags() %} {%- do return("cims") %} {% endmacro %}
{% macro bigquery__regexp_supported_flags() %} {%- do return("imsU-") %} {% endmacro %}
{% macro postgres__regexp_supported_flags() %}
    {%- do return("bceimnpqstwx") %}
{% endmacro %}
{% macro redshift__regexp_supported_flags() %} {%- do return("cip") %} {% endmacro %}
{% macro duckdb__regexp_supported_flags() %} {%- do return("cilmnps") %} {% endmacro %}
{% macro spark__regexp_supported_flags() %} {%- do return("idmsuxU-") %} {% endmacro %}
{% macro databricks__regexp_supported_flags() %}
    {%- do return("idmsuxU-") %}
{% endmacro %}
{% macro fabricspark__regexp_supported_flags() %}
    {%- do return("idmsuxU-") %}
{% endmacro %}
{% macro trino__regexp_supported_flags() %} {%- do return("ims-") %} {% endmacro %}
{% macro athena__regexp_supported_flags() %} {%- do return("ims-") %} {% endmacro %}
{% macro clickhouse__regexp_supported_flags() %}
    {%- do return("imsU-") %}
{% endmacro %}
{% macro vertica__regexp_supported_flags() %} {%- do return("bcimnx") %} {% endmacro %}
{% macro dremio__regexp_supported_flags() %} {%- do return("imsx-") %} {% endmacro %}

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
   ARE embedded-option set keeps working and not just case-insensitivity.
   Caveat: ARE accepts embedded options only at the very start of the pattern and
   has no flagged-group form, so combining `flags` with a pattern that already
   begins with `(?...)` is a syntax error. Pass one or the other. #}
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
   within", so it is already a search. Flags go inline. Note the engine is joni
   (Java syntax), not RE2, so `U` is not available here even though BigQuery and
   ClickHouse accept it: joni throws UNDEFINED_GROUP_OPTION, and Java's `U` means
   UNICODE_CHARACTER_CLASS rather than RE2's ungreedy swap. #}
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
   to turn the full match back into a search. The padding needs (?s) so it can
   span newlines, but that flag is scoped to the padding groups: as a bare
   top-level `(?s)` it would run to the end of the whole pattern and silently
   make `.` cross newlines inside the USER's pattern too, on this adapter only.
   Caveat: a user pattern containing ^ or $ still anchors within the padding. #}
{% macro dremio__regexp_match(string, regex, is_raw, flags) %}
    {%- set regex = elementary.regexp_inline_flags(regex, flags) %}
    regexp_like({{ string }}, '(?s:.*?)(?:{{ regex }})(?s:.*?)')
{% endmacro %}

{# T-SQL has no regular expression support before SQL Server 2025, so fail with
   an explanation instead of emitting SQL that cannot run. #}
{% macro sqlserver__regexp_match(string, regex, is_raw, flags) %}
    {#- Raise only once the test actually runs. dbt renders generic test bodies
        while parsing, where `execute` is false, and a compiler error raised there
        aborts every dbt command for the whole project rather than failing this one
        test. Even `config: enabled: false` does not save it, because the body is
        rendered before the node's config is consulted. Emitting a valid predicate
        keeps parsing working; the node still fails with this message when run. -#}
    {%- if execute %}
        {{
            exceptions.raise_compiler_error(
                "regexp_match: regular expression tests are not supported on SQL Server / Fabric, because T-SQL has no regex functions. Use a LIKE-based test instead."
            )
        }}
    {%- endif %}
    1 = 1
{% endmacro %}

{% macro fabric__regexp_match(string, regex, is_raw, flags) %}
    {{ elementary.sqlserver__regexp_match(string, regex, is_raw, flags) }}
{% endmacro %}
