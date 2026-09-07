{#
    Returns a boolean expression that is true when `regex` matches anywhere in
    `string`. Search semantics, not full-match.

    Snowflake's and Dremio's `regexp_like` implicitly anchor at both ends and
    are NOT drop-in replacements for a search. See their overrides below.

    Args:
        string: the column or expression to test.
        regex: the pattern as the regex engine should see it. Rendering it into
            a SQL literal is `regexp_pattern_literal`'s job.
        is_raw: emit the pattern as a raw literal. Honored on Snowflake
            (`$$...$$`), BigQuery and the Spark family (`r'...'`); a silent
            no-op elsewhere, so on ClickHouse and Redshift, whose lexers consume
            backslashes, escape the pattern yourself.
        flags: regex flags. `i` works on every adapter that takes flags at all;
            anything else is per-engine, see `regexp_supported_flags`.
#}
{% macro regexp_match(string, regex, is_raw=false, flags="") %}
    {%- set flags = elementary.regexp_sanitize_flags(flags) %}
    {{ adapter.dispatch("regexp_match", "elementary")(string, regex, is_raw, flags) }}
{% endmacro %}

{#
    Drops flags the current adapter cannot honor, warning about what was
    removed. Engines reject an unknown option outright, so they have to be
    stripped rather than only warned about.

    `-` clears the flags after it, so it is refused rather than dropped where
    it cannot be honored: dropping it would enable what the caller disabled.
#}
{% macro regexp_sanitize_flags(flags) %}
    {%- if not flags %} {%- do return("") %} {%- endif %}
    {%- set flags = flags if flags is string else flags | join("") %}

    {#- Let sqlserver__regexp_match give the real reason instead. -#}
    {%- if elementary.is_tsql() %} {%- do return("") %} {%- endif %}

    {%- set supported = elementary.regexp_supported_flags() %}

    {#- The grammar is `(?set-clear)`: at most one `-`, at least one letter
        after it. Checked before support, so malformed reports as malformed. -#}
    {%- if flags.count("-") > 1 or flags.endswith("-") %}
        {%- if execute %}
            {{
                exceptions.raise_compiler_error(
                    "regexp_match: malformed flags '"
                    ~ flags
                    ~ "'. A '-' may appear at most once, and must be followed by at least one flag letter."
                )
            }}
        {%- endif %}
        {%- do return("") %}
    {%- endif %}

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

    {#- Dropping letters can leave a dangling `-` ("i-Z" becomes "i-"). -#}
    {%- if kept and kept[-1] == "-" %} {%- do kept.pop() %} {%- endif %}

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

{# For the adapters that take no separate flags argument. RE2, PCRE, Postgres
   ARE and Java all understand this syntax. #}
{% macro regexp_inline_flags(regex, flags) %}
    {%- if flags %} {%- do return("(?" ~ flags ~ ")" ~ regex) %}
    {%- else %} {%- do return(regex) %}
    {%- endif %}
{% endmacro %}

{# The flag alphabet each engine accepts. #}
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

{#
    Renders `regex` as a string literal, escaping the delimiter and nothing
    else. Do not reach for a general-purpose escaper here: `escape_special_chars`
    maps `\` to `\\`, which would turn the pattern `\d+` into a literal
    backslash followed by `d+`.

    Raw literals escape nothing, so the delimiter is chosen to avoid the
    pattern and a pattern leaving no usable delimiter is refused.
#}
{% macro regexp_pattern_literal(regex, is_raw=false) %}
    {%- do return(
        adapter.dispatch("regexp_pattern_literal", "elementary")(
            regex, is_raw
        )
    ) %}
{% endmacro %}

{# Doubling the quote is the ANSI escape and cannot disturb the pattern's own
   backslashes. #}
{% macro default__regexp_pattern_literal(regex, is_raw) %}
    {%- do return("'" ~ regex | replace("'", "''") ~ "'") %}
{% endmacro %}

{# `$$...$$` has no escape mechanism and Snowflake has no alternative tag, so a
   raw pattern containing `$$` is inexpressible. Refuse rather than fall back to
   a quoted literal, which would consume the backslashes `is_raw` preserves. #}
{% macro snowflake__regexp_pattern_literal(regex, is_raw) %}
    {%- if not is_raw %}
        {%- do return("'" ~ regex | replace("'", "''") ~ "'") %}
    {%- endif %}
    {%- if "$$" in regex %}
        {%- if execute %}
            {{
                exceptions.raise_compiler_error(
                    "regexp_match: a raw pattern cannot contain '$$' on Snowflake, because that ends the $$...$$ literal. Got '"
                    ~ regex
                    ~ "'. Pass is_raw=false and double the pattern's backslashes instead."
                )
            }}
        {%- endif %}
        {%- do return("''") %}
    {%- endif %}
    {%- do return("$$" ~ regex ~ "$$") %}
{% endmacro %}

{# BigQuery and the Spark family: no doubled-quote escape, but they do have
   `r'...'`. Only the quote takes a backslash. #}
{% macro regexp_backslash_pattern_literal(regex, is_raw) %}
    {%- if not is_raw %}
        {%- do return("'" ~ regex | replace("'", "\\'") ~ "'") %}
    {%- endif %}
    {%- if "'" not in regex %} {%- do return("r'" ~ regex ~ "'") %} {%- endif %}
    {%- if '"' not in regex %} {%- do return('r"' ~ regex ~ '"') %} {%- endif %}
    {%- if execute %}
        {{
            exceptions.raise_compiler_error(
                "regexp_match: a raw pattern cannot contain both quote characters on "
                ~ adapter.type()
                ~ ", because r'...' has no escape sequences. Got '"
                ~ regex
                ~ "'. Pass is_raw=false and escape the pattern's backslashes instead."
            )
        }}
    {%- endif %}
    {%- do return("''") %}
{% endmacro %}

{% macro bigquery__regexp_pattern_literal(regex, is_raw) %}
    {%- do return(elementary.regexp_backslash_pattern_literal(regex, is_raw)) %}
{% endmacro %}

{% macro spark__regexp_pattern_literal(regex, is_raw) %}
    {%- do return(elementary.regexp_backslash_pattern_literal(regex, is_raw)) %}
{% endmacro %}

{% macro databricks__regexp_pattern_literal(regex, is_raw) %}
    {%- do return(elementary.spark__regexp_pattern_literal(regex, is_raw)) %}
{% endmacro %}

{% macro fabricspark__regexp_pattern_literal(regex, is_raw) %}
    {%- do return(elementary.spark__regexp_pattern_literal(regex, is_raw)) %}
{% endmacro %}

{# ClickHouse consumes backslashes in string literals either way. #}
{% macro clickhouse__regexp_pattern_literal(regex, is_raw) %}
    {%- do return("'" ~ regex | replace("'", "\\'") ~ "'") %}
{% endmacro %}

{# `regexp_instr` is the most widely implemented position function. #}
{% macro default__regexp_match(string, regex, is_raw, flags) %}
    regexp_instr({{ string }}, {{ elementary.regexp_pattern_literal(regex, is_raw) }})
    > 0
{% endmacro %}

{# regexp_like is anchored at both ends here, so it cannot be used. #}
{% macro snowflake__regexp_match(string, regex, is_raw, flags) %}
    {%- set pattern = elementary.regexp_pattern_literal(regex, is_raw) %}
    regexp_instr({{ string }}, {{ pattern }}, 1, 1, 0, '{{ flags }}') > 0
{% endmacro %}

{% macro bigquery__regexp_match(string, regex, is_raw, flags) %}
    {%- set regex = elementary.regexp_inline_flags(regex, flags) %}
    {%- set pattern = elementary.regexp_pattern_literal(regex, is_raw) %}
    regexp_contains({{ string }}, {{ pattern }})
{% endmacro %}

{# Flags go inline rather than via `~*`, so the whole ARE option set works.
   Caveat: ARE takes embedded options only at the very start and has no
   flagged-group form, so `flags` plus a pattern already beginning `(?...)` is a
   syntax error. Pass one or the other. #}
{% macro postgres__regexp_match(string, regex, is_raw, flags) %}
    {%- set regex = elementary.regexp_inline_flags(regex, flags) %}
    {{ string }} ~ {{ elementary.regexp_pattern_literal(regex, is_raw) }}
{% endmacro %}

{# regexp_instr takes a parameters argument, unlike the `~` operator. #}
{% macro redshift__regexp_match(string, regex, is_raw, flags) %}
    {%- set pattern = elementary.regexp_pattern_literal(regex, is_raw) %}
    regexp_instr({{ string }}, {{ pattern }}, 1, 1, 0, '{{ flags }}') > 0
{% endmacro %}

{% macro duckdb__regexp_match(string, regex, is_raw, flags) %}
    regexp_matches(
        {{ string }},
        {{ elementary.regexp_pattern_literal(regex, is_raw) }},
        '{{ flags }}'
    )
{% endmacro %}

{% macro spark__regexp_match(string, regex, is_raw, flags) %}
    {%- set regex = elementary.regexp_inline_flags(regex, flags) %}
    {{ string }} rlike {{ elementary.regexp_pattern_literal(regex, is_raw) }}
{% endmacro %}

{% macro databricks__regexp_match(string, regex, is_raw, flags) %}
    {{ elementary.spark__regexp_match(string, regex, is_raw, flags) }}
{% endmacro %}

{% macro fabricspark__regexp_match(string, regex, is_raw, flags) %}
    {{ elementary.spark__regexp_match(string, regex, is_raw, flags) }}
{% endmacro %}

{# Unlike Snowflake, regexp_like here is "contained within", so already a
   search. The engine is joni (Java syntax), not RE2, hence no `U` in the
   alphabet: joni throws UNDEFINED_GROUP_OPTION, and Java's `U` means
   UNICODE_CHARACTER_CLASS rather than RE2's ungreedy swap. #}
{% macro trino__regexp_match(string, regex, is_raw, flags) %}
    {%- set regex = elementary.regexp_inline_flags(regex, flags) %}
    regexp_like({{ string }}, {{ elementary.regexp_pattern_literal(regex, is_raw) }})
{% endmacro %}

{% macro athena__regexp_match(string, regex, is_raw, flags) %}
    {{ elementary.trino__regexp_match(string, regex, is_raw, flags) }}
{% endmacro %}

{# match() returns UInt8, so compare explicitly to get a real boolean. #}
{% macro clickhouse__regexp_match(string, regex, is_raw, flags) %}
    {%- set regex = elementary.regexp_inline_flags(regex, flags) %}
    match({{ string }}, {{ elementary.regexp_pattern_literal(regex, is_raw) }}) = 1
{% endmacro %}

{% macro vertica__regexp_match(string, regex, is_raw, flags) %}
    {%- set pattern = elementary.regexp_pattern_literal(regex, is_raw) %}
    {%- if flags %} regexp_like({{ string }}, {{ pattern }}, '{{ flags }}')
    {%- else %} regexp_like({{ string }}, {{ pattern }})
    {%- endif %}
{% endmacro %}

{# regexp_like matches the WHOLE input here, so pad the pattern to turn the full
   match back into a search. `(?s:...)` is scoped to the padding on purpose: a
   bare top-level `(?s)` would run to the end and make `.` cross newlines inside
   the user's pattern too. Caveat: a user `^` or `$` still anchors. #}
{% macro dremio__regexp_match(string, regex, is_raw, flags) %}
    {%- set regex = elementary.regexp_inline_flags(regex, flags) %}
    {%- set padded = "(?s:.*?)(?:" ~ regex ~ ")(?s:.*?)" %}
    regexp_like({{ string }}, {{ elementary.regexp_pattern_literal(padded, is_raw) }})
{% endmacro %}

{# T-SQL has no regex before SQL Server 2025. Raise only at run time: dbt
   renders test bodies while parsing, and a compiler error there aborts every
   dbt command for the project rather than failing this one test. #}
{% macro sqlserver__regexp_match(string, regex, is_raw, flags) %}
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
