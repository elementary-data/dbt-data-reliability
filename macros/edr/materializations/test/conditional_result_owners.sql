{#
  Conditional result owners let a single test send its failures to different
  owners, depending on which rows failed. Configured on the test's meta:

    meta:
      conditional_result_owners:
        - condition: "dimension_value = 'JAPAN'"
          owners: ["japan-dataops@example.com"]
        - column: country
          values: [KOREA, TAIWAN]
          owners: ["korea-dataops@example.com"]

  Each condition is SQL evaluated against the test's failing rows. A column rule
  names a column of the tested model instead: for dimension anomalies it is
  matched against that dimension, for other tests against that column of the
  failing rows, and it is skipped when the test doesn't return that column. That
  makes column rules safe to set for many tests at once (for example on a folder),
  while a condition errors on tests whose rows don't have the columns it uses.
  If any rule matches, the result's owners are the union of the owners of every
  matching rule, replacing the default owners. When none matches, the result
  keeps its default owners.
#}
{% macro get_conditional_result_owners(flattened_test) %}
    {# A deliberate guard: each condition becomes one column in a single query,
       and 1500 (plus the anomaly path's group columns) stays under the
       strictest warehouse limits on columns per query (Postgres 1664, Redshift 1600). #}
    {% set max_conditions = 1500 %}
    {% set meta = elementary.insensitive_get_dict_value(flattened_test, "meta") or {} %}
    {% set conditions = meta.get("conditional_result_owners") %}
    {% if not conditions %} {% do return([]) %} {% endif %}
    {% if conditions is mapping %} {% set conditions = [conditions] %} {% endif %}

    {% set test_unique_id = elementary.insensitive_get_dict_value(
        flattened_test, "unique_id"
    ) %}
    {% if conditions is string or conditions is not iterable %}
        {% do exceptions.raise_compiler_error(
            "conditional_result_owners of test `{}` must be a list, each item with a `condition` and `owners`.".format(
                test_unique_id
            )
        ) %}
    {% endif %}
    {% if conditions | length > max_conditions %}
        {% do exceptions.raise_compiler_error(
            "conditional_result_owners of test `{}` has {} conditions; the maximum is {}.".format(
                test_unique_id, conditions | length, max_conditions
            )
        ) %}
    {% endif %}
    {% for item in conditions %}
        {% set is_condition_rule = (
            item is mapping
            and item.get("condition") is string
            and item.get("condition")
            and "column" not in item
        ) %}
        {% set is_column_rule = (
            item is mapping
            and item.get("column") is string
            and item.get("column")
            and "values" in item
            and "condition" not in item
        ) %}
        {% if not (is_condition_rule or is_column_rule) or not item.get("owners") %}
            {% do exceptions.raise_compiler_error(
                "Invalid conditional_result_owners item in test `{}`: {}. Each item needs `owners`, and either a string `condition` or a `column` with `values`.".format(
                    test_unique_id, item
                )
            ) %}
        {% endif %}
    {% endfor %}
    {% do return(conditions) %}
{% endmacro %}

{# The values of a column rule as a list; a single value may be given without a list. #}
{% macro get_column_rule_values(rule) %}
    {% set values = rule.get("values") %}
    {% if values is string or values is number or values is none %}
        {% do return([values]) %}
    {% endif %}
    {% do return(values | list) %}
{% endmacro %}

{# A column rule as a SQL condition on a column of the failing rows. #}
{% macro get_column_rule_condition(quoted_column, rule) %}
    {% set literals = [] %}
    {% set checks = [] %}
    {% for value in elementary.get_column_rule_values(rule) %}
        {% if value is none %} {% do checks.append(quoted_column ~ " is null") %}
        {% elif value is number and value is not sameas true and value is not sameas false %}
            {% do literals.append(value | string) %}
        {% else %} {% do literals.append(elementary.edr_quote(value | string)) %}
        {% endif %}
    {% endfor %}
    {% if literals %}
        {% do checks.append(quoted_column ~ " in (" ~ literals | join(", ") ~ ")") %}
    {% endif %}
    {% do return(checks | join(" or ") or "1 = 0") %}
{% endmacro %}

{# Accepts the same formats as model owners: a string, a comma-separated string, or a list. #}
{% macro normalize_result_owners(owners) %}
    {% set normalized = [] %}
    {% if owners is string %} {% set owners = owners.split(",") %}
    {% elif owners is mapping %} {% set owners = [owners] %}
    {% endif %}
    {% for owner in owners %}
        {% if owner is mapping %}
            {% set owner = owner.get("email") or owner.get("name") %}
        {% endif %}
        {% if owner is string and owner | trim %}
            {% do normalized.append(owner | trim) %}
        {% endif %}
    {% endfor %}
    {% do return(normalized) %}
{% endmacro %}

{# The leading comment names the test, so a failing condition is easy to trace in query history. #}
{% macro get_conditional_result_owners_select_clause(conditions, test_unique_id) %}
    {%- set condition_columns = [] -%}
    {%- for item in conditions -%}
        {%- do condition_columns.append(
            "max(case when ("
            ~ item.condition
            ~ ") then 1 else 0 end) as owners_condition_"
            ~ loop.index0
        ) -%}
    {%- endfor -%}
    {%- do return(
        "/* conditional_result_owners of "
        ~ test_unique_id
        ~ " */ select "
        ~ condition_columns
        | join(", ")
    ) -%}
{% endmacro %}

{# Returns the sorted owners of the conditions matched in a row of get_conditional_result_owners_select_clause, or none. #}
{% macro get_matched_result_owners(conditions, result_row) %}
    {% set matched_owners = [] %}
    {% for item in conditions %}
        {% set is_match = elementary.insensitive_get_dict_value(
            result_row, "owners_condition_" ~ loop.index0
        ) %}
        {% if is_match is not none and is_match | int > 0 %}
            {% for owner in elementary.normalize_result_owners(item.owners) %}
                {% if owner not in matched_owners %}
                    {% do matched_owners.append(owner) %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}
    {% do return((matched_owners | sort | list) or none) %}
{% endmacro %}

{#
  The column names the test's SQL returns, in their actual case so they can be
  quoted. Reads one failing row rather than an empty result, because some
  adapters (ClickHouse) return no column names for an empty result. Only called
  when the test failed or warned, so there is at least one row.
  On T-SQL `sql` already selects from a temp table, so it can be a derived table.
#}
{% macro get_test_returned_column_names() %}
    {% set columns_query = adapter.dispatch(
        "get_test_returned_columns_query", "elementary"
    )() %}
    {% do return(
        elementary.run_query(
            columns_query, lowercase_column_names=false
        ).column_names
        | list
    ) %}
{% endmacro %}

{% macro default__get_test_returned_columns_query() %}
    {% do return("select * from (" ~ sql ~ "\n) results limit 1") %}
{% endmacro %}

{% macro fabric__get_test_returned_columns_query() %}
    {% do return("select top 1 * from (" ~ sql ~ "\n) results") %}
{% endmacro %}

{% macro sqlserver__get_test_returned_columns_query() %}
    {% do return(elementary.fabric__get_test_returned_columns_query()) %}
{% endmacro %}

{#
  For dbt tests, the failing rows are the rows returned by the test's SQL.
  On T-SQL (and with tests_use_temp_tables) `sql` already selects from a temp
  table, so wrapping it in a derived table is safe there too.
#}
{% macro get_dbt_test_result_owners(flattened_test, sample_rows=none) %}
    {% set rules = elementary.get_conditional_result_owners(flattened_test) %}
    {% if not rules or elementary.did_test_pass() %} {% do return(none) %} {% endif %}
    {% set conditions = rules | selectattr("condition", "defined") | list %}
    {% set column_rules = rules | selectattr("column", "defined") | list %}
    {% if column_rules and sample_rows %}
        {# The sampled failing rows already show which columns the test returns
           (lowercased), so skip rules on columns it doesn't return without a query #}
        {% set sampled_columns = sample_rows[0].keys() | map("lower") | list %}
        {% set kept_rules = [] %}
        {% for rule in column_rules %}
            {% if rule.column | lower in sampled_columns %}
                {% do kept_rules.append(rule) %}
            {% else %}
                {% do elementary.debug_log(
                    flattened_test.unique_id
                    ~ ": skipping the conditional_result_owners rule on column "
                    ~ rule.column
                    ~ ", which the test doesn't return."
                ) %}
            {% endif %}
        {% endfor %}
        {% set column_rules = kept_rules %}
    {% endif %}
    {% if column_rules %}
        {# Column rules only apply to the columns the test returns, so read them first.
           The real column names (not lowercased) are needed to quote them. #}
        {% set returned_columns = {} %}
        {% for column in elementary.get_test_returned_column_names() %}
            {% do returned_columns.update({column | lower: column}) %}
        {% endfor %}
        {% for rule in column_rules %}
            {% set column = returned_columns.get(rule.column | lower) %}
            {% if column %}
                {% do conditions.append(
                    {
                        "condition": elementary.get_column_rule_condition(
                            adapter.quote(column), rule
                        ),
                        "owners": rule.owners,
                    }
                ) %}
            {% else %}
                {% do elementary.debug_log(
                    flattened_test.unique_id
                    ~ ": skipping the conditional_result_owners rule on column "
                    ~ rule.column
                    ~ ", which the test doesn't return."
                ) %}
            {% endif %}
        {% endfor %}
    {% endif %}
    {% if not conditions %} {% do return(none) %} {% endif %}
    {% set result_owners_query %}
        {{ elementary.get_conditional_result_owners_select_clause(conditions, flattened_test.unique_id) }}
        from ({{ sql }}
        ) results
    {% endset %}
    {% set rows = elementary.agate_to_dicts(
        elementary.run_query(result_owners_query)
    ) %}
    {% if not rows %} {% do return(none) %} {% endif %}
    {% do return(elementary.get_matched_result_owners(conditions, rows[0])) %}
{% endmacro %}

{% macro get_anomaly_result_owners_group_key(
    full_table_name, column_name, metric_name
) %}
    {# Both sides of the lookup read the same anomaly scores rows, so values are
       compared as-is; quoted identifiers that differ only in case stay distinct. #}
    {% do return((full_table_name, column_name, metric_name)) %}
{% endmacro %}

{#
  For anomaly tests, the failing rows are the anomalous rows of each result group
  (full_table_name, column_name, metric_name). Conditions are evaluated for all
  groups in one grouped query; select_override keeps it a single CTE chain, which
  T-SQL requires. Column rules are matched here against the anomalous rows already
  read, by the position of their column in the test's dimensions.
  Returns a dict of group key (see get_anomaly_result_owners_group_key) to owners.
  The rules are validated on every run, even when nothing is anomalous.
#}
{% macro get_anomaly_result_owners_by_group(flattened_test, anomaly_scores_rows) %}
    {% set rules = elementary.get_conditional_result_owners(flattened_test) %}
    {% if not rules %} {% do return({}) %} {% endif %}
    {% set anomalous_rows = anomaly_scores_rows | selectattr("is_anomalous") | list %}
    {% if not anomalous_rows %} {% do return({}) %} {% endif %}
    {% set conditions = rules | selectattr("condition", "defined") | list %}
    {% set column_rules = rules | selectattr("column", "defined") | list %}
    {% set owners_by_group = {} %}

    {% if conditions %}
        {% set result_owners_query = elementary.get_read_anomaly_scores_query(
            flattened_test,
            additional_where=elementary.edr_is_true("is_anomalous"),
            select_override=elementary.get_conditional_result_owners_select_clause(
                conditions, flattened_test.unique_id
            )
            ~ ", full_table_name, column_name, metric_name",
            group_by="full_table_name, column_name, metric_name",
        ) %}
        {% for row in elementary.agate_to_dicts(
            elementary.run_query(result_owners_query)
        ) %}
            {% set owners = elementary.get_matched_result_owners(conditions, row) %}
            {% if owners %}
                {% do owners_by_group.update(
                    {
                        elementary.get_anomaly_result_owners_group_key(
                            elementary.insensitive_get_dict_value(
                                row, "full_table_name"
                            ),
                            elementary.insensitive_get_dict_value(
                                row, "column_name"
                            ),
                            elementary.insensitive_get_dict_value(
                                row, "metric_name"
                            ),
                        ): owners
                    }
                ) %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {% if column_rules %}
        {# The resolved configuration also has dimensions set in the model config or vars #}
        {% set test_configuration = (
            elementary.get_cache(flattened_test.unique_id) or {}
        ) %}
        {% set dimensions = (
            test_configuration.get("dimensions")
            or (flattened_test.get("test_params") or {}).get("dimensions")
            or []
        ) %}
        {% if dimensions is string %} {% set dimensions = [dimensions] %} {% endif %}
        {% set dimensions = dimensions | map("lower") | list %}
        {# Build a value -> owners lookup per dimension position once,
           then match every anomalous row in a single pass. #}
        {% set owners_by_position = {} %}
        {% for rule in column_rules %}
            {% if rule.column | lower not in dimensions %}
                {% do elementary.debug_log(
                    flattened_test.unique_id
                    ~ ": skipping the conditional_result_owners rule on column "
                    ~ rule.column
                    ~ ", which is not a dimension of the test."
                ) %}
            {% else %}
                {% set position = dimensions.index(rule.column | lower) %}
                {% do owners_by_position.setdefault(position, {}) %}
                {# dimension_value shows nulls as NULL #}
                {% for value in elementary.get_column_rule_values(rule) %}
                    {% set value_key = "NULL" if value is none else value | string %}
                    {% do owners_by_position[position].setdefault(value_key, []) %}
                    {% for owner in elementary.normalize_result_owners(rule.owners) %}
                        {% if owner not in owners_by_position[position][value_key] %}
                            {% do owners_by_position[position][value_key].append(
                                owner
                            ) %}
                        {% endif %}
                    {% endfor %}
                {% endfor %}
            {% endif %}
        {% endfor %}

        {% if owners_by_position %}
            {% set unsplittable = namespace(count=0) %}
            {% for row in anomalous_rows %}
                {% set dimension_value = elementary.insensitive_get_dict_value(
                    row, "dimension_value"
                ) %}
                {# dimension_value joins multiple dimensions with "; ". A single
                   dimension's value is never split, since it may contain "; ". #}
                {% if dimension_value is none %} {% set parts = [] %}
                {% elif dimensions | length == 1 %}
                    {% set parts = [dimension_value | string] %}
                {% else %} {% set parts = (dimension_value | string).split("; ") %}
                {% endif %}
                {% if parts | length != dimensions | length %}
                    {% if dimension_value is not none %}
                        {% set unsplittable.count = unsplittable.count + 1 %}
                    {% endif %}
                {% else %}
                    {% set row_owners = [] %}
                    {% for position, owners_by_value in owners_by_position.items() %}
                        {% for owner in owners_by_value.get(parts[position], []) %}
                            {% if owner not in row_owners %}
                                {% do row_owners.append(owner) %}
                            {% endif %}
                        {% endfor %}
                    {% endfor %}
                    {% if row_owners %}
                        {% set group_key = (
                            elementary.get_anomaly_result_owners_group_key(
                                row.full_table_name,
                                row.column_name,
                                row.metric_name,
                            )
                        ) %}
                        {% set owners = owners_by_group.get(group_key) or [] %}
                        {% for owner in row_owners %}
                            {% if owner not in owners %}
                                {% do owners.append(owner) %}
                            {% endif %}
                        {% endfor %}
                        {% do owners_by_group.update(
                            {group_key: owners | sort | list}
                        ) %}
                    {% endif %}
                {% endif %}
            {% endfor %}
            {% if unsplittable.count %}
                {% do elementary.debug_log(
                    flattened_test.unique_id
                    ~ ": "
                    ~ unsplittable.count
                    ~ " anomalous rows have a dimension value containing \"; \", so it can't be split into the test's "
                    ~ dimensions
                    | length
                    ~ " dimensions; conditional_result_owners column rules don't match them."
                ) %}
            {% endif %}
        {% endif %}
    {% endif %}
    {% do return(owners_by_group) %}
{% endmacro %}
