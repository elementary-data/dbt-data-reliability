{#
  Conditional result owners let a single test send its failures to different
  owners, depending on which rows failed. Configured on the test's meta:

    meta:
      conditional_result_owners:
        - condition: "dimension_value = 'JAPAN'"
          owners: ["japan-dataops@example.com"]

  Each condition is SQL evaluated against the test's failing rows. If any
  condition matches, the result's owners are the union of the owners of every
  matching condition, replacing the default owners. When none matches, the
  result keeps its default owners.
#}
{% macro get_conditional_result_owners(flattened_test) %}
    {# A deliberate guard, not a warehouse limit: each condition becomes one
       aggregate column in a single query, so this keeps that query a sane size. #}
    {% set max_conditions = 100 %}
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
        {% if item is not mapping or item.get(
            "condition"
        ) is not string or not item.get("condition") or not item.get("owners") %}
            {% do exceptions.raise_compiler_error(
                "Invalid conditional_result_owners item in test `{}`: {}. Each item needs a string `condition` and `owners`.".format(
                    test_unique_id, item
                )
            ) %}
        {% endif %}
    {% endfor %}
    {% do return(conditions) %}
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
  For dbt tests, the failing rows are the rows returned by the test's SQL.
  On T-SQL (and with tests_use_temp_tables) `sql` already selects from a temp
  table, so wrapping it in a derived table is safe there too.
#}
{% macro get_dbt_test_result_owners(flattened_test) %}
    {% set conditions = elementary.get_conditional_result_owners(flattened_test) %}
    {% if not conditions or elementary.did_test_pass() %}
        {% do return(none) %}
    {% endif %}
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
  (full_table_name, column_name, metric_name). All groups are evaluated in one
  grouped query; select_override keeps it a single CTE chain, which T-SQL requires.
  Returns a dict of group key (see get_anomaly_result_owners_group_key) to owners.
  The conditions are validated on every run, even when nothing is anomalous.
#}
{% macro get_anomaly_result_owners_by_group(flattened_test, anomaly_scores_rows) %}
    {% set conditions = elementary.get_conditional_result_owners(flattened_test) %}
    {% if not conditions %} {% do return({}) %} {% endif %}
    {% if not (anomaly_scores_rows | selectattr("is_anomalous") | list) %}
        {% do return({}) %}
    {% endif %}
    {% set result_owners_query = elementary.get_read_anomaly_scores_query(
        flattened_test,
        additional_where=elementary.edr_is_true("is_anomalous"),
        select_override=elementary.get_conditional_result_owners_select_clause(
            conditions, flattened_test.unique_id
        )
        ~ ", full_table_name, column_name, metric_name",
        group_by="full_table_name, column_name, metric_name",
    ) %}
    {% set owners_by_group = {} %}
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
    {% do return(owners_by_group) %}
{% endmacro %}
