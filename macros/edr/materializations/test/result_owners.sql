{#
  Result owners let a single test route its failures to different owners,
  depending on which rows failed. Configured on the test's meta:

    meta:
      result_owners:
        - expression: "dimension_value = 'JAPAN'"
          owners: ["japan-dataops@example.com"]

  Each expression is evaluated against the test's failing rows. The owners of
  every matching rule are unioned and replace the result's owners. When no rule
  matches, the result keeps its default owners.
#}
{% macro get_result_owners_rules(flattened_test) %}
    {% set meta = elementary.insensitive_get_dict_value(flattened_test, "meta") or {} %}
    {% set rules = meta.get("result_owners") %}
    {% if not rules %} {% do return([]) %} {% endif %}
    {% if rules is mapping %} {% set rules = [rules] %} {% endif %}

    {% set test_unique_id = elementary.insensitive_get_dict_value(
        flattened_test, "unique_id"
    ) %}
    {% if rules is string or rules is not iterable %}
        {% do exceptions.raise_compiler_error(
            "result_owners of test `{}` must be a list of rules, each with an `expression` and `owners`.".format(
                test_unique_id
            )
        ) %}
    {% endif %}
    {% for rule in rules %}
        {% if rule is not mapping or rule.get(
            "expression"
        ) is not string or not rule.get("expression") or not rule.get("owners") %}
            {% do exceptions.raise_compiler_error(
                "Invalid result_owners rule in test `{}`: {}. Each rule needs a string `expression` and `owners`.".format(
                    test_unique_id, rule
                )
            ) %}
        {% endif %}
    {% endfor %}
    {% do return(rules) %}
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

{% macro get_result_owners_select_clause(rules) %}
    {%- set rule_columns = [] -%}
    {%- for rule in rules -%}
        {%- do rule_columns.append(
            "max(case when ("
            ~ rule.expression
            ~ ") then 1 else 0 end) as result_owners_rule_"
            ~ loop.index0
        ) -%}
    {%- endfor -%}
    {%- do return("select " ~ rule_columns | join(", ")) -%}
{% endmacro %}

{# Runs a query built from get_result_owners_select_clause and returns the matched owners, or none. #}
{% macro get_matched_result_owners(rules, result_owners_query) %}
    {% set rows = elementary.agate_to_dicts(
        elementary.run_query(result_owners_query)
    ) %}
    {% if not rows %} {% do return(none) %} {% endif %}
    {% set matched_owners = [] %}
    {% for rule in rules %}
        {% set is_match = elementary.insensitive_get_dict_value(
            rows[0], "result_owners_rule_" ~ loop.index0
        ) %}
        {% if is_match is not none and is_match | int > 0 %}
            {% for owner in elementary.normalize_result_owners(rule.owners) %}
                {% if owner not in matched_owners %}
                    {% do matched_owners.append(owner) %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}
    {% do return(matched_owners or none) %}
{% endmacro %}

{#
  For dbt tests, the failing rows are the rows returned by the test's SQL.
  On T-SQL (and with tests_use_temp_tables) `sql` already selects from a temp
  table, so wrapping it in a derived table is safe there too.
#}
{% macro get_dbt_test_result_owners(flattened_test) %}
    {% set rules = elementary.get_result_owners_rules(flattened_test) %}
    {% if not rules or elementary.did_test_pass() %} {% do return(none) %} {% endif %}
    {% set result_owners_query %}
        {{ elementary.get_result_owners_select_clause(rules) }}
        from ({{ sql }}
        ) results
    {% endset %}
    {% do return(elementary.get_matched_result_owners(rules, result_owners_query)) %}
{% endmacro %}

{#
  For anomaly tests, the failing rows are the anomalous rows of one result group.
  select_override keeps this a single CTE chain, which T-SQL requires.
#}
{% macro get_anomaly_test_result_owners(
    flattened_test, full_table_name, metric_name, column_name
) %}
    {% set rules = elementary.get_result_owners_rules(flattened_test) %}
    {% if not rules %} {% do return(none) %} {% endif %}
    {% set group_filter %}
        {{ elementary.edr_is_true("is_anomalous") }}
        and upper(full_table_name) = upper({{ elementary.const_as_string(full_table_name) }})
        and metric_name = {{ elementary.const_as_string(metric_name) }}
        {%- if column_name %}
        and upper(column_name) = upper({{ elementary.const_as_string(column_name) }})
        {%- endif %}
    {% endset %}
    {% set result_owners_query = elementary.get_read_anomaly_scores_query(
        flattened_test,
        additional_where=group_filter,
        select_override=elementary.get_result_owners_select_clause(rules),
    ) %}
    {% do return(elementary.get_matched_result_owners(rules, result_owners_query)) %}
{% endmacro %}
