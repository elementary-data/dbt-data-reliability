{% macro get_common_test_config(flattened_test) %}
  {% set test_name = flattened_test["test_original_name"] %}
  {% set test_namespace = flattened_test["test_namespace"] %}
  {% do return(elementary.get_common_test_config_by_namespace_and_name(test_namespace, test_name)) %}
{% endmacro %}

{% macro get_common_test_config_by_namespace_and_name(test_namespace, test_name) %}
  {% set common_tests_configs_mapping = elementary.get_common_tests_configs_mapping() %}
  {% if test_namespace is none %}
    {% set test_namespace = "dbt" %}
  {% endif %}
  {% if test_namespace in common_tests_configs_mapping %}
    {% if test_name in common_tests_configs_mapping[test_namespace] %}
      {% do return(common_tests_configs_mapping[test_namespace][test_name]) %}
    {% endif %}
  {% endif %}
  {% do return(none) %}
{% endmacro %}

{% macro get_common_tests_configs_mapping() %}
  {#
    Configurations for common tests (dbt builtins, dbt_utils, dbt_expectations, etc)
    The format of the mapping is:
    {
      <package_name>: {
        <test_name>: <config>
      }
    }
    The config may contain:
      description: A human readable description of the test.
      failed_row_count_calc: SQL expression to get the number of failed rows from the test result.
      quality_dimension: The quality dimension of the test, see https://www.getdbt.com/blog/data-quality-dimensions/ for details.
  #}
  {% set common_tests_configs_mapping = {
      "dbt": {
        "not_null": {
          "description": "This test validates that there are no `null` values present in a column.",
          "quality_dimension": "completeness",
          "failed_row_count_calc": "count(*)"
        },
        "relationships": {
          "description": "This test validates that all of the records in a child table have a corresponding record in a parent table. This property is referred to as \"referential integrity\".",
          "quality_dimension": "consistency",
          "failed_row_count_calc": "count(*)"
        },
        "unique": {
          "description": "This test validates that there are no duplicate values present in a field.",
          "quality_dimension": "uniqueness",
          "failed_row_count_calc": "sum(n_records)"
        },
        "accepted_values": {
          "description": "This test validates that all of the values in a column are present in a supplied list of `values`. If any values other than those provided in the list are present, then the test will fail.",
          "quality_dimension": "validity",
          "failed_row_count_calc": "sum(n_records)"
        }
      },
      "dbt_expectations": {
        "expect_column_values_to_not_be_null": {
          "description": "Expect column values to not be null.",
          "quality_dimension": "completeness",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_values_to_be_null": {
          "description": "Expect column values to be null.",
          "quality_dimension": "completeness",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_values_to_be_in_set": {
          "description": "Expect each column value to be in a given set.",
          "quality_dimension": "validity",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_values_to_be_between": {
          "description": "Expect each column value to be between two values.",
          "quality_dimension": "validity",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_values_to_not_be_in_set": {
          "description": "Expect each column value not to be in a given set.",
          "quality_dimension": "validity",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_values_to_be_increasing": {
          "description": "Expect column values to be increasing.\nIf strictly: True, then this expectation is only satisfied if each consecutive value is strictly increasing \u2013 equal values are treated as failures.",
          "quality_dimension": "accuracy",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_values_to_be_decreasing": {
          "description": "Expect column values to be decreasing.\nIf strictly: True, then this expectation is only satisfied if each consecutive value is strictly decreasing \u2013 equal values are treated as failures.",
          "quality_dimension": "accuracy",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_value_lengths_to_be_between": {
          "description": "Expect column entries to be strings with length between a min_value value and a max_value value (inclusive).",
          "quality_dimension": "validity",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_value_lengths_to_equal": {
          "description": "Expect column entries to be strings with length equal to the provided value.",
          "quality_dimension": "validity",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_values_to_match_regex": {
          "description": "Expect column entries to be strings that match a given regular expression.\nValid matches can be found anywhere in the string.\nFor example, \"[at]+\" will identify the following strings as expected: \"cat\", \"hat\", \"aa\", \"a\", and \"t\", and the following strings as unexpected: \"fish\", \"dog\".",
          "quality_dimension": "validity",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_values_to_not_match_regex": {
          "description": "Expect column entries to be strings that do NOT match a given regular expression.\nThe regex must not match any portion of the provided string.\nFor example, \"[at]+\" would identify the following strings as expected: \"fish\u201d, \"dog\u201d, and the following as unexpected: \"cat\u201d, \"hat\u201d.",
          "quality_dimension": "validity",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_values_to_match_regex_list": {
          "description": "Expect the column entries to be strings that can be matched to either any of or all of a list of regular expressions. Matches can be anywhere in the string.",
          "quality_dimension": "validity",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_values_to_not_match_regex_list": {
          "description": "Expect the column entries to be strings that do not match any of a list of regular expressions. Matches can be anywhere in the string.",
          "quality_dimension": "validity",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_values_to_match_like_pattern": {
          "description": "Expect column entries to be strings that match a given SQL LIKE pattern.",
          "quality_dimension": "validity",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_values_to_not_match_like_pattern": {
          "description": "Expect column entries to be strings that do not match a given SQL LIKE pattern.",
          "quality_dimension": "validity",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_values_to_match_like_pattern_list": {
          "description": "Expect column entries to be strings that match any of a list of SQL LIKE patterns.",
          "quality_dimension": "validity",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_values_to_not_match_like_pattern_list": {
          "description": "Expect column entries to be strings that do not match any of a list of SQL LIKE patterns.",
          "quality_dimension": "validity",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_pair_values_A_to_be_greater_than_B": {
          "description": "Expect values in column A to be greater than column B.",
          "quality_dimension": "accuracy",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_pair_values_to_be_equal": {
          "description": "Expect the values in column A to be the same as column B.",
          "quality_dimension": "accuracy",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_pair_values_to_be_in_set": {
          "description": "Expect paired values from columns A and B to belong to a set of valid pairs.\nNote: value pairs are expressed as lists within lists",
          "quality_dimension": "validity",
          "failed_row_count_calc": "count(*)"
        },
        "expect_select_column_values_to_be_unique_within_record": {
          "description": "Expect the values for each record to be unique across the columns listed.\nNote that records can be duplicated.",
          "quality_dimension": "validity",
          "failed_row_count_calc": "count(*)"
        },
        "expect_column_values_to_be_unique": {
          "description": "Expect each column value to be unique.",
          "quality_dimension": "uniqueness"
        },
        "expect_compound_columns_to_be_unique": {
          "description": "Expect that the columns are unique together, e.g. a multi-column primary key.",
          "quality_dimension": "uniqueness"
        },
        "expect_column_to_exist": {
          "quality_dimension": "validity",
          "description": "Expect the specified column to exist."
        },
        "expect_row_values_to_have_recent_data": {
          "description": "Expect the model to have rows that are at least as recent as the defined interval prior to the current timestamp.\nOptionally gives the possibility to apply filters on the results.",
          "quality_dimension": "freshness"
        },
        "expect_grouped_row_values_to_have_recent_data": {
          "description": "Expect the model to have grouped rows that are at least as recent as the defined interval prior to the current timestamp.\nUse this to test whether there is recent data for each grouped row defined by group_by (which is a list of columns) and a timestamp_column.\nOptionally gives the possibility to apply filters on the results.",
          "quality_dimension": "freshness"
        },
        "expect_table_column_count_to_be_between": {
          "description": "Expect the number of columns in a model to be between two values.",
          "quality_dimension": "consistency"
        },
        "expect_table_column_count_to_equal_other_table": {
          "description": "Expect the number of columns in a model to match another model.",
          "quality_dimension": "consistency"
        },
        "expect_table_columns_to_not_contain_set": {
          "description": "Expect the columns in a model not to contain a given list.",
          "quality_dimension": "validity"
        },
        "expect_table_columns_to_contain_set": {
          "description": "Expect the columns in a model to contain a given list.",
          "quality_dimension": "consistency"
        },
        "expect_table_column_count_to_equal": {
          "description": "Expect the number of columns in a model to be equal to `expected_number_of_columns`.",
          "quality_dimension": "consistency"
        },
        "expect_table_columns_to_match_ordered_list": {
          "description": "Expect the columns to exactly match a specified list.",
          "quality_dimension": "validity"
        },
        "expect_table_columns_to_match_set": {
          "description": "Expect the columns in a model to match a given set.",
          "quality_dimension": "validity"
        },
        "expect_table_row_count_to_be_between": {
          "description": "Expect the number of rows in a model to be between two values.",
          "quality_dimension": "accuracy"
        },
        "expect_table_row_count_to_equal_other_table": {
          "description": "Expect the number of rows in a model match another model.",
          "quality_dimension": "consistency"
        },
        "expect_table_row_count_to_equal_other_table_times_factor": {
          "description": "Expect the number of rows in a model to match another model times a preconfigured factor.",
          "quality_dimension": "consistency"
        },
        "expect_table_row_count_to_equal": {
          "quality_dimension": "completeness",
          "description": "Expect the number of rows in a model to be equal to `expected_number_of_rows`."
        },
        "expect_column_values_to_be_of_type": {
          "description": "Expect a column to be of a specified data type.",
          "quality_dimension": "validity"
        },
        "expect_column_values_to_be_in_type_list": {
          "quality_dimension": "validity",
          "description": "Expect a column to be one of a specified type list."
        },
        "expect_column_values_to_have_consistent_casing": {
          "description": "Expect a column to have consistent casing.\nBy setting display_inconsistent_columns to true, the number of inconsistent values in the column will be displayed in the terminal\nwhereas the inconsistent values themselves will be returned if the SQL compiled test is run.",
          "quality_dimension": "validity"
        },
        "expect_column_distinct_count_to_equal": {
          "description": "Expect the number of distinct column values to be equal to a given value.",
          "quality_dimension": "accuracy"
        },
        "expect_column_distinct_count_to_be_greater_than": {
          "description": "Expect the number of distinct column values to be greater than a given value.",
          "quality_dimension": "accuracy"
        },
        "expect_column_distinct_count_to_be_less_than": {
          "description": "Expect the number of distinct column values to be less than a given value.",
          "quality_dimension": "accuracy"
        },
        "expect_column_distinct_values_to_be_in_set": {
          "description": "Expect the set of distinct column values to be contained by a given set.",
          "quality_dimension": "validity"
        },
        "expect_column_distinct_values_to_contain_set": {
          "description": "Expect the set of distinct column values to contain a given set.\nIn contrast to expect_column_values_to_be_in_set this ensures not that all column values are members of the given set\nbut that values from the set must be present in the column.",
          "quality_dimension": "validity"
        },
        "expect_column_distinct_values_to_equal_set": {
          "description": "Expect the set of distinct column values to equal a given set.\nIn contrast to expect_column_distinct_values_to_contain_set this ensures not only that a certain set of values are present in the column\nbut that these and only these values are present.",
          "quality_dimension": "validity"
        },
        "expect_column_distinct_count_to_equal_other_table": {
          "description": "Expect the number of distinct column values to be equal to number of distinct values in another model.",
          "quality_dimension": "accuracy"
        },
        "expect_column_mean_to_be_between": {
          "description": "Expect the column mean to be between a min_value value and a max_value value (inclusive).",
          "quality_dimension": "validity"
        },
        "expect_column_median_to_be_between": {
          "description": "Expect the column median to be between a min_value value and a max_value value (inclusive).",
          "quality_dimension": "validity"
        },
        "expect_column_quantile_values_to_be_between": {
          "description": "Expect specific provided column quantiles to be between provided min_value and max_value values.",
          "quality_dimension": "validity"
        },
        "expect_column_stdev_to_be_between": {
          "description": "Expect the column standard deviation to be between a min_value value and a max_value value. Uses sample standard deviation (normalized by N-1).",
          "quality_dimension": "validity"
        },
        "expect_column_unique_value_count_to_be_between": {
          "description": "Expect the number of unique values to be between a min_value value and a max_value value.",
          "quality_dimension": "validity"
        },
        "expect_column_proportion_of_unique_values_to_be_between": {
          "description": "Expect the proportion of unique values to be between a min_value value and a max_value value.\nFor example, in a column containing [1, 2, 2, 3, 3, 3, 4, 4, 4, 4], there are 4 unique values and 10 total values for a proportion of 0.4.",
          "quality_dimension": "uniqueness"
        },
        "expect_column_most_common_value_to_be_in_set": {
          "description": "Expect the most common value to be within the designated value set",
          "quality_dimension": "accuracy"
        },
        "expect_column_max_to_be_between": {
          "description": "Expect the column max to be between a min and max value",
          "quality_dimension": "validity"
        },
        "expect_column_min_to_be_between": {
          "description": "Expect the column min to be between a min and max value",
          "quality_dimension": "validity"
        },
        "expect_column_sum_to_be_between": {
          "description": "Expect the column to sum to be between a min and max value",
          "quality_dimension": "validity"
        },
        "expect_multicolumn_sum_to_equal": {
          "description": "Expects that sum of all rows for a set of columns is equal to a specific value",
          "quality_dimension": "consistency"
        },
        "expect_column_values_to_be_within_n_moving_stdevs": {
          "quality_dimension": "accuracy",
          "description": "A simple anomaly test based on the assumption that differences between periods in a given time series follow a log-normal distribution. Thus, we would expect the logged differences (vs N periods ago) in metric values to be within Z sigma away from a moving average. By applying a list of columns in the `group_by` parameter, you can also test for deviations within a group."
        },
        "expect_column_values_to_be_within_n_stdevs": {
          "quality_dimension": "accuracy",
          "description": "Expects (optionally grouped & summed) metric values to be within Z sigma away from the column average"
        },
        "expect_row_values_to_have_data_for_every_n_datepart": {
          "quality_dimension": "completeness",
          "description": "Expects model to have values for every grouped `date_part`."
        },
        "expect_column_values_to_be_of_type_list": {
          "description": "Expect a column to be one of a specified type list.",
          "quality_dimension": "validity"
        },
        "expect_table_aggregation_to_equal_other_table": {
          "description": "Except an (optionally grouped) expression to match the same (or optionally other) expression in a different table.",
          "quality_dimension": "consistency"
        }
      },
      "dbt_utils": {
        "equal_rowcount": {
          "quality_dimension": "consistency",
          "failed_row_count_calc": "sum(diff_count)",
          "description": "Asserts that two relations have the same number of rows."
        },
        "fewer_rows_than": {
          "quality_dimension": "consistency",
          "failed_row_count_calc": "sum(row_count_delta)",
          "description": "Asserts that the respective model has fewer rows than the model being compared."
        },
        "expression_is_true": {
          "quality_dimension": "accuracy",
          "failed_row_count_calc": "count(*)",
          "description": "Asserts that a valid SQL expression is true for all records."
        },
        "not_empty_string": {
          "quality_dimension": "completeness",
          "failed_row_count_calc": "count(*)",
          "description": "Asserts that a column does not have any values equal to ''."
        },
        "cardinality_equality": {
          "quality_dimension": "consistency",
          "failed_row_count_calc": "sum(num_rows)",
          "description": "Asserts that values in a given column have exactly the same cardinality as values from a different column in a different model."
        },
        "sequential_values": {
          "quality_dimension": "accuracy",
          "failed_row_count_calc": "count(*)",
          "description": "Confirms that a column contains sequential values."
        },
        "accepted_range": {
          "quality_dimension": "validity",
          "failed_row_count_calc": "count(*)",
          "description": "Asserts that a column's values fall inside an expected range."
        },
        "unique_combination_of_columns": {
          "quality_dimension": "uniqueness",
          "failed_row_count_calc": "count(*)",
          "description": "Asserts that the combination of columns is unique."
        },
        "at_least_one": {
          "description": "Asserts that a column has at least one value.",
          "quality_dimension": "completeness"
        },
        "equality": {
          "description": "Asserts the equality of two relations.\nOptionally specify a subset of columns to compare or exclude, and a precision to compare numeric columns on.",
          "quality_dimension": "consistency"
        },
        "mutually_exclusive_ranges": {
          "description": "Asserts that for a given lower_bound_column and upper_bound_column, the ranges between the lower and upper bounds do not overlap with the ranges of another row.",
          "quality_dimension": "accuracy"
        },
        "not_accepted_values": {
          "description": "Asserts that there are no rows that match the given values.",
          "quality_dimension": "validity"
        },
        "not_constant": {
          "description": "Asserts that a column does not have the same value in all rows.",
          "quality_dimension": "validity"
        },
        "not_null_proportion": {
          "description": "Asserts that the proportion of non-null values present in a column is between a specified range [at_least, at_most].",
          "quality_dimension": "completeness"
        },
        "recency": {
          "description": "Asserts that a timestamp column in the reference model contains data that is at least as recent as the defined date interval.",
          "quality_dimension": "freshness"
        },
        "relationships_where": {
          "description": "Asserts the referential integrity between two relations with an added predicate to filter out some rows from the test.",
          "quality_dimension": "consistency"
        }
      },
      "elementary": {
        "schema_changes": {
          "quality_dimension": "validity",
          "description": "Monitors schema changes on the table of deleted, added, type changed columns over time. The test will fail if the table's schema changed from the previous execution of the test."
        },
        "schema_changes_from_baseline": {
          "quality_dimension": "validity",
          "description": "Compares the table's schema against a baseline contract of columns defined in the table's configuration."
        },
        "json_schema": {
          "quality_dimension": "validity",
          "failed_row_count_calc": "count(*)",
          "description": "This test validates that the data in a column is valid according to a JSON schema."
        },
        "volume_anomalies": {
          "quality_dimension": "completeness",
          "description": "Monitors the row count of your table over time."
        },
        "freshness_anomalies": {
          "quality_dimension": "freshness",
          "description": "Monitors the freshness of your table over time, as the expected time between data updates."
        },
        "event_freshness_anomalies": {
          "quality_dimension": "freshness",
          "description": "Monitors the freshness of event data over time, as the expected time it takes each event to load, that is, the time between when the event actually occurs (the event timestamp), and when it is loaded to the database (the update timestamp)."
        },
        "dimension_anomalies": {
          "quality_dimension": "completeness",
          "description": "Monitors the frequency of values in the configured dimensions over time."
        },
        "all_columns_anomalies": {
          "description": "Executes column level monitors and anomaly detection on all of the columns in the table."
        },
        "column_anomalies": {
          "description": "Executes column level monitors and anomaly detection on the column"
        },
        "exposure_schema_validity": {
          "quality_dimension": "validity",
          "description": "Column level exposure validation according to the meta.columns property in exposures.yml"
        },
        "collect_metrics": {
          "description": "Collects metrics for the specified column or table. The test will always pass."
        }
      }
    } %}
    {% do return(common_tests_configs_mapping) %}
{% endmacro %}
