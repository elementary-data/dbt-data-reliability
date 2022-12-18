{%- macro upload_dbt_tests(should_commit=false, cache=true) -%}
    {% set relation = elementary.get_elementary_relation('dbt_tests') %}
    {% if execute and relation %}
        {% set tests = graph.nodes.values() | selectattr('resource_type', '==', 'test') %}
        {% do elementary.upload_artifacts_to_table(relation, tests, elementary.flatten_test, should_commit=should_commit, cache=cache) %}
    {%- endif -%}
    {{- return('') -}}
{%- endmacro -%}




{% macro get_dbt_tests_empty_table_query() %}
    {% set dbt_tests_empty_table_query = elementary.empty_table([('unique_id', 'string'),
                                                                 ('database_name', 'string'),
                                                                 ('schema_name', 'string'),
                                                                 ('name', 'string'),
                                                                 ('short_name', 'string'),
                                                                 ('alias', 'string'),
                                                                 ('test_column_name', 'string'),
                                                                 ('severity', 'string'),
                                                                 ('warn_if', 'string'),
                                                                 ('error_if', 'string'),
                                                                 ('test_params', 'long_string'),
                                                                 ('test_namespace', 'string'),
                                                                 ('tags', 'long_string'),
                                                                 ('model_tags', 'long_string'),
                                                                 ('model_owners', 'long_string'),
                                                                 ('meta', 'long_string'),
                                                                 ('depends_on_macros', 'long_string'),
                                                                 ('depends_on_nodes', 'long_string'),
                                                                 ('parent_model_unique_id', 'string'),
                                                                 ('description', 'long_string'),
                                                                 ('package_name', 'string'),
                                                                 ('type', 'string'),
                                                                 ('original_path', 'long_string'),
                                                                 ('path', 'string'),
                                                                 ('generated_at', 'string')]) %}
    {{ return(dbt_tests_empty_table_query) }}
{% endmacro %}

{% macro flatten_test(node_dict) %}
    {% set config_dict = elementary.safe_get_with_default(node_dict, 'config', {}) %}
    {% set depends_on_dict = elementary.safe_get_with_default(node_dict, 'depends_on', {}) %}

    {% set test_metadata = elementary.safe_get_with_default(node_dict, 'test_metadata', {}) %}
    {% set test_namespace = test_metadata.get('namespace') %}
    {% set test_short_name = test_metadata.get('name') %}
    {% set default_description = elementary.get_default_description(test_short_name, test_namespace) %}

    {% set config_meta_dict = elementary.safe_get_with_default(config_dict, 'meta', {}) %}
    {% set meta_dict = {} %}
    {% if default_description %}
        {% set meta_dict =  {'description': default_description} %} 
    {% endif %}
    {% do meta_dict.update(elementary.safe_get_with_default(node_dict, 'meta', {})) %}
    {% do meta_dict.update(config_meta_dict) %}

    {% set config_tags = elementary.safe_get_with_default(config_dict, 'tags', []) %}
    {% set global_tags = elementary.safe_get_with_default(node_dict, 'tags', []) %}
    {% set meta_tags = elementary.safe_get_with_default(meta_dict, 'tags', []) %}
    {% set tags = elementary.union_lists(config_tags, global_tags) %}
    {% set tags = elementary.union_lists(tags, meta_tags) %}

    {% set test_model_unique_ids = elementary.get_parent_model_unique_ids_from_test_node(node_dict) %}
    {% set test_model_nodes = elementary.get_nodes_by_unique_ids(test_model_unique_ids) %}
    {% set test_models_owners = [] %}
    {% set test_models_tags = [] %}
    {% for test_model_node in test_model_nodes %}
        {% set flatten_test_model_node = elementary.flatten_model(test_model_node) %}
        {% set test_model_owner = flatten_test_model_node.get('owner') %}
        {% if test_model_owner %}
            {% if test_model_owner is string %}
                {% set owners = test_model_owner.split(',') %}
                {% for owner in owners %}
                    {% do test_models_owners.append(owner | trim) %}  
                {% endfor %}
            {% elif test_model_owner is iterable %}
                {% do test_models_owners.extend(test_model_owner) %}
            {% endif %}
        {% endif %}
        {% set test_model_tags = flatten_test_model_node.get('tags') %}
        {% if test_model_tags and test_model_tags is sequence %}
            {% do test_models_tags.extend(test_model_tags) %}
        {% endif %}
    {% endfor %}
    {% set test_models_owners = test_models_owners | unique | list %}
    {% set test_models_tags = test_models_tags | unique | list %}

    {% set test_kwargs = elementary.safe_get_with_default(test_metadata, 'kwargs', {}) %}
    {% set primary_test_model_database, primary_test_model_schema = elementary.get_model_database_and_schema_from_test_node(node_dict) %}

    {% set primary_test_model_id = namespace(data=none) %}
    {% if test_model_unique_ids | length == 1 %}
        {# if only one parent model for this test, simply use this model #}
        {% set primary_test_model_id.data = test_model_unique_ids[0] %}
    {% else %}
      {% set test_model_jinja = test_kwargs.get('model') %}
      {% if test_model_jinja %}
        {% set test_model_name_matches = modules.re.findall("ref\(['\"](\w+)['\"]\)", test_model_jinja) %}
        {% if test_model_name_matches | length == 1 %}
          {% set test_model_name = test_model_name_matches[0] %}
          {% for test_model_unique_id in test_model_unique_ids %}
              {% set split_test_model_unique_id = test_model_unique_id.split('.') %}
              {% if split_test_model_unique_id and split_test_model_unique_id | length > 0 %}
                  {% set test_node_model_name = split_test_model_unique_id[-1] %}
                  {% if test_node_model_name == test_model_name %}
                    {% set primary_test_model_id.data = test_model_unique_id %}
                  {% endif %}
              {% endif %}
          {% endfor %}
        {% endif %}
      {% endif %}
    {% endif %}
    {% set original_file_path = node_dict.get('original_file_path') %}
    {% set flatten_test_metadata_dict = {
        'unique_id': node_dict.get('unique_id'),
        'short_name': test_short_name,
        'alias': node_dict.get('alias'),
        'test_column_name': node_dict.get('column_name'),
        'severity': config_dict.get('severity'),
        'warn_if': config_dict.get('warn_if'),
        'error_if': config_dict.get('error_if'),
        'test_params': test_kwargs,
        'test_namespace': test_namespace,
        'tags': tags,
        'model_tags': test_models_tags,
        'model_owners': test_models_owners,
        'meta': meta_dict,
        'database_name': primary_test_model_database,
        'schema_name': primary_test_model_schema,
        'depends_on_macros': depends_on_dict.get('macros', []),
        'depends_on_nodes': depends_on_dict.get('nodes', []),
        'parent_model_unique_id': primary_test_model_id.data,
        'description': node_dict.get('description'),
        'name': node_dict.get('name'),
        'package_name': node_dict.get('package_name'),
        'type': elementary.get_test_type(original_file_path, test_namespace),
        'original_path': original_file_path,
        'compiled_code': elementary.get_compiled_code(node_dict),
        'path': node_dict.get('path'),
        'generated_at': elementary.datetime_now_utc_as_string()
    }%}
    {{ return(flatten_test_metadata_dict) }}
{% endmacro %}

{% macro get_test_type(test_path, test_namespace = none) %}
    {% set test_type = 'generic' %}
    {%- if test_namespace == 'dbt_expectations' -%}
        {% set test_type = 'expectation' %}
    {%- elif 'tests/generic' in test_path or 'macros/' in test_path -%}
        {% set test_type = 'generic' %}
    {%- elif 'tests/' in test_path -%}
        {% set test_type = 'singular' %}
    {%- endif -%}
    {{- return(test_type) -}}
{%- endmacro -%}

{% macro get_default_description(short_name, test_namespace = none) %}
    {# Relevant for dbt_expectatiopns 0.8.0 #}
    {% set dbt_expectations_descriptions_map = {
        "expect_column_to_exist": "Expect the specified column to exist.",
        "expect_row_values_to_have_recent_data": "Expect the model to have rows that are at least as recent as the defined interval prior to the current timestamp. Optionally gives the possibility to apply filters on the results.",
        "expect_grouped_row_values_to_have_recent_data": "Expect the model to have grouped rows that are at least as recent as the defined interval prior to the current timestamp. Use this to test whether there is recent data for each grouped row defined by `group_by` (which is a list of columns) and a `timestamp_column`. Optionally gives the possibility to apply filters on the results.",
        "expect_table_column_count_to_be_between": "Expect the number of columns in a model to be between two values.",
        "expect_table_column_count_to_equal_other_table": "Expect the number of columns in a model to match another model.",
        "expect_table_columns_to_not_contain_set": "Expect the columns in a model not to contain a given list.",
        "expect_table_columns_to_contain_set": "Expect the columns in a model to contain a given list.",
        "expect_table_column_count_to_equal": "Expect the number of columns in a model to be equal to `expected_number_of_columns`.",
        "expect_table_columns_to_match_ordered_list": "Expect the columns to exactly match a specified list.",
        "expect_table_columns_to_match_set": "Expect the columns in a model to match a given list.",
        "expect_table_row_count_to_be_between": "Expect the number of rows in a model to be between two values.",
        "expect_table_row_count_to_equal_other_table": "Expect the number of rows in a model match another model.",
        "expect_table_row_count_to_equal_other_table_times_factor": "Expect the number of rows in a model to match another model times a preconfigured factor.",
        "expect_table_row_count_to_equal": "Expect the number of rows in a model to be equal to expected_number_of_rows.",
        "expect_column_values_to_be_unique": "Expect each column value to be unique.",
        "expect_column_values_to_not_be_null": "Expect column values to not be null.",
        "expect_column_values_to_be_null": "Expect column values to be null.",
        "expect_column_values_to_be_of_type": "Expect a column to be of a specified data type.",
        "expect_column_values_to_be_in_type_list": "Expect a column to be one of a specified type list.",
        "expect_column_values_to_have_consistent_casing": "Expect a column to have consistent casing. By setting `display_inconsistent_columns` to true, the number of inconsistent values in the column will be displayed in the terminal whereas the inconsistent values themselves will be returned if the SQL compiled test is run.",
        "expect_column_values_to_be_in_set": "Expect each column value to be in a given set.",
        "expect_column_values_to_be_between": "Expect each column value to be between two values.",
        "expect_column_values_to_not_be_in_set": "Expect each column value not to be in a given set.",
        "expect_column_values_to_be_increasing": "Expect column values to be increasing. If `strictly: True`, then this expectation is only satisfied if each consecutive value is strictly increasing – equal values are treated as failures.",
        "expect_column_values_to_be_decreasing": "Expect column values to be decreasing. If `strictly=True`, then this expectation is only satisfied if each consecutive value is strictly decreasing – equal values are treated as failures.",
        "expect_column_value_lengths_to_be_between": "Expect column entries to be strings with length between a min_value value and a max_value value (inclusive).",
        "expect_column_value_lengths_to_equal": "Expect column entries to be strings with length equal to the provided value.",
        "expect_column_values_to_match_regex": 'Expect column entries to be strings that match a given regular expression. Valid matches can be found anywhere in the string, for example "[at]+" will identify the following strings as expected: "cat", "hat", "aa", "a", and "t", and the following strings as unexpected: "fish", "dog". Optionally, `is_raw` indicates the `regex` pattern is a "raw" string and should be escaped. The default is `False`.',
        "expect_column_values_to_not_match_regex": 'Expect column entries to be strings that do NOT match a given regular expression. The regex must not match any portion of the provided string. For example, "[at]+" would identify the following strings as expected: "fish”, "dog”, and the following as unexpected: "cat”, "hat”. Optionally, `is_raw` indicates the `regex` pattern is a "raw" string and should be escaped. The default is `False`.',
        "expect_column_values_to_match_regex_list": 'Expect the column entries to be strings that can be matched to either any of or all of a list of regular expressions. Matches can be anywhere in the string. Optionally, `is_raw` indicates the `regex` patterns are "raw" strings and should be escaped. The default is `False`.',
        "expect_column_values_to_not_match_regex_list": 'Expect the column entries to be strings that do not match any of a list of regular expressions. Matches can be anywhere in the string. Optionally, `is_raw` indicates the `regex` patterns are "raw" strings and should be escaped. The default is `False`.',
        "expect_column_values_to_match_like_pattern": "Expect column entries to be strings that match a given SQL like pattern.",
        "expect_column_values_to_not_match_like_pattern": "Expect column entries to be strings that do not match a given SQL like pattern.",
        "expect_column_values_to_match_like_pattern_list": "Expect the column entries to be strings that match any of a list of SQL like patterns.",
        "expect_column_values_to_not_match_like_pattern_list": "Expect the column entries to be strings that do not match any of a list of SQL like patterns.",
        "expect_column_distinct_count_to_equal": "Expect the number of distinct column values to be equal to a given value.",
        "expect_column_distinct_count_to_be_greater_than": "Expect the number of distinct column values to be greater than a given value.",
        "expect_column_distinct_count_to_be_less_than": "Expect the number of distinct column values to be less than a given value.",
        "expect_column_distinct_values_to_be_in_set": "Expect the set of distinct column values to be contained by a given set.",
        "expect_column_distinct_values_to_contain_set": "Expect the set of distinct column values to contain a given set. In contrast to `expect_column_values_to_be_in_set` this ensures not that all column values are members of the given set but that values from the set must be present in the column.",
        "expect_column_distinct_values_to_equal_set": "Expect the set of distinct column values to equal a given set. In contrast to `expect_column_distinct_values_to_contain_set` this ensures not only that a certain set of values are present in the column but that these and only these values are present.",
        "expect_column_distinct_count_to_equal_other_table": "Expect the number of distinct column values to be equal to number of distinct values in another model.",
        "expect_column_mean_to_be_between": "Expect the column mean to be between a min_value value and a max_value value (inclusive).",
        "expect_column_median_to_be_between": "Expect the column median to be between a min_value value and a max_value value (inclusive).",
        "expect_column_quantile_values_to_be_between": "Expect specific provided column quantiles to be between provided min_value and max_value values.",
        "expect_column_stdev_to_be_between": "Expect the column standard deviation to be between a min_value value and a max_value value. Uses sample standard deviation (normalized by N-1).",
        "expect_column_unique_value_count_to_be_between": "Expect the number of unique values to be between a min_value value and a max_value value.",
        "expect_column_proportion_of_unique_values_to_be_between": "Expect the proportion of unique values to be between a min_value value and a max_value value. For example, in a column containing [1, 2, 2, 3, 3, 3, 4, 4, 4, 4], there are 4 unique values and 10 total values for a proportion of 0.4.",
        "expect_column_most_common_value_to_be_in_set": "Expect the most common value to be within the designated value set.",
        "expect_column_max_to_be_between": "Expect the column max to be between a min and max value.",
        "expect_column_min_to_be_between": "Expect the column min to be between a min and max value.",
        "expect_column_sum_to_be_between": "Expect the column to sum to be between a min and max value.",
        "expect_column_pair_values_A_to_be_greater_than_B": "Expect values in column A to be greater than column B.",
        "expect_column_pair_values_to_be_equal": "Expect the values in column A to be the same as column B.",
        "expect_column_pair_values_to_be_in_set": "Expect paired values from columns A and B to belong to a set of valid pairs. Note: value pairs are expressed as lists within lists",
        "expect_select_column_values_to_be_unique_within_record": "Expect the values for each record to be unique across the columns listed. Note that records can be duplicated.",
        "expect_multicolumn_sum_to_equal": "Expects that sum of all rows for a set of columns is equal to a specific value",
        "expect_compound_columns_to_be_unique": "Expect that the columns are unique together, e.g. a multi-column primary key.",
        "expect_column_values_to_be_within_n_moving_stdevs": "A simple anomaly test based on the assumption that differences between periods in a given time series follow a log-normal distribution. Thus, we would expect the logged differences (vs N periods ago) in metric values to be within Z sigma away from a moving average. By applying a list of columns in the `group_by` parameter, you can also test for deviations within a group.",
        "expect_column_values_to_be_within_n_stdevs": "Expects (optionally grouped & summed) metric values to be within Z sigma away from the column average",
        "expect_row_values_to_have_data_for_every_n_datepart": "Expects model to have values for every grouped `date_part`."
    } %}

    {% set dbt_tests_descriptions_map = {
        "not_null": "This test validates that there are no `null` values present in a column.",
        "unique": "This test validates that there are no duplicate values present in a field.",
        "accepted_values": "This test validates that all of the values in a column are present in a supplied list of `values`. If any values other than those provided in the list are present, then the test will fail.",
        "relationships": 'This test validates that all of the records in a child table have a corresponding record in a parent table. This property is referred to as "referential integrity".'
    } %}

    {% set default_description = none %}
    {% if test_namespace == 'dbt_expectations' %}
        {% set default_description = dbt_expectations_descriptions_map.get(short_name) %}
    {% elif test_namespace == 'dbt' or test_namespace is none %}
        {% set default_description = dbt_tests_descriptions_map.get(short_name) %}
    {% endif %}

    {{ return(default_description) }}
{% endmacro %}
