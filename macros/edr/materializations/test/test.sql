{% materialization test, default %}
  {% set result = elementary.materialize_test(dbt.materialization_test_default) %}
  {% do return(result) %}
{% endmaterialization %}

{% materialization test, adapter="snowflake" %}
  {%- if dbt.materialization_test_snowflake -%}
    {% set materialization_macro = dbt.materialization_test_snowflake %}
  {%- else -%}
    {% set materialization_macro = dbt.materialization_test_default %}
  {%- endif -%}
  {% set result = elementary.materialize_test(materialization_macro) %}
  {% do return(result) %}
{% endmaterialization %}

{% macro materialize_test(materialization_macro) %}
  {% if not elementary.is_elementary_enabled() %}
    {% do return(materialization_macro()) %}
  {% endif %}

  {% set test_unique_id = model.get('unique_id') %}
  {% do elementary.debug_log(test_unique_id ~ ": starting test materialization hook") %}
  {% if elementary.get_config_var("tests_use_temp_tables") %}
    {% set temp_table_sql = elementary.create_test_result_temp_table() %}
    {% do context.update({"sql": temp_table_sql}) %}
    {% do elementary.debug_log(test_unique_id ~ ": created test temp table") %}
  {% endif %}

  {% set test_namespace = model.get('test_metadata', {}).get('namespace') %}
  {% if test_namespace == 'elementary' %}
    {# Custom test materialization is needed only for non-elementary tests #}
    {% do return(materialization_macro()) %}
  {% endif %}

  {% set flattened_test = elementary.flatten_test(model) %}
  {% do elementary.debug_log(test_unique_id ~ ": flattened test node") %}

  {% set result = elementary.handle_dbt_test(flattened_test, materialization_macro) %}
  {% do elementary.debug_log(test_unique_id ~ ": handler called by test type - " ~ elementary.get_test_type(flattened_test)) %}
  {% if elementary.get_config_var("calculate_failed_count") %}
    {% set failed_row_count = elementary.get_failed_row_count(flattened_test) %}
    {% if failed_row_count is not none %}
      {% do elementary.get_cache("elementary_test_failed_row_counts").update({model.unique_id: failed_row_count}) %}
      {% do elementary.debug_log(test_unique_id ~ ": calculated failed row count") %}
    {% endif %}
  {% endif %}
  {% do elementary.debug_log(test_unique_id ~ ": finished test materialization hook") %}
  {% do return(result) %}
{% endmacro %}

{% macro handle_dbt_test(flattened_test, materialization_macro) %}
  {% set result = materialization_macro() %}
  {% set sample_limit = elementary.get_config_var('test_sample_row_count') %}
  
  {% set disable_test_samples = false %}
  {% if "meta" in flattened_test and "disable_test_samples" in flattened_test["meta"] %}
    {% set disable_test_samples = flattened_test["meta"]["disable_test_samples"] %}
  {% endif %}
  
  {% if disable_test_samples %}
    {% set sample_limit = 0 %}
  {% elif elementary.is_pii_table(flattened_test) %}
    {% set sample_limit = 0 %}
  {% endif %}
  
  {% set result_rows = elementary.query_test_result_rows(sample_limit=sample_limit,
                                                         ignore_passed_tests=true,
                                                         flattened_test=flattened_test) %}
  {% set elementary_test_results_row = elementary.get_dbt_test_result_row(flattened_test, result_rows) %}
  {% do elementary.cache_elementary_test_results_rows([elementary_test_results_row]) %}
  {% do return(result) %}
{% endmacro %}

{% macro get_dbt_test_result_row(flattened_test, result_rows=none) %}
    {% if not result_rows %}
      {% set result_rows = [] %}
    {% endif %}

    {% set test_execution_id = elementary.get_node_execution_id(flattened_test) %}
    {% set parent_model_unique_id = elementary.insensitive_get_dict_value(flattened_test, 'parent_model_unique_id') %}
    {% set parent_model = elementary.get_node(parent_model_unique_id) %}
    {% set parent_model_name = elementary.get_table_name_from_node(parent_model) %}
    {% set test_result_dict = {
        'id': test_execution_id,
        'data_issue_id': none,
        'test_execution_id': test_execution_id,
        'test_unique_id': elementary.insensitive_get_dict_value(flattened_test, 'unique_id'),
        'model_unique_id': parent_model_unique_id,
        'detected_at': elementary.insensitive_get_dict_value(flattened_test, 'generated_at'),
        'database_name': elementary.insensitive_get_dict_value(flattened_test, 'database_name'),
        'schema_name': elementary.insensitive_get_dict_value(flattened_test, 'schema_name'),
        'table_name': parent_model_name,
        'column_name': elementary.insensitive_get_dict_value(flattened_test, 'test_column_name'),
        'test_type': elementary.get_test_type(flattened_test),
        'test_sub_type': elementary.insensitive_get_dict_value(flattened_test, 'type'),
        'other': none,
        'owners': elementary.insensitive_get_dict_value(flattened_test, 'model_owners'),
        'tags': elementary.insensitive_get_dict_value(flattened_test, 'model_tags') + elementary.insensitive_get_dict_value(flattened_test, 'tags'),
        'test_results_query': elementary.get_compiled_code(flattened_test),
        'test_name': elementary.insensitive_get_dict_value(flattened_test, 'name'),
        'test_params': elementary.insensitive_get_dict_value(flattened_test, 'test_params'),
        'severity': elementary.insensitive_get_dict_value(flattened_test, 'severity'),
        'test_short_name': elementary.insensitive_get_dict_value(flattened_test, 'short_name'),
        'test_alias': elementary.insensitive_get_dict_value(flattened_test, 'alias'),
        'result_rows': result_rows
    }%}
    {% do return(test_result_dict) %}
{% endmacro %}

{% macro create_test_result_temp_table() %}
  {% set database, schema = elementary.get_package_database_and_schema() %}
  {% set test_id = model["alias"] %}
  {% set relation = elementary.create_temp_table(database, schema, test_id, sql) %}
  {% set new_sql %}
    select * from {{ relation }}
  {% endset %}
  {% do return(new_sql) %}
{% endmacro %}

{% macro query_test_result_rows(sample_limit=none, ignore_passed_tests=false, flattened_test=none) %}
  {% if sample_limit == 0 %} {# performance: no need to run a sql query that we know returns an empty list #}
    {% do return([]) %}
  {% endif %}
  {% if ignore_passed_tests and elementary.did_test_pass() %}
    {% do elementary.debug_log("Skipping sample query because the test passed.") %}
    {% do return([]) %}
  {% endif %}
  
  {% set columns_to_exclude = elementary.get_columns_to_exclude_from_sampling_intelligent(flattened_test) %}
  
  {% set select_clause = "*" %}
  {% if columns_to_exclude %}
    {# Use dbt's built-in parser to get actual column names #}
    {% set test_relation = elementary.create_test_result_relation(flattened_test) %}
    {% set result_columns = adapter.get_columns_in_relation(test_relation) %}
    {% set all_columns = result_columns | map(attribute='name') | list %}
    {% set safe_columns = all_columns | reject("in", columns_to_exclude) | list %}
    {% if safe_columns %}
      {% set select_clause = safe_columns | join(", ") %}
    {% else %}
      {% set select_clause = "1 as _no_non_excluded_columns" %}
    {% endif %}
  {% endif %}
  
  {% set query %}
    with test_results as (
      {{ sql }}
    )
    select {{ select_clause }} from test_results {% if sample_limit is not none %} limit {{ sample_limit }} {% endif %}
  {% endset %}
  {% do return(elementary.agate_to_dicts(elementary.run_query(query))) %}
{% endmacro %}

{% macro get_columns_to_exclude_from_sampling(flattened_test) %}
  {% set columns_to_exclude = [] %}
  
  {% if not flattened_test %}
    {% do return(columns_to_exclude) %}
  {% endif %}
  
  {% if elementary.get_config_var('disable_samples_on_pii_tags') %}
    {% set pii_columns = elementary.get_pii_columns_from_parent_model(flattened_test) %}
    {% set columns_to_exclude = columns_to_exclude + pii_columns %}
  {% endif %}
  
  {% if elementary.is_sampling_disabled_for_column(flattened_test) %}
    {% set test_column_name = elementary.insensitive_get_dict_value(flattened_test, 'test_column_name') %}
    {% if test_column_name and test_column_name not in columns_to_exclude %}
      {% do columns_to_exclude.append(test_column_name) %}
    {% endif %}
  {% endif %}
  
  {% do return(columns_to_exclude) %}
{% endmacro %}

{# Removed complex column mapping macros - replaced with simpler dbt parser-based approach #}

{% macro get_columns_to_exclude_from_sampling_intelligent(flattened_test) %}
  {# This macro uses dbt's built-in parser to get actual test result columns and handle exclusions #}
  {% set original_columns_to_exclude = elementary.get_columns_to_exclude_from_sampling(flattened_test) %}
  
  {% if not original_columns_to_exclude %}
    {% do return([]) %}
  {% endif %}
  
  {# Get the actual test result columns using dbt's built-in parser #}
  {% set test_relation = elementary.create_test_result_relation(flattened_test) %}
  {% set result_columns = adapter.get_columns_in_relation(test_relation) %}
  {% set result_column_names = result_columns | map(attribute='name') | list %}
  
  {# Map original columns to actual result columns using a simple matching strategy #}
  {% set mapped_columns_to_exclude = [] %}
  
  {% for original_column in original_columns_to_exclude %}
    {% set found_match = false %}
    
    {# First, try exact match (case-insensitive) #}
    {% for result_column in result_column_names %}
      {% if result_column.lower() == original_column.lower() %}
        {% do mapped_columns_to_exclude.append(result_column) %}
        {% set found_match = true %}
        {% break %}
      {% endif %}
    {% endfor %}
    
    {# If no exact match, try common dbt test patterns #}
    {% if not found_match %}
      {% set test_name = elementary.insensitive_get_dict_value(flattened_test, 'name') %}
      {% set common_mappings = {
        'unique': 'unique_field',
        'accepted_values': 'value', 
        'relationships': 'from_field',
        'not_null': original_column
      } %}
      
      {% if test_name in common_mappings %}
        {% set expected_column = common_mappings[test_name] %}
        {% if expected_column in result_column_names %}
          {% do mapped_columns_to_exclude.append(expected_column) %}
          {% set found_match = true %}
        {% endif %}
      {% endif %}
    {% endif %}
    
    {# If still no match, try to find the most likely column containing the original data #}
    {% if not found_match %}
      {# Look for columns that might contain the original column's data #}
      {% set non_metadata_columns = result_column_names | reject("in", ['n_records', 'count', 'num_records', 'row_count', 'test_result']) | list %}
      {% if non_metadata_columns | length == 1 %}
        {% do mapped_columns_to_exclude.append(non_metadata_columns[0]) %}
      {% elif original_column in result_column_names %}
        {# If the original column name exists in results, use it #}
        {% do mapped_columns_to_exclude.append(original_column) %}
      {% endif %}
    {% endif %}
  {% endfor %}
  
  {% do return(mapped_columns_to_exclude) %}
{% endmacro %}

{% macro create_test_result_relation(flattened_test) %}
  {# Create a temporary relation for the test results to use dbt's parser #}
  {% set database, schema = elementary.get_package_database_and_schema() %}
  {% set test_id = "tmp_" ~ elementary.get_node_execution_id(flattened_test)[:20] %}
  
  {# Create a temporary table with the test results #}
  {% set temp_sql %}
    with test_results as (
      {{ sql }}
    )
    select * from test_results limit 0
  {% endset %}
  
  {% set relation = elementary.create_temp_table(database, schema, test_id, temp_sql) %}
  {% do return(relation) %}
{% endmacro %}

{% macro is_sampling_disabled_for_column(flattened_test) %}
  {% set test_column_name = elementary.insensitive_get_dict_value(flattened_test, 'test_column_name') %}
  {% set parent_model_unique_id = elementary.insensitive_get_dict_value(flattened_test, 'parent_model_unique_id') %}
  
  {% if not test_column_name or not parent_model_unique_id %}
    {% do return(false) %}
  {% endif %}
  
  {% set parent_model = elementary.get_node(parent_model_unique_id) %}
  {% if parent_model and parent_model.get('columns') %}
    {% set column_config = parent_model.get('columns', {}).get(test_column_name, {}).get('config', {}) %}
    {% set disable_test_samples = elementary.safe_get_with_default(column_config, 'disable_test_samples', false) %}
    {% do return(disable_test_samples) %}
  {% endif %}
  
  {% do return(false) %}
{% endmacro %}


{% macro cache_elementary_test_results_rows(elementary_test_results_rows) %}
  {% do elementary.get_cache("elementary_test_results").update({model.unique_id: elementary_test_results_rows}) %}
{% endmacro %}
