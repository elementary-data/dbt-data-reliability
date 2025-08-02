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
    {% set query_to_get_columns %}
      with test_results as (
        {{ sql }}
      )
      select * from test_results limit 0
    {% endset %}
    {% set columns_result = elementary.run_query(query_to_get_columns) %}
    {% set all_columns = columns_result.column_names %}
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

{% macro get_test_result_column_mapping(flattened_test) %}
  {# This macro maps original column names to the actual column names in test results #}
  {% set test_type = elementary.get_test_type(flattened_test) %}
  {% set test_column_name = elementary.insensitive_get_dict_value(flattened_test, 'test_column_name') %}
  {% set mapping = {} %}
  
  {% if test_type == 'dbt_test' %}
    {% set test_name = elementary.insensitive_get_dict_value(flattened_test, 'name') %}
    
    {# For unique tests, the original column becomes 'unique_field' #}
    {% if test_name == 'unique' and test_column_name %}
      {% do mapping.update({test_column_name: 'unique_field'}) %}
    {% endif %}
    
    {# For accepted_values tests, the original column becomes 'value' #}
    {% if test_name == 'accepted_values' and test_column_name %}
      {% do mapping.update({test_column_name: 'value'}) %}
    {% endif %}
    
    {# For relationships tests, the original column becomes 'from_field' #}
    {% if test_name == 'relationships' and test_column_name %}
      {% do mapping.update({test_column_name: 'from_field'}) %}
    {% endif %}
    
    {# For not_null tests, the original column name is preserved #}
    {% if test_name == 'not_null' and test_column_name %}
      {% do mapping.update({test_column_name: test_column_name}) %}
    {% endif %}
  {% endif %}
  
  {% do return(mapping) %}
{% endmacro %}

{% macro get_test_result_column_mapping_dynamic(flattened_test) %}
  {# This macro dynamically analyzes the test SQL to understand column mappings #}
  {% set test_column_name = elementary.insensitive_get_dict_value(flattened_test, 'test_column_name') %}
  {% set mapping = {} %}
  
  {% if not test_column_name %}
    {% do return(mapping) %}
  {% endif %}
  
  {# Get the compiled SQL for the test #}
  {% set test_sql = elementary.get_compiled_code(flattened_test) %}
  
  {# Look for common patterns in the SQL that indicate column aliasing #}
  {% set sql_lower = test_sql | lower %}
  
  {# Pattern 1: Look for "as unique_field" or "unique_field" after the column name #}
  {% if test_column_name in sql_lower %}
    {# Find the position of the column name in the SQL #}
    {% set column_pos = sql_lower.find(test_column_name | lower) %}
    {% if column_pos != -1 %}
      {# Look for common aliases after the column name #}
      {% set after_column = sql_lower[column_pos + (test_column_name | length):column_pos + (test_column_name | length) + 50] %}
      
      {# Check for "as unique_field" pattern #}
      {% if " as unique_field" in after_column %}
        {% do mapping.update({test_column_name: 'unique_field'}) %}
      {% elif " as value" in after_column %}
        {% do mapping.update({test_column_name: 'value'}) %}
      {% elif " as from_field" in after_column %}
        {% do mapping.update({test_column_name: 'from_field'}) %}
      {% elif " as to_field" in after_column %}
        {% do mapping.update({test_column_name: 'to_field'}) %}
      {% else %}
        {# If no explicit alias found, check if the column name appears in the SELECT clause #}
        {% set select_pattern = "select " ~ test_column_name | lower %}
        {% if select_pattern in sql_lower %}
          {% do mapping.update({test_column_name: test_column_name}) %}
        {% endif %}
      {% endif %}
    {% endif %}
  {% endif %}
  
  {% do return(mapping) %}
{% endmacro %}

{% macro get_columns_to_exclude_from_sampling_with_mapping(flattened_test) %}
  {% set original_columns_to_exclude = elementary.get_columns_to_exclude_from_sampling(flattened_test) %}
  {% set column_mapping = elementary.get_test_result_column_mapping_dynamic(flattened_test) %}
  {% set mapped_columns_to_exclude = [] %}
  
  {% for original_column in original_columns_to_exclude %}
    {% if original_column in column_mapping %}
      {% do mapped_columns_to_exclude.append(column_mapping[original_column]) %}
    {% else %}
      {% do mapped_columns_to_exclude.append(original_column) %}
    {% endif %}
  {% endfor %}
  
  {% do return(mapped_columns_to_exclude) %}
{% endmacro %}

{% macro get_columns_to_exclude_from_sampling_intelligent(flattened_test) %}
  {# This macro uses an intelligent approach to map excluded columns to test result columns #}
  {% set original_columns_to_exclude = elementary.get_columns_to_exclude_from_sampling(flattened_test) %}
  {% set mapped_columns_to_exclude = [] %}
  
  {% if not original_columns_to_exclude %}
    {% do return(mapped_columns_to_exclude) %}
  {% endif %}
  
  {# Get the test result columns by running a sample query #}
  {% set sample_query %}
    with test_results as (
      {{ sql }}
    )
    select * from test_results limit 0
  {% endset %}
  
  {% set columns_result = elementary.run_query(sample_query) %}
  {% set result_columns = columns_result.column_names %}
  
  {# For each column to exclude, find the best match in the result columns #}
  {% for original_column in original_columns_to_exclude %}
    {% set found_match = false %}
    
    {# First, try exact match #}
    {% if original_column in result_columns %}
      {% do mapped_columns_to_exclude.append(original_column) %}
      {% set found_match = true %}
    {% else %}
      {# Try common dbt test patterns #}
      {% set test_name = elementary.insensitive_get_dict_value(flattened_test, 'name') %}
      
      {# For unique tests, check for 'unique_field' #}
      {% if test_name == 'unique' and 'unique_field' in result_columns %}
        {% do mapped_columns_to_exclude.append('unique_field') %}
        {% set found_match = true %}
      {% endif %}
      
      {# For accepted_values tests, check for 'value' #}
      {% if test_name == 'accepted_values' and 'value' in result_columns %}
        {% do mapped_columns_to_exclude.append('value') %}
        {% set found_match = true %}
      {% endif %}
      
      {# For relationships tests, check for 'from_field' #}
      {% if test_name == 'relationships' and 'from_field' in result_columns %}
        {% do mapped_columns_to_exclude.append('from_field') %}
        {% set found_match = true %}
      {% endif %}
      
      {# For custom tests, try to find columns that might contain the original data #}
      {% if not found_match %}
        {# Look for columns that might contain the original column's data #}
        {% for result_column in result_columns %}
          {# Skip metadata columns like n_records, count, etc. #}
          {% if result_column not in ['n_records', 'count', 'num_records', 'row_count'] %}
            {# If this is the only non-metadata column, it's likely the original data #}
            {% set non_metadata_columns = result_columns | reject("in", ['n_records', 'count', 'num_records', 'row_count']) | list %}
            {% if non_metadata_columns | length == 1 %}
              {% do mapped_columns_to_exclude.append(result_column) %}
              {% set found_match = true %}
              {% break %}
            {% endif %}
          {% endif %}
        {% endfor %}
      {% endif %}
    {% endif %}
    
    {# If no match found, add the original column name (will be filtered out if not present) #}
    {% if not found_match %}
      {% do mapped_columns_to_exclude.append(original_column) %}
    {% endif %}
  {% endfor %}
  
  {% do return(mapped_columns_to_exclude) %}
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
