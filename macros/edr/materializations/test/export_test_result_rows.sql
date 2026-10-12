{#
  Writes a failing test's sample rows to an external stage (e.g. the customer's
  own S3 bucket) instead of storing them in the elementary schema.

  Vars:
    test_samples_external_stage       - stage to unload into, e.g. "analytics.elementary_samples.samples_stage"
    test_samples_external_location    - URL the stage points at, e.g. "s3://acme-dq-samples/elementary".
                                        Used to build the pointer that Elementary Cloud links to.
    test_samples_external_region      - optional cloud region of the bucket (e.g. "eu-central-1"),
                                        so the link opens the file directly in the AWS console.
    test_samples_external_file_format - "csv" (default) or "json".
    test_samples_external_row_count   - optional row limit for exported samples (defaults to
                                        test_sample_row_count, -1 for unlimited).

  Returns a single pointer row, which is stored in test_result_rows in place of the samples.
#}
{% macro export_test_result_rows_to_stage(flattened_test, stage, sample_limit) %}
    {% if sample_limit == 0 %} {% do return([]) %} {% endif %}
    {% if elementary.did_test_pass() %}
        {% do elementary.debug_log("Skipping sample export because the test passed.") %}
        {% do return([]) %}
    {% endif %}

    {% set external_row_count = elementary.get_config_var(
        "test_samples_external_row_count"
    ) %}
    {% if external_row_count is not none %}
        {% set sample_limit = external_row_count %}
    {% endif %}
    {% if sample_limit == -1 %} {% set sample_limit = none %} {% endif %}

    {% set file_format = (
        elementary.get_config_var("test_samples_external_file_format")
        or "csv"
    ) | lower %}
    {% if file_format not in ["csv", "json"] %}
        {% do exceptions.raise_compiler_error(
            "test_samples_external_file_format must be 'csv' or 'json', got: "
            ~ file_format
        ) %}
    {% endif %}

    {% set file_path = elementary.get_external_test_samples_path(
        flattened_test, file_format
    ) %}
    {% do elementary.export_test_result_rows(
        stage, file_path, sample_limit, file_format
    ) %}

    {% set location = elementary.get_config_var("test_samples_external_location") %}
    {% if location %} {% set location = location.rstrip("/") ~ "/" ~ file_path %}
    {% else %} {% set location = "@" ~ stage.lstrip("@") ~ "/" ~ file_path %}
    {% endif %}
    {% set pointer = {"_elementary_external_samples_location": location} %}
    {% set region = elementary.get_config_var("test_samples_external_region") %}
    {% if region %}
        {% do pointer.update({"_elementary_external_samples_region": region}) %}
    {% endif %}
    {% do return([pointer]) %}
{% endmacro %}

{% macro get_external_test_samples_path(flattened_test, file_format="csv") %}
    {% set test_unique_id = elementary.insensitive_get_dict_value(
        flattened_test, "unique_id"
    ) %}
    {% do return(test_unique_id ~ "/" ~ invocation_id ~ "." ~ file_format) %}
{% endmacro %}

{% macro export_test_result_rows(
    stage, file_path, sample_limit=none, file_format="csv"
) %}
    {% do return(
        adapter.dispatch("export_test_result_rows", "elementary")(
            stage, file_path, sample_limit, file_format
        )
    ) %}
{% endmacro %}

{% macro default__export_test_result_rows(
    stage, file_path, sample_limit=none, file_format="csv"
) %}
    {% do exceptions.raise_compiler_error(
        "test_samples_external_stage is not supported on adapter "
        ~ target.type
        ~ " yet (supported: snowflake)."
    ) %}
{% endmacro %}

{% macro snowflake__export_test_result_rows(
    stage, file_path, sample_limit=none, file_format="csv"
) %}
    {% set query %}
    copy into @{{ stage.lstrip("@") }}/{{ file_path }}
    from (
        select {% if file_format == "json" %} object_construct_keep_null(*) {% else %} * {% endif %}
        from (
            {{ sql }}
        ) as test_results
        {% if sample_limit is not none %} limit {{ sample_limit }} {% endif %}
    )
    {% if file_format == "json" %}
    file_format = (type = json compression = none)
    {% else %}
    file_format = (
        type = csv
        compression = none
        field_optionally_enclosed_by = '"'
        null_if = ('')
    )
    header = true
    {% endif %}
    single = true
    overwrite = true
    max_file_size = 104857600
    {% endset %}
    {% do elementary.run_query(query) %}
{% endmacro %}
