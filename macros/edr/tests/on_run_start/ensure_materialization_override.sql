{% macro ensure_materialization_override() %}
    {% if elementary.get_config_var("mute_ensure_materialization_override") %}
        {% do return(none) %}
    {% endif %}

    {% set runtime_config = elementary.get_runtime_config() %}
    {% if runtime_config.args.require_explicit_package_overrides_for_builtin_materializations is false %}
        {% do elementary.file_log("Materialization override is enabled.") %}
        {% do return(none) %}
    {% endif %}

    {% set major, minor, revision = dbt_version.split(".") %}
    {% set major = major | int %}
    {% set minor = minor | int %}
    {% if major > 1 or major == 1 and minor >= 8 %}
        {%- set msg %}
AAAA
        {% endset %}
        {% do log(msg, info=true) %}
    {% endif %}
{% endmacro %}
