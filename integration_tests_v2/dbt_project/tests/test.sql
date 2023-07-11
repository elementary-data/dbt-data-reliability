{% set test_name = var("test_name", none) %}

{% if test_name %}

{% set test_args = var("test_args") %}
{% set table_name = var("table_name", none) %}
{% set test_alias = var("test_alias", test_name) %}
{% do config(alias=test_alias) %}

{% set namespace_and_test_name = test_name.split('.') %}
{% if namespace_and_test_name | length == 1 %}
    {% set test_macro = context["test_{}".format(test_name)] %}
{% elif namespace_and_test_name | length == 2 %}
    {% set test_namespace, test_name = namespace_and_test_name %}
    {% set test_macro = context[test_namespace]["test_{}".format(test_name)] %}
{% else %}
    {% do exceptions.raise_compiler_error("Unable to find test macro: '{}'.".format(test_name)) %}
{% endif %}

{% if table_name %}
    {% do test_args.update({"model": api.Relation.create(target.database, "test_seeds", table_name)}) %}
{% endif %}

{{ test_macro(**test_args) }}

{% endif %}
