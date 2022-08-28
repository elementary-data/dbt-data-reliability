{# 
   Copyright 2021 dbt Labs, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. 
#}

{# 
    dbt_utils.surrogate_key is using dbt_utils.type_string which is going to be deprecated from dbt-utils version 0.9.0,
    therefore dbt_utils.surrogate_key is going to be deprecated as well (without having a similar macro at dbt-core).
    We copied this macro, with a small change that supports the deprecation, so we could continue and use it in our project.
#}
{%- macro surrogate_key(field_list) -%}
    {# needed for safe_add to allow for non-keyword arguments see SO post #}
    {# https://stackoverflow.com/questions/13944751/args-kwargs-in-jinja2-macros #}
    {% set frustrating_jinja_feature = varargs %}
    {{ return(adapter.dispatch('surrogate_key', 'elementary')(field_list, *varargs)) }}
{% endmacro %}

{%- macro default__surrogate_key(field_list) -%}
    {%- if varargs|length >= 1 or field_list is string %}
        {%- set error_message = '
        Warning: the `surrogate_key` macro now takes a single list argument instead of \
        multiple string arguments. Support for multiple string arguments will be \
        deprecated in a future release of dbt-utils. The {}.{} model triggered this warning. \
        '.format(model.package_name, model.name) -%}
        {%- do exceptions.warn(error_message) -%}
        {# first argument is not included in varargs, so add first element to field_list_xf #}
        {%- set field_list_xf = [field_list] -%}
        {%- for field in varargs %}
            {%- set _ = field_list_xf.append(field) -%}
        {%- endfor -%}
    {%- else -%}
        {# if using list, just set field_list_xf as field_list #}
        {%- set field_list_xf = field_list -%}
    {%- endif -%}
    {%- set fields = [] -%}
    {%- for field in field_list_xf -%}

        {%- set _ = fields.append(
            "coalesce(cast(" ~ field ~ " as " ~ elementary.type_string() ~ "), '')"
        ) -%}

        {%- if not loop.last %}
            {%- set _ = fields.append("'-'") -%}
        {%- endif -%}

    {%- endfor -%}

    {{elementary.hash(elementary.concat(fields))}}

{%- endmacro -%}
