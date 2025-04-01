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

{%- macro generate_surrogate_key(fields) -%}
    {{ return(adapter.dispatch('generate_surrogate_key', 'elementary')(fields)) }}
{%- endmacro -%}

{%- macro default__generate_surrogate_key(fields) -%}
  {% set concat_macro = dbt.concat or dbt_utils.concat %}
  {% set hash_macro = dbt.hash or dbt_utils.hash %}

  {% set default_null_value = "" %}
  {%- set field_sqls = [] -%}
  {%- for field in fields -%}
    {%- do field_sqls.append(
        "coalesce(cast(" ~ field ~ " as " ~ elementary.edr_type_string() ~ "), '" ~ default_null_value  ~"')"
    ) -%}
    {%- if not loop.last %}
        {%- do field_sqls.append("'-'") -%}
    {%- endif -%}
  {%- endfor -%}
  {{ hash_macro(concat_macro(field_sqls)) }}
{%- endmacro -%}

{%- macro clickhouse__generate_surrogate_key(fields) -%}
  {% set concat_macro = dbt.concat or dbt_utils.concat %}
  {% set hash_macro = dbt.hash or dbt_utils.hash %}

  {% set default_null_value = "" %}
  {%- set field_sqls = [] -%}
  {%- for field in fields -%}
    {%- do field_sqls.append(
        "coalesce(cast(" ~ field ~ " as Nullable(" ~ elementary.edr_type_string() ~ ")), '" ~ default_null_value  ~"')"
    ) -%}
    {%- if not loop.last %}
        {%- do field_sqls.append("'-'") -%}
    {%- endif -%}
  {%- endfor -%}
  {{ hash_macro(concat_macro(field_sqls)) }}
{%- endmacro -%}