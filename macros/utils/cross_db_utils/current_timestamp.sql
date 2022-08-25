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
    dbt_utils.current_timestamp is using dbt_utils.type_timestamp which is going to be deprecated from dbt-utils version 0.9.0,
    therefore dbt_utils.current_timestap is going to be deprecated as well (without having a similar macro at dbt-core).
    We copied this macro, with a small change that supports the deprecation, so we could continue and use it in our project.
#}
{% macro current_timestamp() -%}
  {{ return(adapter.dispatch('current_timestamp', 'elementary')()) }}
{%- endmacro %}

{% macro default__current_timestamp() %}
    current_timestamp::{{elementary.type_timestamp()}}
{% endmacro %}

{% macro redshift__current_timestamp() %}
    getdate()
{% endmacro %}

{% macro bigquery__current_timestamp() %}
    current_timestamp
{% endmacro %}


{# 
    dbt_utils.current_timestamp_in_utc is using dbt_utils.type_timestamp and dbt_utils.current_timestamp which are going to be deprecated from dbt-utils version 0.9.0,
    therefore dbt_utils.current_timestamp_in_utc is going to be deprecated as well (without having a similar macro at dbt-core).
    We copied this macro, with a small change that supports the deprecation, so we could continue and use it in our project.
#}
{% macro current_timestamp_in_utc() -%}
  {{ return(adapter.dispatch('current_timestamp_in_utc', 'elementary')()) }}
{%- endmacro %}

{% macro default__current_timestamp_in_utc() %}
    {{elementary.current_timestamp()}}
{% endmacro %}

{% macro snowflake__current_timestamp_in_utc() %}
    convert_timezone('UTC', {{elementary.current_timestamp()}})::{{elementary.type_timestamp()}}
{% endmacro %}

{% macro postgres__current_timestamp_in_utc() %}
    (current_timestamp at time zone 'utc')::{{elementary.type_timestamp()}}
{% endmacro %}

{# redshift should use default instead of postgres #}
{% macro redshift__current_timestamp_in_utc() %}
    {{ return(elementary.default__current_timestamp_in_utc()) }}
{% endmacro %}
