-- depends_on: {{ ref('any_type_column_anomalies_validation') }}

with training as (
    select * from {{ ref('any_type_column_anomalies_training') }}
),

{% if elementary.table_exists_in_target('any_type_column_anomalies_validation') %}
 validation as (
     select * from {{ ref('any_type_column_anomalies_validation') }}
 ),

 source as (
     select * from training
     union all
     select * from validation
 ),
{% else %}
 source as (
     select * from training
 ),
{% endif %}

 final as (
     select
         date as updated_at,
         null_count_str,
         null_percent_str,
         null_count_float,
         null_percent_float,
         null_count_int,
         null_percent_int,
         null_count_bool,
         null_percent_bool
     from source
 )

select * from final
