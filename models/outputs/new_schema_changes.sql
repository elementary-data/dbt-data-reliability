{% set latest_update %}
    (select max(detected_at) from {{ref ('schema_changes_description')}})
{% endset %}

with schema_changes_desc as (
    select * from {{ref ('schema_changes_description')}}
)

select *
from schema_changes_desc
where detected_at = {{latest_update}}
