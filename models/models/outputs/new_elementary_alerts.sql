
with elementary_alerts as (

    select distinct * from {{ ref('elementary_alerts')}}

),

new_alerts as (

    select *
    from elementary_alerts
    where detected_at > (select max(detected_at) from elementary_alerts)

)

select * from new_alerts