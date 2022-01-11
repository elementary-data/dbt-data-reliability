with elementary_alerts as (

    select * from {{ ref('elementary_alerts')}}

),

new_alerts as (

    select distinct *
    from elementary_alerts
    where alert_created_at = ( select max(alert_created_at) from elementary_alerts )

)

select * from new_alerts