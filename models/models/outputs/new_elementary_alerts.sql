
with elementary_alerts as (

    select * from {{ ref('elementary_alerts')}}

),

new_alerts as (

    select *
    from elementary_alerts
    where detected_at > (select max(detected_at) from {{ this }})

)

select * from new_alerts