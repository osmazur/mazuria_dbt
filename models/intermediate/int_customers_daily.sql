with groups as (

    select
        group_id,
        student_id,
        purchase_seq,
        is_active,
        coalesce(first_attendance_date, started_at, created_at::date) as purchase_date,
        ended_at
    from {{ ref('stg_notion__groups') }}

),

daily_new_repeat as (

    select
        purchase_date                                                           as date_day,
        count(distinct case when purchase_seq = 1 then student_id end)          as new_customers,
        count(distinct case when purchase_seq > 1 then student_id end)          as repeat_customers
    from groups
    where purchase_date is not null
    group by purchase_date

),

calendar as (

    select date_day
    from {{ ref('stg_calendar') }}
    where date_day <= current_date

),

active_daily as (

    select
        cal.date_day,
        count(distinct g.student_id)    as total_active_clients,
        count(distinct g.group_id)      as active_groups
    from calendar cal
    inner join groups g
        on g.purchase_date <= cal.date_day
        and (g.ended_at is null or g.ended_at >= cal.date_day)
        and g.is_active = 'Yes'
    group by cal.date_day

),

final as (

    select
        coalesce(nr.date_day, a.date_day)       as date_day,
        coalesce(nr.new_customers, 0)           as new_customers,
        coalesce(nr.repeat_customers, 0)        as repeat_customers,
        coalesce(nr.new_customers, 0)
            + coalesce(nr.repeat_customers, 0)  as total_customers,
        coalesce(a.total_active_clients, 0)     as total_active_clients,
        coalesce(a.active_groups, 0)            as active_groups
    from daily_new_repeat nr
    full outer join active_daily a on a.date_day = nr.date_day

)

select * from final
