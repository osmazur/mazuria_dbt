with groups as (

    select
        g.group_id,
        g.student_id,
        g.purchase_seq,
        g.is_active,
        g.dict_group_id,
        coalesce(g.first_attendance_date, g.started_at, g.created_at::date) as purchase_date,
        g.ended_at,
        case
            when c.course_name ilike 'абонемент%' then 'subscription'
            else 'group'
        end as purchase_type
    from {{ ref('stg_notion__groups') }} g
    left join {{ ref('stg_notion__dict_courses') }} c
        on c.course_id = g.course_id

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
    where date_day >= '2026-03-01'
        and date_day <= current_date

),

active_daily as (

    select
        cal.date_day,
        count(distinct g.student_id)    as total_active_clients,
        count(distinct case when g.purchase_type = 'group' then g.student_id end)           as active_clients_groups,
        count(distinct case when g.purchase_type = 'subscription' then g.student_id end)    as active_clients_subscriptions,
        count(distinct case when g.purchase_type = 'group' then g.dict_group_id end)        as active_groups
    from calendar cal
    inner join groups g
        on g.purchase_date <= cal.date_day
        and (g.ended_at is null or g.ended_at >= cal.date_day)
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
        coalesce(a.active_clients_groups, 0)    as active_clients_groups,
        coalesce(a.active_clients_subscriptions, 0) as active_clients_subscriptions,
        coalesce(a.active_groups, 0)            as active_groups
    from daily_new_repeat nr
    full outer join active_daily a on a.date_day = nr.date_day

)

select * from final
