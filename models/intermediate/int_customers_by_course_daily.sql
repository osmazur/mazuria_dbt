-- Daily new/active clients per course. The spine is calendar x every course
-- from the courses dictionary, so ALL courses and subscriptions appear on every
-- day (zero-filled where there is no activity) — nothing is dropped.

with courses_dim as (

    select
        course_id,
        course_name,
        case
            when course_name ilike 'абонемент%' then 'subscription'
            else 'group'
        end as purchase_type
    from {{ ref('stg_notion__dict_courses') }}

),

groups as (

    select
        g.student_id,
        g.purchase_seq,
        g.course_id,
        coalesce(g.first_attendance_date, g.started_at, g.created_at::date) as purchase_date,
        g.ended_at
    from {{ ref('stg_notion__groups') }} g

),

calendar as (

    select date_day
    from {{ ref('stg_calendar') }}
    where date_day >= '2026-03-01'
        and date_day <= current_date

),

spine as (

    select
        cal.date_day,
        cd.course_id,
        cd.course_name,
        cd.purchase_type
    from calendar cal
    cross join courses_dim cd

),

new_daily as (

    select
        purchase_date                                                   as date_day,
        course_id,
        count(distinct case when purchase_seq = 1 then student_id end)  as new_customers
    from groups
    where purchase_date >= '2026-03-01'
        and purchase_date <= current_date
    group by purchase_date, course_id

),

active_daily as (

    select
        cal.date_day,
        g.course_id,
        count(distinct g.student_id)    as active_customers
    from calendar cal
    inner join groups g
        on g.purchase_date <= cal.date_day
        and (g.ended_at is null or g.ended_at >= cal.date_day)
    group by cal.date_day, g.course_id

),

final as (

    select
        s.date_day,
        s.course_id,
        s.course_name,
        s.purchase_type,
        coalesce(n.new_customers, 0)        as new_customers,
        coalesce(a.active_customers, 0)     as active_customers
    from spine s
    left join new_daily n
        on n.date_day = s.date_day
        and n.course_id = s.course_id
    left join active_daily a
        on a.date_day = s.date_day
        and a.course_id = s.course_id

)

select * from final
