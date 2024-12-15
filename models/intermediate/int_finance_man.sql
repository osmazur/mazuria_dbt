with stg_calendar as (

	select * from {{ref('stg_calendar')}}
    where date_day >= '2024-05-01' and date_day <= (date_trunc('month', now()) + interval '1 month - 1 day')::date

),

int_google_calendar as (

	select * from {{ref('int_google_calendar')}}

),

stg_gs_teachers as (

	select * from {{ref('stg_gs_teachers')}}
    where teacher_mazuria_email in ('myroslava.hovda@teacher.com', 'iryna.zamriy@teacher.com', 'galyna.chyzhevska@teacher.com')
),

teach_cross as (

    select 
        month_end_date, 
        t.teacher_mazuria_email
    from stg_calendar as c
    cross join stg_gs_teachers as t 
    group by 1,2
),

total_hours as (
    select 
        --event_end_date,
        c.month_end_date,
        event_teacher_email,
        sum(event_time_spent_to_num) as total_hours
    from int_google_calendar
    left join stg_calendar as c on c.date_day = int_google_calendar.event_end_date
    where event_teacher is not null
    group by 1,2

), prep as (
    select 
        tc.month_end_date, 
        tc.teacher_mazuria_email,
        th.total_hours,
        t.hrate,
        t.defrate,
        t.fullrate,
        case 
            when hrate is null then fullrate - defrate
            when defrate is null then total_hours * hrate
            when defrate is not null and fullrate is null then (total_hours * hrate) - defrate
        else 0
        end as stopay
    from teach_cross as tc
    left join total_hours as th on th.month_end_date = tc.month_end_date and tc.teacher_mazuria_email = th.event_teacher_email
    left join stg_gs_teachers as t on t.teacher_mazuria_email = tc.teacher_mazuria_email
)

select * from prep
order by 1 desc