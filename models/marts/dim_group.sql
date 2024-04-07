
with stg_groups_mazuria as (

    select * from {{ref('stg_groups_mazuria')}}

),

calendar as (
    select * from {{ref('dim_calendar')}}
)
    select 
        group_name,
        course_name,
        course_start_date,
        course_end_date,
        course_lentgh_month,
        course_price,
        is_active,
        payment_num,

        calendar.week_start_date,
        calendar.month_name,
        calendar.month_start_date,
        calendar.year_number
    from stg_groups_mazuria as gr
    left join calendar on calendar.date_day = gr.course_start_date