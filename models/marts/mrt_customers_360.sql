-- One row per student: lifetime activity across all enrollments plus a
-- current customer_status. "Last seen" is the latest attended lesson in any
-- group, so a student who moved to another group is not counted as inactive.
-- current_* columns describe the most recent open enrollment.

with enrollments as (

    select * from {{ ref('fct_enrollments') }}
    where student_id is not null

),

students as (

    select student_id, student_name, student_phone, created_at
    from {{ ref('stg_notion__students') }}

),

customer_agg as (

    select
        student_id,
        count(*)                                        as enrollments_count,
        count(*) filter (where is_open)                 as open_enrollments_count,
        bool_or(is_awaiting_start)                      as has_awaiting_start,
        min(first_attendance_date)                      as first_seen_date,
        max(last_attendance_date)                       as last_seen_date,
        sum(present_count)                              as total_present_count,
        sum(absent_count)                               as total_absent_count
    from enrollments
    group by student_id

),

current_enrollment as (

    select distinct on (student_id)
        student_id,
        enrollment_id       as current_enrollment_id,
        course_name         as current_course_name,
        group_name          as current_group_name,
        teacher_name        as current_teacher_name,
        payment_status      as current_payment_status
    from enrollments
    where is_open
    order by student_id, last_attendance_date desc nulls last, created_at desc

),

final as (

    select
        -- ids
        s.student_id,
        ce.current_enrollment_id,
        -- strings
        s.student_name,
        s.student_phone,
        ce.current_course_name,
        ce.current_group_name,
        ce.current_teacher_name,
        ce.current_payment_status,
        case
            when coalesce(a.open_enrollments_count, 0) = 0  then 'former'
            when a.has_awaiting_start                       then 'awaiting_start'
            when a.last_seen_date is null                   then 'awaiting_start'
            when current_date - a.last_seen_date < 30       then 'active'
            when current_date - a.last_seen_date < 60       then 'inactive_30d'
            when current_date - a.last_seen_date < 90       then 'inactive_60d'
            else 'inactive_90d_plus'
        end                                             as customer_status,
        -- numerics
        coalesce(a.enrollments_count, 0)                as enrollments_count,
        coalesce(a.open_enrollments_count, 0)           as open_enrollments_count,
        coalesce(a.total_present_count, 0)              as total_present_count,
        coalesce(a.total_absent_count, 0)               as total_absent_count,
        current_date - a.last_seen_date                 as days_since_last_seen,
        -- dates
        a.first_seen_date,
        a.last_seen_date,
        -- timestamps
        s.created_at

    from students s
    left join customer_agg a on a.student_id = s.student_id
    left join current_enrollment ce on ce.student_id = s.student_id

)

select * from final
