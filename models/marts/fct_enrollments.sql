-- One row per enrollment (a row of the Notion "Group" table: student x group).
-- payment_status is parsed from the Notion "доплатити" formula so the numbers
-- match what staff see in Notion; amount_due is the number at the end of the
-- "🟠 Скоро доплата N" / "🚩 Друга оплата N" statuses.

with enrollments as (

    -- A Notion Group row can link several students/teachers/courses/groups and
    -- base_notion__groups fans those out into several rows. Keep one row per
    -- enrollment; such rows are flagged by the warn-level unique test on
    -- stg_notion__groups.group_id so they can be fixed in Notion.
    select distinct on (group_id) *
    from {{ ref('stg_notion__groups') }}
    order by group_id, student_id, teacher_id, course_id, dict_group_id

),

students as (

    select student_id, student_name, student_phone
    from {{ ref('stg_notion__students') }}

),

courses as (

    select course_id, course_name
    from {{ ref('stg_notion__dict_courses') }}

),

dict_groups as (

    select group_id, group_name
    from {{ ref('stg_notion__dict_groups') }}

),

teachers as (

    select teacher_id, teacher_name
    from {{ ref('stg_notion__teachers') }}

),

final as (

    select
        -- ids
        e.group_id                                          as enrollment_id,
        e.student_id,
        e.course_id,
        e.dict_group_id,
        e.teacher_id,
        -- strings
        s.student_name,
        s.student_phone,
        c.course_name,
        dg.group_name,
        t.teacher_name,
        e.payment_status_raw,
        case
            when e.payment_status_raw ilike '%Очікує старту%'       then 'awaiting_start'
            when e.payment_status_raw ilike '%Внесіть дані%'        then 'payment_missing'
            when e.payment_status_raw ilike '%Оплачено повністю%'   then 'paid_full'
            when e.payment_status_raw ilike '%Друга оплата%'        then 'payment_due'
            when e.payment_status_raw ilike '%Скоро доплата%'       then 'payment_soon'
            when e.payment_status_raw ilike '%Перша частина%'       then 'paid_first'
        end                                                 as payment_status,
        -- numerics
        case
            when e.payment_status_raw ilike any (array['%Друга оплата%', '%Скоро доплата%'])
            then replace(substring(e.payment_status_raw from '(-?[0-9]+(?:[.,][0-9]+)?)\s*$'), ',', '.')::numeric
        end                                                 as amount_due,
        coalesce(e.present_count, 0)                        as present_count,
        coalesce(e.absent_count, 0)                         as absent_count,
        e.total_classes,
        e.attendance_percent,
        e.discount_percent,
        e.purchase_seq,
        current_date - e.last_attendance_date               as days_since_last_attendance,
        -- booleans
        e.ended_at is null                                  as is_open,
        -- in a group but no lesson attended yet (covers Notion "Очікує старту"
        -- and clients who prepaid but have not come yet)
        e.ended_at is null and coalesce(e.present_count, 0) = 0
                                                            as is_awaiting_start,
        -- dates
        e.first_attendance_date,
        e.last_attendance_date,
        e.started_at,
        e.ended_at,
        -- timestamps
        e.created_at,
        e.updated_at

    from enrollments e
    left join students s on s.student_id = e.student_id
    left join courses c on c.course_id = e.course_id
    left join dict_groups dg on dg.group_id = e.dict_group_id
    left join teachers t on t.teacher_id = e.teacher_id

)

select * from final
