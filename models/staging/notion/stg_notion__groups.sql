with base as (

    select * from {{ ref('base_notion__groups') }}
    where not coalesce(archived, false)
        and not coalesce(in_trash, false)

),

final as (

    select
        -- ids
        id                          as group_id,
        student_id,
        course_id,
        dict_group_id,
        teacher_id,
        -- strings
        note,
        payment_info,
        phone,
        is_active,
        -- numerics
        total_classes,
        present_count,
        absent_count,
        attendance_percent,
        discount_percent,
        -- dates
        started_at::date            as started_at,
        ended_at::date              as ended_at,
        -- timestamps
        created_time                as created_at,
        last_edited_time            as updated_at

    from base

)

select * from final
