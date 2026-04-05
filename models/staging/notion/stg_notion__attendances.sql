with base as (

    select * from {{ ref('base_notion__attendances') }}
    where not coalesce(archived, false)
        and not coalesce(in_trash, false)

),

final as (

    select
        -- ids
        id                          as attendance_id,
        unique_id::varchar          as attendance_unique_id,
        -- strings
        student_name,
        student_phone,
        teacher_name,
        course_name,
        group_name,
        payment_info,
        status,
        -- numerics
        is_present,
        is_absent,
        -- dates
        day::date                   as attendance_date,
        -- timestamps
        created_time                as created_at,
        last_edited_time            as updated_at

    from base

)

select * from final
