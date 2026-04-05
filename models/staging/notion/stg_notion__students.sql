with base as (

    select * from {{ ref('base_notion__students') }}
    where not coalesce(archived, false)
        and not coalesce(in_trash, false)

),

final as (

    select
        -- ids
        id                      as student_id,
        unique_id::varchar      as student_unique_id,
        -- strings
        name                    as student_name,
        phone                   as student_phone,
        note                    as student_note,
        -- numerics
        enrollments_count,
        -- timestamps
        notion_created_time     as created_at,
        last_edited_time        as updated_at

    from base

)

select * from final
