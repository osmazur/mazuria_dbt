with base as (

    select * from {{ ref('base_notion__dict_courses') }}
    where not coalesce(archived, false)
        and not coalesce(in_trash, false)

),

final as (

    select
        -- ids
        id              as course_id,
        -- strings
        name            as course_name,
        -- numerics
        price,
        hours,
        classes,
        items,
        -- timestamps
        notion_created_time     as created_at,
        last_edited_time        as updated_at

    from base

)

select * from final
