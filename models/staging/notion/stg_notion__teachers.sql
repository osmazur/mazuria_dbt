with base as (

    select * from {{ ref('base_notion__teachers') }}
    where not coalesce(archived, false)
        and not coalesce(in_trash, false)

),

final as (

    select
        -- ids
        id                      as teacher_id,
        -- strings
        name                    as teacher_name,
        birthday                as teacher_birthday,
        employment_type         as teacher_employment_type,
        -- timestamps
        created_time            as created_at,
        last_edited_time        as updated_at

    from base

)

select * from final
