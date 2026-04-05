with base as (

    select * from {{ ref('base_notion__dict_groups') }}
    where not coalesce(archived, false)
        and not coalesce(in_trash, false)

),

final as (

    select
        -- ids
        id              as group_id,
        -- strings
        name            as group_name,
        days            as group_days,
        hours           as group_hours,
        schedule        as group_schedule,
        -- timestamps
        created_time    as created_at,
        last_edited_time as updated_at

    from base

)

select * from final
