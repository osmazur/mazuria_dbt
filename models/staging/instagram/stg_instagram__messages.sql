with source as (

    select * from {{ source('instagram', 'instagram_messages_snapshot') }}

),

deduplicated as (

    select
        page_id,
        active_conversations_count,
        new_conversations_count,
        report_date::date       as report_date,
        loaded_at,
        row_number() over (
            partition by report_date
            order by loaded_at desc
        ) as rn
    from source

),

final as (

    select
        page_id,
        active_conversations_count,
        new_conversations_count,
        report_date,
        loaded_at
    from deduplicated
    where rn = 1

)

select * from final
