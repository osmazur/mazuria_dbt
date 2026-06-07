with followers as (

    select
        snapshot_date       as date_day,
        followers_count     as instagram_followers,
        media_count         as instagram_media_count
    from {{ ref('stg_instagram__followers') }}

),

messages as (

    select
        report_date         as date_day,
        active_conversations_count  as instagram_messages,
        new_conversations_count     as instagram_new_conversations
    from {{ ref('stg_instagram__messages') }}

),

final as (

    select
        coalesce(f.date_day, m.date_day)    as date_day,
        f.instagram_followers,
        f.instagram_media_count,
        m.instagram_messages,
        m.instagram_new_conversations
    from followers f
    full outer join messages m on m.date_day = f.date_day

)

select * from final
