with followers as (

    select
        snapshot_date                                           as date_day,
        followers_count                                        as instagram_followers,
        media_count                                            as instagram_media_count,
        followers_count - lag(followers_count) over (
            order by snapshot_date
        )                                                      as new_instagram_followers
    from {{ ref('stg_instagram__followers') }}

)

select * from followers
where date_day >= '2026-03-01'
