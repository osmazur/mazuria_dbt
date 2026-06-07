with source as (

    select * from {{ source('instagram', 'instagram_followers_snapshot') }}

),

deduplicated as (

    select
        ig_user_id,
        username,
        followers_count,
        follows_count,
        media_count,
        snapshot_date::date      as snapshot_date,
        loaded_at,
        row_number() over (
            partition by snapshot_date
            order by loaded_at desc
        ) as rn
    from source

),

final as (

    select
        ig_user_id,
        username,
        followers_count,
        follows_count,
        media_count,
        snapshot_date,
        loaded_at
    from deduplicated
    where rn = 1

)

select * from final
