with source as (

    select * from {{ source('instagram', 'instagram_media_insights') }}

),

final as (

    select
        media_id,
        media_type,
        posted_at,
        like_count,
        comments_count,
        impressions,
        reach,
        saved,
        shares,
        snapshot_date::date      as snapshot_date,
        loaded_at
    from source

)

select * from final
