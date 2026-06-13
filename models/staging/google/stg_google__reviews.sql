with source as (

    select * from {{ source('google', 'google_reviews_snapshot') }}

),

deduplicated as (

    select
        place_id,
        name,
        rating,
        user_ratings_total,
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
        place_id,
        name,
        rating,
        user_ratings_total,
        snapshot_date,
        loaded_at
    from deduplicated
    where rn = 1

)

select * from final
