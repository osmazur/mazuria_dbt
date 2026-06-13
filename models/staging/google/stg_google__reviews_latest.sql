-- Latest reviews captured daily from Place Details (≤5 per place, no text).
-- The DAG appends a fresh top-5 every day, so the same review recurs across
-- snapshots; keep one row per review (its most recent capture).

with source as (

    select * from {{ source('google', 'google_reviews_latest') }}

),

deduplicated as (

    select
        review_id,
        place_id,
        author,
        rating,
        published_at::timestamptz   as published_at,
        (published_at::timestamptz)::date as published_date,
        snapshot_date::date         as first_seen_snapshot_date,
        loaded_at,
        row_number() over (
            partition by review_id
            order by loaded_at desc
        ) as rn
    from source

),

final as (

    select
        review_id,
        place_id,
        author,
        rating,
        published_at,
        published_date,
        loaded_at
    from deduplicated
    where rn = 1

)

select * from final
