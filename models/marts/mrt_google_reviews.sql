-- One row per Google review, combining two sources:
--   * manual  — the historical reviews entered by hand (google_reviews_manual
--                seed), since the Places API can't return more than the latest 5.
--   * google_api — reviews captured daily in the top-5 (stg_google__reviews_latest).
-- Keep the manual rows for the back-catalogue and the API rows for anything new.
-- They are expected not to overlap (manual = the older ones); if a review is in
-- both, the api row wins on the (published_date, rating, author) key.

with manual as (

    select
        review_id,
        published_date,
        rating,
        author,
        'manual' as source
    from {{ ref('google_reviews_manual') }}

),

api as (

    select
        review_id,
        published_date,
        rating,
        author,
        'google_api' as source
    from {{ ref('stg_google__reviews_latest') }}

),

unioned as (

    select * from api
    union all
    select * from manual

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by published_date, rating, coalesce(author, '')
            order by case when source = 'google_api' then 0 else 1 end
        ) as rn
    from unioned

)

select
    review_id,
    published_date,
    rating,
    author,
    source
from deduplicated
where rn = 1
