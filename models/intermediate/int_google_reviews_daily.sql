with reviews as (

    select
        snapshot_date                                          as date_day,
        rating                                                 as google_rating,
        user_ratings_total                                     as google_reviews_total,
        user_ratings_total - lag(user_ratings_total) over (
            order by snapshot_date
        )                                                      as new_google_reviews
    from {{ ref('stg_google__reviews') }}

)

select * from reviews
where date_day >= '2026-03-01'
