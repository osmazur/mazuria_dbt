with calendar as (

    select date_day
    from {{ ref('stg_calendar') }}
    where date_day <= current_date

),

finance as (

    select
        transaction_date                                        as date_day,
        sum(case when is_income then total_sum else 0 end)      as revenue,
        sum(case when not is_income then total_sum else 0 end)  as expenses
    from {{ ref('fct_finance') }}
    group by transaction_date

),

customers as (

    select * from {{ ref('int_customers_daily') }}

),

instagram as (

    select * from {{ ref('int_instagram_daily') }}

),

final as (

    select
        cal.date_day,

        -- customers
        coalesce(c.new_customers, 0)            as new_customers,
        coalesce(c.repeat_customers, 0)         as repeat_customers,
        coalesce(c.total_customers, 0)          as total_customers,
        coalesce(c.total_active_clients, 0)     as total_active_clients,
        coalesce(c.active_groups, 0)            as active_groups,

        -- finance
        coalesce(f.revenue, 0)                  as revenue,
        coalesce(f.expenses, 0)                 as expenses,
        coalesce(f.revenue, 0)
            - coalesce(f.expenses, 0)           as profit,

        -- instagram
        ig.instagram_followers,
        ig.instagram_media_count,
        ig.instagram_messages,
        ig.instagram_new_conversations

    from calendar cal
    left join customers c   on c.date_day = cal.date_day
    left join finance f     on f.date_day = cal.date_day
    left join instagram ig  on ig.date_day = cal.date_day

)

select * from final
