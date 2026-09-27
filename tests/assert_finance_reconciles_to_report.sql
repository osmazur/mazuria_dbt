-- The counted rows in fct_finance must add up to exactly what the report shows.
--
-- fct_finance now keeps excluded rows, so the only thing separating it from the
-- business numbers is `exclusion_reason is null`. If someone later adds a filter
-- back into a finance model, or starts summing fct_finance without the filter,
-- the two sides drift apart and this test fails.
--
-- The date window is read off mrt_biz_daily instead of being hardcoded, so the
-- reporting start date lives in one place.

with bounds as (

    select
        min(date_day) as first_day,
        max(date_day) as last_day
    from {{ ref('mrt_biz_daily') }}

),

from_fact as (

    select
        sum(case when is_income then total_sum else 0 end)       as revenue,
        sum(case when not is_income then total_sum else 0 end)   as expenses
    from {{ ref('fct_finance') }}, bounds
    where exclusion_reason is null
      and transaction_date >= bounds.first_day
      and transaction_date <= bounds.last_day

),

from_report as (

    select
        sum(revenue)    as revenue,
        sum(expenses)   as expenses
    from {{ ref('mrt_biz_daily') }}

)

select
    from_fact.revenue    as fact_revenue,
    from_report.revenue  as report_revenue,
    from_fact.expenses   as fact_expenses,
    from_report.expenses as report_expenses
from from_fact, from_report
where coalesce(from_fact.revenue, 0)  <> coalesce(from_report.revenue, 0)
   or coalesce(from_fact.expenses, 0) <> coalesce(from_report.expenses, 0)
