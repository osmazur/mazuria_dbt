-- A counted row with a null is_income falls through both `case when is_income`
-- and `case when not is_income` in mrt_biz_daily, so it silently contributes to
-- neither revenue nor expenses. Such rows must carry an exclusion_reason
-- ('unknown_direction') so they show up as excluded rather than vanishing.

select
    transaction_id,
    transaction_date,
    payment_type,
    total_sum
from {{ ref('fct_finance') }}
where exclusion_reason is null
  and is_income is null
