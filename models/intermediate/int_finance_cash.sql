with stg_gs_transactions_kasa as (

	select 
		*
	from {{ref('stg_gs_transactions_kasa')}}

),

final as (
    select 
        --week_start_date,
        --week_end_date,
        transaction_date,
        payment_type,
        is_income,
        'payment_received' as tr_sub_type,
        operation_num,
        source,
        total_sum,
        -- Mirrors mrt_biz_daily: a null is_income fell through both
        -- `case when is_income` and `case when not is_income`, so the row
        -- silently counted as neither income nor expense.
        case
            when transaction_date is null then 'no_date'
            when is_income is null then 'unknown_direction'
        end as exclusion_reason
    from stg_gs_transactions_kasa
)

select * from final