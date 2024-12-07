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
        total_sum
    from stg_gs_transactions_kasa
)

select * from final