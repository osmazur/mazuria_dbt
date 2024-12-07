
with mazuria_gs_raw_transactions_kasa as (

	select 
		*
	from {{ source('raw', 'mazuria_gs_raw_transactions_kasa') }}

),

final as (

	select 
        date(transaction_date) as transaction_date,
        operation_num::int as operation_num,
        quant_num::int as quant_num,
        sum_kasa,
        sum_added,
        sum::int as total_sum,
        'cash' as payment_type,
        date(date_added) as date_added,
        source,
        is_income::boolean as is_income
	from mazuria_gs_raw_transactions_kasa

)

select * from final
