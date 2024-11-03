
with mazuria_gs_raw_transactions_kasa as (

	select 
		*
	from {{ source('raw', 'mazuria_gs_raw_transactions_kasa') }}

),

final as (

	select 
        date(start_week) as week_start_date,
        date(end_week) as week_end_date,
        operation_num::int as operation_num,
        sum::int as total_sum,
        'cash' as payment_type,
        date(date_added) as date_added,
        source
	from mazuria_gs_raw_transactions_kasa

)

select * from final
