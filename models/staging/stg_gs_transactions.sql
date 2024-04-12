
with mazuria_gs_raw_transactions as (

	select 
		*
	from {{ source('raw', 'mazuria_gs_raw_transactions') }}

),

final as (

	select 
-- transaction main info
        transaction_name,
        type as transaction_type,
        created_date as transaction_created_date,
        category as transaction_category,
        amount as transaction_amount,
        description as transaction_description,
-- other   
        teacher_name,
        group_name,
        month,
        expense_type,
        quantity,
        comment,
-- textile
        textile_type,
        textile_size,
        price_bought as price_bought,
        price_sell as textile_price_sell
	from mazuria_gs_raw_transactions

)

select * from final
