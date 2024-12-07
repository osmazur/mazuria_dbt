with int_finance_card as (

	select 
		*
	from {{ref('int_finance_card')}}

),

int_finance_cash as (

	select 
		*
	from {{ref('int_finance_cash')}}

),

card as (
    select
        -- week_start_date,
        -- week_end_date,
        transaction_date,
        payment_type,
        is_income,
        tr_sub_type,
        -- operation_num,
        total_sum
    from int_finance_card
),

cash as (
    select
        -- week_start_date,
        -- week_end_date,
        transaction_date,
        payment_type,
        is_income,
        tr_sub_type,
        -- operation_num,
        total_sum
    from int_finance_cash
)

select * from card
union all
select * from cash