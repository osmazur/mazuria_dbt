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

int_finance_man as (

	select 
		*
	from {{ref('int_finance_man')}}

),

card as (

    select
        transaction_date,
        payment_type,
        is_income,
        tr_sub_type,
        total_sum
    from int_finance_card
),

cash as (

    select
        transaction_date,
        payment_type,
        is_income,
        tr_sub_type,
        total_sum
    from int_finance_cash
),

man as (

    select
        month_end_date as transaction_date,
        'man' payment_type,
        false as is_income,
        'salary' as tr_sub_type,
        sum(stopay) as total_sum
    from int_finance_man
    group by 1,2,3
)

select * from card
union all
select * from cash
union all
select * from man