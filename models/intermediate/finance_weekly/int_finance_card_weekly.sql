with int_finance_card as (

	select 
		*
	from {{ref('int_finance_card')}}

),

calendar as (

	select 
		*
	from {{ref('int_calendar')}}


),
prep as (

    select
        --transaction_date,
        calendar.week_start_date,
        calendar.week_end_date,
        payment_type,
        is_income,
        tr_sub_type,
        count(transaction_id) as operation_num,
        sum(total_sum) as total_sum
    from int_finance_card 
    left join calendar on transaction_date = date_day
    group by 1,2,3,4,5

)
select * from prep

-- select sub_type, sum(total_sum) from money
-- group by 1
-- order by 2 desc

