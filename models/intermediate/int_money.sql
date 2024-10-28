with stg_pb_transactions as (

	select 
		*
	from {{ref('stg_pb_transactions')}}

),

prep as (

    select
        *,
        case
		    when doc_type = 'p' and transaction_type = 'D' and aut_cntr_mfo_name = 'Казначейство України' and ultmt is not null
		    then 'taxes_FOP'
            when doc_type = 'p' and transaction_type = 'D' and aut_cntr_mfo_name = 'Казначейство України'
		    then 'taxes_salary'
		    when doc_type in ('m','p') and transaction_type = 'C'
		    then 'payment_receive'
		    when  doc_type = 'm' and transaction_type = 'D'
		    then 'komisiya'
		    when (doc_type = 'p' and transaction_type = 'D' and uetr is null) or transaction_id = '3598543753'
		    then 'salary'
		    else 'payment_sent' 
		end as sub_type
    from stg_pb_transactions

),

money as (

    select
        transaction_id,
        date_client_pr,
        date_time_dat_od_tim_p,
        case when sub_type = 'payment_receive' then true else false end as is_income,
        sub_type,
        total_sum,
        comment,
        doc_type,
        transaction_type,
        doc_number,
        ultmt,
        aut_cntr_mfo_name
    from prep

)
select * from money
order by sub_type desc

-- select sub_type, sum(total_sum) from money
-- group by 1
-- order by 2 desc

