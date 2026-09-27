with stg_pb_transactions as (

	select 
		*
	from {{ref('stg_pb_transactions')}}

),

prep as (

    select
        *,
        case
		    when doc_type = 'p' and transaction_type_code = 'D' and aut_cntr_mfo_name = 'Казначейство України' and ultmt is not null
		    then 'taxes_FOP'
            when doc_type = 'p' and transaction_type_code = 'D' and aut_cntr_mfo_name = 'Казначейство України'
		    then 'taxes_salary'
		    when doc_type in ('m','p') and transaction_type_code = 'C'
		    then 'payment_received'
		    when  doc_type = 'm' and transaction_type_code = 'D'
		    then 'komisiya'
		    when (doc_type = 'p' and transaction_type_code = 'D' and uetr is null) or transaction_id in ('3598543753', '3703526170')
		    then 'salary'
		    else 'payment_sent' 
		end as tr_sub_type
    from stg_pb_transactions

),

money as (

    select
        transaction_id,
        transaction_date,
        transaction_timestamp,
        --count(transaction_id) as operation_num,
        case when tr_sub_type = 'payment_received' then true else false end as is_income,
        tr_sub_type,
        payment_type,
        total_sum,
        comment,
        doc_type,
        uetr,
        transaction_type_code,
        doc_number,
        ultmt,
        aut_cntr_mfo_name,
        -- Tag instead of filter (see stg_pb_transactions). The staging tag wins;
        -- the rest reproduce the old WHERE clause exactly, including the fact
        -- that `comment not like` also dropped every null-comment row.
        coalesce(
            exclusion_reason,
            case
                when comment is null then 'null_comment'
                when comment like '%Переказ власних%' then 'own_transfer'
                when transaction_date is null then 'no_date'
            end
        ) as exclusion_reason
    from prep

)
select * from money


