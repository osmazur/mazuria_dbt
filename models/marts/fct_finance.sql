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
	from {{ref('int_finance_off')}}

),

-- Every transaction, including the ones that do not reach the report.
-- `exclusion_reason is null` means the row counts; anything else is why it was
-- left out. NEVER sum this model without that filter — mrt_biz_daily applies it.
card as (

    select
        transaction_id,
        transaction_date,
        payment_type,
        is_income,
        tr_sub_type,
        comment,
        total_sum,
        exclusion_reason
    from int_finance_card
),

cash as (

    -- Cash has no natural id: the kasa sheet numbers operations within a day,
    -- so date + operation_num is the id a human can look the row up by.
    select
        'cash-' || to_char(transaction_date, 'YYYYMMDD')
                || '-' || operation_num             as transaction_id,
        transaction_date,
        payment_type,
        is_income,
        tr_sub_type,
        source                                      as comment,
        total_sum,
        exclusion_reason
    from int_finance_cash
),

off as (

    -- Already aggregated: one salary row per month, so one id per month.
    select
        'off-' || to_char(month_end_date, 'YYYY-MM') as transaction_id,
        month_end_date as transaction_date,
        'off' payment_type,
        false as is_income,
        'salary' as tr_sub_type,
        'Зарплата викладачів' as comment,
        sum(stopay) as total_sum,
        cast(null as varchar) as exclusion_reason
    from int_finance_man
    group by 1,2,3,4,5,6,8
)

select * from card
union all
select * from cash
union all
select * from off
