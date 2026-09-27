
with mazuria_pb_raw_transactions as (

	select 
		*
	from {{ source('raw', 'mazuria_pb_raw_transactions') }}

),

final as (

	select 
        (data::json->>'ID')::varchar as transaction_id,
        --(data::json->>'ID')::bigint as transaction_id,
        TO_DATE(data::json->>'DAT_KL', 'DD.MM.YYYY') as transaction_date,
        TO_TIMESTAMP(data::json->>'DATE_TIME_DAT_OD_TIM_P', 'DD.MM.YYYY HH24:MI') AS transaction_timestamp,
        data::json->>'CCY' as currency,
        (data::json->>'SUM')::decimal as total_sum,
        data::json->>'OSND' as comment,
        data::json->>'REFN' as ref_type,
        data::json->>'DOC_TYP' as doc_type,
        data::json->>'TRANTYPE' as transaction_type_code,
        (data::json->>'REF')::varchar as ref_number,
        (data::json->>'UETR')::varchar as uetr,
        data::json->>'ULTMT' as ultmt,
        data::json->>'DAT_OD' as date_recipient,
        (data::json->>'NUM_DOC')::varchar as doc_number,
        data::json->>'AUT_CNTR_MFO_NAME' as aut_cntr_mfo_name,
        data::json->>'loaded_at' as loaded_at,
        'card' as payment_type

        -- data::json->>'TIM_P' as time_pr,
        -- (data::json->>'SUM_E')::decimal as sum_e,
        -- data::json->>'DLR' as dlr,
        -- data::json->>'PR_PR' as pr_pr,
        -- data::json->>'FL_REAL' as fl_real,
        -- data::json->>'AUT_MY_ACC' as aut_my_acc,
        -- data::json->>'AUT_MY_CRF' as aut_my_crf,
        -- data::json->>'AUT_MY_MFO' as aut_my_mfo,
        -- data::json->>'AUT_MY_NAM' as aut_my_nam,
        -- data::json->>'AUT_CNTR_ACC' as aut_cntr_acc,
        -- data::json->>'AUT_CNTR_CRF' as aut_cntr_crf,
        -- data::json->>'AUT_CNTR_MFO' as aut_cntr_mfo,
        -- data::json->>'AUT_CNTR_NAM' as aut_cntr_nam,
        -- data::json->>'AUT_MY_MFO_CITY' as aut_my_mfo_city,
        -- data::json->>'AUT_MY_MFO_NAME' as aut_my_mfo_name,
        -- data::json->>'AUT_CNTR_MFO_CITY' as aut_cntr_mfo_city,
        -- data::json->>'TECHNICAL_TRANSACTION_ID' as technical_transaction_id
	from mazuria_pb_raw_transactions

),

-- Rows are no longer dropped here. Duplicates and blocklisted ids are kept and
-- tagged instead, so the finance models can report what was left out of the
-- final numbers and why. fct_finance carries the tag; mrt_biz_daily filters on it.
flagged as (

    select
        *,
        row_number() over (
            partition by transaction_id
            order by transaction_timestamp desc
        ) > 1 as is_duplicate
    from final

),

excluded as (

    select
        transaction_id,
        reason
    from {{ ref('finance_excluded_transactions') }}

)

select
    f.*,
    case
        -- `transaction_id not in (...)` evaluated to NULL for a null id, so these
        -- rows were dropped here too — silently. Tagged now, still not counted.
        when f.transaction_id is null then 'no_transaction_id'
        when f.is_duplicate then 'duplicate'
        when x.transaction_id is not null then coalesce(x.reason, 'manual_exclusion')
    end as exclusion_reason
from flagged as f
left join excluded as x on x.transaction_id = f.transaction_id
