
with mazuria_pb_raw_transactions as (

	select 
		*
	from {{ source('raw', 'mazuria_pb_raw_transactions') }}

),

final as (

	select 
        (data::json->>'ID')::bigint as transaction_id,
        TO_DATE(data::json->>'DAT_KL', 'DD.MM.YYYY') as date_client_pr,
        TO_TIMESTAMP(data::json->>'DATE_TIME_DAT_OD_TIM_P', 'DD.MM.YYYY HH24:MI') AS date_time_dat_od_tim_p,
        data::json->>'CCY' as currency,
        (data::json->>'SUM')::decimal as total_sum,
        data::json->>'OSND' as comment,
        data::json->>'REFN' as ref_type,
        data::json->>'DOC_TYP' as doc_type,
        data::json->>'TRANTYPE' as transaction_type,
        (data::json->>'REF')::varchar as ref_number,
        (data::json->>'UETR')::varchar as uetr,
        data::json->>'ULTMT' as ultmt,
        data::json->>'DAT_OD' as date_recipient,
        (data::json->>'NUM_DOC')::varchar as doc_number,
        data::json->>'AUT_CNTR_MFO_NAME' as aut_cntr_mfo_name,
        data::json->>'loaded_at' as loaded_at

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

)

select * from final
