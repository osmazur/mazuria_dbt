
with stg_people_mazuria as(

    select * from {{ref('stg_people_mazuria')}}

),
stg_groups_mazuria as (

     select * from {{ref('stg_groups_mazuria')}}
),

finale as (

    select 
        cs.customer_phone||cs.group_name as uid,
        cs.customer_id,
        cs.date_add,
        cs.group_name,
        cs.course_name,
        cs.customer_status,
        cs.first_payment_date,
        cs.second_payment_date,
        cs.third_payment_date,
        cs.customer_phone,
        cs.comment,

        gr.course_start_date,
        gr.course_end_date,
        gr.course_lentgh_month,
        gr.course_price,
        gr.is_active,
        gr.payment_num
    from stg_people_mazuria as cs
    left join stg_groups_mazuria as gr on gr.group_name = cs.group_name

)

select * from finale
