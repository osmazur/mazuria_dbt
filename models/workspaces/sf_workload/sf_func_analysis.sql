with stg_sf_sql_func_cnt as (

	select 
		*
	from {{ref('stg_sf_sql_func_cnt_all')}}

),

sf_sql_func_dict as (
    select * 
    from "mazuria"."embucket"."stg_sf_sql_func_dict"
), finale as (
select 
    dict.function_name as dfname, 
    dict.category,
    -- case 
    --     when dict.category is null then 'NOT_PARSED'
    --     when dict.category = '' then 'NO_CATEGORY'
    --     else dict.category
    --     end as category,
    cnt.function_name as cfname,
    --case when dict.function_name is null then false else true end is_sf_function
    is_compiled,
    case when cnt.project like '%demo%' then true else false end is_demo_project,
    sum(function_cnt)
from sf_sql_func_dict as dict
left join stg_sf_sql_func_cnt as cnt on cnt.function_name = dict.function_name
group by 1,2,3,4,5
) select * from finale --where category is null